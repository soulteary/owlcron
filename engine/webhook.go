package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

// WebHookManager Web Hook 管理器
type WebHookManager struct {
	engine *Engine
	logger *zap.Logger
	client *http.Client
	policy *webHookPolicy
}

// NewWebHookManager 创建 Web Hook 管理器
func NewWebHookManager(engine *Engine, logger *zap.Logger) *WebHookManager {
	var config *types.Config
	if engine != nil {
		config = engine.config
	}
	policy := newWebHookPolicy(config)
	return &WebHookManager{engine: engine, logger: logger, policy: policy, client: policy.client()}
}

// WebHookPayload is an immutable notification snapshot. Credentials are never copied from configuration.
type WebHookPayload struct {
	URL  string          `json:"url"`
	Body json.RawMessage `json:"body"`
}

func (whm *WebHookManager) enqueueWebHook(ctx context.Context, target, action, occurrence string, data map[string]interface{}) error {
	if target == "" {
		return nil
	}
	parsed, err := url.Parse(target)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil {
		return errors.New("invalid webhook URL (HTTP(S), without user credentials, required)")
	}
	body, err := json.Marshal(whm.prepareHookData(action, data))
	if err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(occurrence + "\x00" + action + "\x00" + target))
	return whm.engine.enqueueNotification(ctx, fmt.Sprintf("webhook-%x", sum), "webhook", WebHookPayload{URL: target, Body: body})
}

func hookOccurrence(data map[string]interface{}) string {
	if id, ok := data["id"].(string); ok && id != "" {
		return id
	}
	if id, ok := data["notification_occurrence"].(string); ok && id != "" {
		return id
	}
	return fmt.Sprintf("occurrence-%d", time.Now().UnixNano())
}

// FireWebHook durably queues a hook before returning.
func (whm *WebHookManager) FireWebHook(target string, action string, data map[string]interface{}) {
	if err := whm.enqueueWebHook(whm.engine.ctx, target, action, hookOccurrence(data), data); err != nil {
		whm.logger.Error("Failed to queue web hook", zap.String("action", action), zap.String("error", whm.engine.RedactSensitiveText(err.Error())))
	}
}

func (whm *WebHookManager) FireUniversalWebHook(action string, data map[string]interface{}) {
	whm.FireWebHook(whm.engine.config.UniversalWebHook, action, data)
}

// prepareHookData 准备 Web Hook 数据
func (whm *WebHookManager) prepareHookData(action string, data map[string]interface{}) map[string]interface{} {
	data = whm.engine.sanitizeWebHookData(data)
	hookData := make(map[string]interface{})

	// 复制原始数据
	for k, v := range data {
		hookData[k] = v
	}

	// 设置动作
	hookData["action"] = action

	// 设置基础 URL
	hookData["base_app_url"] = whm.engine.config.BaseAppURL

	// 准备文本模板
	templates := whm.engine.config.WebHookTextTemplates
	if templates == nil {
		templates = getDefaultWebHookTemplates()
	}

	// 应用文本模板
	if template, exists := templates[action]; exists {
		text := whm.substituteTemplate(template, hookData)
		hookData["text"] = text
		hookData["content"] = text
		hookData["message"] = text
	}

	// 添加配置键
	if whm.engine.config.WebHookConfigKeys != nil {
		for _, key := range whm.engine.config.WebHookConfigKeys {
			switch key {
			case "base_app_url":
				hookData["base_app_url"] = whm.engine.config.BaseAppURL

			}
		}
	}

	// 添加自定义数据
	if whm.engine.config.WebHookCustomData != nil {
		for k, v := range whm.engine.config.WebHookCustomData {
			hookData[k] = v
		}
	}

	// The cluster authentication secret is never a webhook field, even in legacy overrides.
	delete(hookData, "secret_key")
	return whm.engine.sanitizeWebHookData(hookData)
}

// DeliverWebHook performs one bounded attempt. The durable queue owns retries.
func (whm *WebHookManager) DeliverWebHook(ctx context.Context, payload WebHookPayload, idempotencyKey string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := whm.ValidateWebHookURL(ctx, payload.URL); err != nil {
		var denied *webHookPolicyError
		return &NotificationDeliveryError{Err: err, Permanent: errors.As(err, &denied)}
	}
	parsed, _ := parseWebHookURL(payload.URL)
	if parsed.Scheme == "http" {
		ctx = context.WithValue(ctx, webHookHTTPContextKey{}, true)
	}
	if !json.Valid(payload.Body) {
		return &NotificationDeliveryError{Err: errors.New("invalid webhook JSON"), Permanent: true}
	}
	// Reapply current redaction policy to persisted legacy payloads before signing.
	body, err := whm.engine.cleanWebHookBody(payload.Body)
	if err != nil {
		return &NotificationDeliveryError{Err: errors.New("invalid webhook payload"), Permanent: true}
	}
	payload.Body = body
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, payload.URL, bytes.NewReader(payload.Body))
	if err != nil {
		return &NotificationDeliveryError{Err: errors.New("invalid webhook request"), Permanent: true}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey)
	if whm.engine != nil {
		if err := whm.engine.webHookSigner.sign(req, payload.Body, idempotencyKey); err != nil {
			return &NotificationDeliveryError{Err: errors.New("webhook signing failed"), Permanent: true}
		}
	}
	resp, err := whm.client.Do(req)
	if err != nil {
		var denied *webHookPolicyError
		return &NotificationDeliveryError{Err: errors.New("webhook transport failed"), Permanent: errors.As(err, &denied)}
	}
	defer resp.Body.Close()
	// Bound both transferred response bytes and the request lifetime, including slow bodies.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 32*1024))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	retry := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode >= 500
	return &NotificationDeliveryError{Err: fmt.Errorf("webhook HTTP status %d", resp.StatusCode), Permanent: !retry, RetryAfter: parseWebHookRetryAfter(resp.Header.Get("Retry-After"), time.Now())}
}

func parseWebHookRetryAfter(value string, now time.Time) time.Duration {
	var delay time.Duration
	if seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil {
		if seconds <= 0 {
			return 0
		}
		if seconds >= 86400 {
			return 24 * time.Hour
		}
		delay = time.Duration(seconds) * time.Second
	} else if date, err := http.ParseTime(value); err == nil {
		delay = date.Sub(now)
	}
	if delay < 0 {
		return 0
	}
	if delay > 24*time.Hour {
		return 24 * time.Hour
	}
	return delay
}

// substituteTemplate 替换模板变量
func (whm *WebHookManager) substituteTemplate(template string, data map[string]interface{}) string {
	result := template

	// 替换 [key] 格式的变量
	for key, value := range data {
		placeholder := "[" + key + "]"
		result = strings.ReplaceAll(result, placeholder, fmt.Sprintf("%v", value))
	}

	return result
}

// getDefaultWebHookTemplates 获取默认 Web Hook 模板
func getDefaultWebHookTemplates() map[string]string {
	return map[string]string{
		"job_start":          "Job started on [hostname]: [event_title] [job_details_url]",
		"job_complete":       "Job completed successfully on [hostname]: [event_title] [job_details_url]",
		"job_failure":        "Job failed on [hostname]: [event_title]: Error [code]: [description] [job_details_url]",
		"job_launch_failure": "Failed to launch scheduled event: [event_title]: [description] [edit_event_url]",
	}
}

// FireJobStartHook 触发任务开始 Web Hook
func (whm *WebHookManager) FireJobStartHook(job *types.Job) {
	data := whm.jobToMap(job)
	data["job_details_url"] = whm.engine.config.BaseAppURL + "/#JobDetails?id=" + job.ID
	data["edit_event_url"] = whm.engine.config.BaseAppURL + "/#Schedule?sub=edit_event&id=" + job.Event

	// 任务级别的 Web Hook
	if job.WebHook != "" {
		whm.FireWebHook(job.WebHook, "job_start", data)
	}

	// 全局 Web Hook
	whm.FireUniversalWebHook("job_start", data)
}

// enqueueJobCompleteHook participates in completion recovery: any storage failure keeps the completion spool.
func (whm *WebHookManager) enqueueJobCompleteHook(job *types.Job) error {
	return whm.enqueueJobCompleteHookTargets(job, job.WebHook, whm.engine.config.UniversalWebHook)
}

func (whm *WebHookManager) enqueueJobCompleteHookTargets(job *types.Job, jobURL, globalURL string) error {
	action := "job_complete"
	if job.Code != 0 {
		action = "job_failure"
	}
	data := whm.jobToMap(job)
	data["job_details_url"] = whm.engine.config.BaseAppURL + "/#JobDetails?id=" + job.ID
	data["edit_event_url"] = whm.engine.config.BaseAppURL + "/#Schedule?sub=edit_event&id=" + job.Event
	if err := whm.enqueueWebHook(whm.engine.ctx, jobURL, action, job.ID, data); err != nil {
		return err
	}
	return whm.enqueueWebHook(whm.engine.ctx, globalURL, action, job.ID, data)
}

func (whm *WebHookManager) FireJobCompleteHook(job *types.Job) {
	if err := whm.enqueueJobCompleteHook(job); err != nil {
		whm.logger.Error("Failed to queue completion web hook", zap.String("error", whm.engine.RedactSensitiveText(err.Error())))
	}
}

// FireJobLaunchFailureHook 触发任务启动失败 Web Hook
func (whm *WebHookManager) FireJobLaunchFailureHook(event *types.Event, description string) {
	data := map[string]interface{}{
		"notification_occurrence": fmt.Sprintf("launch-%s-%d", event.ID, time.Now().UnixNano()),
		"event_title":             event.Title,
		"event":                   event.ID,
		"description":             description,
		"edit_event_url":          whm.engine.config.BaseAppURL + "/#Schedule?sub=edit_event&id=" + event.ID,
		"hostname":                getHostname(),
	}

	// 事件级别的 Web Hook
	if event.WebHook != "" {
		whm.FireWebHook(event.WebHook, "job_launch_failure", data)
	}

	// 全局 Web Hook
	whm.FireUniversalWebHook("job_launch_failure", data)
}

// jobToMap 将任务转换为 map
func (whm *WebHookManager) jobToMap(job *types.Job) map[string]interface{} {
	return map[string]interface{}{
		"id":            job.ID,
		"event":         job.Event,
		"event_title":   job.EventTitle,
		"hostname":      job.Hostname,
		"code":          job.Code,
		"description":   job.Description,
		"elapsed":       job.Elapsed,
		"time_start":    job.TimeStart,
		"time_end":      job.TimeEnd,
		"category":      job.Category,
		"plugin":        job.Plugin,
		"progress":      job.Progress,
		"log_file_size": job.LogFileSize,
	}
}
