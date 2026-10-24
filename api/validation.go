package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/soulteary/owlcron/types"
)

// ValidateEvent 验证事件数据
func ValidateEvent(event *types.Event) error {
	if event.LogExpireDays != nil && (*event.LogExpireDays < 0 || *event.LogExpireDays > 36500) {
		return fmt.Errorf("log_expire_days must be between 0 and 36500")
	}
	if strings.ContainsAny(event.Title, "<>") {
		return fmt.Errorf("event title must not contain HTML")
	}
	if event.Detached.Bool() {
		return fmt.Errorf("detached execution is not supported")
	}
	if event.ID == "" {
		return fmt.Errorf("event id is required")
	}

	if event.Title == "" {
		return fmt.Errorf("event title is required")
	}

	if event.Plugin == "" {
		return fmt.Errorf("event plugin is required")
	}

	// 验证 ID 格式
	if !isValidID(event.ID) {
		return fmt.Errorf("invalid event id format")
	}

	if err := ValidateTiming(event.Timing); err != nil {
		return err
	}
	if event.Timezone != "" {
		if _, err := time.LoadLocation(event.Timezone); err != nil {
			return fmt.Errorf("invalid timezone: %w", err)
		}
	}
	// 验证超时时间
	if event.MemoryLimit < 0 || event.MemorySustain < 0 || event.CPULimit < 0 || event.CPUSustain < 0 || event.LogMaxSize < 0 || event.RetryDelay < 0 || event.Stagger < 0 {
		return fmt.Errorf("resource limits and delays must be non-negative")
	}
	if event.Timeout < 0 {
		return fmt.Errorf("timeout must be non-negative")
	}

	// 验证重试次数
	if event.Retries < 0 {
		return fmt.Errorf("retries must be non-negative")
	}

	// 验证最大并发数
	if event.MaxChildren < 0 {
		return fmt.Errorf("max_children must be non-negative")
	}

	// 验证队列大小
	if event.QueueMax < 0 {
		return fmt.Errorf("queue_max must be non-negative")
	}

	return nil
}

func (s *Server) validateEventPluginParams(event *types.Event) error {
	plugin, err := s.engine.GetJobManager().GetPluginFromStorage(event.Plugin)
	if err != nil {
		return fmt.Errorf("event plugin not found: %w", err)
	}
	allowed := make(map[string]bool, len(plugin.Params))
	for _, param := range plugin.Params {
		if param != nil {
			allowed[param.ID] = true
		}
	}
	for key := range event.Params {
		if !allowed[key] {
			return fmt.Errorf("unknown plugin parameter: %s", key)
		}
	}
	return nil
}

// ValidatePlugin 验证插件数据
func ValidatePlugin(plugin *types.Plugin) error {
	if strings.ContainsAny(plugin.Title, "<>") {
		return fmt.Errorf("plugin title must not contain HTML")
	}
	ids := map[string]bool{}
	for _, param := range plugin.Params {
		if param == nil || !isValidID(param.ID) || ids[param.ID] {
			return fmt.Errorf("plugin parameter ids must be valid and unique")
		}
		ids[param.ID] = true
	}
	if plugin.ID == "" {
		return fmt.Errorf("plugin id is required")
	}

	if plugin.Title == "" {
		return fmt.Errorf("plugin title is required")
	}

	if plugin.Command == "" {
		return fmt.Errorf("plugin command is required")
	}

	// 验证 ID 格式
	if !isValidID(plugin.ID) {
		return fmt.Errorf("invalid plugin id format")
	}

	return nil
}

// ValidateCategory 验证分类数据
func ValidateCategory(category *types.Category) error {
	if strings.ContainsAny(category.Title+category.Description, "<>") {
		return fmt.Errorf("category title and description must not contain HTML")
	}
	if category.ID == "" {
		return fmt.Errorf("category id is required")
	}

	if category.Title == "" {
		return fmt.Errorf("category title is required")
	}

	// 验证 ID 格式
	if !isValidID(category.ID) {
		return fmt.Errorf("invalid category id format")
	}

	return nil
}

// ValidateServerGroup 验证服务器组数据
func ValidateServerGroup(group *types.ServerGroup) error {
	if strings.ContainsAny(group.Title, "<>") {
		return fmt.Errorf("group title must not contain HTML")
	}
	if group.ID == "" {
		return fmt.Errorf("server group id is required")
	}

	if group.Title == "" {
		return fmt.Errorf("server group title is required")
	}

	// 验证 ID 格式
	if !isValidID(group.ID) {
		return fmt.Errorf("invalid server group id format")
	}

	// 验证正则表达式
	if group.Regexp != "" {
		if _, err := regexp.Compile(group.Regexp); err != nil {
			return fmt.Errorf("invalid regexp: %w", err)
		}
	}

	return nil
}

// ValidateUser 验证用户数据
func ValidateUser(user *types.User) error {
	if user.ID == "" {
		return fmt.Errorf("user id is required")
	}

	if user.Username == "" {
		return fmt.Errorf("username is required")
	}

	// 验证用户名格式
	if !isValidUsername(user.Username) {
		return fmt.Errorf("invalid username format")
	}

	// 验证邮箱格式（如果提供）
	if user.Email != "" && !isValidEmail(user.Email) {
		return fmt.Errorf("invalid email format")
	}

	return nil
}

// ValidateAPIKey 验证 API Key 数据
func ValidateAPIKey(apiKey *types.APIKey) error {
	if apiKey.ID == "" {
		return fmt.Errorf("api key id is required")
	}

	if apiKey.Title == "" {
		return fmt.Errorf("api key title is required")
	}

	return nil
}

// RequireParams 要求参数存在（查询字符串或 JSON body）
func RequireParams(r *http.Request, params ...string) error {
	var bodyParams map[string]interface{}
	if r.Method == http.MethodPost && r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&bodyParams); err != nil {
			bodyParams = nil
		}
	}

	for _, param := range params {
		value := r.URL.Query().Get(param)
		if value != "" {
			continue
		}
		if bodyParams != nil {
			if _, ok := bodyParams[param]; ok {
				continue
			}
		}
		return fmt.Errorf("missing required parameter: %s", param)
	}
	return nil
}

// RequireBody 要求请求体存在
func RequireBody(r *http.Request) error {
	if r.Body == nil {
		return fmt.Errorf("request body is required")
	}
	return nil
}

// isValidID 验证 ID 格式
func isValidID(id string) bool {
	// ID 应该只包含字母、数字、下划线和连字符
	matched, _ := regexp.MatchString(`^[a-zA-Z0-9_-]+$`, id)
	return matched && len(id) > 0 && len(id) <= 100
}

// isValidUsername 验证用户名格式
func isValidUsername(username string) bool {
	// 用户名应该只包含字母、数字、下划线和连字符
	matched, _ := regexp.MatchString(`^[a-zA-Z0-9_.-]+$`, username)
	return matched && len(username) >= 3 && len(username) <= 50
}

// isValidEmail 验证邮箱格式
func isValidEmail(email string) bool {
	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`)
	return emailRegex.MatchString(email)
}

// ValidateTiming 验证时间规则
func ValidateTiming(timing *types.Timing) error {
	if timing == nil {
		return nil
	}

	// 验证分钟
	for _, minute := range timing.Minutes {
		if minute < 0 || minute > 59 {
			return fmt.Errorf("invalid minute: %d (must be 0-59)", minute)
		}
	}

	// 验证小时
	for _, hour := range timing.Hours {
		if hour < 0 || hour > 23 {
			return fmt.Errorf("invalid hour: %d (must be 0-23)", hour)
		}
	}

	// 验证星期
	for _, weekday := range timing.Weekdays {
		if weekday < 0 || weekday > 6 {
			return fmt.Errorf("invalid weekday: %d (must be 0-6)", weekday)
		}
	}

	// 验证日期
	for _, day := range timing.Days {
		if day < 1 || day > 31 {
			return fmt.Errorf("invalid day: %d (must be 1-31)", day)
		}
	}

	// 验证月份
	for _, month := range timing.Months {
		if month < 1 || month > 12 {
			return fmt.Errorf("invalid month: %d (must be 1-12)", month)
		}
	}

	return nil
}

// SanitizeString 清理字符串
func SanitizeString(s string) string {
	// 移除控制字符
	s = strings.TrimSpace(s)
	// 移除 HTML 标签
	htmlRegex := regexp.MustCompile(`<[^>]*>`)
	s = htmlRegex.ReplaceAllString(s, "")
	return s
}
