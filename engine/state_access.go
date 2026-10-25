package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

// AddDeadJob 将任务记录到历史任务列表
func (e *Engine) AddDeadJob(job *types.Job) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.deadJobs[job.ID] = types.CloneJob(job)
}

// GetDeadJobs 获取已完成任务
func (e *Engine) GetDeadJobs() map[string]*types.Job {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make(map[string]*types.Job, len(e.deadJobs))
	for k, v := range e.deadJobs {
		result[k] = types.CloneJob(v)
	}
	return result
}

// GetDeadJob 获取单个历史任务
func (e *Engine) GetDeadJob(jobID string) (*types.Job, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	job, exists := e.deadJobs[jobID]
	return types.CloneJob(job), exists
}

// DeleteDeadJob 删除历史任务
func (e *Engine) DeleteDeadJob(jobID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.deadJobs, jobID)
}

// UpdateMasterState 更新主服务器状态
func (e *Engine) UpdateMasterState(enabled bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.state.Enabled = enabled
}

// GetEventQueues 获取事件队列长度
func (e *Engine) GetEventQueues() map[string]int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make(map[string]int, len(e.eventQueue))
	for k, v := range e.eventQueue {
		result[k] = v
	}
	return result
}

// FlushEventQueue 清空指定事件队列计数
func (e *Engine) FlushEventQueue(eventID string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	prev := e.eventQueue[eventID]
	delete(e.eventQueue, eventID)
	return prev
}

// LogActivity 记录活动日志（存储可用时持久化）
func (e *Engine) LogActivity(action string, data map[string]interface{}) {
	payload := map[string]interface{}{
		"action": action,
		"time":   time.Now().Unix(),
		"data":   data,
	}

	if e.storage != nil {
		ctx := context.Background()
		_ = e.storage.ListUnshift(ctx, "logs/activity", payload)
	}
}

// GetActivity 获取活动日志
func (e *Engine) GetActivity(offset, limit int) ([]map[string]interface{}, error) {
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	result := make([]map[string]interface{}, 0)

	if e.storage == nil {
		return result, nil
	}
	ctx := context.Background()
	items, err := e.storage.ListGet(ctx, "logs/activity", offset, limit)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return result, nil
		}
		return nil, err
	}

	for _, item := range items {
		raw, err := json.Marshal(item)
		if err != nil {
			continue
		}
		record := map[string]interface{}{}
		if err := json.Unmarshal(raw, &record); err != nil {
			continue
		}
		result = append(result, record)
	}
	return result, nil
}

// LookupJob reads both live state and persistent history after a restart.
func (e *Engine) LookupJob(id string) (*types.Job, bool) {
	e.mu.RLock()
	job := types.CloneJob(e.activeJobs[id])
	if job == nil {
		job = types.CloneJob(e.deadJobs[id])
	}
	e.mu.RUnlock()
	if job == nil || job.Complete {
		if deleted, err := e.IsJobHistoryDeleted(id); err != nil || deleted {
			return nil, false
		}
	}
	if job != nil {
		if job.Complete && job.Action == "" {
			job.Action = "job_complete"
		}
		sanitizeJobReports(job)
		return job, true
	}
	if e.storage == nil {
		return nil, false
	}
	data, err := e.storage.Get(context.Background(), "jobs:"+id)
	if err != nil {
		return nil, false
	}
	job = &types.Job{}
	if json.Unmarshal(data, job) != nil {
		return nil, false
	}
	if job.Complete && job.Action == "" {
		job.Action = "job_complete"
	}
	sanitizeJobReports(job)
	return job, true
}
func (e *Engine) UpdateJobProgress(id string, progress float64) (*types.Job, bool) {
	if progress < 0 {
		progress = 0
	}
	if progress > 1 {
		progress = 1
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	job, ok := e.activeJobs[id]
	if !ok {
		return nil, false
	}
	job.Progress = progress
	return types.CloneJob(job), true
}

func (jm *JobManager) persistActiveJob(job *types.Job) error {
	if jm.engine.storage == nil {
		return fmt.Errorf("storage not initialized")
	}
	return upsertStorageList(context.Background(), jm.engine.storage, "active_jobs", job.ID, types.CloneJob(job), false)
}
func (jm *JobManager) removeActiveJobStorage(id string) {
	if jm.engine.storage != nil {
		_ = removeStorageList(context.Background(), jm.engine.storage, "active_jobs", id)
	}
}

// RecoverJobs records interrupted work as failed instead of silently losing it or
// re-executing commands whose side effects may already have happened.
func (jm *JobManager) RecoverJobs() error {
	if err := jm.recoverActiveCompletionJournals(); err != nil {
		return err
	}
	if err := jm.retryPendingCompletions(true); err != nil {
		return err
	}
	if jm.engine.storage == nil {
		return nil
	}
	items, err := jm.engine.storage.ListGet(context.Background(), "active_jobs", 0, 0)
	if err != nil {
		return fmt.Errorf("load active jobs for recovery: %w", err)
	}
	for _, item := range items {
		data, _ := json.Marshal(item)
		var job types.Job
		if json.Unmarshal(data, &job) != nil || job.ID == "" {
			continue
		}
		if job.Hostname != "local" && job.Hostname != jm.getHostname() {
			if jm.engine.IsMaster() {
				jm.engine.mu.Lock()
				jm.engine.activeJobs[job.ID] = types.CloneJob(&job)
				jm.engine.mu.Unlock()
			}
			continue
		}
		if err := stopRecoveredJobProcess(&job); err != nil {
			return fmt.Errorf("recover active job %s: %w", job.ID, err)
		}
		job.Code = 1
		job.AbortReason = "Server restarted while job was active"
		job.Description = job.AbortReason
		job.Retries = 0
		jm.finishLocalJob(&job)
	}
	return nil
}

// Completion delivery is durable and retried until the master acknowledges it.
func (jm *JobManager) enqueueRemoteCompletion(job *types.Job) error {
	if jm.engine.storage == nil {
		return fmt.Errorf("storage not initialized")
	}
	if deleted, err := jm.engine.IsJobHistoryDeleted(job.ID); err != nil {
		return err
	} else if deleted {
		return nil
	}
	stored := false
	if job.LogFileSize > 0 {
		if err := jm.engine.requireDurableJobLog(context.Background(), job.ID, job.LogFileSize, true); err != nil {
			return err
		}
		stored = true
	}
	snapshot := types.CloneJob(job)
	snapshot.LogFile = ""
	envelope := map[string]interface{}{"id": job.ID, "job": snapshot, "log_stored": stored}
	if err := upsertStorageList(context.Background(), jm.engine.storage, "remote_completions", job.ID, envelope, false); err != nil {
		jm.logger.Error("Failed to persist remote completion", zap.Error(err))
		return err
	}
	// Admission sends only this newly durable envelope. Replaying the entire
	// shared outbox here makes a burst of N completions send N*(N+1)/2 frames
	// while holding finishMu. Periodic/reconnect recovery replays unacked work.
	if jm.engine.comm != nil {
		_ = jm.engine.comm.SendToMaster("finish_job", envelope)
	}
	return nil
}
func (jm *JobManager) RetryRemoteCompletions() {
	if jm.engine.storage == nil || jm.engine.comm == nil || jm.engine.IsMaster() {
		return
	}
	items, err := jm.engine.storage.ListGet(context.Background(), "remote_completions", 0, 0)
	if err != nil {
		return
	}
	for _, item := range items {
		raw, err := json.Marshal(item)
		var envelope struct {
			Job *types.Job `json:"job"`
		}
		if err != nil || json.Unmarshal(raw, &envelope) != nil || envelope.Job == nil || envelope.Job.Hostname != jm.getHostname() {
			continue
		}
		if deleted, err := jm.engine.IsJobHistoryDeleted(envelope.Job.ID); err != nil {
			return
		} else if deleted {
			jm.AckRemoteCompletion(envelope.Job.ID)
			continue
		}
		if err := jm.engine.comm.SendToMaster("finish_job", item); err != nil {
			return
		}
	}
}
func (jm *JobManager) AckRemoteCompletion(id string) {
	if jm.engine.storage != nil {
		_ = removeStorageList(context.Background(), jm.engine.storage, "remote_completions", id)
	}
}

func (jm *JobManager) RestoreRemoteJobs() error {
	if jm.engine.storage == nil {
		return nil
	}
	items, err := jm.engine.storage.ListGet(context.Background(), "active_jobs", 0, 0)
	if err != nil {
		return err
	}
	for _, item := range items {
		raw, _ := json.Marshal(item)
		var job types.Job
		if json.Unmarshal(raw, &job) != nil || job.ID == "" || job.Hostname == jm.getHostname() || job.Hostname == "local" {
			continue
		}
		jm.engine.mu.Lock()
		if jm.engine.activeJobs[job.ID] == nil {
			jm.engine.activeJobs[job.ID] = types.CloneJob(&job)
		}
		jm.engine.mu.Unlock()
	}
	return nil
}
func (e *Engine) UpdateActiveJob(id string, updates map[string]interface{}) (*types.Job, error) {
	e.jobManager.finishMu.Lock()
	defer e.jobManager.finishMu.Unlock()
	e.mu.Lock()
	current := e.activeJobs[id]
	if current == nil {
		e.mu.Unlock()
		return nil, fmt.Errorf("job not found")
	}
	job := types.CloneJob(current)
	patch := map[string]interface{}{}
	for _, key := range []string{"event_title", "progress", "timeout", "retries", "retry_delay", "chain", "chain_error", "notify_success", "notify_fail", "web_hook", "memory_limit", "memory_sustain", "cpu_limit", "cpu_sustain", "log_max_size"} {
		if v, ok := updates[key]; ok {
			patch[key] = v
		}
	}
	if title, ok := updates["title"]; ok {
		patch["event_title"] = title
	}
	raw, err := json.Marshal(patch)
	if err == nil {
		err = json.Unmarshal(raw, job)
	}
	if err != nil {
		e.mu.Unlock()
		return nil, err
	}
	if job.Timeout < 0 || job.Retries < 0 || job.RetryDelay < 0 || job.MemoryLimit < 0 || job.MemorySustain < 0 || job.CPULimit < 0 || job.CPUSustain < 0 || job.LogMaxSize < 0 {
		e.mu.Unlock()
		return nil, fmt.Errorf("job limits must be non-negative")
	}
	if job.Progress < 0 || job.Progress > 1 {
		e.mu.Unlock()
		return nil, fmt.Errorf("progress must be between zero and one")
	}
	job.UpdateVersion = current.UpdateVersion + 1
	e.mu.Unlock()
	// Reserve recovery space before accepting larger runtime metadata. Disk I/O
	// must not hold the engine state lock used by heartbeats and leadership.
	if err := e.jobManager.checkpointRuntimeJob(job); err != nil {
		return nil, err
	}
	e.mu.Lock()
	e.activeJobs[id] = types.CloneJob(job)
	e.mu.Unlock()
	if err := e.jobManager.persistActiveJob(job); err != nil {
		return job, err
	}
	if job.Hostname != "local" && job.Hostname != e.Hostname() {
		if err := e.comm.SendToSlave(job.Hostname, "update_job", map[string]interface{}{"id": id, "updates": patch}); err != nil {
			return job, err
		}
	}
	return job, nil
}
