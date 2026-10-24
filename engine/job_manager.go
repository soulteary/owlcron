package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/soulteary/owlcron/monitor"
	"github.com/soulteary/owlcron/plugin"
	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

// JobManager 任务管理器
type JobManager struct {
	engine          *Engine
	logger          *zap.Logger
	mu              sync.RWMutex
	finishMu        sync.Mutex
	logMu           sync.Mutex
	logLocks        [64]sync.Mutex
	logUploadSlots  chan struct{}
	liveLogMu       sync.Mutex
	liveLogJobs     map[string]bool
	workers         sync.WaitGroup
	pluginExecutor  *plugin.PluginExecutor
	resourceMonitor *monitor.ResourceMonitor
	webHookManager  *WebHookManager
	mailManager     *MailManager
	running         map[string]context.CancelFunc
}

// NewJobManager 创建新的任务管理器
func NewJobManager(engine *Engine, logger *zap.Logger) (*JobManager, error) {
	uploadConcurrency := engine.config.LogUploadConcurrency
	if uploadConcurrency <= 0 {
		uploadConcurrency = 4
	}
	jm := &JobManager{
		logUploadSlots:  make(chan struct{}, uploadConcurrency),
		liveLogJobs:     make(map[string]bool),
		engine:          engine,
		running:         make(map[string]context.CancelFunc),
		logger:          logger,
		pluginExecutor:  plugin.NewPluginExecutor(logger),
		resourceMonitor: monitor.NewResourceMonitor(logger),
		webHookManager:  NewWebHookManager(engine, logger),
		mailManager:     NewMailManager(engine, logger),
	}
	return jm, nil
}

// LaunchOrQueueJob 启动任务或加入队列
func (jm *JobManager) LaunchOrQueueJob(event *types.Event, timestamp int64) error {
	_, err := jm.LaunchOrQueueJobWithIDs(event, timestamp)
	return err
}
func (jm *JobManager) LaunchOrQueueJobWithIDs(event *types.Event, timestamp int64) ([]string, error) {
	if !jm.engine.IsMaster() {
		return nil, fmt.Errorf("only master server can launch jobs")
	}
	if event == nil {
		return nil, fmt.Errorf("missing event")
	}
	if event.Detached.Bool() {
		return nil, fmt.Errorf("detached execution is not supported")
	}
	ids, err := jm.LaunchJobWithIDs(event, timestamp)
	if err != nil && event.Queue.Bool() && jm.engine.ctx.Err() == nil {
		err = jm.queueJob(event, timestamp)
		if err == nil {
			return []string{}, nil
		}
	}
	return ids, err
}

// LaunchJob validates and reserves all jobs before starting processes, so concurrent
// API and scheduler launches observe the same concurrency limits.
func (jm *JobManager) LaunchJob(event *types.Event, timestamp int64) error {
	_, err := jm.LaunchJobWithIDs(event, timestamp)
	return err
}
func (jm *JobManager) LaunchJobWithIDs(event *types.Event, timestamp int64) ([]string, error) {
	return jm.launchJobWithToken(event, timestamp, "")
}

// LaunchQueuedJob preserves a queue item's job IDs across interrupted admission.
func (jm *JobManager) LaunchQueuedJob(event *types.Event, timestamp int64, token string) error {
	_, err := jm.launchJobWithToken(event, timestamp, token)
	return err
}

func (jm *JobManager) launchJobWithToken(event *types.Event, timestamp int64, token string) ([]string, error) {
	if jm.engine.ctx.Err() != nil {
		return nil, fmt.Errorf("engine is shutting down")
	}
	if !jm.engine.EnsureLeadership() {
		return nil, fmt.Errorf("current node does not own leadership")
	}
	if token != "" {
		if ids, found, err := jm.resumeQueueAdmission(token); found || err != nil {
			return ids, err
		}
	}
	if event == nil {
		return nil, fmt.Errorf("missing event")
	}
	event = types.CloneEvent(event)
	if event.Detached.Bool() {
		return nil, fmt.Errorf("detached execution is not supported")
	}
	jm.mu.Lock()
	if !event.Enabled.Bool() {
		jm.mu.Unlock()
		return nil, fmt.Errorf("event is disabled: %s", event.Title)
	}
	plug, err := jm.GetPluginFromStorage(event.Plugin)
	if err != nil {
		jm.mu.Unlock()
		return nil, fmt.Errorf("plugin not found: %s: %w", event.Plugin, err)
	}
	if !plug.Enabled.Bool() || plug.Command == "" {
		jm.mu.Unlock()
		return nil, fmt.Errorf("plugin is disabled or has no command: %s", plug.Title)
	}
	if event.Category != "" {
		cat, ok := jm.engine.GetCategory(event.Category)
		if !ok || !cat.Enabled.Bool() {
			jm.mu.Unlock()
			return nil, fmt.Errorf("category is missing or disabled: %s", event.Category)
		}
	}
	if err := jm.checkConcurrencyLimits(event); err != nil {
		jm.mu.Unlock()
		return nil, err
	}
	servers := jm.chooseServers(event)
	if len(servers) == 0 {
		jm.mu.Unlock()
		return nil, fmt.Errorf("no available servers: %s", event.Target)
	}
	jobs := make([]*types.Job, 0, len(servers))
	for idx, server := range servers {
		job := jm.createJob(event, server, timestamp, idx)
		job.Command = plug.Command
		job.PluginTitle = plug.Title
		job.Env = make(map[string]string, len(plug.Env))
		for k, v := range plug.Env {
			job.Env[k] = v
		}
		job.CWD = plug.CWD
		job.UID = plug.UID
		job.GID = plug.GID

		if job.Params == nil {
			job.Params = make(map[string]interface{})
		}
		supplied := job.Params
		job.Params = make(map[string]interface{})
		for _, param := range plug.Params {
			if param == nil || param.ID == "" {
				jm.mu.Unlock()
				return nil, fmt.Errorf("invalid plugin parameter")
			}
			value := param.Value
			if value == nil {
				value = param.Default
			}
			if param.Type != "hidden" {
				if suppliedValue, ok := supplied[param.ID]; ok {
					value = suppliedValue
				}
			}
			job.Params[param.ID] = value
		}

		jobs = append(jobs, job)
	}
	// Multiplex must also respect the remaining event/global/category capacity.
	if event.MaxChildren > 0 {
		n := 0
		for _, j := range jm.engine.GetActiveJobs() {
			if j.Event == event.ID {
				n++
			}
		}
		if n+len(jobs) > event.MaxChildren {
			jm.mu.Unlock()
			return nil, fmt.Errorf("multiplex exceeds event concurrency limit")
		}
	}
	if event.Category != "" {
		if category, ok := jm.engine.GetCategory(event.Category); ok && category.MaxChildren > 0 {
			count := 0
			for _, job := range jm.engine.GetActiveJobs() {
				if job.Category == event.Category {
					count++
				}
			}
			if count+len(jobs) > category.MaxChildren {
				jm.mu.Unlock()
				return nil, fmt.Errorf("multiplex exceeds category concurrency limit")
			}
		}
	}
	if jm.engine.config.MaxJobs > 0 && len(jm.engine.GetActiveJobs())+len(jobs) > jm.engine.config.MaxJobs {
		jm.mu.Unlock()
		return nil, fmt.Errorf("multiplex exceeds global concurrency limit")
	}
	if err := jm.checkNodeCapacity(jobs); err != nil {
		jm.mu.Unlock()
		return nil, err
	}
	if err := jm.stageQueueAdmission(token, jobs); err != nil {
		jm.mu.Unlock()
		return nil, err
	}
	jm.engine.mu.Lock()
	for _, job := range jobs {
		jm.engine.activeJobs[job.ID] = types.CloneJob(job)
	}
	jm.engine.mu.Unlock()
	jm.mu.Unlock()
	for _, job := range jobs {
		jm.dispatchJob(job)
	}
	ids := make([]string, 0, len(jobs))
	for _, job := range jobs {
		ids = append(ids, job.ID)
	}
	return ids, nil
}
func (jm *JobManager) dispatchJob(job *types.Job) {
	if job.Hostname == "local" || job.Hostname == jm.getHostname() {
		jm.launchLocalJob(job)
	} else if err := jm.sendJobToRemoteServer(job.Hostname, job); err != nil {
		job.Code = 1
		job.Description = err.Error()
		jm.finishLocalJob(job)
	}
}
func (jm *JobManager) createJob(event *types.Event, hostname string, timestamp int64, index int) *types.Job {
	if hostname == "local" {
		hostname = jm.getHostname()
	}
	job := &types.Job{ID: jm.generateJobID(), Event: event.ID, EventTitle: event.Title, Category: event.Category, Plugin: event.Plugin, Hostname: hostname, Target: event.Target, Params: types.CloneMap(event.Params), TimeStart: float64(time.Now().UnixNano()) / 1e9, Now: timestamp, Timeout: event.Timeout, CWD: event.CWD, UID: event.UID, GID: event.GID, WebHook: event.WebHook, Retries: event.Retries, RetryDelay: event.RetryDelay, NotifySuccess: event.NotifySuccess, NotifyFail: event.NotifyFail, Detached: event.Detached.Bool(), ChainData: types.CloneMap(event.ChainData), ChainParams: types.CloneMap(event.ChainParams)}
	job.Source = event.Source
	job.SourceEvent = event.SourceEvent
	job.SourceJob = event.SourceJob
	job.ChainCode = event.ChainCode
	job.ChainDescription = event.ChainDescription
	job.MemoryLimit = event.MemoryLimit
	job.MemorySustain = event.MemorySustain
	job.CPULimit = event.CPULimit
	job.CPUSustain = event.CPUSustain
	job.LogMaxSize = event.LogMaxSize
	job.LogFile = filepath.Join(jm.engine.config.LogDir, "jobs", job.ID+".log")
	if event.Multiplex.Bool() && event.Stagger > 0 && index > 0 {
		job.When = float64(time.Now().Unix()) + float64(event.Stagger*index)
		job.Pending = true
	}
	return job
}

// LaunchRemoteJob executes the authenticated master's immutable job snapshot.
func (jm *JobManager) LaunchRemoteJob(job *types.Job) error {
	if jm.engine.ctx.Err() != nil {
		return fmt.Errorf("engine is shutting down")
	}
	if job == nil || job.Detached || job.ID == "" || job.Command == "" || !regexp.MustCompile(`^[a-zA-Z0-9_-]+$`).MatchString(job.ID) {
		return fmt.Errorf("invalid remote job")
	}
	job = types.CloneJob(job)
	job.Hostname = jm.getHostname()
	job.LogFile = filepath.Join(jm.engine.config.LogDir, "jobs", job.ID+".log")
	jm.engine.mu.Lock()
	if _, ok := jm.engine.activeJobs[job.ID]; ok {
		jm.engine.mu.Unlock()
		return nil
	}
	if _, ok := jm.engine.deadJobs[job.ID]; ok {
		jm.engine.mu.Unlock()
		return nil
	}
	jm.engine.activeJobs[job.ID] = types.CloneJob(job)
	jm.engine.mu.Unlock()
	jm.launchLocalJob(job)
	return nil
}
func (jm *JobManager) launchLocalJob(input *types.Job) {
	job := types.CloneJob(input)
	if job.LogFile == "" {
		job.LogFile = filepath.Join(jm.engine.config.LogDir, "jobs", job.ID+".log")
	}
	parent := jm.engine.ctx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	// Publish an execution token before starting the child, closing the window
	// where a daemon can die after exec but before its PID is persisted.
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		cancel()
		job.Code = 1
		job.Description = "Cannot initialize process recovery identity"
		jm.finishLocalJob(job)
		return
	}
	job.ProcessToken = hex.EncodeToString(token)
	job.ProcessIdentity = ""
	job.PID = 0
	jm.mu.Lock()
	if _, exists := jm.running[job.ID]; exists {
		jm.mu.Unlock()
		cancel()
		return
	}
	jm.running[job.ID] = cancel
	jm.mu.Unlock()
	if err := jm.publishJob(job); err != nil {
		cancel()
		job.Code = 1
		job.AbortReason = "Failed to persist active job"
		job.Description = err.Error()
		jm.finishLocalJob(job)
		return
	}
	jm.workers.Add(1)
	go func() {
		defer jm.workers.Done()
		defer cancel()
		if delay := time.Until(time.Unix(0, int64(job.When*1e9))); job.When > 0 && delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				job.AbortReason = "Cancelled before launch"
				job.Code = 1
				job.Description = job.AbortReason
				jm.finishLocalJob(job)
				return
			}
		}
		job.Pending = false
		job.When = 0
		job.TimeStart = float64(time.Now().UnixNano()) / 1e9
		timeoutDone := make(chan struct{})
		defer close(timeoutDone)
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-timeoutDone:
					return
				case <-ticker.C:
					current, ok := jm.engine.LookupJob(job.ID)
					if ok && current.Timeout > 0 && time.Since(time.Unix(0, int64(current.TimeStart*1e9))) >= time.Duration(current.Timeout)*time.Second {
						jm.engine.mu.Lock()
						if live := jm.engine.activeJobs[job.ID]; live != nil {
							live.AbortReason = fmt.Sprintf("Exceeded maximum run time (%d seconds)", current.Timeout)
						}
						jm.engine.mu.Unlock()
						cancel()
						return
					}
				}
			}
		}()

		if err := jm.createLogFile(job); err != nil {
			job.Code = 1
			job.Description = err.Error()
			jm.finishLocalJob(job)
			return
		}
		logFile, err := os.OpenFile(job.LogFile, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			job.Code = 1
			job.Description = err.Error()
			jm.finishLocalJob(job)
			return
		}
		command, args := plugin.ParseCommand(job.Command)
		proc := exec.CommandContext(ctx, command, args...)
		proc.Env = jm.buildEnv(job, nil)
		proc.Dir = job.CWD
		proc.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		proc.WaitDelay = time.Second
		proc.Cancel = func() error {
			if proc.Process == nil {
				return os.ErrProcessDone
			}
			return syscall.Kill(-proc.Process.Pid, syscall.SIGKILL)
		}
		if err := configureCredential(proc, job); err != nil {
			logFile.Close()
			job.Code = 1
			job.Description = err.Error()
			jm.finishLocalJob(job)
			return
		}
		logWriter, writerErr := newBoundedLogWriter(logFile, jm.jobLogLimit(job), func(err error) {
			jm.engine.mu.Lock()
			if current := jm.engine.activeJobs[job.ID]; current != nil {
				current.AbortReason = err.Error()
			}
			jm.engine.mu.Unlock()
			cancel()
		})
		if writerErr != nil {
			logFile.Close()
			job.Code = 1
			job.Description = writerErr.Error()
			jm.finishLocalJob(job)
			return
		}
		result, runErr := jm.pluginExecutor.ExecutePrepared(ctx, proc, job, logWriter, func(pid int) {
			job.PID = pid
			identity, err := processIdentity(pid)
			// A short-lived plugin may already be a zombie. Its normal Wait and
			// completion protocol still decide the result; only live, uninspectable
			// processes require cancellation.
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				cancel()
				return
			}
			job.ProcessIdentity = identity
			if err := jm.publishJob(job); err != nil {
				cancel()
			}
			jm.webHookManager.FireJobStartHook(types.CloneJob(job))
		}, func(data map[string]interface{}) {
			if !jm.engine.config.AllowEventUpdatesFromJobs {
				delete(data, "update_event")
			}
			applyPluginUpdate(job, data)
			if err := jm.publishJob(job); err != nil {
				cancel()
			}
		})
		if proc.Process != nil {
			_ = syscall.Kill(-proc.Process.Pid, syscall.SIGKILL)
		}
		if logWriter.Err() != nil {
			runErr = logWriter.Err()
		}
		if syncErr := logFile.Sync(); syncErr != nil && runErr == nil {
			runErr = syncErr
		}
		if closeErr := logFile.Close(); closeErr != nil && runErr == nil {
			runErr = closeErr
		}
		// Abort metadata is owned by the API/monitor and copied back after the child exits.
		jm.engine.mu.RLock()
		if current := jm.engine.activeJobs[job.ID]; current != nil {
			job.AbortReason = current.AbortReason
			cp := types.CloneJob(current)
			job.CPU = cp.CPU
			job.Mem = cp.Mem
			job.FlaggedTime = cp.FlaggedTime
			if current.UpdateVersion > job.UpdateVersion {
				mergeRuntimeJob(job, current)
			}
		}
		jm.engine.mu.RUnlock()
		if runErr != nil {
			job.Code = 1
			job.Description = runErr.Error()
			var exit *exec.ExitError
			if errors.As(runErr, &exit) {
				job.Code = exit.ExitCode()
			}
			if ctx.Err() != nil && job.AbortReason == "" {
				job.AbortReason = ctx.Err().Error()
				job.Description = job.AbortReason
			}
		} else if result != nil {
			job.Code = result.Code
			job.Description = result.Description
			job.Progress = result.Progress
		}
		if job.AbortReason != "" {
			job.Code = 1
			job.Description = "Job Aborted: " + job.AbortReason
		}
		// Release ownership before a retry reserves this same job ID.
		jm.mu.Lock()
		delete(jm.running, job.ID)
		jm.mu.Unlock()
		jm.finishLocalJob(job)
	}()
}
func configureCredential(proc *exec.Cmd, job *types.Job) error {
	if job.UID == "" && job.GID == "" {
		return nil
	}
	uid := os.Getuid()
	gid := os.Getgid()
	if job.UID != "" {
		u, err := user.Lookup(job.UID)
		if err != nil {
			u, err = user.LookupId(job.UID)
		}
		if err != nil {
			return err
		}
		uid, _ = strconv.Atoi(u.Uid)
		gid, _ = strconv.Atoi(u.Gid)
	}
	if job.GID != "" {
		g, err := user.LookupGroup(job.GID)
		if err != nil {
			g, err = user.LookupGroupId(job.GID)
		}
		if err != nil {
			return err
		}
		gid, _ = strconv.Atoi(g.Gid)
	}
	proc.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}
	return nil
}
func applyPluginUpdate(job *types.Job, data map[string]interface{}) {
	if v, ok := data["progress"].(float64); ok {
		job.Progress = v
	}
	if v, ok := data["complete"].(bool); ok {
		job.Complete = v
	}
	if v, ok := data["code"].(float64); ok {
		job.Code = int(v)
	}
	if v, ok := data["description"].(string); ok {
		job.Description = v
	}
	if v, ok := data["perf"]; ok {
		job.Perf = v
	}
	if v, ok := data["chain"].(string); ok {
		job.Chain = v
	}
	if v, ok := data["chain_error"].(string); ok {
		job.ChainError = v
	}
	if v, ok := data["chain_data"].(map[string]interface{}); ok {
		job.ChainData = types.CloneMap(v)
	}
	if v, ok := data["chain_params"].(map[string]interface{}); ok {
		job.ChainParams = types.CloneMap(v)
	}
	if v, ok := data["table"]; ok {
		job.Table = v
	}
	if v, ok := data["html"]; ok {
		job.HTML = v
	}
	if v, ok := data["label"].(string); ok {
		job.Label = v
	}
	if v, ok := data["update_event"].(map[string]interface{}); ok {
		job.UpdateEvent = types.CloneMap(v)
	}
	if v, ok := data["notify_success"].(string); ok {
		job.NotifySuccess = v
	}
	if v, ok := data["notify_fail"].(string); ok {
		job.NotifyFail = v
	}
}
func (jm *JobManager) publishJob(job *types.Job) error {
	jm.finishMu.Lock()
	defer jm.finishMu.Unlock()
	sanitizeJobReports(job)
	jm.engine.mu.Lock()
	if current := jm.engine.activeJobs[job.ID]; current != nil && current.UpdateVersion > job.UpdateVersion {
		mergeRuntimeJob(job, current)
	}
	snapshot := types.CloneJob(job)
	if current := jm.engine.activeJobs[job.ID]; current != nil {
		snapshot.AbortReason = current.AbortReason
		cp := types.CloneJob(current)
		snapshot.CPU = cp.CPU
		snapshot.Mem = cp.Mem
		snapshot.FlaggedTime = current.FlaggedTime
	}
	jm.engine.activeJobs[job.ID] = types.CloneJob(snapshot)
	jm.engine.mu.Unlock()
	if err := jm.persistActiveJob(snapshot); err != nil {
		return err
	}
	if !jm.engine.IsMaster() && jm.engine.comm != nil {
		_ = jm.engine.comm.SendToMaster("job_update", snapshot)
	}
	return nil
}
func (jm *JobManager) CompleteRemoteJob(job *types.Job) bool {
	jm.finishLocalJob(types.CloneJob(job))
	completed, ok := jm.engine.GetDeadJob(job.ID)
	return ok && completed.Complete && completed.StorageError == ""
}

// finishLocalJob 完成本地任务
func (jm *JobManager) finishLocalJob(job *types.Job) {
	jm.finishMu.Lock()
	locked := true
	defer func() {
		if locked {
			jm.finishMu.Unlock()
		}
	}()
	jm.mu.Lock()
	delete(jm.running, job.ID)
	jm.mu.Unlock()
	jm.engine.mu.RLock()
	if current := jm.engine.activeJobs[job.ID]; current != nil {
		if current.UpdateVersion > job.UpdateVersion {
			mergeRuntimeJob(job, current)
		}
		if current.AbortReason != "" {
			job.AbortReason = current.AbortReason
			job.Code = 1
			job.Description = "Job Aborted: " + current.AbortReason
		}
	}
	jm.engine.mu.RUnlock()
	if job.TimeEnd <= 0 {
		job.TimeEnd = float64(time.Now().UnixNano()) / 1e9
		job.Elapsed = job.TimeEnd - job.TimeStart
	}
	if !jm.engine.config.AllowEventUpdatesFromJobs {
		job.UpdateEvent = nil
	}
	sanitizeJobReports(job)
	job.Complete = true
	job.Action = "job_complete"
	job.Pending = false
	if job.Code == 0 {
		job.Progress = 1
	}
	if info, err := os.Stat(job.LogFile); err == nil {
		job.LogFileSize = info.Size()
	}
	jm.engine.mu.Lock()
	if _, done := jm.engine.deadJobs[job.ID]; done {
		jm.engine.mu.Unlock()
		return
	}
	jm.engine.mu.Unlock()

	jm.logger.Info("Job completed", zap.String("job_id", job.ID), zap.Int("code", job.Code))

	// 重试逻辑
	if job.AbortReason == "" && job.Code != 0 && job.Retries > 0 && job.RetriesUsed < job.Retries {
		job.RetriesUsed++
		jm.logger.Info("Retrying job",
			zap.String("job_id", job.ID),
			zap.Int("retry", job.RetriesUsed),
			zap.Int("max_retries", job.Retries))

		delay := job.RetryDelay
		if delay <= 0 {
			delay = 0
		}

		retryJob := *types.CloneJob(job)
		retryJob.Code = 0
		retryJob.Complete = false
		retryJob.Description = ""
		retryJob.TimeStart = float64(time.Now().Unix())
		retryJob.TimeEnd = 0
		retryJob.Elapsed = 0
		retryJob.PID = 0
		retryJob.Progress = 0
		retryJob.CPU = nil
		retryJob.Mem = nil
		retryJob.FlaggedTime = 0
		retryJob.When = 0
		retryJob.Pending = true

		if delay > 0 {
			retryJob.When = float64(time.Now().Unix()) + float64(delay)
		}

		jm.engine.mu.Lock()
		jm.engine.activeJobs[job.ID] = types.CloneJob(&retryJob)
		jm.engine.mu.Unlock()
		jm.finishMu.Unlock()
		locked = false
		jm.dispatchJob(&retryJob)
		return
	}

	if err := jm.writeCompletionSpool(job); err != nil {
		job.StorageError = err.Error()
		jm.logger.Error("Failed to save recovery spool", zap.Error(err))
	}
	if err := jm.persistCompletion(job); err != nil {
		job.StorageError = err.Error()
		jm.logger.Error("Completion persistence pending recovery", zap.String("job_id", job.ID), zap.Error(err))
	} else {
		job.StorageError = ""
		if !jm.engine.IsMaster() && jm.engine.comm != nil {
			if err := jm.enqueueRemoteCompletion(job); err != nil {
				job.StorageError = err.Error()
			}
		}
		if job.StorageError == "" {
			_ = os.Remove(jm.completionSpoolPath(job.ID))
		}
	}

	jm.resourceMonitor.Forget(job.ID)
	// 更新状态
	jm.engine.mu.Lock()
	jm.engine.state.JobCodes[job.Event] = job.Code
	jm.engine.state.Stats["jobs_completed"]++
	if job.Code != 0 {
		jm.engine.state.Stats["jobs_failed"]++
	}
	jm.engine.state.Stats["jobs_elapsed"] += job.Elapsed
	jm.engine.state.Stats["jobs_log_size"] += float64(job.LogFileSize)
	delete(jm.engine.activeJobs, job.ID)
	jm.engine.deadJobs[job.ID] = types.CloneJob(job)
	if job.PID > 0 {
		delete(jm.engine.kids, job.PID)
	}
	jm.engine.mu.Unlock()

	if !jm.engine.IsMaster() && jm.engine.comm != nil {
		return
	}

	jm.engine.LogActivity("job_complete", map[string]interface{}{
		"job_id":    job.ID,
		"event_id":  job.Event,
		"hostname":  job.Hostname,
		"code":      job.Code,
		"complete":  job.Complete,
		"elapsed_s": job.Elapsed,
	})

	event, exists := jm.engine.scheduler.GetEvent(job.Event)

	if exists && len(job.UpdateEvent) > 0 && jm.engine.config.AllowEventUpdatesFromJobs {
		raw, err := json.Marshal(job.UpdateEvent)
		updated := types.CloneEvent(event)
		if err == nil {
			err = json.Unmarshal(raw, updated)
		}
		updated.ID = event.ID
		if err == nil && updated.Title != "" && updated.Plugin != "" && updated.Timeout >= 0 && updated.Retries >= 0 && !strings.ContainsAny(updated.Title, "<>") {
			if err = jm.engine.scheduler.AddEvent(updated); err == nil {
				jm.engine.comm.BroadcastToSockets("update", map[string]interface{}{"schedule": eventArray(jm.engine.scheduler.GetAllEvents())})
			}
		}
		if err != nil {
			jm.logger.Warn("Plugin event update failed", zap.String("event_id", job.Event), zap.Error(err))
		}
	}

	// 触发链式反应
	if exists {
		chainIDs := event.GetChainIDs()
		if job.Chain != "" {
			chainIDs = []string{job.Chain}
		}
		chainErrorIDs := event.GetChainErrorIDs()
		if job.ChainError != "" {
			chainErrorIDs = []string{job.ChainError}
		}

		if job.Code == 0 && len(chainIDs) > 0 {
			for _, chainID := range chainIDs {
				chainID := chainID
				go func() {
					if err := jm.engine.scheduler.ChainReaction(job, chainID); err != nil {
						jm.logger.Error("Chain reaction failed", zap.String("chain_id", chainID), zap.Error(err))
					}
				}()
			}
		} else if job.Code != 0 && len(chainErrorIDs) > 0 {
			for _, chainID := range chainErrorIDs {
				chainID := chainID
				go func() {
					if err := jm.engine.scheduler.ChainReaction(job, chainID); err != nil {
						jm.logger.Error("Chain error reaction failed", zap.String("chain_id", chainID), zap.Error(err))
					}
				}()
			}
		}
	}

	// 检查事件队列（任务完成后可能可以启动排队的任务）
	if exists && event.Queue.Bool() {
		go jm.engine.scheduler.checkEventQueues()
	}
}

// finishJobStorage 持久化任务记录到存储
func (jm *JobManager) finishJobStorage(job *types.Job) error {
	if jm.engine.storage == nil {
		return fmt.Errorf("storage not initialized")
	}
	ctx := context.Background()
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	atomic, ok := jm.engine.storage.(storage.AtomicStorage)
	if !ok {
		return storage.ErrUnsupported
	}
	if err = atomic.Update(ctx, "jobs:"+job.ID, func(existing []byte) ([]byte, error) {
		if len(existing) > 0 {
			var marker struct {
				Deleted bool `json:"deleted"`
			}
			if err := json.Unmarshal(existing, &marker); err != nil {
				return nil, err
			}
			if marker.Deleted {
				return nil, ErrJobHistoryDeleted
			}
		}
		return data, nil
	}); err != nil {
		return err
	}
	return storage.HistoryUpsert(ctx, jm.engine.storage, job)
}

// AbortJob 中止任务
func (jm *JobManager) AbortJob(jobID string, reason string) error {
	jm.engine.mu.Lock()
	job, ok := jm.engine.activeJobs[jobID]
	if ok {
		job.AbortReason = reason
	}
	jm.engine.mu.Unlock()
	if !ok {
		return fmt.Errorf("job not found: %s", jobID)
	}
	if job.Hostname != "local" && job.Hostname != jm.getHostname() {
		return jm.engine.comm.SendToSlave(job.Hostname, "abort_job", map[string]interface{}{"id": jobID, "reason": reason})
	}
	jm.mu.RLock()
	cancel := jm.running[jobID]
	jm.mu.RUnlock()
	if cancel == nil {
		return fmt.Errorf("job is not running: %s", jobID)
	}
	cancel()
	return nil
}

// MonitorAllActiveJobs 监控所有活动任务
func (jm *JobManager) MonitorAllActiveJobs() {
	jm.engine.mu.RLock()
	jobs := make(map[string]*types.Job)
	for k, v := range jm.engine.activeJobs {
		jobs[k] = types.CloneJob(v)
	}
	jm.engine.mu.RUnlock()

	now := time.Now().Unix()

	for id, job := range jobs {
		if job.Pending || (job.Hostname != "local" && job.Hostname != jm.getHostname()) {
			continue
		}
		// 检查超时
		if job.Timeout > 0 && (now-int64(job.TimeStart)) >= int64(job.Timeout) {
			jm.logger.Warn("Job exceeded timeout", zap.String("job_id", id))
			jm.AbortJob(id, fmt.Sprintf("Exceeded maximum run time (%d seconds)", job.Timeout))
			continue
		}

		// 监控资源使用
		if job.Pending {
			continue
		}
		if err := jm.resourceMonitor.MonitorJobResources(job); err != nil {
			jm.logger.Debug("Failed to monitor job resources", zap.String("job_id", id), zap.Error(err))
		}

		// 检查内存和 CPU 限制
		// Worker execution uses the admitted job snapshot, even without a local event cache.
		{
			category, catExists := jm.engine.GetCategory(job.Category)
			memoryLimit := job.MemoryLimit
			memorySustain := job.MemorySustain
			cpuLimit := job.CPULimit
			cpuSustain := job.CPUSustain

			if catExists {
				if memoryLimit == 0 {
					memoryLimit = category.MemoryLimit
				}
				if memorySustain == 0 {
					memorySustain = category.MemorySustain
				}
				if cpuLimit == 0 {
					cpuLimit = category.CPULimit
				}
				if cpuSustain == 0 {
					cpuSustain = category.CPUSustain
				}
			}

			if memoryLimit == 0 {
				memoryLimit = jm.engine.config.JobMemoryMax
			}
			if memorySustain == 0 {
				memorySustain = jm.engine.config.JobMemorySustain
			}
			if cpuLimit == 0 {
				cpuLimit = jm.engine.config.JobCPUMax
			}
			if cpuSustain == 0 {
				cpuSustain = jm.engine.config.JobCPUSustain
			}

			exceeded, reason := jm.resourceMonitor.CheckResourceLimits(
				job,
				memoryLimit,
				memorySustain,
				cpuLimit,
				cpuSustain,
			)

			jm.engine.mu.Lock()
			if current := jm.engine.activeJobs[id]; current != nil {
				current.CPU = job.CPU
				current.Mem = job.Mem
				current.FlaggedTime = job.FlaggedTime
			}
			jm.engine.mu.Unlock()
			if exceeded {
				jm.logger.Warn("Job exceeded resource limits", zap.String("job_id", id), zap.String("reason", reason))
				jm.AbortJob(id, reason)
			}
		}
	}
}

// MonitorServerResources 监控服务器资源
func (jm *JobManager) MonitorServerResources() {
	cpu, mem, err := jm.resourceMonitor.MonitorServerResources()
	if err == nil {
		jm.engine.mu.Lock()
		jm.engine.multi.Data["cpu"] = cpu
		jm.engine.multi.Data["mem"] = mem
		jm.engine.mu.Unlock()
	} else {
		jm.logger.Debug("Server resource monitoring unavailable", zap.Error(err))
	}

	// 监控每个活跃任务的日志文件大小
	jm.engine.mu.RLock()
	jobs := make(map[string]*types.Job)
	for k, v := range jm.engine.activeJobs {
		jobs[k] = types.CloneJob(v)
	}
	jm.engine.mu.RUnlock()

	for id, job := range jobs {
		if job.Hostname != jm.getHostname() && job.Hostname != "local" {
			continue
		}
		if !job.Pending && !jm.engine.IsMaster() {
			jm.scheduleLiveLogSync(job)
		}
		maxLogSize := job.LogMaxSize
		if maxLogSize == 0 {
			if category, ok := jm.engine.GetCategory(job.Category); ok {
				maxLogSize = category.LogMaxSize
			}
		}
		if maxLogSize == 0 {
			maxLogSize = jm.engine.config.JobLogMaxSize
		}
		if job.LogFile != "" {
			if fi, err := os.Stat(job.LogFile); err == nil {
				job.LogFileSize = fi.Size()
				jm.engine.mu.Lock()
				if current := jm.engine.activeJobs[id]; current != nil {
					current.LogFileSize = fi.Size()
				}
				jm.engine.mu.Unlock()
				if maxLogSize > 0 && fi.Size() > maxLogSize {
					jm.logger.Warn("Job log exceeded max size",
						zap.String("job_id", id),
						zap.Int64("size", fi.Size()),
						zap.Int64("max", maxLogSize))
					jm.AbortJob(id, fmt.Sprintf("Job log file exceeded maximum size (%d bytes)", maxLogSize))
				}
			}
		}
	}
}

// checkConcurrencyLimits 检查并发限制
func (jm *JobManager) checkConcurrencyLimits(event *types.Event) error {
	activeJobs := jm.engine.GetActiveJobs()

	// 检查事件最大并发数
	if event.MaxChildren > 0 {
		count := 0
		for _, job := range activeJobs {
			if job.Event == event.ID {
				count++
			}
		}
		if count >= event.MaxChildren {
			return fmt.Errorf("maximum of %d jobs already running for event: %s", event.MaxChildren, event.Title)
		}
	}

	// 检查分类最大并发数
	if event.Category != "" {
		cat, catExists := jm.engine.GetCategory(event.Category)
		if catExists && cat.MaxChildren > 0 {
			count := 0
			for _, job := range activeJobs {
				if job.Category == event.Category {
					count++
				}
			}
			if count >= cat.MaxChildren {
				return fmt.Errorf("maximum of %d jobs already running for category: %s", cat.MaxChildren, cat.Title)
			}
		}
	}

	// 检查全局最大任务数
	if jm.engine.config.MaxJobs > 0 && len(activeJobs) >= jm.engine.config.MaxJobs {
		return fmt.Errorf("global maximum of %d jobs already running", jm.engine.config.MaxJobs)
	}

	return nil
}

// chooseServers 选择服务器
func (jm *JobManager) chooseServers(event *types.Event) []string {
	// 如果目标是 "local" 或空，返回本地服务器
	if event.Target == "" || event.Target == "local" {
		return jm.availableServers([]string{"local"})
	}

	// 从存储加载服务器组
	var servers []string
	allServers := jm.engine.GetCommunication().GetAllServers()

	// 如果指定了服务器组，从存储加载并用正则过滤
	if event.Target != "" && event.Target != "local" {
		groups, err := jm.engine.LoadServerGroupsFromStorage()
		if err == nil {
			if group, exists := groups[event.Target]; exists {
				if group.Regexp != "" {
					re, reErr := regexp.Compile(group.Regexp)
					if reErr == nil {
						for hostname := range allServers {
							if !allServers[hostname].Disabled && re.MatchString(hostname) {
								servers = append(servers, hostname)
							}
						}
					}
				}

			}
		}
	}

	if len(servers) == 0 {
		if server, ok := allServers[event.Target]; ok && !server.Disabled {
			servers = []string{event.Target}
		}
	}
	if len(servers) == 0 {
		return nil
	}
	servers = jm.availableServers(servers)
	if len(servers) == 0 {
		return nil
	}
	sort.Strings(servers)

	// Multiplex: return all servers (one job per server)
	if event.Multiplex.Bool() {
		return servers
	}

	switch event.Algo {
	case "random":
		return []string{jm.chooseRandomServer(servers)}
	case "round_robin":
		jm.engine.mu.Lock()
		robin := jm.engine.state.Robins[event.ID]
		jm.engine.state.Robins[event.ID] = (robin + 1) % len(servers)
		selected := servers[robin%len(servers)]
		jm.engine.mu.Unlock()
		return []string{selected}
	case "least_busy":
		return []string{jm.chooseLeastBusyServer(servers, allServers)}
	case "least_cpu":
		return []string{jm.chooseLeastCPUServer(servers, allServers)}
	case "least_mem":
		return []string{jm.chooseLeastMemServer(servers, allServers)}
	case "prefer_first":
		sort.Strings(servers)
		return []string{servers[0]}
	case "prefer_last":
		sort.Strings(servers)
		return []string{servers[len(servers)-1]}
	default:
		if len(servers) == 1 {
			return servers
		}
		return []string{jm.chooseRandomServer(servers)}
	}
}

// chooseRandomServer 随机选择服务器
func (jm *JobManager) chooseRandomServer(servers []string) string {
	if len(servers) == 0 {
		return "local"
	}
	if len(servers) == 1 {
		return servers[0]
	}
	// 使用时间戳作为随机种子
	index := int(time.Now().UnixNano()) % len(servers)
	return servers[index]
}

// chooseLeastBusyServer 选择最不忙的服务器
func (jm *JobManager) chooseLeastBusyServer(servers []string, allServers map[string]*types.Server) string {
	if len(servers) == 0 {
		return "local"
	}

	// 计算每个服务器的忙碌程度（活动任务数）
	activeJobs := jm.engine.GetActiveJobs()
	serverBusyCount := make(map[string]int)

	for _, job := range activeJobs {
		if count, exists := serverBusyCount[job.Hostname]; exists {
			serverBusyCount[job.Hostname] = count + 1
		} else {
			serverBusyCount[job.Hostname] = 1
		}
	}

	// 找到最不忙的服务器
	leastBusy := servers[0]
	leastBusyCount := serverBusyCount[leastBusy]

	for _, server := range servers[1:] {
		count := serverBusyCount[server]
		if count < leastBusyCount {
			leastBusy = server
			leastBusyCount = count
		}
	}

	return leastBusy
}

// chooseLeastCPUServer 选择 CPU 使用最低的服务器
func (jm *JobManager) chooseLeastCPUServer(servers []string, allServers map[string]*types.Server) string {
	if len(servers) == 0 {
		return "local"
	}

	best := servers[0]
	bestCPU := float64(999999)

	for _, server := range servers {
		s, ok := allServers[server]
		if !ok {
			continue
		}
		cpu := float64(0)
		if s.Data != nil {
			if v, ok := s.Data["cpu"].(float64); ok {
				cpu = v
			}
		}
		if cpu < bestCPU {
			bestCPU = cpu
			best = server
		}
	}

	return best
}

// chooseLeastMemServer 选择内存使用最低的服务器
func (jm *JobManager) chooseLeastMemServer(servers []string, allServers map[string]*types.Server) string {
	if len(servers) == 0 {
		return "local"
	}

	best := servers[0]
	bestMem := float64(999999999999)

	for _, server := range servers {
		s, ok := allServers[server]
		if !ok {
			continue
		}
		mem := float64(0)
		if s.Data != nil {
			if v, ok := s.Data["mem"].(float64); ok {
				mem = v
			}
		}
		if mem < bestMem {
			bestMem = mem
			best = server
		}
	}

	return best
}

// queueJob 将任务加入队列
func (jm *JobManager) queueJob(event *types.Event, timestamp int64) error {
	// 检查队列大小限制
	if event.QueueMax > 0 {
		jm.engine.mu.RLock()
		queueLength := jm.engine.eventQueue[event.ID]
		jm.engine.mu.RUnlock()

		if queueLength >= event.QueueMax {
			return fmt.Errorf("event queue is full (max: %d)", event.QueueMax)
		}
	}

	queueItem := map[string]interface{}{
		"event_id":  event.ID,
		"timestamp": timestamp,
		"event":     types.CloneEvent(event),
	}

	// 持久化到存储队列
	queueKey := fmt.Sprintf("global/event_queue/%s", event.ID)
	ctx := context.Background()
	if err := jm.engine.storage.ListPush(ctx, queueKey, queueItem); err != nil {
		// 回退到文件队列
		task := &types.QueueTask{
			Action: "launch_job",
			When:   float64(timestamp),
			Data:   queueItem,
		}
		if err2 := jm.engine.queue.EnqueueExternal(task); err2 != nil {
			return fmt.Errorf("failed to enqueue job: %w", err2)
		}
	}

	// 更新队列长度
	jm.engine.mu.Lock()
	jm.engine.eventQueue[event.ID]++
	jm.engine.mu.Unlock()

	jm.logger.Info("Job queued", zap.String("event_id", event.ID), zap.Int64("timestamp", timestamp))
	return nil
}

// generateJobID 生成任务ID
func (jm *JobManager) generateJobID() string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return "j" + hex.EncodeToString(buf)
}

// parseCommand 解析命令
func (jm *JobManager) parseCommand(cmd string) (string, []string) {
	// 简单的命令解析（支持引号和转义）
	// 如果需要更复杂的解析，可以使用 shell-quote 库
	// 这里实现一个简化版本

	if cmd == "" {
		return "", nil
	}

	// 简单的空格分割，但保留引号内的内容
	var parts []string
	var current strings.Builder
	inQuotes := false
	quoteChar := byte(0)

	for i := 0; i < len(cmd); i++ {
		char := cmd[i]

		if char == '\\' && i+1 < len(cmd) {
			// 转义字符
			current.WriteByte(cmd[i+1])
			i++
			continue
		}

		if (char == '"' || char == '\'') && !inQuotes {
			// 开始引号
			inQuotes = true
			quoteChar = char
			continue
		}

		if char == quoteChar && inQuotes {
			// 结束引号
			inQuotes = false
			quoteChar = 0
			continue
		}

		if char == ' ' && !inQuotes {
			// 空格分隔符
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
			continue
		}

		current.WriteByte(char)
	}

	// 添加最后一部分
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	if len(parts) == 0 {
		return cmd, nil
	}

	return parts[0], parts[1:]
}

// buildEnv 构建环境变量
func (jm *JobManager) buildEnv(job *types.Job, event *types.Event) []string {
	env := os.Environ()
	for k, v := range job.Env {
		env = append(env, k+"="+v)
	}

	// 添加任务相关环境变量
	env = append(env, fmt.Sprintf("JOB_ID=%s", job.ID))
	env = append(env, fmt.Sprintf("JOB_LOG=%s", job.LogFile))
	env = append(env, fmt.Sprintf("JOB_NOW=%d", job.Now))
	env = append(env, "OWLCRON_JOB_TOKEN="+job.ProcessToken)

	// 添加任务参数
	for k, v := range job.Params {
		if k == "JOB_ID" || k == "JOB_LOG" || k == "JOB_NOW" || k == "OWLCRON_JOB_TOKEN" || strings.Contains(k, "=") {
			continue
		}
		env = append(env, fmt.Sprintf("%s=%v", k, v))
	}

	return env
}

// createLogFile 创建日志文件
func (jm *JobManager) createLogFile(job *types.Job) error {
	dir := filepath.Dir(job.LogFile)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}

	file, err := os.OpenFile(job.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}

	// 写入日志头
	header := fmt.Sprintf("# Job ID: %s\n# Event Title: %s\n# Hostname: %s\n# Date/Time: %s\n\n",
		job.ID, job.EventTitle, job.Hostname, time.Now().Format(time.RFC3339))
	writer, err := newBoundedLogWriter(file, jm.jobLogLimit(job), nil)
	if err != nil {
		_ = file.Close()
		return err
	}
	_, err = writer.Write([]byte(header))
	return errors.Join(err, file.Close())
}

// createLogWriter 创建日志写入器
func (jm *JobManager) createLogWriter(logFile string) *os.File {
	file, err := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		jm.logger.Error("Failed to open log file", zap.Error(err))
		return nil
	}
	return file
}

// getHostname 获取主机名
func (jm *JobManager) getHostname() string {
	return jm.engine.Hostname()
}

func (jm *JobManager) Wait() { jm.workers.Wait() }

func eventArray(events map[string]*types.Event) []*types.Event {
	out := make([]*types.Event, 0, len(events))
	for _, event := range events {
		out = append(out, event)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func mergeRuntimeJob(job, current *types.Job) {
	job.UpdateVersion = current.UpdateVersion
	job.EventTitle = current.EventTitle
	job.Timeout = current.Timeout
	job.Retries = current.Retries
	job.RetryDelay = current.RetryDelay
	job.Chain = current.Chain
	job.ChainError = current.ChainError
	job.NotifySuccess = current.NotifySuccess
	job.NotifyFail = current.NotifyFail
	job.WebHook = current.WebHook
	job.MemoryLimit = current.MemoryLimit
	job.MemorySustain = current.MemorySustain
	job.CPULimit = current.CPULimit
	job.CPUSustain = current.CPUSustain
	job.LogMaxSize = current.LogMaxSize
}
