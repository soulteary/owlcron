package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

// Engine 是 OwlCron 的核心引擎
type Engine struct {
	clusterTLS    *clusterTLS
	nodeAuth      *nodeAuthConfig
	webHookSigner *webHookSigner
	// 配置
	config *types.Config
	logger *zap.Logger

	// 状态
	activeJobs map[string]*types.Job
	deadJobs   map[string]*types.Job
	eventQueue map[string]int           // 事件ID -> 队列长度
	kids       map[int]*types.JobWorker // PID -> Worker
	state      *types.State
	multi      *types.MultiServer

	// 组件
	scheduler           *Scheduler
	jobManager          *JobManager
	queue               *Queue
	comm                *Communication
	discovery           *Discovery
	storage             storage.Storage
	notifications       *NotificationQueue
	notificationRunning bool
	notificationSpoolMu sync.Mutex

	// 互斥锁
	mu                        sync.RWMutex
	roleMu                    sync.Mutex
	lifecycleMu               sync.Mutex
	started                   bool
	ready                     atomic.Bool
	draining                  atomic.Bool
	readinessMu               sync.Mutex
	readinessProbe            *readinessProbe
	startedAt                 int64
	starting                  bool
	stateOwner                bool
	restartRequested          bool
	leaseMu                   sync.Mutex
	leaseToken                string
	leaseUntil                int64
	statePredecessors         []string
	shutdownOnce              sync.Once
	shutdownErr               error
	loopWG                    sync.WaitGroup
	failoverMu                sync.Mutex
	stateSaveMu               sync.Mutex
	historyMigrationMu        sync.Mutex
	historyMigrated           bool
	maintenanceMu             sync.Mutex
	maintenanceLaunchMu       sync.Mutex
	maintenanceRunning        bool
	maintenanceStopping       bool
	completionRecoveryRunning bool
	deadJobRetentionCursor    string
	lastMaintenance           time.Time
	localLogCursor            string

	// 上下文
	ctx    context.Context
	cancel context.CancelFunc

	// 通道
	jobChan      chan *types.Job
	eventChan    chan *types.Event
	shutdownChan chan struct{}
}

// NewEngine 创建新的引擎实例
func NewEngine(config *types.Config, logger *zap.Logger) (*Engine, error) {
	if config == nil {
		return nil, fmt.Errorf("configuration is required")
	}
	if err := types.ValidateHTTPConfig(config); err != nil {
		return nil, err
	}
	if config.JobLogHardMaxSize < 0 || config.JobLogHardMaxSize > 1<<30 || config.LogUploadConcurrency < 0 || config.LogUploadConcurrency > 64 {
		return nil, fmt.Errorf("invalid log hard limit or upload concurrency")
	}
	if config.SMTPPort < 0 || config.SMTPPort > 65535 || config.SMTPTimeoutSeconds < 0 || config.SMTPTimeoutSeconds > 300 || config.MaxEmailsPerDay < 0 {
		return nil, fmt.Errorf("invalid SMTP port, timeout or daily limit")
	}
	switch config.SMTPTLSMode {
	case "", "starttls", "implicit", "none":
	default:
		return nil, fmt.Errorf("invalid SMTP TLS mode")
	}
	if config.SMTPTLSMode == "none" && (config.SMTPUsername != "" || config.SMTPPassword != "") {
		return nil, fmt.Errorf("SMTP authentication requires TLS")
	}
	if (config.SMTPUsername == "") != (config.SMTPPassword == "") {
		return nil, fmt.Errorf("SMTP username and password must be configured together")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if config.LogDir == "" {
		config.LogDir = "logs"
	}
	if config.QueueDir == "" {
		config.QueueDir = "queue"
	}
	if config.MasterPingFreq <= 0 {
		config.MasterPingFreq = 20
	}
	if config.MasterPingTimeout <= 0 {
		config.MasterPingTimeout = 80
	}
	if config.HTTPPort <= 0 {
		config.HTTPPort = 3012
	}
	if err := validateWebHookPolicy(config); err != nil {
		return nil, err
	}
	clusterSecurity, err := initClusterTLS(config)
	if err != nil {
		return nil, err
	}
	nodeSecurity, err := loadNodeAuthConfig(config)
	if err != nil {
		return nil, err
	}
	signer, err := loadWebHookSigner(config)
	if err != nil {
		return nil, err
	}
	leaseToken, err := newLeadershipToken()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())

	engine := &Engine{
		clusterTLS: clusterSecurity, nodeAuth: nodeSecurity,
		config:        config,
		webHookSigner: signer,
		leaseToken:    leaseToken,
		logger:        logger,
		activeJobs:    make(map[string]*types.Job),
		deadJobs:      make(map[string]*types.Job),
		eventQueue:    make(map[string]int),
		kids:          make(map[int]*types.JobWorker),
		state:         &types.State{Enabled: true, Cursors: make(map[string]int64), Stats: make(map[string]float64), FlaggedJobs: make(map[string]map[string]int64), Robins: make(map[string]int), JobCodes: make(map[string]int)},
		multi:         &types.MultiServer{Data: make(map[string]interface{})},
		ctx:           ctx,
		cancel:        cancel,
		jobChan:       make(chan *types.Job, 100),
		eventChan:     make(chan *types.Event, 100),
		shutdownChan:  make(chan struct{}),
	}

	// 初始化组件
	engine.scheduler, err = NewScheduler(engine, logger)
	if err != nil {
		return nil, err
	}

	engine.jobManager, err = NewJobManager(engine, logger)
	if err != nil {
		return nil, err
	}

	engine.queue, err = NewQueue(engine, logger)
	if err != nil {
		return nil, err
	}

	engine.comm, err = NewCommunication(engine, logger)
	if err != nil {
		return nil, err
	}

	engine.discovery, err = NewDiscovery(engine, logger)
	if err != nil {
		return nil, err
	}

	// 初始化存储系统
	storageEngine := config.StorageEngine
	if storageEngine == "" {
		storageEngine = "Filesystem"
	}
	storageBaseDir := config.StorageBaseDir
	if storageBaseDir == "" {
		storageBaseDir = "data"
	}
	storageConfig := &storage.StorageConfig{
		Engine:  storageEngine,
		BaseDir: storageBaseDir,
		Options: config.StorageOptions,
	}
	engine.storage, err = storage.NewStorage(storageConfig)
	if err != nil {
		return nil, err
	}

	engine.notifications, err = NewNotificationQueue(engine.storage, NotificationQueueConfig{
		Workers: config.NotificationWorkers, MaxAttempts: config.NotificationMaxAttempts,
		DeliveryTimeout: time.Duration(max(config.SMTPTimeoutSeconds, 30)) * time.Second,
		LeaseDuration:   time.Duration(max(config.SMTPTimeoutSeconds, 30)+30) * time.Second,
	}, engine.deliverNotification)
	if err != nil {
		cancel()
		return nil, err
	}
	engine.notifications.redactError = engine.RedactSensitiveText
	return engine, nil
}

// Startup 启动引擎
func (e *Engine) Startup() error {
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	if e.ctx.Err() != nil {
		return fmt.Errorf("engine has shut down")
	}
	if e.started {
		return nil
	}
	e.mu.Lock()
	e.starting = true
	e.mu.Unlock()
	defer func() { e.mu.Lock(); e.starting = false; e.mu.Unlock() }()
	if err := e.createDirectories(); err != nil {
		return err
	}
	if err := e.loadStateFromStorage(); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	e.mu.Lock()
	e.startedAt = time.Now().Unix()
	e.state.Stats["started"] = float64(e.startedAt)
	e.mu.Unlock()
	if err := e.ensureDefaultAdmin(); err != nil {
		return err
	}
	if err := e.ensureHistoryMigrated(e.ctx); err != nil {
		return fmt.Errorf("migrate history: %w", err)
	}
	if err := e.RetryHistoryDeletions(e.ctx); err != nil {
		return fmt.Errorf("recover history cleanup: %w", err)
	}
	if err := e.initializeReadiness(); err != nil {
		return fmt.Errorf("initialize readiness marker: %w", err)
	}
	if err := e.comm.Startup(); err != nil {
		return err
	}
	if err := e.discovery.Startup(); err != nil {
		return err
	}
	if err := e.checkMasterEligibility(); err != nil {
		return err
	}
	if e.GetMulti().Eligible {
		servers, err := e.comm.loadServerList()
		if err != nil {
			e.mu.Lock()
			e.multi.Eligible = false
			e.mu.Unlock()
			return fmt.Errorf("reload server roster for startup: %w", err)
		}
		if len(servers) == 0 {
			e.goMaster()
		} else {
			// Give preferred eligible hosts the opportunity to lead first.
			e.masterFailover()
		}
	}
	if err := e.jobManager.RecoverJobs(); err != nil {
		return err
	}
	if err := e.queue.Startup(); err != nil {
		return err
	}
	e.mu.Lock()
	e.starting = false
	e.mu.Unlock()
	e.started = true
	e.ready.Store(true)
	e.loopWG.Add(1)
	go func() { defer e.loopWG.Done(); e.mainLoop() }()
	return nil
}

func (e *Engine) Shutdown() error {
	e.draining.Store(true)
	e.ready.Store(false)
	e.shutdownOnce.Do(func() {
		e.lifecycleMu.Lock()
		defer e.lifecycleMu.Unlock()
		e.mu.Lock()
		e.multi.Master = false
		e.multi.Slave = false
		e.mu.Unlock()
		e.maintenanceLaunchMu.Lock()
		e.maintenanceStopping = true
		e.cancel()
		e.maintenanceLaunchMu.Unlock()
		e.abortAllLocalJobs()
		if e.discovery != nil {
			e.discovery.Shutdown()
		}
		if e.scheduler != nil {
			e.scheduler.Shutdown()
		}
		if e.queue != nil {
			e.queue.Shutdown()
		}
		e.waitForJobs()
		e.jobManager.Wait()
		if e.comm != nil {
			e.comm.Shutdown()
		}
		e.loopWG.Wait()
		if e.jobManager != nil && e.jobManager.webHookManager != nil {
			e.jobManager.webHookManager.client.CloseIdleConnections()
		}
		if err := e.saveStateToStorage(); err != nil {
			e.logger.Error("Failed to save final state", zap.Error(err))
			if !errors.Is(err, errStaleStateOwner) {
				e.shutdownErr = errors.Join(e.shutdownErr, fmt.Errorf("save final state: %w", err))
			}
		}
		if err := e.releaseLeadership(); err != nil {
			e.shutdownErr = errors.Join(e.shutdownErr, fmt.Errorf("release leadership: %w", err))
		}
		if closer, ok := e.storage.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				e.shutdownErr = errors.Join(e.shutdownErr, fmt.Errorf("close storage: %w", err))
			}
		}
		close(e.shutdownChan)
	})
	return e.shutdownErr
}

// loadStateFromStorage 从存储加载全局状态
func (e *Engine) loadStateFromStorage() error {
	if e.storage == nil {
		return fmt.Errorf("storage not initialized")
	}
	ctx := context.Background()
	data, err := e.storage.Get(ctx, "global/state")
	if err != nil {
		return err
	}
	var state types.State
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if state.Cursors == nil {
		state.Cursors = make(map[string]int64)
	}
	if state.Stats == nil {
		state.Stats = make(map[string]float64)
	}
	if state.FlaggedJobs == nil {
		state.FlaggedJobs = make(map[string]map[string]int64)
	}
	if state.Robins == nil {
		state.Robins = make(map[string]int)
	}
	if state.JobCodes == nil {
		state.JobCodes = make(map[string]int)
	}
	e.mu.Lock()
	if e.startedAt > 0 {
		state.Stats["started"] = float64(e.startedAt)
	}
	e.state = &state
	e.mu.Unlock()
	e.logger.Info("Loaded global state from storage", zap.Int("cursors", len(state.Cursors)))
	return nil
}

// saveStateToStorage 保存全局状态到存储
func (e *Engine) saveStateToStorage() error {
	e.stateSaveMu.Lock()
	defer e.stateSaveMu.Unlock()
	if e.storage == nil {
		return fmt.Errorf("storage not initialized")
	}
	e.mu.RLock()
	ownsState := e.stateOwner || e.multi.Master
	data, err := json.Marshal(e.state)
	e.mu.RUnlock()
	if !ownsState {
		return nil
	}
	owned, ownershipErr := e.ownsStoredLeadership()
	if ownershipErr != nil {
		return fmt.Errorf("verify state ownership: %w", ownershipErr)
	}
	if !owned {
		return nil
	}
	if err != nil {
		return err
	}
	atomic, ok := e.storage.(storage.AtomicStorage)
	if !ok {
		return fmt.Errorf("global state requires atomic storage updates")
	}
	return atomic.Update(context.Background(), "global/state", func(previous []byte) ([]byte, error) {
		var ownership struct {
			Owner string `json:"_lease_owner"`
		}
		if len(previous) > 0 {
			if err := json.Unmarshal(previous, &ownership); err != nil {
				return nil, err
			}
			if ownership.Owner != "" && ownership.Owner != e.leaseToken {
				return nil, errStaleStateOwner
			}
		}
		var next types.State
		if err := json.Unmarshal(data, &next); err != nil {
			return nil, err
		}
		if len(previous) > 0 {
			var stored types.State
			if err := json.Unmarshal(previous, &stored); err != nil {
				return nil, err
			}
			// A demoting master may finish saving just after a takeover. It must never
			// regress another master's already committed schedule cursors.
			for id, cursor := range stored.Cursors {
				if value, present := next.Cursors[id]; present && value < cursor {
					next.Cursors[id] = cursor
				}
			}
		}
		return encodeOwnedState(&next, e.leaseToken)
	})
}

// ShutdownChan 返回关闭通道
func (e *Engine) ShutdownChan() <-chan struct{} {
	return e.shutdownChan
}

// createDirectories 创建必要的目录
func (e *Engine) createDirectories() error {
	dirs := []string{
		filepath.Join(e.config.LogDir, "jobs"),
		e.config.QueueDir,
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
	}

	return nil
}

// checkMasterEligibility 基于 servers 列表和 server_groups 的 master 正则判定资格
func (e *Engine) checkMasterEligibility() error {
	hostname := e.Hostname()
	servers, err := e.comm.loadServerList()
	if err != nil {
		e.mu.Lock()
		e.multi.Eligible = false
		e.mu.Unlock()
		return fmt.Errorf("load server roster: %w", err)
	}
	eligible := false
	cluster := len(servers) > 0
	if len(servers) > 1 && (e.config.StorageEngine == "" || e.config.StorageEngine == "Filesystem") && !e.config.SharedStorage {
		return fmt.Errorf("multiple servers require shared_storage and a common atomic storage namespace")
	}
	if len(servers) == 0 {
		eligible = true
	} else {
		groups, groupErr := e.LoadServerGroupsFromStorage()
		if groupErr != nil {
			e.mu.Lock()
			e.multi.Eligible = false
			e.mu.Unlock()
			return groupErr
		}
		eligible = e.buildMasterCandidates(servers, groups)[hostname]
	}
	e.mu.Lock()
	e.multi.Eligible = eligible
	e.multi.Cluster = cluster
	// Nonmasters need a timeout reference even before their first heartbeat.
	e.multi.LastPingReceived = time.Now().Unix()
	e.mu.Unlock()
	return nil
}

// mainLoop 主循环
func (e *Engine) mainLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return

		case <-ticker.C:
			e.tick()

		case job := <-e.jobChan:
			e.handleJob(job)

		case event := <-e.eventChan:
			e.handleEvent(event)
		}
	}
}

// tick 每秒执行
func (e *Engine) tick() {
	e.runCompletionRecovery()
	e.runNotifications()
	now := time.Now().Unix()
	e.runHistoryMaintenanceIfDue(time.Unix(now, 0))

	e.mu.RLock()
	isMaster := e.multi.Master
	isSlave := e.multi.Slave
	isEligible := e.multi.Eligible
	lastPingSent := e.multi.LastPingSent
	lastPingRecv := e.multi.LastPingReceived
	hasJobs := len(e.activeJobs) > 0
	e.mu.RUnlock()

	if isMaster && !e.EnsureLeadership() {
		isMaster = false
	}
	if isMaster {
		e.resetDailyStats(time.Unix(now, 0))
		e.comm.ExpireDisconnectedJobs(time.Unix(now, 0))
		if now-lastPingSent >= int64(e.config.MasterPingFreq) {
			e.comm.SendMasterPings()
			e.mu.Lock()
			e.multi.LastPingSent = now
			e.mu.Unlock()
		}
	} else {
		timeout := int64(e.config.MasterPingTimeout)
		if timeout <= 0 {
			timeout = 80
		}
		if lastPingRecv > 0 && now-lastPingRecv >= timeout {
			if isEligible {
				e.masterFailover()
			} else if isSlave {
				e.slaveFailover()
			}
		}
	}

	if isMaster || hasJobs {
		e.jobManager.MonitorServerResources()
	}
	if hasJobs {
		e.jobManager.MonitorAllActiveJobs()
	}

	e.broadcastStatus()
	if e.IsMaster() {
		e.comm.BroadcastToSockets("update", map[string]interface{}{"state": e.GetState(), "eventQueue": e.GetEventQueues()})
	}
}

// socketStatus is shared by authentication and periodic updates so a browser
// receives the complete status contract from its first authenticated message.
func (e *Engine) socketStatus() map[string]interface{} {
	e.mu.RLock()
	activeJobsCopy := make(map[string]*types.Job, len(e.activeJobs))
	for k, v := range e.activeJobs {
		activeJobsCopy[k] = types.CloneJob(v)
	}
	queueCopy := make(map[string]int, len(e.eventQueue))
	for k, v := range e.eventQueue {
		queueCopy[k] = v
	}
	statsCopy := make(map[string]float64, len(e.state.Stats))
	for key, value := range e.state.Stats {
		statsCopy[key] = value
	}
	uptime := int64(0)
	if started := e.state.Stats["started"]; started > 0 {
		uptime = time.Now().Unix() - int64(started)
		if uptime < 0 {
			uptime = 0
		}
	}
	status := map[string]interface{}{
		"version":         "0.9.102",
		"hostname":        e.Hostname(),
		"epoch":           time.Now().Unix(),
		"master":          e.multi.Master,
		"master_hostname": e.multi.MasterHostname,
		"enabled":         e.state.Enabled,
		"stats":           statsCopy,
		"active_jobs":     activeJobsCopy,
		"queue":           queueCopy,
		"data":            cloneData(e.multi.Data),
		"uptime":          uptime,
	}
	e.mu.RUnlock()
	status["servers"] = e.comm.GetAllServers()
	return status
}

// broadcastStatus 向已连接的 WebSocket 客户端广播状态
func (e *Engine) broadcastStatus() {
	status := e.socketStatus()
	e.comm.BroadcastToSockets("status", status)
	if !e.IsMaster() {
		e.comm.SendToMaster("status", status)
	}
}

// handleJob 处理任务
func (e *Engine) handleJob(job *types.Job) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.activeJobs[job.ID] = types.CloneJob(job)
}

// handleEvent 处理事件更新并广播到 WebSocket 客户端
func (e *Engine) handleEvent(event *types.Event) {
	e.scheduler.mu.Lock()
	e.scheduler.events[event.ID] = types.CloneEvent(event)
	e.scheduler.mu.Unlock()

	if e.comm != nil && e.comm.wsHandler != nil {
		e.comm.wsHandler.BroadcastEventUpdate(event)
	}
}

// masterFailover 主服务器故障转移（基于 hostname 字典序排名 + HTTP 探活）
func (e *Engine) masterFailover() {
	if !e.failoverMu.TryLock() {
		return
	}
	defer e.failoverMu.Unlock()
	if e.ctx.Err() != nil || e.IsMaster() {
		return
	}
	e.mu.Lock()
	e.multi.LastPingReceived = time.Now().Unix()
	e.mu.Unlock()

	e.logger.Info("No master ping received, initiating failover")

	servers, err := e.comm.loadServerList()
	if err != nil || len(servers) == 0 {
		e.logger.Warn("Cannot load server list for failover, going idle")
		e.goIdle()
		return
	}

	groups, err := e.LoadServerGroupsFromStorage()
	if err != nil {
		e.logger.Warn("Cannot load server groups for failover, going idle")
		e.goIdle()
		return
	}

	candidates := e.buildMasterCandidates(servers, groups)
	hostname := e.Hostname()
	if _, ok := candidates[hostname]; !ok {
		e.logger.Info("Not a master candidate, going idle")
		e.goIdle()
		return
	}

	sorted := sortedHostnames(candidates)
	rank := -1
	for i, h := range sorted {
		if h == hostname {
			rank = i
			break
		}
	}

	if rank == 0 {
		e.goMaster()
		return
	}

	superiors := sorted[:rank]
	alive := e.probeServers(superiors)

	if len(alive) == 0 {
		e.logger.Info("No superior candidates alive, becoming master")
		e.goMaster()
	} else {
		e.logger.Info("Superior candidates still alive, staying as slave", zap.Strings("alive", alive))
	}
}

// buildMasterCandidates 构建主节点候选列表
func (e *Engine) buildMasterCandidates(servers []string, groups map[string]*types.ServerGroup) map[string]bool {
	candidates := make(map[string]bool)
	for _, g := range groups {
		if !g.Master || g.Regexp == "" {
			continue
		}
		re, err := regexp.Compile(g.Regexp)
		if err != nil {
			continue
		}
		for _, h := range servers {
			if re.MatchString(h) {
				candidates[h] = true
			}
		}
	}
	return candidates
}

// Candidate liveness is authoritative only when backed by the shared lease.
// An anonymous HTTP status response must never veto a legitimate takeover.
func (e *Engine) probeServers(hostnames []string) []string {
	if e.storage == nil {
		return nil
	}
	raw, err := e.storage.Get(e.ctx, "global/master_lease")
	if err != nil {
		return nil
	}
	var lease leadershipLease
	if json.Unmarshal(raw, &lease) != nil || lease.Owner == "" || lease.Expires <= time.Now().UnixNano() {
		return nil
	}
	for _, host := range hostnames {
		if lease.Hostname == host {
			return []string{host}
		}
	}
	return nil
}
func (e *Engine) probeServer(hostname string) bool {
	return len(e.probeServers([]string{hostname})) > 0
}

// goIdle 进入空闲状态
func (e *Engine) goIdle() {
	e.roleMu.Lock()
	defer e.roleMu.Unlock()
	e.goIdleLocked()
}

// goIdleLocked requires roleMu, including callers that recheck a timeout after
// waiting for a concurrent authenticated master frame to refresh liveness.
func (e *Engine) goIdleLocked() {
	e.mu.Lock()
	e.multi.MasterHostname = ""
	e.multi.Master = false
	e.multi.Slave = false
	e.multi.Cluster = false
	e.mu.Unlock()
	e.scheduler.Shutdown()
	e.mu.Lock()
	e.stateOwner = false
	e.mu.Unlock()
	e.releaseLeadership()
	e.comm.ResetCluster()
}

func (e *Engine) slaveFailover() {
	e.roleMu.Lock()
	defer e.roleMu.Unlock()
	timeout := int64(e.config.MasterPingTimeout)
	if timeout <= 0 {
		timeout = 80
	}
	e.mu.RLock()
	stale := !e.multi.Master && e.multi.Slave && e.multi.LastPingReceived > 0 && time.Now().Unix()-e.multi.LastPingReceived >= timeout
	e.mu.RUnlock()
	if stale {
		e.goIdleLocked()
	}
}

func (e *Engine) goMaster() {
	e.roleMu.Lock()
	defer e.roleMu.Unlock()
	if e.ctx.Err() != nil || e.IsMaster() {
		return
	}
	if err := e.claimLeadership(); err != nil {
		e.logger.Debug("Leadership unavailable", zap.Error(err))
		return
	}
	if err := e.claimStateOwnership(); err != nil {
		e.logger.Error("Cannot fence leadership state", zap.Error(err))
		e.releaseLeadership()
		return
	}
	if err := e.loadStateFromStorage(); err != nil && !errors.Is(err, storage.ErrNotFound) {
		e.logger.Error("Cannot recover leadership state", zap.Error(err))
		e.releaseLeadership()
		return
	}
	e.mu.Lock()
	e.stateOwner = true
	e.multi.Master = true
	e.multi.Slave = false
	e.multi.Cluster = true
	e.multi.MasterHostname = e.Hostname()
	e.multi.LastPingSent = time.Now().Unix()
	e.mu.Unlock()
	if err := e.jobManager.RestoreRemoteJobs(); err != nil {
		e.logger.Error("Cannot recover remote reservations", zap.Error(err))
		e.mu.Lock()
		e.multi.Master = false
		e.stateOwner = false
		e.mu.Unlock()
		e.releaseLeadership()
		return
	}
	if err := e.scheduler.Startup(); err != nil {
		e.logger.Error("Failed to start scheduler", zap.Error(err))
		e.mu.Lock()
		e.multi.Master = false
		e.stateOwner = false
		e.mu.Unlock()
		e.releaseLeadership()
		return
	}
	e.comm.SetupCluster()
}

func (e *Engine) goSlave() {
	e.roleMu.Lock()
	defer e.roleMu.Unlock()
	e.mu.Lock()
	e.multi.Master = false
	e.multi.Slave = true
	e.multi.Cluster = true
	e.mu.Unlock()
	e.scheduler.Shutdown()
	e.mu.Lock()
	e.stateOwner = false
	e.mu.Unlock()
	e.releaseLeadership()
	e.comm.ResetCluster()
}

// AcceptMasterPing is called only after the WebSocket peer has proved its node
// identity. Lexical ordering resolves simultaneous masters deterministically.
func (e *Engine) AcceptMasterPing(hostname string, epoch int64) bool {
	if hostname == "" || hostname == e.Hostname() || e.ctx.Err() != nil {
		return false
	}
	authoritative := false
	if e.storage != nil {
		raw, err := e.storage.Get(e.ctx, "global/master_lease")
		var lease leadershipLease
		authoritative = err == nil && json.Unmarshal(raw, &lease) == nil && lease.Hostname == hostname && lease.Owner != "" && lease.Expires > time.Now().UnixNano()
	}
	if !authoritative && !e.config.ClusterAllowInsecure {
		return false
	}

	e.roleMu.Lock()
	defer e.roleMu.Unlock()
	e.mu.Lock()
	if !authoritative && e.multi.Master && e.Hostname() < hostname {
		e.mu.Unlock()
		return false
	}
	if !authoritative && !e.multi.Master && e.multi.MasterHostname != "" && e.multi.MasterHostname < hostname &&
		time.Now().Unix()-e.multi.LastPingReceived < int64(e.config.MasterPingTimeout) {
		e.mu.Unlock()
		return false
	}
	e.multi.Master = false
	e.multi.Slave = true
	e.multi.Cluster = true
	e.multi.MasterHostname = hostname
	e.multi.LastPingReceived = time.Now().Unix()
	e.mu.Unlock()
	e.scheduler.Shutdown()
	e.mu.Lock()
	e.stateOwner = false
	e.mu.Unlock()
	e.releaseLeadership()
	e.comm.ResetCluster()
	return true
}

// abortAllLocalJobs 中止所有本地任务
func (e *Engine) abortAllLocalJobs() {
	for id, job := range e.GetActiveJobs() {
		if !job.Detached && (job.Hostname == "local" || job.Hostname == e.Hostname()) {
			_ = e.jobManager.AbortJob(id, "Shutting down server")
		}
	}
}

// waitForJobs 等待所有任务完成
func (e *Engine) waitForJobs() {
	for {
		e.mu.RLock()
		count := 0
		for _, job := range e.activeJobs {
			if !job.Detached && (job.Hostname == "local" || job.Hostname == e.Hostname()) {
				count++
			}
		}
		e.mu.RUnlock()

		if count == 0 {
			break
		}

		time.Sleep(250 * time.Millisecond)
	}
}

// GetActiveJobs 获取所有活动任务
func (e *Engine) GetActiveJobs() map[string]*types.Job {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make(map[string]*types.Job)
	for k, v := range e.activeJobs {
		result[k] = types.CloneJob(v)
	}
	return result
}

// GetState 获取状态
func (e *Engine) GetState() *types.State {
	e.mu.RLock()
	defer e.mu.RUnlock()
	data, _ := json.Marshal(e.state)
	var copy types.State
	_ = json.Unmarshal(data, &copy)
	return &copy
}

// GetMulti 获取多服务器状态
func (e *Engine) GetMulti() *types.MultiServer {
	e.mu.RLock()
	defer e.mu.RUnlock()
	copy := *e.multi
	copy.Data = cloneData(e.multi.Data)
	return &copy
}

// IsMaster 是否为主服务器
func (e *Engine) IsMaster() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	copy := *e.multi
	copy.Data = cloneData(e.multi.Data)
	return copy.Master && (e.leaseUntil == 0 || e.leaseUntil > time.Now().UnixNano())
}

// sortedHostnames 返回字典序排序的 hostname 列表
func sortedHostnames(m map[string]bool) []string {
	result := make([]string, 0, len(m))
	for h := range m {
		result = append(result, h)
	}
	sort.Strings(result)
	return result
}

// getHostname 获取主机名
func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

// GetStorage 获取存储实例
func (e *Engine) GetStorage() storage.Storage {
	return e.storage
}

// GetConfig 获取配置
func (e *Engine) GetConfig() *types.Config {
	return e.config
}

// GetScheduler 获取调度器实例
func (e *Engine) GetScheduler() *Scheduler {
	return e.scheduler
}

// GetJobManager 获取任务管理器实例
func (e *Engine) GetJobManager() *JobManager {
	return e.jobManager
}

// GetCommunication 获取通信管理器实例
func (e *Engine) GetCommunication() *Communication {
	return e.comm
}

// GetCategory 获取分类
func (e *Engine) GetCategory(categoryID string) (*types.Category, bool) {
	categories, err := e.LoadCategoriesFromStorage()
	if err != nil {
		return nil, false
	}
	category, exists := categories[categoryID]
	return category, exists
}

// Hostname permits independently addressable nodes on one host.
func (e *Engine) Hostname() string {
	if e.config.Hostname != "" {
		return e.config.Hostname
	}
	return getHostname()
}

func cloneData(data map[string]interface{}) map[string]interface{} {
	raw, _ := json.Marshal(data)
	var copy map[string]interface{}
	_ = json.Unmarshal(raw, &copy)
	return copy
}
