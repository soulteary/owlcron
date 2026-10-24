package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

// Queue 队列管理器
type Queue struct {
	engine *Engine
	logger *zap.Logger
	mu     sync.RWMutex

	// 内部队列（内存）
	internalQueue map[string]*types.QueueTask

	// 定时器
	internalTicker *time.Ticker
	externalTicker *time.Ticker

	started     bool
	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewQueue 创建新的队列管理器
func NewQueue(engine *Engine, logger *zap.Logger) (*Queue, error) {
	return &Queue{
		engine:        engine,
		logger:        logger,
		internalQueue: make(map[string]*types.QueueTask),
	}, nil
}

// Startup 启动队列管理器
func (q *Queue) Startup() error {
	q.lifecycleMu.Lock()
	defer q.lifecycleMu.Unlock()
	if q.started {
		return nil
	}
	ctx, cancel := context.WithCancel(q.engine.ctx)
	q.cancel = cancel
	q.internalTicker = time.NewTicker(time.Second)
	q.externalTicker = time.NewTicker(time.Minute)
	q.started = true
	q.wg.Add(2)
	go q.monitorQueue(ctx, q.internalTicker, q.processInternalQueue)
	go q.monitorQueue(ctx, q.externalTicker, q.processExternalQueue)
	q.processExternalQueue()
	return nil
}

func (q *Queue) Shutdown() {
	q.lifecycleMu.Lock()
	defer q.lifecycleMu.Unlock()
	if q.started {
		q.cancel()
		q.internalTicker.Stop()
		q.externalTicker.Stop()
		q.wg.Wait()
		q.started = false
	}
	q.saveInternalQueue()
}

func (q *Queue) monitorQueue(ctx context.Context, ticker *time.Ticker, process func()) {
	defer q.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			process()
		}
	}
}

// processInternalQueue 处理内部队列
func (q *Queue) processInternalQueue() {
	q.mu.Lock()
	now := float64(time.Now().Unix())
	ready := make([]*types.QueueTask, 0)
	for key, task := range q.internalQueue {
		if task.When <= 0 || now >= task.When {
			ready = append(ready, task)
			delete(q.internalQueue, key)
		}
	}
	q.mu.Unlock()
	// Execution may re-enqueue a delayed retry. Never call it under q.mu.
	for _, task := range ready {
		q.executeTask(task)
	}
}

// processExternalQueue 处理外部队列
func (q *Queue) processExternalQueue() {
	queueDir := q.engine.config.QueueDir
	pattern := filepath.Join(queueDir, "*.json")

	matches, err := filepath.Glob(pattern)
	if err != nil {
		q.logger.Error("Failed to glob queue files", zap.Error(err))
		return
	}

	for _, file := range matches {
		q.processQueueFile(file)
	}
}

// processQueueFile 处理队列文件
func (q *Queue) processQueueFile(file string) {
	data, err := os.ReadFile(file)
	if err != nil {
		q.logger.Error("Failed to read queue file", zap.String("file", file), zap.Error(err))
		return
	}

	var task types.QueueTask
	if err := json.Unmarshal(data, &task); err != nil {
		q.logger.Error("Failed to parse queue file", zap.String("file", file), zap.Error(err))
		os.Remove(file)
		return
	}

	// Future tasks remain durable on disk until due, including across restart.
	if task.When > float64(time.Now().Unix()) {
		return
	}
	q.executeTask(&task)
	_ = os.Remove(file)
}

// executeTask 执行任务
func (q *Queue) executeTask(task *types.QueueTask) {
	q.logger.Debug("Executing queue task", zap.String("action", task.Action))

	switch task.Action {
	case "launch_job":
		eventID, _ := task.Data["event_id"].(string)
		whenFloat, _ := task.Data["timestamp"].(float64)
		if whenFloat == 0 {
			if ts, ok := task.Data["timestamp"].(int64); ok {
				whenFloat = float64(ts)
			}
		}
		if eventID == "" {
			q.logger.Warn("Invalid launch_job task: missing event_id")
			return
		}
		event, exists := q.engine.GetScheduler().GetEvent(eventID)
		if !exists {
			q.logger.Warn("launch_job event not found", zap.String("event_id", eventID))
			return
		}
		if whenFloat <= 0 {
			whenFloat = float64(time.Now().Unix())
		}
		_ = q.engine.jobManager.LaunchOrQueueJob(event, int64(whenFloat))

	case "launchLocalJob":
		jobJSON, _ := json.Marshal(task.Data["job"])
		var job types.Job
		if err := json.Unmarshal(jobJSON, &job); err == nil && job.ID != "" {
			q.engine.jobManager.launchLocalJob(&job)
		}
	case "masterSocketEmit":
		// 实现主服务器 Socket 发送
		if event, ok := task.Data["event"].(string); ok {
			if data, ok := task.Data["data"]; ok {
				q.engine.comm.BroadcastToSockets(event, data)
			}
		}
	default:
		q.logger.Warn("Unknown queue task action", zap.String("action", task.Action))
	}
}

// EnqueueInternal 加入内部队列
func (q *Queue) EnqueueInternal(task *types.QueueTask) {
	q.mu.Lock()
	defer q.mu.Unlock()

	key := q.generateQueueKey()
	q.internalQueue[key] = task

	q.logger.Debug("Enqueued internal task", zap.String("key", key), zap.String("action", task.Action))
}

// EnqueueExternal 加入外部队列
func (q *Queue) EnqueueExternal(task *types.QueueTask) error {
	queueDir := q.engine.config.QueueDir
	if err := os.MkdirAll(queueDir, 0700); err != nil {
		return err
	}

	key := q.generateQueueKey()
	file := filepath.Join(queueDir, key+".json")

	data, err := json.Marshal(task)
	if err != nil {
		return err
	}

	// 先写入临时文件，然后重命名（原子操作）
	tmpFile := file + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0600); err != nil {
		return err
	}

	if err := os.Rename(tmpFile, file); err != nil {
		os.Remove(tmpFile)
		return err
	}

	q.logger.Debug("Enqueued external task", zap.String("file", file), zap.String("action", task.Action))
	return nil
}

// saveInternalQueue 保存内部队列到外部队列
func (q *Queue) saveInternalQueue() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for key, task := range q.internalQueue {
		if err := q.EnqueueExternal(task); err == nil {
			delete(q.internalQueue, key)
		}
	}
}

func (q *Queue) generateQueueKey() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return hex.EncodeToString(buf)
}
