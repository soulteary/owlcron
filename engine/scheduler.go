package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

// Scheduler 调度器
type Scheduler struct {
	engine  *Engine
	logger  *zap.Logger
	mu      sync.RWMutex
	queueMu sync.Mutex

	// 事件列表
	events map[string]*types.Event

	// 定时器
	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	done        chan struct{}
	ticking     bool
}

// NewScheduler 创建新的调度器
func NewScheduler(engine *Engine, logger *zap.Logger) (*Scheduler, error) {
	return &Scheduler{
		engine: engine,
		logger: logger,
		events: make(map[string]*types.Event),
	}, nil
}

// Startup 启动调度器
func (s *Scheduler) Startup() error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.cancel != nil {
		return nil
	}
	if err := s.LoadEventsFromStorage(); err != nil {
		return err
	}
	s.preloadEventQueues()
	ctx, cancel := context.WithCancel(s.engine.ctx)
	s.cancel = cancel
	s.done = make(chan struct{})
	go s.minuteLoop(ctx, s.done)
	return nil
}

// Shutdown stops the role-owned loop and waits for any tick to finish.
func (s *Scheduler) Shutdown() {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.cancel == nil {
		return
	}
	s.cancel()
	<-s.done
	s.cancel = nil
	s.done = nil
	if err := s.engine.saveStateToStorage(); err != nil {
		s.logger.Error("Failed to save scheduler state", zap.Error(err))
	}
}

// preloadEventQueues 启动时从存储预加载事件队列长度
func (s *Scheduler) preloadEventQueues() {
	lengths := make(map[string]int)
	for _, event := range s.GetAllEvents() {
		if !event.Queue.Bool() {
			continue
		}
		key := fmt.Sprintf("global/event_queue/%s", event.ID)
		info, err := s.engine.storage.ListGetInfo(s.engine.ctx, key)
		if err == nil && info.Length > 0 {
			lengths[event.ID] = int(info.Length)
		}
	}
	s.engine.mu.Lock()
	s.engine.eventQueue = lengths
	s.engine.mu.Unlock()
}

// minuteLoop 分钟循环
func (s *Scheduler) minuteLoop(ctx context.Context, done chan struct{}) {
	defer close(done)
	grace := time.Duration(s.engine.config.SchedulerStartupGrace) * time.Second
	if grace < 0 {
		grace = 0
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	// Initial recovery must finish before the first schedule dispatch.
	for {
		s.engine.mu.RLock()
		starting := s.engine.starting
		s.engine.mu.RUnlock()
		if !starting {
			break
		}
		timer.Reset(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
	// Tick once after startup grace, then at wall-clock minute boundaries.
	// Persisted cursors prevent duplicate launches when a role restarts.
	s.MinuteTick()
	for {
		next := time.Now().Truncate(time.Minute).Add(time.Minute)
		timer.Reset(time.Until(next))
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			s.MinuteTick()
		}
	}
}

func (s *Scheduler) MinuteTick() { s.MinuteTickAt(time.Now()) }

// MinuteTickAt is the deterministic scheduling entrypoint. Event locks protect
// snapshots only; dispatch, queue storage and notifications run outside them.
func (s *Scheduler) MinuteTickAt(now time.Time) {
	s.engine.mu.RLock()
	canSchedule := s.engine.multi.Master && s.engine.state.Enabled && !s.engine.starting
	s.engine.mu.RUnlock()
	if !canSchedule || !s.engine.EnsureLeadership() {
		return
	}
	s.mu.Lock()
	if s.ticking {
		s.mu.Unlock()
		return
	}
	s.ticking = true
	events := make([]*types.Event, 0, len(s.events))
	for _, event := range s.events {
		events = append(events, types.CloneEvent(event))
	}
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.ticking = false; s.mu.Unlock() }()
	nowUnix := now.Truncate(time.Minute).Unix()
	for _, event := range events {
		if !event.Enabled.Bool() {
			continue
		}
		s.engine.mu.RLock()
		cursor, exists := s.engine.state.Cursors[event.ID]
		s.engine.mu.RUnlock()
		if !exists {
			cursor = nowUnix - 60
			s.engine.mu.Lock()
			s.engine.state.Cursors[event.ID] = cursor
			s.engine.mu.Unlock()
		}
		if !event.CatchUp.Bool() && cursor < nowUnix-60 {
			cursor = nowUnix - 60
		}
		for cursor < nowUnix {
			if !s.engine.EnsureLeadership() {
				return
			}
			cursor += 60
			if s.checkEventTiming(event, cursor) {
				if err := s.engine.jobManager.LaunchOrQueueJob(event, cursor); err != nil {
					s.logger.Error("Failed to launch scheduled event", zap.String("event_id", event.ID), zap.Error(err))
					s.engine.LogActivity("warning", map[string]interface{}{"event_id": event.ID, "event_title": event.Title, "description": err.Error()})
					if event.NotifyFail != "" {
						go s.engine.jobManager.mailManager.SendEventErrorEmail(event, err.Error(), event.NotifyFail)
					}
					s.engine.jobManager.webHookManager.FireJobLaunchFailureHook(event, err.Error())
					s.engine.mu.Lock()
					s.engine.state.Stats["jobs_failed"]++
					s.engine.mu.Unlock()
					// Cronicle backtracks a failed launch so catch-up retries the same
					// minute after capacity or configuration recovers.
					cursor -= 60
					break
				}
			}
			s.engine.mu.Lock()
			if s.engine.state.Cursors[event.ID] < cursor {
				s.engine.state.Cursors[event.ID] = cursor
			}
			s.engine.mu.Unlock()
		}
		s.engine.mu.Lock()
		if s.engine.state.Cursors[event.ID] < cursor {
			s.engine.state.Cursors[event.ID] = cursor
		}
		s.engine.mu.Unlock()
	}
	if err := s.engine.saveStateToStorage(); err != nil {
		s.logger.Warn("Failed to persist schedule cursors", zap.Error(err))
	}
	s.checkEventQueues()
}

// checkEventTiming 检查事件时间匹配
func (s *Scheduler) checkEventTiming(event *types.Event, timestamp int64) bool {
	if event.Timing == nil {
		return false
	}

	t := time.Unix(timestamp, 0)

	// 加载时区
	loc := time.Local
	if event.Timezone != "" {
		var err error
		loc, err = time.LoadLocation(event.Timezone)
		if err != nil {
			s.logger.Warn("Invalid timezone", zap.String("timezone", event.Timezone), zap.Error(err))
			loc = time.Local
		}
	}
	t = t.In(loc)

	timing := event.Timing

	// 检查分钟
	if len(timing.Minutes) > 0 && !contains(timing.Minutes, t.Minute()) {
		return false
	}

	// 检查小时
	if len(timing.Hours) > 0 && !contains(timing.Hours, t.Hour()) {
		return false
	}

	// 检查星期
	if len(timing.Weekdays) > 0 && !contains(timing.Weekdays, int(t.Weekday())) {
		return false
	}

	// 检查日期
	if len(timing.Days) > 0 && !contains(timing.Days, t.Day()) {
		return false
	}

	// 检查月份
	if len(timing.Months) > 0 && !contains(timing.Months, int(t.Month())) {
		return false
	}

	// 检查年份
	if len(timing.Years) > 0 && !contains(timing.Years, t.Year()) {
		return false
	}

	return true
}

// checkEventQueues 检查事件队列
func (s *Scheduler) checkEventQueues() {
	if !s.queueMu.TryLock() {
		return
	}
	defer s.queueMu.Unlock()
	s.engine.mu.RLock()
	eventQueues := make(map[string]int)
	for eventID, length := range s.engine.eventQueue {
		eventQueues[eventID] = length
	}
	s.engine.mu.RUnlock()

	ctx := s.engine.ctx

	for eventID, length := range eventQueues {
		if !s.engine.EnsureLeadership() {
			return
		}
		if length == 0 {
			continue
		}

		event, exists := s.GetEvent(eventID)
		if !exists || !event.Enabled.Bool() {
			continue
		}

		activeJobs := s.engine.GetActiveJobs()
		activeCount := 0
		for _, job := range activeJobs {
			if job.Event == eventID {
				activeCount++
			}
		}

		if event.MaxChildren > 0 && activeCount >= event.MaxChildren {
			continue
		}

		// Keep the head durable until immutable jobs have been admitted. Stable
		// tokens make replay idempotent if a process exits before removing it.
		queueKey := fmt.Sprintf("global/event_queue/%s", eventID)
		items, err := s.engine.storage.ListGet(ctx, queueKey, 0, 1)
		if err != nil || len(items) == 0 {
			continue
		}
		item, token, err := s.ensureHeadQueueToken(queueKey, items[0])
		if err != nil {
			continue
		}

		timestamp := time.Now().Unix()
		raw, _ := json.Marshal(item)
		var queued struct {
			Timestamp int64        `json:"timestamp"`
			Now       int64        `json:"now"`
			Event     *types.Event `json:"event"`
		}
		if json.Unmarshal(raw, &queued) == nil {
			// Cronicle 0.9.102 queues the event snapshot directly. Accept that
			// shape as well as owlcron's explicit event envelope.
			if queued.Event == nil {
				var legacy types.Event
				if json.Unmarshal(raw, &legacy) == nil && legacy.ID == eventID {
					queued.Event = &legacy
				}
			}
			if queued.Event != nil && queued.Event.ID == eventID {
				queued.Event.Enabled = event.Enabled
				event = queued.Event
			}
			if queued.Timestamp > 0 {
				timestamp = queued.Timestamp
			} else if queued.Now > 0 {
				timestamp = queued.Now
			}
		}
		if err := s.engine.jobManager.LaunchQueuedJob(event, timestamp, token); err != nil {
			s.logger.Debug("Queued event remains pending", zap.String("event_id", eventID), zap.Error(err))
			continue
		}
		if !s.engine.EnsureLeadership() {
			return
		}
		if err := s.removeAdmittedQueueHead(queueKey, token); err != nil {
			s.logger.Warn("Admitted queue head retained for idempotent replay", zap.String("event_id", eventID), zap.Error(err))
		}
	}
}

// deleteEventQueues 删除指定事件的队列
func (s *Scheduler) deleteEventQueues(eventID string) {
	queueKey := fmt.Sprintf("global/event_queue/%s", eventID)
	_ = s.engine.storage.ListDelete(s.engine.ctx, queueKey, true)

	s.engine.mu.Lock()
	delete(s.engine.eventQueue, eventID)
	s.engine.mu.Unlock()
}

// contains 检查切片是否包含值
func contains(slice []int, value int) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

// AddEvent 添加事件
func (s *Scheduler) AddEvent(event *types.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 保存到存储
	if err := s.SaveEventToStorage(event); err != nil {
		return fmt.Errorf("failed to save event to storage: %w", err)
	}

	s.events[event.ID] = types.CloneEvent(event)
	return nil
}

// RemoveEvent 移除事件
func (s *Scheduler) RemoveEvent(eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 从存储删除
	if err := s.DeleteEventFromStorage(eventID); err != nil {
		return fmt.Errorf("failed to delete event from storage: %w", err)
	}

	delete(s.events, eventID)
	s.deleteEventQueues(eventID)
	s.engine.mu.Lock()
	delete(s.engine.state.Cursors, eventID)
	delete(s.engine.state.JobCodes, eventID)
	delete(s.engine.state.Robins, eventID)
	s.engine.mu.Unlock()
	return nil
}

// GetEvent 获取事件
func (s *Scheduler) GetEvent(eventID string) (*types.Event, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	event, exists := s.events[eventID]
	return types.CloneEvent(event), exists
}

// GetAllEvents 获取所有事件
func (s *Scheduler) GetAllEvents() map[string]*types.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]*types.Event)
	for k, v := range s.events {
		result[k] = types.CloneEvent(v)
	}
	return result
}

// FlushEventQueue removes the durable queue while excluding concurrent drains.
func (s *Scheduler) FlushEventQueue(eventID string) (int, error) {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	count := 0
	if err := storage.UpdateList(s.engine.ctx, s.engine.storage, "global/event_queue/"+eventID, func(items []interface{}) ([]interface{}, error) {
		count = len(items)
		return []interface{}{}, nil
	}); err != nil {
		return 0, err
	}
	s.engine.mu.Lock()
	delete(s.engine.eventQueue, eventID)
	s.engine.mu.Unlock()
	return count, nil
}
