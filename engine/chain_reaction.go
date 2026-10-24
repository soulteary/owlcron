package engine

import (
	"fmt"

	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

// ChainReaction 处理链式反应
func (s *Scheduler) ChainReaction(oldJob *types.Job, chainEventID string) error {
	if !s.engine.IsMaster() {
		return fmt.Errorf("only master server can trigger chain reactions")
	}

	// 查找链式事件
	event, exists := s.GetEvent(chainEventID)
	if !exists {
		// 尝试从存储加载
		if err := s.LoadEventsFromStorage(); err == nil {
			event, exists = s.GetEvent(chainEventID)
		}
		if !exists {
			err := fmt.Errorf("chain reaction event not found: %s", chainEventID)
			s.logger.Error("Chain reaction failed", zap.Error(err))
			return err
		}
	}

	if !event.Enabled.Bool() {
		return nil
	}
	// 复制事件以避免修改原始对象
	chainEvent := *types.CloneEvent(event)
	if chainEvent.Params == nil {
		chainEvent.Params = make(map[string]interface{})
	}

	// 合并 chain_params（来自源事件定义）
	if oldJob.ChainParams != nil {
		for k, v := range oldJob.ChainParams {
			chainEvent.Params[k] = v
		}
	}

	// Cronicle passes data and source metadata at the top level; only explicit
	// chain_params override the target event's parameters.
	chainEvent.ChainData = types.CloneMap(oldJob.ChainData)
	chainEvent.Source = "Chain Reaction (" + oldJob.EventTitle + ")"
	chainEvent.SourceEvent = oldJob.Event
	chainEvent.SourceJob = oldJob.ID
	chainEvent.ChainCode = oldJob.Code
	chainEvent.ChainDescription = oldJob.Description

	s.logger.Info("Triggering chain reaction",
		zap.String("source_job", oldJob.ID),
		zap.String("chain_event", chainEventID),
		zap.String("chain_title", event.Title))

	timestamp := oldJob.Now
	if timestamp == 0 {
		timestamp = int64(oldJob.TimeStart)
	}
	return s.engine.jobManager.LaunchOrQueueJob(&chainEvent, timestamp)
}
