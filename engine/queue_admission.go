package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

type queueAdmission struct {
	Token     string       `json:"token"`
	EventID   string       `json:"event_id"`
	Timestamp int64        `json:"timestamp"`
	Jobs      []*types.Job `json:"jobs"`
}

var queueTokenPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

func (s *Scheduler) ensureHeadQueueToken(key string, item interface{}) (interface{}, string, error) {
	raw, err := json.Marshal(item)
	if err != nil {
		return nil, "", err
	}
	var head map[string]interface{}
	if err = json.Unmarshal(raw, &head); err != nil {
		return nil, "", err
	}
	if token, ok := head["queue_token"].(string); ok && queueTokenPattern.MatchString(token) {
		return head, token, nil
	}
	token, err := newLeadershipToken()
	if err != nil {
		return nil, "", err
	}
	err = storage.UpdateList(s.engine.ctx, s.engine.storage, key, func(items []interface{}) ([]interface{}, error) {
		if len(items) == 0 {
			return nil, fmt.Errorf("queue head disappeared")
		}
		current, _ := json.Marshal(items[0])
		if !bytes.Equal(raw, current) {
			return nil, fmt.Errorf("queue head changed")
		}
		head["queue_token"] = token
		items[0] = head
		return items, nil
	})
	return head, token, err
}
func (s *Scheduler) removeAdmittedQueueHead(key, token string) error {
	var remaining int
	err := storage.UpdateList(s.engine.ctx, s.engine.storage, key, func(items []interface{}) ([]interface{}, error) {
		remaining = len(items)
		if len(items) == 0 {
			return items, nil
		}
		raw, _ := json.Marshal(items[0])
		var head map[string]interface{}
		_ = json.Unmarshal(raw, &head)
		if head["queue_token"] != token {
			return nil, fmt.Errorf("queue head changed before admission completed")
		}
		remaining = len(items) - 1
		return items[1:], nil
	})
	if err == nil {
		s.engine.mu.Lock()
		s.engine.eventQueue[strings.TrimPrefix(key, "global/event_queue/")] = remaining
		s.engine.mu.Unlock()
	}
	return err
}

func (jm *JobManager) loadQueueAdmission(token string) (*queueAdmission, error) {
	if !queueTokenPattern.MatchString(token) {
		return nil, fmt.Errorf("invalid queue admission token")
	}
	raw, err := jm.engine.storage.Get(context.Background(), "queue_admissions:"+token)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var admission queueAdmission
	if err = json.Unmarshal(raw, &admission); err != nil {
		return nil, err
	}
	if admission.Token != token || admission.EventID == "" || len(admission.Jobs) == 0 {
		return nil, fmt.Errorf("invalid queue admission record")
	}
	seen := map[string]bool{}
	for _, job := range admission.Jobs {
		if job == nil || job.ID == "" || job.Event != admission.EventID || job.Now != admission.Timestamp || seen[job.ID] {
			return nil, fmt.Errorf("invalid queue admission job snapshot")
		}
		seen[job.ID] = true
	}
	return &admission, nil
}

// stageQueueAdmission records immutable snapshots before exposing admission
// or starting any subprocess. Queue replay uses these same IDs.
func (jm *JobManager) stageQueueAdmission(token string, jobs []*types.Job) error {
	if token == "" {
		return nil
	}
	if len(jobs) == 0 || jobs[0] == nil {
		return fmt.Errorf("queue admission has no jobs")
	}
	eventID, timestamp := jobs[0].Event, jobs[0].Now
	if !queueTokenPattern.MatchString(token) {
		return fmt.Errorf("invalid queue admission token")
	}
	atomic, ok := jm.engine.storage.(storage.AtomicStorage)
	if !ok {
		return storage.ErrUnsupported
	}
	admission := queueAdmission{Token: token, EventID: eventID, Timestamp: timestamp, Jobs: jobs}
	encoded, err := json.Marshal(&admission)
	if err != nil {
		return err
	}
	return atomic.Update(context.Background(), "queue_admissions:"+token, func(existing []byte) ([]byte, error) {
		if len(existing) > 0 {
			return nil, fmt.Errorf("queue admission already exists")
		}
		return encoded, nil
	})
}

// resumeQueueAdmission starts only snapshots with neither a durable active
// reservation nor a historical record. Known IDs are never admitted twice.
func (jm *JobManager) resumeQueueAdmission(token string) ([]string, bool, error) {
	admission, err := jm.loadQueueAdmission(token)
	if err != nil {
		return nil, false, err
	}
	if admission == nil {
		return nil, false, nil
	}
	ids := make([]string, 0, len(admission.Jobs))
	for _, job := range admission.Jobs {
		if job == nil {
			return nil, true, fmt.Errorf("invalid admission job")
		}
		ids = append(ids, job.ID)
	}
	err = jm.resumePreparedQueueAdmission(admission)
	return ids, true, err
}
func (jm *JobManager) resumePreparedQueueAdmission(admission *queueAdmission) error {
	if !jm.engine.EnsureLeadership() {
		return fmt.Errorf("leadership lost")
	}
	jm.mu.Lock()
	defer jm.mu.Unlock()
	items, err := jm.engine.storage.ListGet(context.Background(), "active_jobs", 0, 0)
	if err != nil {
		return err
	}
	knownActive := map[string]bool{}
	for _, item := range items {
		raw, _ := json.Marshal(item)
		var job types.Job
		if json.Unmarshal(raw, &job) == nil {
			knownActive[job.ID] = true
		}
	}
	pending := make([]*types.Job, 0)
	for _, snapshot := range admission.Jobs {
		if snapshot == nil || snapshot.ID == "" {
			return fmt.Errorf("invalid prepared queue job")
		}
		if deleted, err := jm.engine.IsJobHistoryDeleted(snapshot.ID); err != nil {
			return err
		} else if deleted {
			continue
		}
		if _, known := jm.engine.LookupJob(snapshot.ID); known || knownActive[snapshot.ID] {
			continue
		}
		job := types.CloneJob(snapshot)
		// A local event with no reservation yet follows the new leader on takeover.
		if job.Target == "" || job.Target == "local" {
			job.Hostname = jm.getHostname()
			job.LogFile = filepath.Join(jm.engine.config.LogDir, "jobs", job.ID+".log")
		}
		pending = append(pending, job)
	}
	if len(pending) > 0 {
		event, exists := jm.engine.scheduler.GetEvent(admission.EventID)
		if !exists || !event.Enabled.Bool() {
			return fmt.Errorf("queued event no longer enabled")
		}
		if err := jm.checkConcurrencyLimits(event); err != nil {
			return err
		}
		active := jm.engine.GetActiveJobs()
		eventCount := 0
		for _, job := range active {
			if job.Event == admission.EventID {
				eventCount++
			}
		}
		if event.MaxChildren > 0 && eventCount+len(pending) > event.MaxChildren {
			return fmt.Errorf("queued admission exceeds event concurrency limit")
		}
		if jm.engine.config.MaxJobs > 0 && len(active)+len(pending) > jm.engine.config.MaxJobs {
			return fmt.Errorf("queued admission exceeds global concurrency limit")
		}
		categoryCounts := map[string]int{}
		for _, job := range active {
			categoryCounts[job.Category]++
		}
		for _, job := range pending {
			categoryCounts[job.Category]++
			if job.Category == "" {
				continue
			}
			category, ok := jm.engine.GetCategory(job.Category)
			if !ok || !category.Enabled.Bool() {
				return fmt.Errorf("queued category is disabled")
			}
			if category.MaxChildren > 0 && categoryCounts[job.Category] > category.MaxChildren {
				return fmt.Errorf("queued admission exceeds category concurrency limit")
			}
		}
		// One atomic list update reserves the whole multiplex batch. A failed write
		// leaves every prepared snapshot replayable rather than stranding a subset.
		if err := storage.UpdateList(context.Background(), jm.engine.storage, "active_jobs", func(items []interface{}) ([]interface{}, error) {
			replacement := map[string]bool{}
			for _, job := range pending {
				replacement[job.ID] = true
			}
			kept := make([]interface{}, 0, len(items)+len(pending))
			for _, item := range items {
				raw, _ := json.Marshal(item)
				var job types.Job
				if json.Unmarshal(raw, &job) == nil && replacement[job.ID] {
					continue
				}
				kept = append(kept, item)
			}
			for _, job := range pending {
				kept = append(kept, job)
			}
			return kept, nil
		}); err != nil {
			return err
		}
		jm.engine.mu.Lock()
		for _, job := range pending {
			jm.engine.activeJobs[job.ID] = types.CloneJob(job)
		}
		jm.engine.mu.Unlock()
	}
	// dispatchJob acquires jm.mu while starting a local process. Reserve all IDs
	// first, then release it for execution.
	jm.mu.Unlock()
	for _, job := range pending {
		jm.dispatchJob(job)
	}
	jm.mu.Lock()
	return nil
}
