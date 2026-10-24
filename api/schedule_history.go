package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"
)

// handleGetSchedule 返回调度全量数据
func (s *Server) handleGetSchedule(w http.ResponseWriter, r *http.Request) {
	s.sendResponse(w, 0, s.loadScheduleData(), "")
}

// handleGetHistory 返回历史任务
func (s *Server) handleGetHistory(w http.ResponseWriter, r *http.Request) {
	offset, limit := parseOffsetLimit(r, 0, 100)
	loaded, err := s.sortedDeadJobs()
	if err != nil {
		s.sendError(w, 1, "Failed to load history: "+err.Error())
		return
	}
	jobs := s.authorizedJobViews(r, loaded)
	s.sendResponse(w, 0, map[string]interface{}{"rows": paginateJobs(jobs, offset, limit), "list": map[string]int{"length": len(jobs)}}, "")
}

// handleGetEventHistory 返回指定事件的历史任务
func (s *Server) handleGetEventHistory(w http.ResponseWriter, r *http.Request) {
	offset, limit := parseOffsetLimit(r, 0, 100)
	eventID := readRequestID(r)
	if eventID == "" {
		s.sendError(w, 1, "Missing event id")
		return
	}
	loaded, err := s.sortedDeadJobs()
	if err != nil {
		s.sendError(w, 1, "Failed to load history: "+err.Error())
		return
	}
	all := s.authorizedJobViews(r, loaded)
	filtered := make([]*jobView, 0)
	for _, job := range all {
		if job.Event == eventID {
			filtered = append(filtered, job)
		}
	}
	s.sendResponse(w, 0, map[string]interface{}{"rows": paginateJobs(filtered, offset, limit), "list": map[string]int{"length": len(filtered)}}, "")
}

// handleFlushEventQueue 清空事件队列
func (s *Server) handleFlushEventQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}
	eventID := r.URL.Query().Get("id")
	if eventID == "" {
		var req struct {
			ID string `json:"id"`
		}
		_ = decodeBody(r, &req)
		eventID = req.ID
	}
	if eventID == "" {
		s.sendError(w, 1, "Missing event id")
		return
	}
	prev, err := s.engine.GetScheduler().FlushEventQueue(eventID)
	if err != nil {
		s.sendError(w, 1, "Failed to flush queue: "+err.Error())
		return
	}
	s.sendResponse(w, 0, map[string]interface{}{
		"id":      eventID,
		"flushed": prev,
	}, "")
}

type jobView struct {
	ID        string
	Event     string
	TimeStart float64
	Payload   interface{}
}

func (s *Server) sortedDeadJobs() ([]*jobView, error) {
	dead := make(map[string]*types.Job)
	if store := s.engine.GetStorage(); store != nil {
		if err := s.engine.EnsureHistoryMigrated(context.Background()); err != nil {
			return nil, err
		}
		jobs, err := storage.HistoryJobs(context.Background(), store)
		if err != nil {
			return nil, err
		}
		for _, job := range jobs {
			dead[job.ID] = job
		}
	}
	for id, job := range s.engine.GetDeadJobs() {
		dead[id] = job
	}
	jobs := make([]*jobView, 0, len(dead))
	for _, job := range dead {
		if deleted, err := s.engine.IsJobHistoryDeleted(job.ID); err != nil {
			return nil, err
		} else if deleted {
			continue
		}
		if job.Complete && job.Action == "" {
			job.Action = "job_complete"
		}
		jobs = append(jobs, &jobView{
			ID:        job.ID,
			Event:     job.Event,
			TimeStart: job.TimeStart,
			Payload:   job,
		})
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].TimeStart == jobs[j].TimeStart {
			return jobs[i].ID > jobs[j].ID
		}
		return jobs[i].TimeStart > jobs[j].TimeStart
	})
	return jobs, nil
}

func paginateJobs(items []*jobView, offset, limit int) []interface{} {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 100
	}
	if offset >= len(items) {
		return []interface{}{}
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	result := make([]interface{}, 0, end-offset)
	for _, item := range items[offset:end] {
		result = append(result, item.Payload)
	}
	return result
}

func parseOffsetLimit(r *http.Request, defaultOffset, defaultLimit int) (int, int) {
	if r.Method == http.MethodPost && r.Body != nil {
		data, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(data))
		var req map[string]interface{}
		if json.Unmarshal(data, &req) == nil {
			query := r.URL.Query()
			for _, key := range []string{"id", "offset", "limit"} {
				if v, ok := req[key]; ok {
					query.Set(key, fmt.Sprint(v))
				}
			}
			r.URL.RawQuery = query.Encode()
		}
	}
	offset := defaultOffset
	limit := defaultLimit
	if raw := r.URL.Query().Get("offset"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			offset = parsed
		}
	}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			limit = parsed
		}
	}
	if limit > 10000 {
		limit = 10000
	}
	return offset, limit
}

func decodeBody(r *http.Request, target interface{}) error {
	if r.Body == nil {
		return nil
	}
	return json.NewDecoder(r.Body).Decode(target)
}

func nowUnix() int64 {
	return time.Now().Unix()
}

func (s *Server) authorizedJobViews(r *http.Request, jobs []*jobView) []*jobView {
	auth := GetAuthContext(r)
	out := make([]*jobView, 0, len(jobs))
	if auth == nil {
		return out
	}
	for _, view := range jobs {
		job, ok := view.Payload.(*types.Job)
		if ok && s.engine.CanAccess(auth.Privileges, job.Category, job.Target) {
			out = append(out, view)
		}
	}
	return out
}
