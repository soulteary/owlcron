package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"sort"
	"strconv"
	"time"
)

func (s *Server) handleGetJobStatus(w http.ResponseWriter, r *http.Request) {
	jobID := readRequestID(r)
	if jobID == "" {
		s.sendError(w, 1, "Missing job id")
		return
	}
	if job, ok := s.engine.GetActiveJobs()[jobID]; ok {
		s.sendResponse(w, 0, map[string]interface{}{
			"job":      job,
			"complete": false,
		}, "")
		return
	}
	if job, ok := s.engine.LookupJob(jobID); ok {
		s.sendResponse(w, 0, map[string]interface{}{
			"job":      job,
			"complete": true,
		}, "")
		return
	}
	s.sendError(w, 1, "Job not found")
}

func (s *Server) handleUpdateJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}
	params, err := requestParameters(r)
	if err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	id := stringParam(params, "id")
	if id == "" {
		s.sendError(w, 1, "Missing job id")
		return
	}
	job, err := s.engine.UpdateActiveJob(id, params)
	if err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	s.sendResponse(w, 0, map[string]interface{}{"job": job, "count": 1}, "")
}
func (s *Server) handleUpdateJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}
	params, err := requestParameters(r)
	if err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	ids, err := s.selectedJobIDs(r, params)
	if err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	updates, ok := params["updates"].(map[string]interface{})
	if !ok {
		updates = params
	}
	count := 0
	for _, id := range ids {
		if _, err := s.engine.UpdateActiveJob(id, updates); err == nil {
			count++
		} else {
			s.sendError(w, 1, err.Error())
			return
		}
	}
	s.sendResponse(w, 0, map[string]interface{}{"count": count, "updated": count}, "")
}
func (s *Server) handleAbortJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}
	params, err := requestParameters(r)
	if err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	ids, err := s.selectedJobIDs(r, params)
	if err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	reason := stringParam(params, "reason")
	if reason == "" {
		reason = "Aborted via API"
	}
	count := 0
	for _, id := range ids {
		if err := s.engine.GetJobManager().AbortJob(id, reason); err == nil {
			count++
		}
	}
	s.sendResponse(w, 0, map[string]interface{}{"count": count, "aborted": count}, "")
}
func (s *Server) selectedJobIDs(r *http.Request, params map[string]interface{}) ([]string, error) {
	requested := map[string]bool{}
	if id := stringParam(params, "id"); id != "" {
		requested[id] = true
	}
	if ids, ok := params["ids"].([]interface{}); ok {
		for _, value := range ids {
			if id, ok := value.(string); ok {
				requested[id] = true
			}
		}
	}
	selectors := []string{"event", "category", "plugin", "target", "hostname"}
	hasSelector := len(requested) > 0
	for _, key := range selectors {
		if stringParam(params, key) != "" {
			hasSelector = true
		}
	}
	if !hasSelector {
		return nil, fmt.Errorf("job ids or a job selector are required")
	}
	out := []string{}
	auth := GetAuthContext(r)
	if auth == nil {
		return out, nil
	}
	for id, job := range s.engine.GetActiveJobs() {
		if len(requested) > 0 && !requested[id] {
			continue
		}
		values := map[string]string{"event": job.Event, "category": job.Category, "plugin": job.Plugin, "target": job.Target, "hostname": job.Hostname}
		matches := true
		for _, key := range selectors {
			if value := stringParam(params, key); value != "" && values[key] != value {
				matches = false
			}
		}
		if matches && s.engine.CanAccess(auth.Privileges, job.Category, job.Target) {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (s *Server) handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, 1, "Invalid request body: "+err.Error())
		return
	}
	if err := s.engine.DeleteJobHistory(req.ID); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	s.sendResponse(w, 0, nil, "")
}

const defaultLogPageSize int64 = 64 * 1024
const maxLogPageSize int64 = 1024 * 1024

func logRequestRange(r *http.Request) (string, int64, int64, error) {
	params, err := requestParameters(r)
	if err != nil {
		return "", 0, 0, err
	}
	id := stringParam(params, "id")
	if id == "" {
		return "", 0, 0, fmt.Errorf("Missing job id")
	}
	values := []int64{0, defaultLogPageSize}
	for i, key := range []string{"offset", "limit"} {
		if value, ok := params[key]; ok {
			n, err := strconv.ParseInt(fmt.Sprint(value), 10, 64)
			if err != nil || n < 0 || (key == "limit" && n == 0) {
				return "", 0, 0, fmt.Errorf("Invalid log %s", key)
			}
			values[i] = n
		}
	}
	if values[1] > maxLogPageSize {
		return "", 0, 0, fmt.Errorf("Log limit exceeds %d bytes", maxLogPageSize)
	}
	return id, values[0], values[1], nil
}

func (s *Server) handleGetJobLog(w http.ResponseWriter, r *http.Request) {
	s.handleJobLogRange(w, r, false)
}
func (s *Server) handleGetLiveJobLog(w http.ResponseWriter, r *http.Request) {
	s.handleJobLogRange(w, r, false)
}
func (s *Server) handleFetchDeleteJobLog(w http.ResponseWriter, r *http.Request) {
	s.handleJobLogRange(w, r, true)
}
func (s *Server) handleJobLogRange(w http.ResponseWriter, r *http.Request, remove bool) {
	id, offset, limit, err := logRequestRange(r)
	if err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	if !remove && r.Method == http.MethodGet && r.URL.Query().Get("stream") == "1" && r.URL.Query().Get("download") == "1" {
		if offset != 0 {
			s.sendError(w, 1, "Streaming download must begin at offset zero")
			return
		}
		started, err := streamJobLog(w, r, id, func(offset, limit int64) ([]byte, int64, error) {
			return s.engine.ReadJobLogRange(r.Context(), id, offset, limit)
		})
		if err != nil && !started {
			s.sendError(w, 1, err.Error())
		}
		return
	}
	data, total, err := s.engine.ReadJobLogRange(r.Context(), id, offset, limit)
	if err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	if remove {
		if job, ok := s.engine.LookupJob(id); !ok || !job.Complete {
			s.sendError(w, 1, "Cannot fetch and delete a running job log")
			return
		}
		if offset != 0 || int64(len(data)) != total {
			s.sendError(w, 1, "Log exceeds one page; download all pages before explicitly deleting the log")
			return
		}
		if err := s.engine.DeleteJobLog(id); err != nil {
			s.sendError(w, 1, err.Error())
			return
		}
	}
	s.sendJobLog(w, r, id, data, offset, total)
}
func (s *Server) handleGetLogWatchAuth(w http.ResponseWriter, r *http.Request) {
	id := readRequestID(r)
	if id == "" {
		s.sendError(w, 1, "Missing job id")
		return
	}
	if _, exists := s.engine.LookupJob(id); !exists {
		s.sendError(w, 1, "Job not found")
		return
	}
	token := makeJobLogToken(s.engine, id)
	if token == "" {
		s.sendError(w, 1, "Log token authentication unavailable")
		return
	}
	s.sendResponse(w, 0, map[string]interface{}{"token": token, "ttl": 300}, "")
}

// readRequestID preserves the Cronicle POST JSON contract and GET download URLs.
func readRequestID(r *http.Request) string {
	if id := r.URL.Query().Get("id"); id != "" {
		return id
	}
	if r.Body == nil {
		return ""
	}
	var req struct {
		ID string `json:"id"`
	}
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		return ""
	}
	return req.ID
}
func (s *Server) sendJobLog(w http.ResponseWriter, r *http.Request, id string, data []byte, offset, total int64) {
	next := offset + int64(len(data))
	eof := next >= total
	if r.Method == http.MethodGet {
		w.Header().Set("X-Log-Offset", strconv.FormatInt(offset, 10))
		w.Header().Set("X-Log-Next-Offset", strconv.FormatInt(next, 10))
		w.Header().Set("X-Log-Total-Size", strconv.FormatInt(total, 10))
		w.Header().Set("X-Log-EOF", strconv.FormatBool(eof))
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.URL.Query().Get("download") == "1" {
			w.Header().Set("Content-Disposition", "attachment; filename=\""+id+".log\"")
		}
		_, _ = w.Write(data)
		return
	}
	s.sendResponse(w, 0, map[string]interface{}{"log": string(data), "log_base64": base64.StdEncoding.EncodeToString(data), "offset": offset, "next_offset": next, "total_size": total, "eof": eof}, "")
}

// streamJobLog freezes the initial byte length and never buffers more than one page.
// Once headers are sent, any failure leaves an incomplete Content-Length response.
func streamJobLog(w http.ResponseWriter, r *http.Request, id string, read func(int64, int64) ([]byte, int64, error)) (bool, error) {
	data, total, err := read(0, defaultLogPageSize)
	if err != nil {
		return false, err
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": id + ".log"}))
	w.Header().Set("Content-Length", strconv.FormatInt(total, 10))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	defer func() { _ = http.NewResponseController(w).SetWriteDeadline(time.Time{}) }()
	for offset := int64(0); offset < total; {
		if err := r.Context().Err(); err != nil {
			return true, err
		}
		remaining := total - offset
		if int64(len(data)) > remaining {
			data = data[:remaining]
		}
		if len(data) == 0 {
			return true, io.ErrUnexpectedEOF
		}
		if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return true, err
		}
		n, err := w.Write(data)
		if err != nil {
			return true, err
		}
		if n != len(data) {
			return true, io.ErrShortWrite
		}
		offset += int64(n)
		if offset == total {
			break
		}
		limit := defaultLogPageSize
		if limit > total-offset {
			limit = total - offset
		}
		data, _, err = read(offset, limit)
		if err != nil {
			return true, err
		}
	}
	return true, nil
}
