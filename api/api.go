package api

import (
	"encoding/json"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/soulteary/owlcron/engine"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

// Server API 服务器
type Server struct {
	engine       *engine.Engine
	logger       *zap.Logger
	mu           sync.RWMutex
	loginLimiter *loginLimiter
}

// NewServer 创建 API 服务器
func NewServer(eng *engine.Engine, logger *zap.Logger) *Server {
	return &Server{
		engine:       eng,
		logger:       logger,
		loginLimiter: newLoginLimiter(eng.GetConfig()),
	}
}

// RegisterRoutes 注册路由
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	register := func(path string, handler http.HandlerFunc) {
		secured := s.AuthenticationMiddleware(s.authorize(path, handler))
		mux.Handle(path, secured)
		// 兼容 Cronicle 风格的版本化路径：/v1
		mux.Handle(path+"/v1", secured)
	}

	register("/api/app/get_notifications", s.handleGetNotifications)
	register("/api/app/retry_notification", s.handleRetryNotification)

	// 状态 API
	register("/api/app/status", s.handleStatus)
	register("/api/app/ping", s.handlePing)
	register("/api/app/echo", s.handleEcho)

	// 事件 API
	register("/api/app/get_schedule", s.handleGetSchedule)
	register("/api/app/create_event", s.handleCreateEvent)
	register("/api/app/update_event", s.handleUpdateEvent)
	register("/api/app/delete_event", s.handleDeleteEvent)
	register("/api/app/get_event", s.handleGetEvent)
	register("/api/app/list_events", s.handleListEvents)
	register("/api/app/get_event_history", s.handleGetEventHistory)
	register("/api/app/get_history", s.handleGetHistory)
	register("/api/app/run_event", s.handleRunEvent)
	register("/api/app/abort_event", s.handleAbortEvent)
	register("/api/app/flush_event_queue", s.handleFlushEventQueue)

	// 任务 API
	register("/api/app/get_job", s.handleGetJob)
	register("/api/app/get_job_details", s.handleGetJob)
	register("/api/app/get_job_status", s.handleGetJobStatus)
	register("/api/app/list_jobs", s.handleListJobs)
	register("/api/app/get_active_jobs", s.handleListJobs)
	register("/api/app/update_job", s.handleUpdateJob)
	register("/api/app/update_jobs", s.handleUpdateJobs)
	register("/api/app/abort_job", s.handleAbortJob)
	register("/api/app/abort_jobs", s.handleAbortJobs)
	register("/api/app/delete_job", s.handleDeleteJob)
	register("/api/app/get_job_log", s.handleGetJobLog)
	register("/api/app/get_live_job_log", s.handleGetLiveJobLog)
	register("/api/app/fetch_delete_job_log", s.handleFetchDeleteJobLog)
	register("/api/app/get_log_watch_auth", s.handleGetLogWatchAuth)

	// 插件 API
	register("/api/app/create_plugin", s.handleCreatePlugin)
	register("/api/app/update_plugin", s.handleUpdatePlugin)
	register("/api/app/delete_plugin", s.handleDeletePlugin)
	register("/api/app/get_plugin", s.handleGetPlugin)
	register("/api/app/list_plugins", s.handleListPlugins)
	register("/api/app/get_plugins", s.handleListPlugins)

	// 分类 API
	register("/api/app/create_category", s.handleCreateCategory)
	register("/api/app/update_category", s.handleUpdateCategory)
	register("/api/app/delete_category", s.handleDeleteCategory)
	register("/api/app/get_category", s.handleGetCategory)
	register("/api/app/list_categories", s.handleListCategories)
	register("/api/app/get_categories", s.handleListCategories)

	// 服务器组 API
	register("/api/app/create_server_group", s.handleCreateServerGroup)
	register("/api/app/update_server_group", s.handleUpdateServerGroup)
	register("/api/app/delete_server_group", s.handleDeleteServerGroup)
	register("/api/app/get_server_group", s.handleGetServerGroup)
	register("/api/app/list_server_groups", s.handleListServerGroups)
	register("/api/app/list_servers", s.handleListServers)
	register("/api/app/get_servers", s.handleListServers)

	// 用户 API
	register("/api/app/check_user_exists", s.handleCheckUserExists)
	register("/api/user/login", s.handleUserLogin)
	register("/api/user/logout", s.handleUserLogout)
	register("/api/user/resume_session", s.handleResumeSession)
	register("/api/user/create", s.handleUserCreate)
	register("/api/user/admin_create", s.handleUserCreate)
	register("/api/user/admin_get_users", s.handleAdminGetUsers)
	register("/api/user/admin_get_user", s.handleAdminGetUser)
	register("/api/user/admin_update", s.handleAdminUpdateUser)
	register("/api/user/admin_delete", s.handleAdminDeleteUser)
	register("/api/user/update", s.handleUpdateOwnUser)
	register("/api/user/delete", s.handleDeleteOwnUser)

	// 管理和配置 API
	register("/api/app/get_master_state", s.handleGetMasterState)
	register("/api/app/update_master_state", s.handleUpdateMasterState)
	register("/api/app/get_activity", s.handleGetActivity)
	register("/api/app/config", s.handleConfig)
	register("/api/app/check_add_server", s.handleCheckAddServer)
	register("/api/app/add_server", s.handleAddServer)
	register("/api/app/remove_server", s.handleRemoveServer)
	register("/api/app/restart_server", s.handleRestartServer)
	register("/api/app/shutdown_server", s.handleShutdownServer)

	// API Key 管理
	register("/api/app/get_api_keys", s.handleListAPIKeys)
	register("/api/app/get_api_key", s.handleGetAPIKey)
	register("/api/app/create_api_key", s.handleCreateAPIKey)
	register("/api/app/update_api_key", s.handleUpdateAPIKey)
	register("/api/app/delete_api_key", s.handleDeleteAPIKey)
}

// sendResponse sends a Cronicle-compatible JSON response.
// Fields from data (if map) are flattened to the top level alongside "code",
// matching the format the frontend expects: { "code":0, "user":{...}, ... }
func (s *Server) sendResponse(w http.ResponseWriter, code int, data interface{}, description string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	resp := map[string]interface{}{
		"code": code,
	}
	if description != "" {
		resp["description"] = description
	}

	if data != nil {
		switch v := data.(type) {
		case map[string]string:
			for k, val := range v {
				resp[k] = val
			}
		case map[string]interface{}:
			for k, val := range v {
				resp[k] = val
			}
		default:
			resp["data"] = data
		}
	}

	json.NewEncoder(w).Encode(resp)
}

// sendError 发送错误响应
func (s *Server) sendError(w http.ResponseWriter, code int, description string) {
	s.sendResponse(w, code, nil, description)
}

// sendJSCallback wraps data as a JavaScript function call (JSONP-style),
// used by /api/app/config which is loaded via <script> tag.
func (s *Server) sendJSCallback(w http.ResponseWriter, funcName string, data interface{}) {
	w.Header().Set("Content-Type", "text/javascript")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.WriteHeader(http.StatusOK)

	jsonBytes, err := json.Marshal(data)
	if err != nil {
		w.Write([]byte(funcName + "({\"code\":1,\"description\":\"JSON marshal error\"});\n"))
		return
	}
	w.Write([]byte(funcName + "(" + string(jsonBytes) + ");\n"))
}

// handleStatus 处理状态请求
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	state := s.engine.GetState()
	multi := s.engine.GetMulti()

	data := map[string]interface{}{
		"version":         "0.9.102",
		"hostname":        s.engine.Hostname(),
		"master":          multi.Master,
		"master_hostname": multi.MasterHostname,
		"enabled":         state.Enabled,
		"active_jobs":     len(s.engine.GetActiveJobs()),
	}

	s.sendResponse(w, 0, data, "")
}

// handlePing 处理 ping 请求
func (s *Server) handlePing(w http.ResponseWriter, r *http.Request) {
	s.sendResponse(w, 0, nil, "")
}

// handleEcho 处理 echo 请求
func (s *Server) handleEcho(w http.ResponseWriter, r *http.Request) {
	s.sendResponse(w, 0, map[string]interface{}{
		"query": r.URL.Query(),
	}, "")
}

// handleCreateEvent 处理创建事件
func (s *Server) handleCreateEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}

	var event types.Event
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		s.sendError(w, 1, "Invalid request body: "+err.Error())
		return
	}

	if event.ID == "" {
		event.ID = generateID()
	}

	// 验证事件数据
	if err := ValidateEvent(&event); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	if err := s.validateEventPluginParams(&event); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}

	// 设置创建和修改时间
	now := time.Now().Unix()
	if event.Created == 0 {
		event.Created = now
	}
	event.Modified = now

	// 添加到调度器（会自动保存到存储）
	if err := s.engine.GetScheduler().CreateEvent(&event); err != nil {
		s.sendError(w, 1, "Failed to create event: "+err.Error())
		return
	}
	s.configurationChanged("schedule")

	s.sendResponse(w, 0, map[string]interface{}{"id": event.ID}, "")
}

// handleUpdateEvent 处理更新事件
func (s *Server) handleUpdateEvent(w http.ResponseWriter, r *http.Request) {
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
	original, exists := s.engine.GetScheduler().GetEvent(id)
	if !exists {
		s.sendError(w, 1, "Event not found")
		return
	}
	raw, _ := json.Marshal(original)
	var merged map[string]interface{}
	_ = json.Unmarshal(raw, &merged)
	for key, value := range params {
		if key != "id" && key != "created" && key != "modified" && key != "session_id" && key != "api_key" {
			merged[key] = value
		}
	}
	raw, err = json.Marshal(merged)
	if err != nil {
		s.sendError(w, 1, "Invalid event data")
		return
	}
	var event types.Event
	if err = json.Unmarshal(raw, &event); err != nil {
		s.sendError(w, 1, "Invalid event data: "+err.Error())
		return
	}
	if err := ValidateEvent(&event); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	event.Modified = time.Now().Unix()
	if err := s.validateEventPluginParams(&event); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	if err := s.engine.GetScheduler().AddEvent(&event); err != nil {
		s.sendError(w, 1, "Failed to update event: "+err.Error())
		return
	}
	s.configurationChanged("schedule")
	s.sendResponse(w, 0, nil, "")
}

// handleDeleteEvent 处理删除事件
func (s *Server) handleDeleteEvent(w http.ResponseWriter, r *http.Request) {
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

	// 检查事件是否存在
	_, exists := s.engine.GetScheduler().GetEvent(req.ID)
	if !exists {
		s.sendError(w, 1, "Event not found")
		return
	}

	// 删除事件（会自动从存储删除）
	if err := s.engine.GetScheduler().RemoveEvent(req.ID); err != nil {
		s.sendError(w, 1, "Failed to delete event: "+err.Error())
		return
	}
	s.configurationChanged("schedule")

	s.sendResponse(w, 0, nil, "")
}

// handleGetEvent 处理获取事件
func (s *Server) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	eventID := readRequestID(r)
	if eventID == "" {
		s.sendError(w, 1, "Missing event id")
		return
	}

	// 从调度器获取事件
	event, exists := s.engine.GetScheduler().GetEvent(eventID)
	if !exists {
		s.sendError(w, 1, "Event not found")
		return
	}

	s.sendResponse(w, 0, map[string]interface{}{"event": event}, "")
}

// handleListEvents 处理列出事件
func (s *Server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	// 从调度器获取所有事件
	eventsMap := s.engine.GetScheduler().GetAllEvents()
	events := make([]*types.Event, 0, len(eventsMap))
	for _, event := range eventsMap {
		events = append(events, event)
	}
	s.sendResponse(w, 0, events, "")
}

// handleRunEvent 处理运行事件
func (s *Server) handleRunEvent(w http.ResponseWriter, r *http.Request) {
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
	event, exists := s.engine.GetScheduler().GetEvent(id)
	if !exists {
		s.sendError(w, 1, "Event not found")
		return
	}
	event = types.CloneEvent(event)
	auth := GetAuthContext(r)
	timestamp := time.Now().Unix()
	if auth == nil {
		s.sendError(w, 1, "Authentication required")
		return
	}
	if !auth.Privileges["job_read_only"] || auth.IsAdmin {
		if now, ok := params["now"].(float64); ok && now > 0 {
			timestamp = int64(now)
		}
		raw, _ := json.Marshal(event)
		var merged map[string]interface{}
		_ = json.Unmarshal(raw, &merged)
		for _, key := range []string{"target", "plugin", "timezone", "timeout", "retries", "retry_delay", "notify_success", "notify_fail", "web_hook"} {
			if value, ok := params[key]; ok {
				merged[key] = value
			}
		}
		if auth.IsAdmin || auth.Privileges["edit_events"] {
			for _, key := range []string{"cwd", "uid", "gid"} {
				if value, ok := params[key]; ok {
					merged[key] = value
				}
			}
		}
		if override, ok := params["params"].(map[string]interface{}); ok {
			mergedParams := types.CloneMap(event.Params)
			if mergedParams == nil {
				mergedParams = map[string]interface{}{}
			}
			for key, value := range override {
				mergedParams[key] = value
			}
			merged["params"] = mergedParams
		}
		raw, err = json.Marshal(merged)
		if err != nil {
			s.sendError(w, 1, "Invalid event overrides")
			return
		}
		if err = json.Unmarshal(raw, event); err != nil {
			s.sendError(w, 1, "Invalid event overrides")
			return
		}
	}
	if !s.engine.CanAccess(auth.Privileges, event.Category, event.Target) {
		s.sendError(w, 1, "Category or server group access denied")
		return
	}
	if err := ValidateEvent(event); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	event.Enabled = true
	if err := s.validateEventPluginParams(event); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	if auth.APIKey != nil {
		event.Source = "API Key (" + auth.APIKey.Title + ")"
	} else if auth.User != nil {
		event.Source = "Manual (" + auth.User.Username + ")"
	}
	ids, err := s.engine.GetJobManager().LaunchOrQueueJobWithIDs(event, timestamp)
	if err != nil {
		s.sendError(w, 1, "Failed to run event: "+err.Error())
		return
	}
	if ids == nil {
		ids = []string{}
	}
	s.sendResponse(w, 0, map[string]interface{}{"ids": ids}, "")
}

// handleAbortEvent 处理中止事件
func (s *Server) handleAbortEvent(w http.ResponseWriter, r *http.Request) {
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

	// 获取所有活动任务
	jobs := s.engine.GetActiveJobs()
	abortedCount := 0

	// 中止该事件的所有任务
	for jobID, job := range jobs {
		if job.Event == req.ID {
			if err := s.engine.GetJobManager().AbortJob(jobID, "Event aborted via API"); err != nil {
				s.logger.Warn("Failed to abort job", zap.String("job_id", jobID), zap.Error(err))
			} else {
				abortedCount++
			}
		}
	}

	s.sendResponse(w, 0, map[string]interface{}{
		"aborted_jobs": abortedCount,
	}, "")
}

// handleGetJob 处理获取任务
func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	jobID := readRequestID(r)
	if jobID == "" {
		s.sendError(w, 1, "Missing job id")
		return
	}
	job, exists := s.engine.LookupJob(jobID)
	if !exists {
		s.sendError(w, 1, "Job not found")
		return
	}
	s.sendResponse(w, 0, map[string]interface{}{"job": job, "complete": job.Complete, "token": makeJobLogToken(s.engine, jobID)}, "")
}

// handleListJobs 处理列出任务
func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	s.sendResponse(w, 0, map[string]interface{}{"jobs": s.engine.GetActiveJobs()}, "")
}

// handleAbortJob 处理中止任务
func (s *Server) handleAbortJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}

	var req struct {
		ID     string `json:"id"`
		Reason string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, 1, "Invalid request body: "+err.Error())
		return
	}

	reason := req.Reason
	if reason == "" {
		reason = "Aborted via API"
	}

	// 中止任务
	if err := s.engine.GetJobManager().AbortJob(req.ID, reason); err != nil {
		s.sendError(w, 1, "Failed to abort job: "+err.Error())
		return
	}

	s.sendResponse(w, 0, nil, "")
}

// getHostname 获取主机名
func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "localhost"
	}
	return hostname
}
