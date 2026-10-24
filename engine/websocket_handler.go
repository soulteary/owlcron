package engine

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/soulteary/owlcron/types"
	ws "github.com/soulteary/owlcron/websocket"
	"go.uber.org/zap"
)

// WebSocketHandler WebSocket 消息处理器
type WebSocketHandler struct {
	engine      *Engine
	logger      *zap.Logger
	channels    map[string]map[string]*ws.Client  // channel -> clientID -> client
	logWatchers map[string]map[string]*logWatcher // jobID -> clientID -> watcher
	challenges  map[string]string
	mu          sync.RWMutex
}

// logWatcher 日志观察者
type logWatcher struct {
	client *ws.Client
	offset int64
	cancel chan struct{}
	once   sync.Once
}

func (w *logWatcher) stop() { w.once.Do(func() { close(w.cancel) }) }

// NewWebSocketHandler 创建 WebSocket 处理器
func NewWebSocketHandler(engine *Engine, logger *zap.Logger) *WebSocketHandler {
	return &WebSocketHandler{
		engine:      engine,
		logger:      logger,
		challenges:  make(map[string]string),
		channels:    make(map[string]map[string]*ws.Client),
		logWatchers: make(map[string]map[string]*logWatcher),
	}
}

// OnConnect 客户端连接时调用
func (h *WebSocketHandler) OnConnect(client *ws.Client) {
	h.logger.Info("WebSocket client connected", zap.String("client_id", client.ID))

	if client.NodeEndpoint {
		challenge := newNodeChallenge()
		h.mu.Lock()
		h.challenges[client.ID] = challenge
		h.mu.Unlock()
		h.sendResponse(client, "node_challenge", map[string]string{"hostname": h.engine.Hostname(), "challenge": challenge})
	}
	// State is sent only after authentication succeeds.
}

// OnDisconnect 客户端断开时调用
func (h *WebSocketHandler) OnDisconnect(client *ws.Client) {
	h.logger.Info("WebSocket client disconnected", zap.String("client_id", client.ID))

	// 清理该客户端的所有日志观察者
	h.mu.Lock()
	delete(h.challenges, client.ID)
	for channel, clients := range h.channels {
		delete(clients, client.ID)
		if len(clients) == 0 {
			delete(h.channels, channel)
		}
	}
	for jobID, watchers := range h.logWatchers {
		if w, ok := watchers[client.ID]; ok {
			w.stop()
			delete(watchers, client.ID)
			if len(watchers) == 0 {
				delete(h.logWatchers, jobID)
			}
		}
	}
	h.mu.Unlock()
}

// OnMessage 收到消息时调用
func (h *WebSocketHandler) OnMessage(client *ws.Client, message *ws.Message) {
	h.logger.Debug("WebSocket message received",
		zap.String("client_id", client.ID),
		zap.String("type", message.Type))

	if message.Type != "auth" && message.Type != "ping" {
		_, node, _, _, _, _, _, _ := client.Identity()
		if !node {
			if _, ok := h.currentPrivileges(client); !ok {
				h.sendError(client, "Authentication required")
				return
			}
		}
	}
	if client.IsNodeMaster() && message.Type != "auth" && message.Type != "ping" {
		h.handleNodeMessage(client, message)
		return
	}
	switch message.Type {
	case "auth":
		h.handleAuth(client, message)
	case "subscribe":
		h.handleSubscribe(client, message)
	case "unsubscribe":
		h.handleUnsubscribe(client, message)
	case "ping":
		h.handlePing(client, message)
	case "watch_job_log":
		h.handleWatchJobLog(client, message)
	case "unwatch_job_log":
		h.handleUnwatchJobLog(client, message)
	default:
		h.logger.Warn("Unknown message type", zap.String("type", message.Type))
	}
}

// OnError 发生错误时调用
func (h *WebSocketHandler) OnError(client *ws.Client, err error) {
	h.logger.Error("WebSocket error", zap.String("client_id", client.ID), zap.Error(err))
}

// handleAuth 处理认证（校验 API Key 或 Session）
func (h *WebSocketHandler) handleAuth(client *ws.Client, message *ws.Message) {
	_, wasNode, _, _, _, _, _, _ := client.Identity()
	if wasNode {
		h.sendError(client, "Connection already authenticated")
		return
	}
	client.ClearAuthentication()
	var data struct {
		APIKey    string `json:"api_key"`
		SessionID string `json:"session_id"`
		NodeAuthData
	}
	if err := json.Unmarshal(message.Data, &data); err != nil {
		h.sendError(client, "Invalid auth data")
		return
	}
	if client.NodeEndpoint {
		h.mu.Lock()
		challenge := h.challenges[client.ID]
		delete(h.challenges, client.ID)
		h.mu.Unlock()
		if data.APIKey != "" || data.SessionID != "" {
			h.sendError(client, "Node authentication required")
			return
		}
		if h.engine.config.ClusterTLSRequireClientCert {
			if client.NodeTLS == nil || len(client.NodeTLS.VerifiedChains) == 0 || len(client.NodeTLS.PeerCertificates) == 0 || client.NodeTLS.PeerCertificates[0].VerifyHostname(data.Hostname) != nil {
				h.sendError(client, "Node certificate identity rejected")
				return
			}
		}
		hostname, master, err := h.engine.VerifyNodeAuthFor(data.NodeAuthData, h.engine.Hostname(), challenge)
		if err != nil || !master {
			h.sendError(client, "Node authentication failed")
			return
		}
		if !h.engine.AcceptMasterPing(hostname, data.Timestamp) {
			h.sendError(client, "Master identity rejected")
			return
		}
		client.SetAuthentication(hostname, true, true, nil, "node", data.LeaseOwner, 0)
		h.sendResponse(client, "auth_success", map[string]interface{}{"authenticated": true, "master": true, "node": h.engine.MakeNodeAuthFor(h.engine.Hostname(), false, hostname, data.Nonce)})
		h.sendStatus(client)
		h.engine.GetJobManager().RetryRemoteCompletions()
		return
	}
	if data.Master || data.Hostname != "" || data.Signature != "" {
		h.sendError(client, "Node credentials require cluster endpoint")
		return
	}
	if data.APIKey != "" {
		key, err := h.engine.LoadAPIKeyFromStorage(data.APIKey)
		if err == nil && key.Enabled && key.Key != "" && subtle.ConstantTimeCompare([]byte(key.Key), []byte(data.APIKey)) == 1 {
			client.SetAuthentication(key.Title, false, false, key.Privileges, "api_key", data.APIKey, 0)
		}
	} else if data.SessionID != "" {
		session, err := h.engine.LoadSessionFromStorage(data.SessionID)
		if err == nil && time.Now().Unix() < session.Expires {
			user, err := h.engine.ValidateUserSession(session)
			if err == nil && user.Enabled {
				client.SetAuthentication(user.Username, false, false, user.Privileges, "session", session.ID, session.Expires)
			}
		}
	}
	if !client.IsAuthenticated() {
		h.sendError(client, "Authentication failed")
		return
	}
	h.sendResponse(client, "auth_success", map[string]interface{}{"authenticated": true, "master": false})
	h.sendStatus(client)
	h.sendScheduleData(client)
}

func (h *WebSocketHandler) currentPrivileges(client *ws.Client) (map[string]bool, bool) {
	auth, node, _, _, _, kind, credential, expires := client.Identity()
	if !auth {
		return nil, false
	}
	if node {
		return nil, false
	}
	if expires > 0 && time.Now().Unix() >= expires {
		client.ClearAuthentication()
		return nil, false
	}
	switch kind {
	case "session":
		session, err := h.engine.LoadSessionFromStorage(credential)
		if err == nil && time.Now().Unix() < session.Expires {
			user, err := h.engine.ValidateUserSession(session)
			if err == nil && user.Enabled {
				return user.Privileges, true
			}
		}
	case "api_key":
		key, err := h.engine.LoadAPIKeyFromStorage(credential)
		if err == nil && key.Enabled && key.Key != "" && subtle.ConstantTimeCompare([]byte(key.Key), []byte(credential)) == 1 {
			return key.Privileges, true
		}
	}
	client.ClearAuthentication()
	return nil, false
}

func (h *WebSocketHandler) handleNodeMessage(client *ws.Client, message *ws.Message) {
	_, _, _, hostname, _, _, _, _ := client.Identity()
	if !h.allowIncomingMaster(client) {
		h.sendError(client, "Master identity rejected")
		return
	}
	switch message.Type {
	case "master_ping":
		var data struct {
			Epoch int64 `json:"epoch"`
		}
		if json.Unmarshal(message.Data, &data) != nil {
			return
		}
		if h.engine.AcceptMasterPing(hostname, data.Epoch) {
			h.sendStatus(client)
		}
	case "launch_job":
		multi := h.engine.GetMulti()
		if multi.Master || multi.MasterHostname != hostname {
			h.sendError(client, "Master identity rejected")
			return
		}
		var job types.Job
		if err := json.Unmarshal(message.Data, &job); err != nil {
			h.sendError(client, "Invalid job data")
			return
		}
		if err := h.engine.GetJobManager().LaunchRemoteJob(&job); err != nil {
			h.sendError(client, "Job launch failed: "+err.Error())
		}
	case "abort_job":
		var data struct {
			ID     string `json:"id"`
			Reason string `json:"reason"`
		}
		if json.Unmarshal(message.Data, &data) == nil {
			_ = h.engine.GetJobManager().AbortJob(data.ID, data.Reason)
		}
	case "finish_job_ack":
		var data struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(message.Data, &data) == nil && data.ID != "" {
			h.engine.GetJobManager().AckRemoteCompletion(data.ID)
		}
	case "update_job":
		var data struct {
			ID      string                 `json:"id"`
			Updates map[string]interface{} `json:"updates"`
		}
		if json.Unmarshal(message.Data, &data) != nil || data.ID == "" {
			h.sendError(client, "Invalid job update")
			return
		}
		if _, err := h.engine.UpdateActiveJob(data.ID, data.Updates); err != nil {
			h.sendError(client, "Job update failed: "+err.Error())
		}
	case "shutdown_server":
		h.engine.RequestShutdown()
	case "restart_server":
		h.engine.RequestRestart()
	case "job_log_data":
	default:
		h.sendError(client, "Unsupported node message")
	}
}

func (h *WebSocketHandler) AllowMaster(client *ws.Client) bool {
	return h.allowMaster(client, false)
}

func (h *WebSocketHandler) allowIncomingMaster(client *ws.Client) bool {
	return h.allowMaster(client, true)
}

func (h *WebSocketHandler) allowMaster(client *ws.Client, incoming bool) bool {
	if incoming {
		// Authenticate after obtaining the role fence. A queued frame must not
		// reuse a lease/registry/client check made before a role transition.
		h.engine.roleMu.Lock()
		defer h.engine.roleMu.Unlock()
	}
	if !client.IsNodeMaster() {
		return false
	}
	_, _, _, hostname, _, _, owner, _ := client.Identity()
	authoritative := h.engine.isRegisteredNode(hostname) && h.engine.isRegisteredNode(h.engine.Hostname()) && h.engine.ValidateNodeLeadership(hostname, owner)
	allowed := false
	if authoritative && incoming {
		// Every authenticated inbound frame proves liveness. A burst of slow
		// job admissions can otherwise keep master_ping behind the same reader
		// until the idle timer clears MasterHostname. The exact stored lease
		// owner permits restoring that idle follower, never a revoked owner.
		h.engine.mu.Lock()
		multi := h.engine.multi
		allowed = !multi.Master && (multi.MasterHostname == hostname || multi.MasterHostname == "")
		if allowed {
			multi.MasterHostname = hostname
			multi.Slave = true
			multi.Cluster = true
			multi.LastPingReceived = time.Now().Unix()
		}
		h.engine.mu.Unlock()
	} else if authoritative {
		multi := h.engine.GetMulti()
		allowed = !multi.Master && multi.MasterHostname == hostname
		if !multi.Master && multi.MasterHostname == "" {
			// A locally idle follower pauses outbound reports. Its still-valid
			// authenticated connection may carry the queued inbound frame that
			// restores liveness; idle is not a stored credential revocation.
			return false
		}
	}
	if !allowed {
		// A connection that has observed revocation must never become usable
		// again if the same process later reacquires leadership.
		client.ClearAuthentication()
		if client.Conn != nil {
			_ = client.Conn.Close()
		}
	}
	return allowed
}

// sendScheduleData pushes the full schedule payload (events, categories,
// plugins, server_groups, state) to a freshly-authenticated client so the
// frontend has the data it needs before any page activates.
func (h *WebSocketHandler) sendScheduleData(client *ws.Client) {
	eventsMap := h.engine.GetScheduler().GetAllEvents()
	events := make([]interface{}, 0, len(eventsMap))
	privileges, ok := h.currentPrivileges(client)
	if !ok {
		return
	}
	for _, event := range eventsMap {
		if h.engine.CanAccess(privileges, event.Category, event.Target) {
			events = append(events, event)
		}
	}

	categoriesMap, _ := h.engine.LoadCategoriesFromStorage()
	categories := make([]interface{}, 0, len(categoriesMap))
	for id, cat := range categoriesMap {
		if privileges["admin"] || !privileges["cat_limit"] || privileges["cat_"+id] {
			categories = append(categories, cat)
		}
	}

	serverGroupsMap, _ := h.engine.LoadServerGroupsFromStorage()
	serverGroups := make([]interface{}, 0, len(serverGroupsMap))
	for id, group := range serverGroupsMap {
		if privileges["admin"] || !privileges["grp_limit"] || privileges["grp_"+id] {
			serverGroups = append(serverGroups, group)
		}
	}

	storage := h.engine.GetStorage()
	plugins := make([]interface{}, 0)
	if storage != nil {
		items, err := storage.ListGet(context.Background(), "plugins", 0, 10000)
		if err == nil {
			for _, item := range items {
				plugins = append(plugins, item)
			}
		}
	}

	state := h.engine.ScopedState(privileges)

	h.sendResponse(client, "update", map[string]interface{}{
		"schedule":      events,
		"categories":    categories,
		"plugins":       plugins,
		"server_groups": serverGroups,
		"state":         state,
		"epoch":         time.Now().Unix(),
	})
}

// handleSubscribe 处理订阅
func (h *WebSocketHandler) handleSubscribe(client *ws.Client, message *ws.Message) {
	if !client.IsAuthenticated() {
		h.sendError(client, "Authentication required")
		return
	}

	var subData struct {
		Channels []string `json:"channels"`
	}

	if err := json.Unmarshal(message.Data, &subData); err != nil {
		h.sendError(client, "Invalid subscribe data")
		return
	}

	// 订阅频道
	h.mu.Lock()
	for _, channel := range subData.Channels {
		if h.channels[channel] == nil {
			h.channels[channel] = make(map[string]*ws.Client)
		}
		h.channels[channel][client.ID] = client
	}
	h.mu.Unlock()

	h.logger.Debug("Client subscribed to channels", zap.String("client_id", client.ID), zap.Strings("channels", subData.Channels))
	h.sendResponse(client, "subscribed", map[string]interface{}{
		"channels": subData.Channels,
	})
}

// handleUnsubscribe 处理取消订阅
func (h *WebSocketHandler) handleUnsubscribe(client *ws.Client, message *ws.Message) {
	if !client.IsAuthenticated() {
		h.sendError(client, "Authentication required")
		return
	}

	var unsubData struct {
		Channels []string `json:"channels"`
	}

	if err := json.Unmarshal(message.Data, &unsubData); err != nil {
		// 如果没有提供频道列表，取消所有订阅
		h.mu.Lock()
		for channel := range h.channels {
			delete(h.channels[channel], client.ID)
			if len(h.channels[channel]) == 0 {
				delete(h.channels, channel)
			}
		}
		h.mu.Unlock()
		h.sendResponse(client, "unsubscribed", nil)
		return
	}

	// 取消指定频道订阅
	h.mu.Lock()
	for _, channel := range unsubData.Channels {
		if h.channels[channel] != nil {
			delete(h.channels[channel], client.ID)
			if len(h.channels[channel]) == 0 {
				delete(h.channels, channel)
			}
		}
	}
	h.mu.Unlock()

	h.logger.Debug("Client unsubscribed from channels", zap.String("client_id", client.ID), zap.Strings("channels", unsubData.Channels))
	h.sendResponse(client, "unsubscribed", map[string]interface{}{
		"channels": unsubData.Channels,
	})
}

// handlePing 处理 ping
func (h *WebSocketHandler) handlePing(client *ws.Client, message *ws.Message) {
	client.Touch()
	h.sendResponse(client, "pong", nil)
}

// sendStatus 发送状态
func (h *WebSocketHandler) sendStatus(client *ws.Client) {
	if client.IsNodeMaster() {
		if h.AllowMaster(client) {
			h.sendResponse(client, "status", h.engine.socketStatus())
		}
		return
	}
	status, allowed := h.FilterBroadcast(client, "status", h.engine.socketStatus())
	if allowed {
		h.sendResponse(client, "status", status)
	}
}

// sendResponse 发送响应
func (h *WebSocketHandler) sendResponse(client *ws.Client, msgType string, data interface{}) {
	message := map[string]interface{}{
		"type": msgType,
		"data": data,
	}

	jsonData, err := json.Marshal(message)
	if err != nil {
		h.logger.Error("Failed to marshal message", zap.Error(err))
		return
	}

	if err := client.Send(jsonData); err != nil {
		h.logger.Error("Failed to send message to client", zap.String("client_id", client.ID), zap.Error(err))
	}
}

// sendError 发送错误
func (h *WebSocketHandler) sendError(client *ws.Client, errorMsg string) {
	h.sendResponse(client, "error", map[string]interface{}{
		"message": errorMsg,
	})
}

// BroadcastJobUpdate 广播任务更新
func (h *WebSocketHandler) BroadcastJobUpdate(job *types.Job) {
	update := map[string]interface{}{
		"job": job,
	}

	// 广播到所有认证的客户端
	h.broadcastToChannel("job_update", update)
	// 也广播到特定任务的频道
	h.broadcastToChannel(fmt.Sprintf("job:%s", job.ID), update)
}

// BroadcastEventUpdate 广播事件更新
func (h *WebSocketHandler) BroadcastEventUpdate(event *types.Event) {
	update := map[string]interface{}{
		"event": event,
	}

	// 广播到所有认证的客户端
	h.broadcastToChannel("event_update", update)
	// 也广播到特定事件的频道
	h.broadcastToChannel(fmt.Sprintf("event:%s", event.ID), update)
}

// broadcastToChannel 广播到指定频道
func (h *WebSocketHandler) broadcastToChannel(channel string, data interface{}) {
	message := map[string]interface{}{
		"type":    "broadcast",
		"channel": channel,
		"data":    data,
	}

	jsonData, err := json.Marshal(message)
	if err != nil {
		h.logger.Error("Failed to marshal broadcast message", zap.Error(err))
		return
	}

	h.mu.RLock()
	clients := h.channels[channel]
	if clients == nil {
		h.mu.RUnlock()
		return
	}

	// 复制客户端列表，避免长时间持有锁
	clientList := make([]*ws.Client, 0, len(clients))
	for _, client := range clients {
		clientList = append(clientList, client)
	}
	h.mu.RUnlock()

	// 发送消息
	for _, client := range clientList {
		filtered, allowed := h.FilterBroadcast(client, "broadcast", data)
		if !allowed {
			continue
		}
		message["data"] = filtered
		jsonData, _ = json.Marshal(message)
		if err := client.Send(jsonData); err != nil {
			h.logger.Debug("Failed to send broadcast message", zap.String("client_id", client.ID), zap.Error(err))
		}
	}
}

// handleWatchJobLog 处理实时日志订阅
func (h *WebSocketHandler) handleWatchJobLog(client *ws.Client, message *ws.Message) {
	if !client.IsAuthenticated() {
		h.sendError(client, "Authentication required")
		return
	}

	var reqData struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(message.Data, &reqData); err != nil || reqData.JobID == "" {
		h.sendError(client, "Invalid watch_job_log request")
		return
	}

	// 查找任务的日志文件
	h.engine.mu.RLock()
	job, exists := h.engine.activeJobs[reqData.JobID]
	h.engine.mu.RUnlock()

	privileges, authenticated := h.currentPrivileges(client)
	if !authenticated || !exists || !h.engine.CanAccess(privileges, job.Category, job.Target) || job.LogFile == "" {
		h.sendError(client, "Job not found or has no log file")
		return
	}

	cancelCh := make(chan struct{})
	watcher := &logWatcher{
		client: client,
		offset: 0,
		cancel: cancelCh,
	}

	h.mu.Lock()
	if h.logWatchers[reqData.JobID] == nil {
		h.logWatchers[reqData.JobID] = make(map[string]*logWatcher)
	}
	// 如果已有观察者，先取消
	if old, ok := h.logWatchers[reqData.JobID][client.ID]; ok {
		old.stop()
	}
	h.logWatchers[reqData.JobID][client.ID] = watcher
	h.mu.Unlock()

	h.logger.Info("Client watching job log", zap.String("client_id", client.ID), zap.String("job_id", reqData.JobID))
	h.sendResponse(client, "watching_log", map[string]interface{}{"job_id": reqData.JobID})

	go h.tailJobLog(reqData.JobID, job.LogFile, watcher)
}

// handleUnwatchJobLog 处理取消日志订阅
func (h *WebSocketHandler) handleUnwatchJobLog(client *ws.Client, message *ws.Message) {
	var reqData struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(message.Data, &reqData); err != nil || reqData.JobID == "" {
		return
	}

	h.mu.Lock()
	if watchers, ok := h.logWatchers[reqData.JobID]; ok {
		if w, ok := watchers[client.ID]; ok {
			w.stop()
			delete(watchers, client.ID)
			if len(watchers) == 0 {
				delete(h.logWatchers, reqData.JobID)
			}
		}
	}
	h.mu.Unlock()
}

// tailJobLog 持续读取日志文件并推送新内容
func (h *WebSocketHandler) tailJobLog(jobID, logFile string, watcher *logWatcher) {
	defer func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if watchers := h.logWatchers[jobID]; watchers != nil && watchers[watcher.client.ID] == watcher {
			delete(watchers, watcher.client.ID)
			if len(watchers) == 0 {
				delete(h.logWatchers, jobID)
			}
		}
	}()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-watcher.cancel:
			return
		case <-h.engine.ctx.Done():
			return
		case <-ticker.C:
			privileges, ok := h.currentPrivileges(watcher.client)
			if !ok {
				return
			}
			if job, found := h.engine.LookupJob(jobID); !found || !h.engine.CanAccess(privileges, job.Category, job.Target) {
				return
			}
			data, newOffset := h.readLogChunk(logFile, watcher.offset)
			if len(data) > 0 {
				watcher.offset = newOffset
				h.sendResponse(watcher.client, "log_data", map[string]interface{}{
					"job_id": jobID,
					"data":   string(data),
					"offset": newOffset,
				})
			}

			// 检查任务是否仍在运行
			h.engine.mu.RLock()
			_, stillActive := h.engine.activeJobs[jobID]
			h.engine.mu.RUnlock()

			if !stillActive {
				// 发送最后一批数据
				finalData, finalOffset := h.readLogChunk(logFile, watcher.offset)
				if len(finalData) > 0 {
					h.sendResponse(watcher.client, "log_data", map[string]interface{}{
						"job_id": jobID,
						"data":   string(finalData),
						"offset": finalOffset,
					})
				}
				h.sendResponse(watcher.client, "log_complete", map[string]interface{}{
					"job_id": jobID,
				})
				return
			}
		}
	}
}

// readLogChunk 从指定偏移读取日志块
func (h *WebSocketHandler) readLogChunk(logFile string, offset int64) ([]byte, int64) {
	f, err := os.Open(logFile)
	if err != nil {
		return nil, offset
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil || fi.Size() <= offset {
		return nil, offset
	}

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, offset
	}

	maxChunk := int64(64 * 1024) // 64 KB per chunk
	remaining := fi.Size() - offset
	if remaining > maxChunk {
		remaining = maxChunk
	}

	buf := make([]byte, remaining)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		return nil, offset
	}

	return buf[:n], offset + int64(n)
}

// BroadcastToAll 广播到所有认证的客户端
func (h *WebSocketHandler) BroadcastToAll(msgType string, data interface{}) {
	// 通过 WebSocket 服务器广播
	comm := h.engine.GetCommunication()
	if comm != nil && comm.wsServer != nil {
		comm.wsServer.Broadcast(msgType, data)
	}
}

// FilterBroadcast keeps browser updates subject to the same category/group
// restrictions as API reads, and immediately honors revoked sessions/keys.
func (h *WebSocketHandler) FilterBroadcast(client *ws.Client, msgType string, data interface{}) (interface{}, bool) {
	privileges, ok := h.currentPrivileges(client)
	if !ok {
		return nil, false
	}
	if privileges["admin"] {
		return data, true
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, false
	}
	var payload interface{}
	if json.Unmarshal(raw, &payload) != nil {
		return nil, false
	}
	filtered := h.filterReadable(payload, privileges)
	return filtered, filtered != nil
}
func (h *WebSocketHandler) filterReadable(value interface{}, privileges map[string]bool) interface{} {
	switch data := value.(type) {
	case []interface{}:
		out := []interface{}{}
		for _, item := range data {
			if filtered := h.filterReadable(item, privileges); filtered != nil {
				out = append(out, filtered)
			}
		}
		return out
	case map[string]interface{}:
		if category, ok := data["category"].(string); ok {
			target, _ := data["target"].(string)
			if !h.engine.CanAccess(privileges, category, target) {
				return nil
			}
		}
		for key, item := range data {
			if key == "servers" {
				if values, ok := item.(map[string]interface{}); ok {
					out := map[string]interface{}{}
					for hostname, server := range values {
						if privileges["grp_limit"] {
							groupPrivileges := make(map[string]bool, len(privileges))
							for name, enabled := range privileges {
								groupPrivileges[name] = enabled
							}
							groupPrivileges["cat_limit"] = false
							if !h.engine.CanAccess(groupPrivileges, "", hostname) {
								continue
							}
						}
						out[hostname] = h.filterReadable(server, privileges)
					}
					data[key] = out
				}
				continue
			}
			if key == "queue" || key == "event_queue" || key == "eventQueue" {
				if values, ok := item.(map[string]interface{}); ok && (privileges["cat_limit"] || privileges["grp_limit"]) {
					out := map[string]interface{}{}
					for id, value := range values {
						if event, exists := h.engine.GetScheduler().GetEvent(id); exists && h.engine.CanAccess(privileges, event.Category, event.Target) {
							out[id] = value
						}
					}
					data[key] = out
				}
				continue
			}
			if key == "state" && (privileges["cat_limit"] || privileges["grp_limit"]) {
				data[key] = h.engine.ScopedState(privileges)
				continue
			}
			if key == "categories" || key == "server_groups" {
				prefix, limited := "cat_", privileges["cat_limit"]
				if key == "server_groups" {
					prefix, limited = "grp_", privileges["grp_limit"]
				}
				if limited {
					if items, ok := item.([]interface{}); ok {
						out := []interface{}{}
						for _, item := range items {
							if obj, ok := item.(map[string]interface{}); ok {
								id, _ := obj["id"].(string)
								if privileges[prefix+id] {
									out = append(out, item)
								}
							}
						}
						data[key] = out
					}
				}
				continue
			}
			switch key {
			case "schedule", "active_jobs", "job", "event", "rows", "jobs", "data":
				if mapped, ok := item.(map[string]interface{}); ok && key == "active_jobs" {
					out := map[string]interface{}{}
					for id, job := range mapped {
						if v := h.filterReadable(job, privileges); v != nil {
							out[id] = v
						}
					}
					data[key] = out
				} else {
					data[key] = h.filterReadable(item, privileges)
				}
			}
		}
		return data
	}
	return value
}
