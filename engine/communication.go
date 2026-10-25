package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	gorillaWS "github.com/gorilla/websocket"
	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	ws "github.com/soulteary/owlcron/websocket"
	"go.uber.org/zap"
)

// Communication owns authenticated node connections. c.mu guards peer state;
// writeMu serializes Gorilla writes independently of connection reads.
type Communication struct {
	engine     *Engine
	logger     *zap.Logger
	mu         sync.RWMutex
	slaves     map[string]*SlaveConnection
	sockets    map[string]*SocketConnection
	wsServer   *ws.Server
	wsHandler  *WebSocketHandler
	started    bool
	closed     bool
	dispatchMu sync.RWMutex
	wg         sync.WaitGroup
	departed   map[string]int64
}

type SlaveConnection struct {
	Hostname     string
	IP           string
	Socket       *gorillaWS.Conn
	Disabled     bool
	ActiveJobs   map[string]*types.Job
	Data         map[string]interface{}
	LastSeen     int64
	Master       bool
	writeMu      sync.Mutex
	reconnecting bool
	pingPending  bool
	cancel       context.CancelFunc
}

type SocketConnection struct {
	ID       string
	Socket   *gorillaWS.Conn
	Auth     bool
	Master   bool
	User     string
	LastSeen int64
	writeMu  sync.Mutex
}

func NewCommunication(engine *Engine, logger *zap.Logger) (*Communication, error) {
	return &Communication{engine: engine, logger: logger, slaves: make(map[string]*SlaveConnection), sockets: make(map[string]*SocketConnection), departed: make(map[string]int64)}, nil
}
func (c *Communication) Startup() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return nil
	}
	c.wsHandler = NewWebSocketHandler(c.engine, c.logger)
	c.wsServer = ws.NewServer(c.wsHandler, c.logger)
	c.started = true
	return nil
}
func (c *Communication) RegisterRoutes(mux *http.ServeMux) {
	c.mu.RLock()
	server := c.wsServer
	c.mu.RUnlock()
	if server != nil {
		mux.HandleFunc("/ws", server.HandleWebSocket)
		mux.HandleFunc("/cluster/ws", c.serveNodeWebSocket)
	}
}
func (c *Communication) Shutdown() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.ResetCluster()
	c.mu.Lock()
	sockets := c.sockets
	c.sockets = make(map[string]*SocketConnection)
	server := c.wsServer
	c.mu.Unlock()
	for _, socket := range sockets {
		if socket.Socket != nil {
			_ = socket.Socket.Close()
		}
	}
	if server != nil {
		server.Shutdown()
	}
	c.wg.Wait()
}

// ResetCluster cancels outbound peer loops whenever this node relinquishes
// leadership; reconnect loops cannot resurrect a stale master's connections.
func (c *Communication) ResetCluster() {
	c.dispatchMu.Lock()
	c.mu.Lock()
	peers := c.slaves
	c.slaves = make(map[string]*SlaveConnection)
	for _, peer := range peers {
		if peer.cancel != nil {
			peer.cancel()
		}
	}
	c.mu.Unlock()
	c.dispatchMu.Unlock()
	for _, peer := range peers {
		c.mu.RLock()
		conn := peer.Socket
		c.mu.RUnlock()
		if conn != nil {
			_ = conn.Close()
		}
	}
}
func (c *Communication) SetupCluster() {
	if !c.engine.IsMaster() {
		return
	}
	c.mu.Lock()
	hostname := c.engine.Hostname()
	if _, exists := c.slaves[hostname]; !exists {
		c.slaves[hostname] = &SlaveConnection{Hostname: hostname, Master: true, ActiveJobs: map[string]*types.Job{}, Data: map[string]interface{}{}}
	}
	c.mu.Unlock()
	if err := c.LoadServersFromStorage(); err != nil {
		c.logger.Warn("Cannot load peers", zap.Error(err))
	}
}

func (c *Communication) nodeURL(hostname, ip string) string {
	endpoint, _ := clusterNodeEndpoint(hostname, ip, c.engine.config.HTTPPort, c.engine.config.ClusterAllowInsecure)
	return endpoint
}

func (c *Communication) ConnectToSlave(hostname, ip string) error {
	if hostname == "" {
		return fmt.Errorf("hostname is required")
	}
	if hostname == c.engine.Hostname() {
		return nil
	}
	if (c.engine.config.StorageEngine == "" || c.engine.config.StorageEngine == "Filesystem") && !c.engine.config.SharedStorage {
		return fmt.Errorf("worker connections require shared_storage and the common filesystem storage namespace")
	}
	if !c.engine.IsMaster() {
		return fmt.Errorf("only master may connect to workers")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return fmt.Errorf("communication has shut down")
	}
	if peer, exists := c.slaves[hostname]; exists {
		connected := peer.Socket != nil
		c.mu.Unlock()
		if connected {
			return nil
		}
		return fmt.Errorf("worker %s is reconnecting", hostname)
	}
	ctx, cancel := context.WithCancel(c.engine.ctx)
	peer := &SlaveConnection{Hostname: hostname, IP: ip, ActiveJobs: map[string]*types.Job{}, Data: map[string]interface{}{}, LastSeen: time.Now().Unix(), cancel: cancel}
	c.slaves[hostname] = peer
	delete(c.departed, hostname)
	c.wg.Add(1)
	c.mu.Unlock()
	defer c.wg.Done()
	conn, err := c.dialNode(ctx, peer)
	if err != nil {
		c.startReconnect(ctx, peer)
		return err
	}
	if !c.installConnection(peer, conn) {
		return fmt.Errorf("worker connection no longer needed")
	}
	if !c.startTask(func() { c.readSlaveLoop(ctx, peer, conn) }) {
		_ = conn.Close()
	}
	return nil
}

func (c *Communication) dialNode(ctx context.Context, peer *SlaveConnection) (*gorillaWS.Conn, error) {
	endpoint, err := clusterNodeEndpoint(peer.Hostname, peer.IP, c.engine.config.HTTPPort, c.engine.config.ClusterAllowInsecure)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := c.engine.clusterTLS.clientConfig(peer.Hostname)
	if err != nil {
		return nil, err
	}
	dialer := gorillaWS.Dialer{HandshakeTimeout: 5 * time.Second, TLSClientConfig: tlsConfig, Proxy: nil}
	conn, response, err := dialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, fmt.Errorf("cluster connection failed")
	}
	conn.SetReadLimit(2 << 20)
	failure := func(err error) (*gorillaWS.Conn, error) { _ = conn.Close(); return nil, err }
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var challengeMessage ws.Message
	if err = conn.ReadJSON(&challengeMessage); err != nil || challengeMessage.Type != "node_challenge" {
		return failure(fmt.Errorf("invalid node challenge"))
	}
	var challenge struct {
		Hostname  string `json:"hostname"`
		Challenge string `json:"challenge"`
	}
	if err = json.Unmarshal(challengeMessage.Data, &challenge); err != nil || challenge.Hostname != peer.Hostname || len(challenge.Challenge) != 64 {
		return failure(fmt.Errorf("invalid node challenge identity"))
	}
	proof := c.engine.MakeNodeAuthFor(c.engine.Hostname(), true, peer.Hostname, challenge.Challenge)
	if proof.Signature == "" {
		return failure(fmt.Errorf("node authentication is not configured"))
	}
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err = conn.WriteJSON(map[string]interface{}{"type": "auth", "data": proof}); err != nil {
		return failure(fmt.Errorf("node authentication write failed"))
	}

	for {
		var response ws.Message
		if err = conn.ReadJSON(&response); err != nil {
			return failure(err)
		}
		if response.Type == "error" {
			return failure(fmt.Errorf("worker authentication failed"))
		}
		if response.Type != "auth_success" {
			continue
		}
		var auth struct {
			Node NodeAuthData `json:"node"`
		}
		if err = json.Unmarshal(response.Data, &auth); err != nil {
			return failure(err)
		}
		hostname, master, verifyErr := c.engine.VerifyNodeAuthFor(auth.Node, c.engine.Hostname(), proof.Nonce)
		if verifyErr != nil || hostname != peer.Hostname || master {
			return failure(fmt.Errorf("invalid worker identity"))
		}
		_ = conn.SetReadDeadline(time.Now().Add(time.Duration(c.engine.config.MasterPingTimeout) * time.Second))
		return conn, nil
	}
}
func (c *Communication) installConnection(peer *SlaveConnection, conn *gorillaWS.Conn) bool {
	c.dispatchMu.Lock()
	defer c.dispatchMu.Unlock()
	if !c.engine.ValidateNodeLeadership(c.engine.Hostname(), c.engine.LeadershipToken()) {
		_ = conn.Close()
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.slaves[peer.Hostname] != peer || !c.engine.IsMaster() {
		_ = conn.Close()
		return false
	}
	peer.Socket = conn
	peer.LastSeen = time.Now().Unix()
	return true
}
func (c *Communication) readSlaveLoop(ctx context.Context, peer *SlaveConnection, conn *gorillaWS.Conn) {
	defer func() {
		_ = conn.Close()
		c.mu.Lock()
		current := c.slaves[peer.Hostname] == peer && peer.Socket == conn
		if current {
			peer.Socket = nil
		}
		c.mu.Unlock()
		if current {
			c.handleSlaveDisconnect(peer)
			c.startReconnect(ctx, peer)
		}
	}()
	for {
		var message map[string]interface{}
		// This deadline bounds waiting for the next frame, not time spent
		// committing the previous frame to a slow backend.
		_ = conn.SetReadDeadline(time.Now().Add(time.Duration(c.engine.config.MasterPingTimeout) * time.Second))
		if err := conn.ReadJSON(&message); err != nil {
			return
		}
		c.mu.Lock()
		peer.LastSeen = time.Now().Unix()
		c.mu.Unlock()
		c.handleSlaveMessageFrom(peer, conn, message)
	}
}

func (c *Communication) handleSlaveDisconnect(peer *SlaveConnection) {
	// A transport break does not mean that a running subprocess has failed. Keep
	// reservations while reconnecting and let its durable completion arrive.
	c.logger.Warn("Worker disconnected; awaiting recovery", zap.String("hostname", peer.Hostname))
}
func (c *Communication) startReconnect(ctx context.Context, peer *SlaveConnection) {
	c.mu.Lock()
	if peer.reconnecting || c.slaves[peer.Hostname] != peer || c.closed {
		c.mu.Unlock()
		return
	}
	peer.reconnecting = true
	c.mu.Unlock()
	c.startTask(func() {
		defer func() { c.mu.Lock(); peer.reconnecting = false; c.mu.Unlock() }()
		backoff := time.Second
		for {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if !c.engine.IsMaster() {
				return
			}
			conn, err := c.dialNode(ctx, peer)
			if err != nil {
				if backoff < 30*time.Second {
					backoff *= 2
				}
				continue
			}
			if !c.installConnection(peer, conn) {
				return
			}
			// Clear the reconnect flag before handing off so a quick read failure can
			// start a replacement loop immediately.
			c.mu.Lock()
			peer.reconnecting = false
			c.mu.Unlock()
			if !c.startTask(func() { c.readSlaveLoop(ctx, peer, conn) }) {
				_ = conn.Close()
			}
			return
		}
	})
}

func (c *Communication) SendToSlave(hostname, messageType string, data interface{}) error {
	if !c.engine.IsMaster() || !c.engine.isRegisteredNode(hostname) || !c.engine.ValidateNodeLeadership(c.engine.Hostname(), c.engine.LeadershipToken()) {
		return fmt.Errorf("current leadership required")
	}

	c.mu.RLock()
	peer := c.slaves[hostname]
	if peer == nil || peer.Socket == nil || peer.Disabled {
		c.mu.RUnlock()
		return fmt.Errorf("worker %s is unavailable", hostname)
	}
	conn := peer.Socket
	c.mu.RUnlock()
	peer.writeMu.Lock()
	defer peer.writeMu.Unlock()
	// Lease verification may involve a remote storage request. Neither it nor
	// a slow socket write may hold the global peer lock: inbound heartbeats,
	// other peers and connection retirement all need that lock to progress.
	if !c.engine.IsMaster() || !c.engine.isRegisteredNode(hostname) || !c.engine.ValidateNodeLeadership(c.engine.Hostname(), c.engine.LeadershipToken()) {
		return fmt.Errorf("worker connection is no longer authoritative")
	}
	c.mu.RLock()
	current := !c.closed && c.slaves[hostname] == peer && peer.Socket == conn && !peer.Disabled
	c.mu.RUnlock()
	if !current {
		return fmt.Errorf("worker connection is no longer authoritative")
	}
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return conn.WriteJSON(map[string]interface{}{"type": messageType, "data": data})
}
func (c *Communication) SendToMaster(messageType string, data interface{}) error {
	c.mu.RLock()
	server := c.wsServer
	c.mu.RUnlock()
	if server == nil {
		return fmt.Errorf("no master connection")
	}
	return server.SendToMaster(messageType, data)
}
func (c *Communication) handleSlaveMessage(peer *SlaveConnection, message map[string]interface{}) {
	c.handleSlaveMessageFrom(peer, nil, message)
}
func (c *Communication) handleSlaveMessageFrom(peer *SlaveConnection, conn *gorillaWS.Conn, message map[string]interface{}) {
	c.dispatchMu.RLock()
	defer c.dispatchMu.RUnlock()
	c.mu.RLock()
	current := !c.closed && c.slaves[peer.Hostname] == peer && (conn == nil || peer.Socket == conn)
	c.mu.RUnlock()
	if !current || !c.engine.isRegisteredNode(peer.Hostname) || !c.engine.ValidateNodeLeadership(c.engine.Hostname(), c.engine.LeadershipToken()) || !c.engine.IsMaster() {
		return
	}
	msgType, _ := message["type"].(string)
	data, _ := message["data"].(map[string]interface{})
	switch msgType {
	case "status":
		c.mu.Lock()
		peer.Data = cloneData(data)
		c.mu.Unlock()
		if jobs, ok := data["active_jobs"].(map[string]interface{}); ok {
			c.mergeSlaveActiveJobs(peer, jobs)
		}

	case "job_update":
		c.handleSlaveJobUpdate(peer, data)
	case "job_rejected":
		c.handleSlaveJobRejected(peer, data)
	case "finish_job":
		c.handleSlaveFinishJob(peer, data)
	case "fetch_job_log":
		c.handleFetchJobLog(peer, data)
	}
}
func (c *Communication) handleSlaveJobRejected(peer *SlaveConnection, data map[string]interface{}) {
	id, _ := data["id"].(string)
	token, _ := data["dispatch_token"].(string)
	reason, _ := data["reason"].(string)
	if id == "" || token == "" {
		return
	}
	c.engine.jobManager.finishRejectedRemoteJob(peer.Hostname, id, token, c.engine.RedactSensitiveText(reason))
	c.mu.Lock()
	if job := peer.ActiveJobs[id]; job != nil && job.DispatchToken == token {
		delete(peer.ActiveJobs, id)
	}
	c.mu.Unlock()
}
func (c *Communication) handleFetchJobLog(peer *SlaveConnection, data map[string]interface{}) {
	id, _ := data["job_id"].(string)
	if id == "" {
		return
	}
	job, found := c.engine.LookupJob(id)
	if !found || job.Hostname != peer.Hostname {
		return
	}
	if deleted, err := c.engine.IsJobLogDeleted(id); err != nil || deleted {
		return
	}
	offset := int64(0)
	if v, ok := data["offset"].(float64); ok && v >= 0 {
		offset = int64(v)
	}
	logData, total, err := c.engine.ReadJobLogRange(context.Background(), id, offset, storage.JobLogChunkSize)
	if err != nil {
		return
	}
	_ = c.SendToSlave(peer.Hostname, "job_log_data", map[string]interface{}{"job_id": id, "log": string(logData), "offset": offset, "next_offset": offset + int64(len(logData)), "total_size": total})
}
func (c *Communication) mergeSlaveActiveJobs(peer *SlaveConnection, jobs map[string]interface{}) {
	parsed := make(map[string]*types.Job)
	for id, raw := range jobs {
		data, _ := json.Marshal(raw)
		var job types.Job
		if json.Unmarshal(data, &job) != nil || job.ID != id || job.Hostname != peer.Hostname || job.Complete {
			continue
		}
		parsed[id] = &job
	}
	c.mu.Lock()
	peer.ActiveJobs = parsed
	c.mu.Unlock()
	c.engine.mu.Lock()
	for id, job := range parsed {
		if _, finished := c.engine.deadJobs[id]; finished {
			continue
		}
		c.engine.activeJobs[id] = types.CloneJob(job)
	}
	c.engine.mu.Unlock()
}
func (c *Communication) handleSlaveJobUpdate(peer *SlaveConnection, data map[string]interface{}) {
	id, _ := data["id"].(string)
	if id == "" {
		return
	}
	c.engine.mu.Lock()
	defer c.engine.mu.Unlock()
	if job := c.engine.activeJobs[id]; job != nil && job.Hostname == peer.Hostname {
		if progress, ok := data["progress"].(float64); ok && progress >= 0 && progress <= 1 {
			job.Progress = progress
		}
		if pid, ok := data["pid"].(float64); ok {
			job.PID = int(pid)
		}
	}
}
func (c *Communication) handleSlaveFinishJob(peer *SlaveConnection, data map[string]interface{}) {
	jobData := data
	if wrapped, ok := data["job"].(map[string]interface{}); ok {
		jobData = wrapped
	}
	raw, _ := json.Marshal(jobData)
	var job types.Job
	if json.Unmarshal(raw, &job) != nil || job.ID == "" || job.Hostname != peer.Hostname || !job.Complete {
		return
	}
	if deleted, err := c.engine.IsJobHistoryDeleted(job.ID); err != nil {
		return
	} else if deleted {
		c.mu.Lock()
		delete(peer.ActiveJobs, job.ID)
		c.mu.Unlock()
		_ = c.SendToSlave(peer.Hostname, "finish_job_ack", map[string]interface{}{"id": job.ID})
		return
	}
	existing, found := c.engine.LookupJob(job.ID)
	if !found || existing.Hostname != peer.Hostname {
		return
	}
	if completed, ok := c.engine.GetDeadJob(job.ID); ok && completed.Hostname == peer.Hostname && completed.Complete && completed.StorageError == "" {
		// This process already committed the entire completion. Preserve that
		// first snapshot and acknowledge replays without re-reading the log or
		// repeating history/notification/reservation writes.
		c.ackCompletedRemoteJob(peer, job.ID)
		return
	}
	if stored, ok := data["log_stored"].(bool); ok && stored {
		if err := c.engine.requireDurableJobLog(context.Background(), job.ID, job.LogFileSize, true); err != nil {
			return
		}
		job.LogFile = ""
	}
	if logText, ok := data["log"].(string); ok {
		if err := c.engine.writeJobLog(context.Background(), job.ID, []byte(logText)); err != nil {
			return
		}
		job.LogFile = ""
		job.LogFileSize = int64(len(logText))
	}
	job.LogFile = ""
	durable := false
	if !existing.Complete {
		durable = c.engine.jobManager.CompleteRemoteJob(&job)
	}
	// A job record alone is insufficient: all history indexes and removal of
	// the active reservation must commit before releasing the worker's outbox.
	// CompleteRemoteJob already committed this transaction on success. A
	// duplicate delivery or failed first commit still needs the recovery pass.
	if !durable {
		if err := c.engine.jobManager.persistCompletion(&job); err != nil {
			return
		}
		job.StorageError = ""
		c.engine.AddDeadJob(&job)
	}
	c.ackCompletedRemoteJob(peer, job.ID)
}

func (c *Communication) ackCompletedRemoteJob(peer *SlaveConnection, id string) {
	// The durable transaction has already removed its reservation. Reconcile
	// stale in-memory reservations too before exposing the acknowledged result.
	c.engine.mu.Lock()
	delete(c.engine.activeJobs, id)
	c.engine.mu.Unlock()
	c.mu.Lock()
	delete(peer.ActiveJobs, id)
	c.mu.Unlock()
	_ = c.SendToSlave(peer.Hostname, "finish_job_ack", map[string]interface{}{"id": id})
}
func (c *Communication) loadServerList() ([]string, error) {
	if c.engine.storage == nil {
		return nil, fmt.Errorf("storage not initialized")
	}
	items, err := c.engine.storage.ListGet(context.Background(), "servers", 0, 10000)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var result []string
	for index, item := range items {
		raw, err := json.Marshal(item)
		if err != nil {
			return nil, fmt.Errorf("invalid server record %d: %w", index, err)
		}
		var server types.Server
		if err := json.Unmarshal(raw, &server); err != nil {
			return nil, fmt.Errorf("invalid server record %d: %w", index, err)
		}
		if server.Hostname == "" {
			return nil, fmt.Errorf("server record %d has no hostname", index)
		}
		if !seen[server.Hostname] && !server.Disabled {
			seen[server.Hostname] = true
			result = append(result, server.Hostname)
		}
	}
	return result, nil
}
func (c *Communication) SendMasterPings() {
	if !c.engine.IsMaster() {
		return
	}
	now := time.Now().Unix()
	c.engine.mu.Lock()
	c.engine.multi.LastPingSent = now
	c.engine.mu.Unlock()
	// A worker stalled behind storage or a socket write must not stall the
	// lease-renewal loop or heartbeats for every other worker. Coalesce ticks
	// while one send is pending; shutdown tracks the bounded tasks in c.wg.
	c.mu.Lock()
	var peers []*SlaveConnection
	for _, peer := range c.slaves {
		if peer.Socket != nil && !peer.Disabled && !peer.pingPending {
			peer.pingPending = true
			peers = append(peers, peer)
		}
	}
	c.mu.Unlock()
	for _, peer := range peers {
		if !c.startTask(func() {
			defer func() {
				c.mu.Lock()
				// The flag belongs to this connection generation, never a
				// replacement registered under the same hostname.
				peer.pingPending = false
				c.mu.Unlock()
			}()
			c.mu.RLock()
			current := c.slaves[peer.Hostname] == peer
			c.mu.RUnlock()
			if current {
				if err := c.SendToSlave(peer.Hostname, "master_ping", map[string]interface{}{"hostname": c.engine.Hostname(), "epoch": time.Now().Unix()}); err != nil {
					c.logger.Debug("Worker heartbeat failed", zap.String("hostname", peer.Hostname), zap.Error(err))
				}
			}
		}) {
			c.mu.Lock()
			peer.pingPending = false
			c.mu.Unlock()
		}
	}
}
func (c *Communication) BroadcastToSlaves(messageType string, data interface{}) {
	c.mu.RLock()
	var names []string
	for name, peer := range c.slaves {
		if peer.Socket != nil && !peer.Disabled {
			names = append(names, name)
		}
	}
	c.mu.RUnlock()
	for _, name := range names {
		if err := c.SendToSlave(name, messageType, data); err != nil {
			c.logger.Debug("Worker send failed", zap.String("hostname", name), zap.Error(err))
		}
	}
}
func (c *Communication) GetAllServers() map[string]*types.Server {
	state := c.engine.GetState()
	multi := c.engine.GetMulti()
	hostname := c.engine.Hostname()
	uptime := int64(0)
	if started := state.Stats["started"]; started > 0 {
		uptime = time.Now().Unix() - int64(started)
		if uptime < 0 {
			uptime = 0
		}
	}
	result := map[string]*types.Server{hostname: {Hostname: hostname, Master: multi.Master, Uptime: uptime, Data: multi.Data}}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for name, peer := range c.slaves {
		if name != hostname {
			// Peer.Data holds the status envelope. Server.Data only contains
			// resource metrics, without recursively embedding jobs or servers.
			metrics, _ := peer.Data["data"].(map[string]interface{})
			master, _ := peer.Data["master"].(bool)
			var peerUptime int64
			switch value := peer.Data["uptime"].(type) {
			case float64:
				peerUptime = int64(value)
			case int64:
				peerUptime = value
			}
			if peerUptime < 0 {
				peerUptime = 0
			}
			result[name] = &types.Server{Hostname: name, IP: peer.IP, Master: master, Uptime: peerUptime, Disabled: peer.Disabled || peer.Socket == nil, Data: cloneData(metrics)}
		}
	}
	return result
}
func (c *Communication) AddSocket(id string, conn *gorillaWS.Conn) *SocketConnection {
	c.mu.Lock()
	defer c.mu.Unlock()
	socket := &SocketConnection{ID: id, Socket: conn, LastSeen: time.Now().Unix()}
	c.sockets[id] = socket
	return socket
}
func (c *Communication) RemoveSocket(id string) {
	c.mu.Lock()
	socket := c.sockets[id]
	delete(c.sockets, id)
	c.mu.Unlock()
	if socket != nil && socket.Socket != nil {
		_ = socket.Socket.Close()
	}
}
func (c *Communication) BroadcastToSockets(messageType string, data interface{}) {
	// Browser connections are owned by ws.Server, not the obsolete socket map.
	c.mu.RLock()
	handler := c.wsHandler
	c.mu.RUnlock()
	if handler != nil {
		handler.BroadcastToAll(messageType, data)
	}
}
func (c *Communication) GetSlaveConnection(hostname string) (*SlaveConnection, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	peer, found := c.slaves[hostname]
	if !found {
		return nil, false
	}
	copy := &SlaveConnection{Hostname: peer.Hostname, IP: peer.IP, Socket: peer.Socket, Disabled: peer.Disabled, LastSeen: peer.LastSeen, Master: peer.Master, Data: cloneData(peer.Data), ActiveJobs: map[string]*types.Job{}}
	for id, job := range peer.ActiveJobs {
		copy.ActiveJobs[id] = types.CloneJob(job)
	}
	return copy, true
}
func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
			return ipnet.IP.String()
		}
	}
	return ""
}

// ExpireDisconnectedJobs releases reservations only after the recovery window.
// An uncertain remote execution is never retried automatically.
func (c *Communication) ExpireDisconnectedJobs(now time.Time) {
	if !c.engine.IsMaster() {
		return
	}
	timeout := c.engine.config.DeadJobTimeout
	if timeout <= 0 {
		timeout = 120
	}
	c.mu.RLock()
	expired := map[string]bool{}
	for name, peer := range c.slaves {
		if peer.Socket == nil && peer.LastSeen > 0 && now.Unix()-peer.LastSeen >= int64(timeout) {
			expired[name] = true
		}
	}
	for name, lastSeen := range c.departed {
		if now.Unix()-lastSeen >= int64(timeout) {
			expired[name] = true
		}
	}
	c.mu.RUnlock()
	for _, job := range c.engine.GetActiveJobs() {
		if !expired[job.Hostname] {
			continue
		}
		job.Code = 1
		job.Complete = true
		job.AbortReason = "Worker did not reconnect before the recovery timeout"
		job.Description = job.AbortReason
		job.Retries = 0
		job.LogFile = ""
		c.engine.jobManager.CompleteRemoteJob(job)
	}
}

// DisconnectSlave removes transport and cancels reconnection while preserving
// dispatched reservations through the normal recovery window.
func (c *Communication) DisconnectSlave(hostname string) {
	c.mu.Lock()
	peer := c.slaves[hostname]
	delete(c.slaves, hostname)
	var conn *gorillaWS.Conn
	if peer != nil {
		conn = peer.Socket
		if peer.cancel != nil {
			peer.cancel()
		}
		c.departed[hostname] = time.Now().Unix()
	}
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// startTask serializes WaitGroup admission with Shutdown's closed gate.
func (c *Communication) startTask(fn func()) bool {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}
	c.wg.Add(1)
	c.mu.Unlock()
	go func() { defer c.wg.Done(); fn() }()
	return true
}
