package websocket

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// Server WebSocket 服务器
type Server struct {
	upgrader websocket.Upgrader
	clients  map[string]*Client
	mu       sync.RWMutex
	logger   *zap.Logger
	handler  Handler
	stopping bool
	pending  int
	readers  sync.WaitGroup
}

// Handler 消息处理器接口
type Handler interface {
	OnConnect(client *Client)
	OnDisconnect(client *Client)
	OnMessage(client *Client, message *Message)
	OnError(client *Client, err error)
}

// Client WebSocket 客户端
type Client struct {
	NodeTLS        *tls.ConnectionState
	NodeEndpoint   bool
	ID             string
	Conn           *websocket.Conn
	Auth           bool
	Master         bool
	User           string
	LastSeen       int64
	Node           bool
	Privileges     map[string]bool
	Credential     string
	CredentialKind string
	Expires        int64
	mu             sync.Mutex
	server         *Server
}

// Message 消息结构
type Message struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// NewServer 创建 WebSocket 服务器
func NewServer(handler Handler, logger *zap.Logger) *Server {
	return &Server{
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				origin := r.Header.Get("Origin")
				if origin == "" {
					return true
				}
				u, err := url.Parse(origin)
				return err == nil && u.Host == r.Host && (u.Scheme == "http" || u.Scheme == "https")
			},
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
		clients: make(map[string]*Client),
		logger:  logger,
		handler: handler,
	}
}

// HandleWebSocket 处理 WebSocket 连接
func (s *Server) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	s.handleWebSocket(w, r, false)
}

// HandleNodeWebSocket is a separate authentication surface for cluster peers.
func (s *Server) HandleNodeWebSocket(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Origin") != "" {
		http.Error(w, "node endpoint rejects browser origins", http.StatusForbidden)
		return
	}
	s.handleWebSocket(w, r, true)
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request, nodeEndpoint bool) {
	s.mu.Lock()
	if s.stopping || len(s.clients)+s.pending >= 1024 {
		s.mu.Unlock()
		http.Error(w, "WebSocket service unavailable", http.StatusServiceUnavailable)
		return
	}
	s.pending++
	s.readers.Add(1)
	s.mu.Unlock()
	launched := false
	defer func() {
		if !launched {
			s.readers.Done()
		}
	}()
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.mu.Lock()
		s.pending--
		s.mu.Unlock()
		s.logger.Error("Failed to upgrade connection", zap.Error(err))
		return
	}

	clientID := generateClientID()
	client := &Client{
		NodeTLS:      r.TLS,
		NodeEndpoint: nodeEndpoint,
		ID:           clientID,
		Conn:         conn,
		Auth:         false,
		LastSeen:     time.Now().Unix(),
		server:       s,
	}

	s.mu.Lock()
	s.pending--
	if s.stopping {
		s.mu.Unlock()
		conn.Close()
		return
	}
	s.clients[clientID] = client
	s.mu.Unlock()

	s.logger.Info("New WebSocket client connected", zap.String("client_id", clientID))

	// 调用处理器
	if s.handler != nil {
		s.handler.OnConnect(client)
	}

	// 启动读写循环
	launched = true
	go func() { defer s.readers.Done(); s.readLoop(client) }()
}

// readLoop 读取循环
func (s *Server) readLoop(client *Client) {
	defer func() {
		s.removeClient(client.ID)
		if s.handler != nil {
			s.handler.OnDisconnect(client)
		}
		client.Conn.Close()
	}()

	authDeadline := time.Now().Add(10 * time.Second)
	setDeadline := func() {
		deadline := authDeadline
		if client.IsAuthenticated() {
			deadline = time.Now().Add(60 * time.Second)
		}
		client.Conn.SetReadDeadline(deadline)
	}
	setDeadline()
	client.Conn.SetReadLimit(1024 * 1024)
	client.Conn.SetPongHandler(func(string) error {
		setDeadline()
		return nil
	})

	for {
		_, data, err := client.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				s.logger.Error("WebSocket read error", zap.Error(err))
				if s.handler != nil {
					s.handler.OnError(client, err)
				}
			}
			break
		}

		var message Message
		if err := json.Unmarshal(data, &message); err != nil {
			s.logger.Error("Failed to unmarshal message", zap.Error(err))
			continue
		}

		client.Touch()
		setDeadline()

		if s.handler != nil {
			s.handler.OnMessage(client, &message)
		}
		setDeadline()
	}
}

// Send 发送消息给客户端（接受 []byte）
func (c *Client) Send(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Conn == nil {
		return fmt.Errorf("client is disconnected")
	}

	c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return c.Conn.WriteMessage(websocket.TextMessage, data)
}

// SendMessage 发送消息给客户端（接受类型和数据）
func (c *Client) SendMessage(messageType string, data interface{}) error {
	message := Message{
		Type: messageType,
	}

	var err error
	message.Data, err = json.Marshal(data)
	if err != nil {
		return err
	}

	jsonData, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return c.Send(jsonData)
}

// Broadcast 广播消息给所有认证的客户端
func (s *Server) Broadcast(messageType string, data interface{}) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	message := Message{
		Type: messageType,
	}

	var err error
	message.Data, err = json.Marshal(data)
	if err != nil {
		s.logger.Error("Failed to marshal broadcast data", zap.Error(err))
		return
	}

	jsonData, err := json.Marshal(message)
	if err != nil {
		s.logger.Error("Failed to marshal broadcast message", zap.Error(err))
		return
	}

	for _, client := range s.clients {
		if client.IsAuthenticated() {
			clientData := jsonData
			if filter, ok := s.handler.(interface {
				FilterBroadcast(*Client, string, interface{}) (interface{}, bool)
			}); ok {
				filtered, allowed := filter.FilterBroadcast(client, messageType, data)
				if !allowed {
					continue
				}
				clientData, err = json.Marshal(map[string]interface{}{"type": messageType, "data": filtered})
				if err != nil {
					continue
				}
			}
			if err := client.Send(clientData); err != nil {
				s.logger.Debug("Failed to send broadcast message", zap.String("client_id", client.ID), zap.Error(err))
			}
		}
	}
}

// BroadcastToMaster 广播消息给主服务器连接
func (s *Server) BroadcastToMaster(messageType string, data interface{}) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	message := Message{
		Type: messageType,
	}

	var err error
	message.Data, err = json.Marshal(data)
	if err != nil {
		s.logger.Error("Failed to marshal broadcast data", zap.Error(err))
		return
	}

	jsonData, err := json.Marshal(message)
	if err != nil {
		s.logger.Error("Failed to marshal broadcast message", zap.Error(err))
		return
	}

	for _, client := range s.clients {
		if client.IsNodeMaster() && s.allowMaster(client) {
			if err := client.Send(jsonData); err != nil {
				s.logger.Debug("Failed to send broadcast message", zap.String("client_id", client.ID), zap.Error(err))
			}
		}
	}
}

// SendToMaster reports transport failure so workers can retain durable results.
func (s *Server) SendToMaster(messageType string, data interface{}) error {
	s.mu.RLock()
	clients := make([]*Client, 0, len(s.clients))
	for _, client := range s.clients {
		if client.IsNodeMaster() && s.allowMaster(client) {
			clients = append(clients, client)
		}
	}
	s.mu.RUnlock()
	if len(clients) == 0 {
		return fmt.Errorf("no authenticated master connection")
	}
	var lastErr error
	for _, client := range clients {
		if err := client.SendMessage(messageType, data); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return lastErr
}

func (s *Server) allowMaster(client *Client) bool {
	if filter, ok := s.handler.(interface{ AllowMaster(*Client) bool }); ok {
		return filter.AllowMaster(client)
	}
	return true
}

// Authentication state is shared by receive and broadcast goroutines.
func (c *Client) SetAuthentication(user string, node, master bool, privileges map[string]bool, kind, credential string, expires int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Auth = true
	c.User = user
	c.Node = node
	c.Master = node && master
	c.Privileges = privileges
	c.CredentialKind = kind
	c.Credential = credential
	c.Expires = expires
}
func (c *Client) ClearAuthentication() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Auth = false
	c.Master = false
	c.Node = false
}
func (c *Client) Identity() (bool, bool, bool, string, map[string]bool, string, string, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := make(map[string]bool, len(c.Privileges))
	for key, value := range c.Privileges {
		p[key] = value
	}
	return c.Auth, c.Node, c.Master, c.User, p, c.CredentialKind, c.Credential, c.Expires
}
func (c *Client) IsAuthenticated() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.Auth }
func (c *Client) IsNodeMaster() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Auth && c.Node && c.Master
}
func (c *Client) Touch() { c.mu.Lock(); defer c.mu.Unlock(); c.LastSeen = time.Now().Unix() }

// removeClient 移除客户端
func (s *Server) removeClient(clientID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clients, clientID)
}

// GetClient 获取客户端
func (s *Server) GetClient(clientID string) *Client {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clients[clientID]
}

// GetClients 获取所有客户端
func (s *Server) GetClients() map[string]*Client {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]*Client)
	for k, v := range s.clients {
		result[k] = v
	}
	return result
}

// generateClientID 生成客户端ID
var clientSequence atomic.Uint64

func generateClientID() string {
	return fmt.Sprintf("%x-%x", time.Now().UnixNano(), clientSequence.Add(1))
}

// Shutdown prevents new connections and waits for all message handlers before returning.
func (s *Server) Shutdown() {
	s.mu.Lock()
	s.stopping = true
	clients := make([]*Client, 0, len(s.clients))
	for _, c := range s.clients {
		clients = append(clients, c)
	}
	s.mu.Unlock()
	for _, c := range clients {
		if c.Conn != nil {
			c.Conn.Close()
		}
	}
	s.readers.Wait()
}
