package engine

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Discovery 服务发现管理器
type Discovery struct {
	engine *Engine
	logger *zap.Logger
	mu     sync.RWMutex

	conn          *net.UDPConn
	nearbyServers map[string]*NearbyServer
	broadcastIP   string

	broadcastTicker *time.Ticker
	lastBroadcast   int64
	lifecycleMu     sync.Mutex
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

// NearbyServer 附近服务器信息
type NearbyServer struct {
	Hostname string `json:"hostname"`
	IP       string `json:"ip"`
	LastSeen int64  `json:"last_seen"`
}

// discoveryMessage UDP 心跳消息格式（与 example 兼容）
type discoveryMessage struct {
	Action   string `json:"action"`
	Hostname string `json:"hostname"`
	IP       string `json:"ip"`
}

func NewDiscovery(engine *Engine, logger *zap.Logger) (*Discovery, error) {
	return &Discovery{
		engine:        engine,
		logger:        logger,
		nearbyServers: make(map[string]*NearbyServer),
		broadcastIP:   "255.255.255.255",
		lastBroadcast: 0,
	}, nil
}

func (d *Discovery) Startup() error {
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	if d.cancel != nil {
		return nil
	}
	ctx, cancel := context.WithCancel(d.engine.ctx)
	d.cancel = cancel
	if d.engine.config.UDPBroadcastPort == 0 {
		d.logger.Info("UDP broadcast disabled")
		return nil
	}

	d.logger.Info("Discovery manager starting up", zap.Int("port", d.engine.config.UDPBroadcastPort))

	d.broadcastIP = d.calcBroadcastIP()
	d.logger.Info("Using broadcast IP", zap.String("ip", d.broadcastIP))

	if err := d.startUDPListener(); err != nil {
		d.logger.Warn("Failed to start UDP listener, discovery will be limited", zap.Error(err))
		return nil
	}

	d.broadcastTicker = time.NewTicker(time.Duration(d.engine.config.MasterPingFreq) * time.Second)
	d.wg.Add(1)
	go d.broadcastLoop(ctx)

	return nil
}

func (d *Discovery) Shutdown() {
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	if d.cancel != nil {
		d.cancel()
	}
	if d.broadcastTicker != nil {
		d.broadcastTicker.Stop()
	}
	if d.conn != nil {
		_ = d.conn.Close()
	}
	d.wg.Wait()
	d.cancel = nil
}

func (d *Discovery) startUDPListener() error {
	addr := &net.UDPAddr{
		IP:   net.IPv4(0, 0, 0, 0),
		Port: d.engine.config.UDPBroadcastPort,
	}
	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		return err
	}
	d.conn = conn
	d.wg.Add(1)
	go d.receiveLoop(conn)
	return nil
}

func (d *Discovery) receiveLoop(conn *net.UDPConn) {
	defer d.wg.Done()
	buffer := make([]byte, 4096)
	for {
		n, addr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			if d.engine.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			d.logger.Error("UDP read error", zap.Error(err))
			continue
		}
		d.handleUDPMessage(buffer[:n], addr)
	}
}

func (d *Discovery) handleUDPMessage(data []byte, addr *net.UDPAddr) {
	if len(data) == 0 || data[0] != '{' {
		return
	}

	var msg discoveryMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		d.logger.Debug("Failed to parse UDP message", zap.Error(err))
		return
	}

	if msg.Action != "heartbeat" || msg.Hostname == "" {
		return
	}

	myHostname := d.engine.Hostname()
	if msg.Hostname == myHostname {
		return
	}

	ip := msg.IP
	if ip == "" {
		ip = addr.IP.String()
	}

	d.updateNearbyServer(msg.Hostname, ip)
}

func (d *Discovery) broadcastLoop(ctx context.Context) {
	defer d.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.broadcastTicker.C:
			d.broadcastHeartbeat()
			d.cleanupExpired()
		}
	}
}

func (d *Discovery) broadcastHeartbeat() {
	if d.engine.GetMulti().Cluster {
		return
	}

	now := time.Now().Unix()
	freq := int64(d.engine.config.MasterPingFreq)
	if freq <= 0 {
		freq = 20
	}
	if now-d.lastBroadcast < freq {
		return
	}
	d.lastBroadcast = now

	msg := discoveryMessage{
		Action:   "heartbeat",
		Hostname: d.engine.Hostname(),
		IP:       getLocalIP(),
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return
	}
	payload = append(payload, '\n')
	d.sendUDPBroadcast(payload)
}

func (d *Discovery) sendUDPBroadcast(data []byte) error {
	addr := &net.UDPAddr{
		IP:   net.ParseIP(d.broadcastIP),
		Port: d.engine.config.UDPBroadcastPort,
	}
	conn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Write(data)
	return err
}

func (d *Discovery) updateNearbyServer(hostname, ip string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.nearbyServers[hostname]; !exists {
		d.logger.Info("Discovered nearby server", zap.String("hostname", hostname), zap.String("ip", ip))
	}
	d.nearbyServers[hostname] = &NearbyServer{
		Hostname: hostname,
		IP:       ip,
		LastSeen: time.Now().Unix(),
	}
}

// cleanupExpired 清理长时间未发心跳的邻居
func (d *Discovery) cleanupExpired() {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now().Unix()
	timeout := int64(d.engine.config.MasterPingTimeout)
	if timeout <= 0 {
		timeout = 80
	}
	for hostname, server := range d.nearbyServers {
		if now-server.LastSeen > timeout*3 {
			delete(d.nearbyServers, hostname)
			d.logger.Info("Nearby server expired", zap.String("hostname", hostname))
		}
	}
}

// calcBroadcastIP 从网络接口计算广播地址
func (d *Discovery) calcBroadcastIP() string {
	if d.engine.config.BroadcastIP != "" {
		return d.engine.config.BroadcastIP
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return "255.255.255.255"
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			ip4 := ipNet.IP.To4()
			if ip4 == nil {
				continue
			}
			broadcast := make(net.IP, 4)
			for i := range ip4 {
				broadcast[i] = ip4[i] | ^ipNet.Mask[i]
			}
			return broadcast.String()
		}
	}
	return "255.255.255.255"
}

func (d *Discovery) GetNearbyServers() map[string]*NearbyServer {
	d.mu.RLock()
	defer d.mu.RUnlock()
	result := make(map[string]*NearbyServer)
	for k, v := range d.nearbyServers {
		copy := *v
		result[k] = &copy
	}
	return result
}
