package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

// Child runs the production entry point in a distinct address space, including
// normal configuration, HTTP serving, signals and storage recovery.
func TestProcessDaemonChild(t *testing.T) {
	if os.Getenv("OWLCRON_PROCESS_ACCEPTANCE_CHILD") != "1" {
		return
	}
	path := os.Getenv("OWLCRON_PROCESS_ACCEPTANCE_CONFIG")
	// Diagnostics are available only in this test-only daemon entry point.
	// No production HTTP/debug endpoint is exposed.
	snapshots := make(chan os.Signal, 1)
	stopSnapshots := make(chan struct{})
	signal.Notify(snapshots, syscall.SIGUSR1)
	defer signal.Stop(snapshots)
	defer close(stopSnapshots)
	go func() {
		for {
			select {
			case <-stopSnapshots:
				return
			case <-snapshots:
				buf := make([]byte, 1<<20)
				n := runtime.Stack(buf, true)
				if err := os.WriteFile(path+".goroutines.tmp", buf[:n], 0600); err == nil {
					_ = os.Rename(path+".goroutines.tmp", path+".goroutines")
				}
			}
		}
	}()
	os.Args = []string{os.Args[0], "-config", path}
	flag.CommandLine = flag.NewFlagSet("owlcron-process", flag.ExitOnError)
	main()
}

type processFixture struct {
	Config                       *types.Config
	Dir, ConfigPath, URL, APIKey string
	Client                       *http.Client
	ChildTimeout                 time.Duration
	processMu                    sync.Mutex
	currentProcess               *daemonProcess
	remoteMu                     sync.Mutex
	remoteStore                  storage.Storage
}

func newProcessFixture(t *testing.T) *processFixture {
	t.Helper()
	if testing.Short() {
		t.Skip("independent-process acceptance")
	}
	dir := t.TempDir()
	port := processFreePort(t)
	f := &processFixture{Dir: dir, ConfigPath: filepath.Join(dir, "config.json"), APIKey: "process-acceptance-api-key", URL: fmt.Sprintf("http://127.0.0.1:%d", port), Client: &http.Client{Timeout: 3 * time.Second}}
	f.Config = &types.Config{HTTPPort: port, Hostname: "node-" + filepath.Base(dir), BaseAppURL: f.URL, StorageEngine: "Filesystem", StorageBaseDir: filepath.Join(dir, "data"), LogDir: filepath.Join(dir, "logs"), QueueDir: filepath.Join(dir, "queue"), SecretKey: "process-acceptance-secret-0123456789abcdef", ClusterAllowInsecure: true, UDPBroadcastPort: -1, SchedulerStartupGrace: 0, MasterPingFreq: 1, MasterPingTimeout: 3}
	f.Put(t, "api_keys:"+f.APIKey, &types.APIKey{ID: f.APIKey, Key: f.APIKey, Enabled: true, Privileges: types.Privileges{"admin": true}})
	return f
}

// Test servers must not use the kernel's ephemeral client-port range. A closed
// :0 probe can otherwise be reused by a peer connection before daemon startup.
var processPorts = struct {
	sync.Mutex
	once        sync.Once
	next        int
	first, last int
	err         error
	used        map[int]bool
}{next: 10000, used: make(map[int]bool)}

func processFreePort(t *testing.T) int {
	t.Helper()
	processPorts.once.Do(func() {
		var raw []byte
		switch runtime.GOOS {
		case "linux":
			raw, processPorts.err = os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
		case "darwin":
			raw, processPorts.err = exec.Command("/usr/sbin/sysctl", "-n", "net.inet.ip.portrange.first", "net.inet.ip.portrange.last").Output()
		default:
			processPorts.err = fmt.Errorf("unsupported process-test platform: %s", runtime.GOOS)
		}
		if processPorts.err == nil {
			_, processPorts.err = fmt.Sscan(string(raw), &processPorts.first, &processPorts.last)
		}
		if processPorts.err == nil && (processPorts.first < 1 || processPorts.last > 65535 || processPorts.first > processPorts.last) {
			processPorts.err = fmt.Errorf("invalid ephemeral port range %d..%d", processPorts.first, processPorts.last)
		}
	})
	if processPorts.err != nil {
		t.Fatalf("read ephemeral client-port range: %v", processPorts.err)
	}
	processPorts.Lock()
	defer processPorts.Unlock()
	for attempts := 0; attempts < 65536-1024; attempts++ {
		port := processPorts.next
		processPorts.next++
		if processPorts.next > 65535 {
			processPorts.next = 1024
		}
		if processPorts.used[port] || (port >= processPorts.first && port <= processPorts.last) {
			continue
		}
		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue
		}
		listener.Close()
		processPorts.used[port] = true
		t.Cleanup(func() {
			processPorts.Lock()
			delete(processPorts.used, port)
			processPorts.Unlock()
		})
		return port
	}
	t.Fatal("no unused process-test port outside the ephemeral client range")
	return 0
}

func (f *processFixture) Store(t *testing.T) storage.Storage {
	t.Helper()
	f.remoteMu.Lock()
	defer f.remoteMu.Unlock()
	if f.remoteStore != nil {
		return f.remoteStore
	}
	s, e := storage.NewStorage(&storage.StorageConfig{Engine: f.Config.StorageEngine, BaseDir: f.Config.StorageBaseDir, Options: f.Config.StorageOptions})
	if e != nil {
		t.Fatal(e)
	}
	if f.Config.StorageEngine != "Filesystem" {
		f.remoteStore = s
		if closer, ok := s.(interface{ Close() error }); ok {
			t.Cleanup(func() { _ = closer.Close() })
		}
	}
	return s
}
func (f *processFixture) Put(t *testing.T, key string, v any) {
	t.Helper()
	b, e := json.Marshal(v)
	if e == nil {
		e = f.Store(t).Put(context.Background(), key, b)
	}
	if e != nil {
		t.Fatal(e)
	}
}
func (f *processFixture) Get(t *testing.T, key string, v any) bool {
	t.Helper()
	b, e := f.Store(t).Get(context.Background(), key)
	if e != nil {
		return false
	}
	if e = json.Unmarshal(b, v); e != nil {
		t.Fatalf("decode %s: %v", key, e)
	}
	return true
}
func (f *processFixture) Request(t *testing.T, path string, body any) map[string]any {
	t.Helper()
	b, e := json.Marshal(body)
	if e != nil {
		t.Fatal(e)
	}
	r, e := http.NewRequest("POST", f.URL+path, bytes.NewReader(b))
	if e != nil {
		t.Fatal(e)
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-API-Key", f.APIKey)
	resp, e := f.Client.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	var out map[string]any
	if e = json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&out); e != nil {
		t.Fatalf("%s status %d decode: %v", path, resp.StatusCode, e)
	}
	return out
}

type boundedProcessLog struct {
	mu   sync.Mutex
	b    []byte
	race bool
}

func (l *boundedProcessLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.b = append(l.b, p...)
	if bytes.Contains(l.b, []byte("WARNING: DATA RACE")) {
		l.race = true
	}
	if len(l.b) > 32768 {
		l.b = append([]byte(nil), l.b[len(l.b)-32768:]...)
	}
	return len(p), nil
}
func (l *boundedProcessLog) HadRace() bool  { l.mu.Lock(); defer l.mu.Unlock(); return l.race }
func (l *boundedProcessLog) String() string { l.mu.Lock(); defer l.mu.Unlock(); return string(l.b) }

type daemonProcess struct {
	cmd    *exec.Cmd
	done   chan struct{}
	log    *boundedProcessLog
	mu     sync.Mutex
	err    error
	once   sync.Once
	groups []int
}

func (f *processFixture) Start(t *testing.T) *daemonProcess {
	t.Helper()
	raw, e := json.Marshal(f.Config)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(f.ConfigPath, raw, 0600); e != nil {
		t.Fatal(e)
	}
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	childTimeout := 8 * time.Minute
	if f.ChildTimeout != 0 {
		if f.ChildTimeout < time.Second || f.ChildTimeout > 48*time.Hour {
			t.Fatal("process fixture child timeout must be between 1s and 48h")
		}
		childTimeout = f.ChildTimeout
	}
	cmd := exec.Command(exe, "-test.run=^TestProcessDaemonChild$", "-test.timeout="+childTimeout.String())
	// Do not inherit application secrets, CLI child switches, or test-only knobs.
	for _, v := range os.Environ() {
		k := strings.SplitN(v, "=", 2)[0]
		if strings.HasPrefix(k, "OWLCRON_") {
			continue
		}
		cmd.Env = append(cmd.Env, v)
	}
	cmd.Env = append(cmd.Env, "OWLCRON_PROCESS_ACCEPTANCE_CHILD=1", "OWLCRON_PROCESS_ACCEPTANCE_CONFIG="+f.ConfigPath)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	p := &daemonProcess{cmd: cmd, done: make(chan struct{}), log: &boundedProcessLog{}}
	cmd.Stdout = p.log
	cmd.Stderr = p.log
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	f.processMu.Lock()
	f.currentProcess = p
	f.processMu.Unlock()
	go func() { e := cmd.Wait(); p.mu.Lock(); p.err = e; p.mu.Unlock(); close(p.done) }()
	t.Cleanup(func() {
		p.Kill(t)
		if t.Failed() {
			t.Logf("daemon diagnostics (redacted): %s", p.diagnostic(f.Config))
		}
	})
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-p.done:
			t.Fatalf("daemon exited before readiness: %s", p.diagnostic(f.Config))
		default:
		}
		resp, e := f.Client.Get(f.URL + "/api/app/status")
		if e == nil {
			io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return p
			}
		}
		time.Sleep(40 * time.Millisecond)
	}
	t.Fatalf("daemon readiness timeout: %s", p.diagnostic(f.Config))
	return nil
}
func (p *daemonProcess) diagnostic(c *types.Config) string {
	s := p.log.String()
	secrets := []string{c.SecretKey, c.NodeAuthPrivateKey, c.SMTPPassword}
	for _, key := range []string{"password", "secret_key", "access_key", "session_token"} {
		if value, ok := c.StorageOptions[key].(string); ok {
			secrets = append(secrets, value)
		}
	}
	for _, secret := range secrets {
		if secret != "" {
			s = strings.ReplaceAll(s, secret, "[REDACTED]")
		}
	}
	return s
}

// KillDaemonOnly models a daemon crash while its plugin descendants remain alive.
// Cleanup still owns and eventually kills the entire original process group.
func (p *daemonProcess) rememberDescendants() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	raw, err := exec.CommandContext(ctx, "/bin/ps", "-axo", "pid=,ppid=,pgid=").Output()
	if err != nil {
		return
	}
	type row struct{ pid, parent, group int }
	var rows []row
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		pid, _ := strconv.Atoi(fields[0])
		parent, _ := strconv.Atoi(fields[1])
		group, _ := strconv.Atoi(fields[2])
		rows = append(rows, row{pid, parent, group})
	}
	owned := map[int]bool{p.cmd.Process.Pid: true}
	for changed := true; changed; {
		changed = false
		for _, r := range rows {
			if owned[r.parent] && !owned[r.pid] {
				owned[r.pid] = true
				changed = true
			}
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	seen := map[int]bool{}
	for _, group := range p.groups {
		seen[group] = true
	}
	for _, r := range rows {
		if owned[r.pid] && r.group > 0 && r.group != syscall.Getpgrp() && !seen[r.group] {
			seen[r.group] = true
			p.groups = append(p.groups, r.group)
		}
	}
}
func (p *daemonProcess) KillDaemonOnly(t *testing.T) {
	t.Helper()
	p.rememberDescendants()
	if e := p.cmd.Process.Kill(); e != nil && !strings.Contains(e.Error(), "process already finished") {
		t.Fatal(e)
	}
	select {
	case <-p.done:
	case <-time.After(8 * time.Second):
		t.Fatal("daemon did not exit after SIGKILL")
	}
}
func (p *daemonProcess) Kill(t *testing.T) { t.Helper(); p.finish(t, syscall.SIGKILL) }
func (p *daemonProcess) Stop(t *testing.T) { t.Helper(); p.finish(t, syscall.SIGTERM) }
func (p *daemonProcess) finish(t *testing.T, sig syscall.Signal) {
	t.Helper()
	p.once.Do(func() {
		defer func() {
			if p.log.HadRace() {
				t.Error("independent daemon reported a data race")
			}
		}()
		p.rememberDescendants()
		_ = syscall.Kill(-p.cmd.Process.Pid, sig)
		select {
		case <-p.done:
		case <-time.After(12 * time.Second):
			_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
			select {
			case <-p.done:
			case <-time.After(5 * time.Second):
				t.Error("child did not exit after SIGKILL")
			}
		}
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		p.mu.Lock()
		groups := append([]int(nil), p.groups...)
		p.mu.Unlock()
		for _, group := range groups {
			_ = syscall.Kill(-group, syscall.SIGKILL)
		}
	})
}
func awaitProcess(t *testing.T, timeout time.Duration, description string, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

// TCP gate cuts established sockets as well as new dials. It forwards opaque TLS
// bytes and therefore exercises real certificate and node authentication paths.
type tcpGate struct {
	listener       net.Listener
	target         string
	mu             sync.Mutex
	paused, closed bool
	conns          map[net.Conn]bool
	wg             sync.WaitGroup
}

func newTCPGate(t *testing.T, target string) *tcpGate {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	g := &tcpGate{listener: l, target: target, conns: make(map[net.Conn]bool)}
	g.wg.Add(1)
	go g.accept()
	t.Cleanup(func() { g.Close() })
	return g
}
func (g *tcpGate) Addr() string { return g.listener.Addr().String() }
func (g *tcpGate) Pause() {
	g.mu.Lock()
	g.paused = true
	for c := range g.conns {
		c.Close()
	}
	g.mu.Unlock()
}
func (g *tcpGate) Resume() { g.mu.Lock(); g.paused = false; g.mu.Unlock() }
func (g *tcpGate) Close() {
	g.mu.Lock()
	g.closed = true
	g.paused = true
	g.listener.Close()
	for c := range g.conns {
		c.Close()
	}
	g.mu.Unlock()
	g.wg.Wait()
}
func (g *tcpGate) accept() {
	defer g.wg.Done()
	for {
		a, e := g.listener.Accept()
		if e != nil {
			return
		}
		g.mu.Lock()
		if g.paused || g.closed {
			g.mu.Unlock()
			a.Close()
			continue
		}
		g.conns[a] = true
		g.wg.Add(1)
		g.mu.Unlock()
		go g.forward(a)
	}
}
func (g *tcpGate) forward(a net.Conn) {
	defer g.wg.Done()
	defer a.Close()
	b, e := net.DialTimeout("tcp", g.target, time.Second)
	if e != nil {
		g.mu.Lock()
		delete(g.conns, a)
		g.mu.Unlock()
		return
	}
	defer b.Close()
	g.mu.Lock()
	if g.paused || g.closed {
		delete(g.conns, a)
		g.mu.Unlock()
		return
	}
	g.conns[b] = true
	g.mu.Unlock()
	done := make(chan struct{})
	go func() { io.Copy(b, a); b.Close(); close(done) }()
	io.Copy(a, b)
	a.Close()
	<-done
	g.mu.Lock()
	delete(g.conns, a)
	delete(g.conns, b)
	g.mu.Unlock()
}

func configureProcessTLS(t *testing.T, fixtures ...*processFixture) {
	t.Helper()
	dir := t.TempDir()
	pub, priv, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "process acceptance CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, e := x509.CreateCertificate(rand.Reader, ca, ca, pub, priv)
	if e != nil {
		t.Fatal(e)
	}
	caPath := filepath.Join(dir, "ca.pem")
	os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	peers := map[string]string{}
	for i, f := range fixtures {
		np, nk, e := ed25519.GenerateKey(rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		f.Config.NodeAuthPrivateKey = base64.StdEncoding.EncodeToString(nk.Seed())
		peers[f.Config.Hostname] = base64.StdEncoding.EncodeToString(np)
		cp, ck, e := ed25519.GenerateKey(rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		cert := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), Subject: pkix.Name{CommonName: f.Config.Hostname}, DNSNames: []string{f.Config.Hostname, "localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		cd, e := x509.CreateCertificate(rand.Reader, cert, ca, cp, priv)
		if e != nil {
			t.Fatal(e)
		}
		kd, e := x509.MarshalPKCS8PrivateKey(ck)
		if e != nil {
			t.Fatal(e)
		}
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cd})
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kd})
		f.Config.ClusterTLSCAFile = caPath
		f.Config.ClusterTLSCertFile = filepath.Join(f.Dir, "tls.pem")
		f.Config.ClusterTLSKeyFile = filepath.Join(f.Dir, "tls.key")
		os.WriteFile(f.Config.ClusterTLSCertFile, certPEM, 0600)
		os.WriteFile(f.Config.ClusterTLSKeyFile, keyPEM, 0600)
		f.Config.ClusterTLSRequireClientCert = true
		f.Config.ClusterAllowInsecure = false
		pair, e := tls.X509KeyPair(certPEM, keyPEM)
		if e != nil {
			t.Fatal(e)
		}
		f.Client = &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}}}
		t.Cleanup(func() { f.Client.CloseIdleConnections() })
		f.URL = fmt.Sprintf("https://127.0.0.1:%d", f.Config.HTTPPort)
		f.Config.BaseAppURL = f.URL
	}
	for _, f := range fixtures {
		f.Config.NodeAuthPeers = peers
	}
}

func TestProcessTCPGateConcurrentCutAndResume(t *testing.T) {
	if testing.Short() {
		t.Skip("independent-process acceptance network harness")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var serverWG sync.WaitGroup
	serverWG.Add(1)
	go func() {
		defer serverWG.Done()
		for {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			serverWG.Add(1)
			go func() { defer serverWG.Done(); defer c.Close(); io.Copy(c, c) }()
		}
	}()
	gate := newTCPGate(t, listener.Addr().String())
	defer func() { gate.Close(); listener.Close(); serverWG.Wait() }()
	var clients []net.Conn
	for range 12 {
		c, e := net.DialTimeout("tcp", gate.Addr(), time.Second)
		if e != nil {
			t.Fatal(e)
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(3 * time.Second))
		if _, e = c.Write([]byte("ready")); e != nil {
			t.Fatal(e)
		}
		reply := make([]byte, 5)
		if _, e = io.ReadFull(c, reply); e != nil {
			t.Fatal(e)
		}
		clients = append(clients, c)
	}
	gate.Pause()
	for _, c := range clients {
		c.SetReadDeadline(time.Now().Add(time.Second))
		if _, err = c.Read(make([]byte, 1)); err == nil {
			t.Fatal("paused gate kept an established connection alive")
		}
	}
	c, err := net.DialTimeout("tcp", gate.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(time.Second))
	c.Write([]byte("blocked"))
	if _, err = c.Read(make([]byte, 1)); err == nil {
		t.Fatal("paused gate forwarded a new connection")
	}
	c.Close()
	gate.Resume()
	c, err = net.DialTimeout("tcp", gate.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	if _, err = c.Write([]byte("restored")); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 8)
	if _, err = io.ReadFull(c, reply); err != nil || string(reply) != "restored" {
		t.Fatalf("gate did not recover: %q %v", reply, err)
	}
}

func TestProcessPortsStayOutsideClientRange(t *testing.T) {
	ports := make(chan int, 32)
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ports <- processFreePort(t)
		}()
	}
	wg.Wait()
	close(ports)
	seen := map[int]bool{}
	for port := range ports {
		if seen[port] {
			t.Fatalf("concurrent fixtures selected the same port: %d", port)
		}
		seen[port] = true
		if port >= processPorts.first && port <= processPorts.last {
			t.Fatalf("server port %d is inside the ephemeral client range", port)
		}
	}
	if len(seen) != 32 {
		t.Fatalf("reserved %d distinct ports, want 32", len(seen))
	}
}
