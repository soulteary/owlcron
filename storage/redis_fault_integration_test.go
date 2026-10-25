package storage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func redisFaultAddress(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("OWLCRON_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set OWLCRON_TEST_REDIS_ADDR to a real isolated Redis 8.4+ service")
	}
	return addr
}

func redisFaultStorage(t *testing.T, addr, prefix string) *RedisStorage {
	t.Helper()
	s, err := NewRedisStorage(&StorageConfig{Engine: "Redis", Options: map[string]interface{}{
		"addr": addr, "prefix": prefix,
		"username":        os.Getenv("OWLCRON_TEST_REDIS_USERNAME"),
		"password":        os.Getenv("OWLCRON_TEST_REDIS_PASSWORD"),
		"dial_timeout_ms": 500, "read_timeout_ms": 2000, "write_timeout_ms": 2000,
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// redisFaultFrame reads RESP, including the RESP3 handshake, without emulating
// Redis. Every successful reply below comes from an independent real server.
type redisFaultFrame struct {
	raw   []byte
	text  string
	parts []redisFaultFrame
}

func readRedisFaultFrame(r *bufio.Reader, depth int) (redisFaultFrame, error) {
	if depth > 32 {
		return redisFaultFrame{}, fmt.Errorf("RESP nesting limit")
	}
	line, err := r.ReadBytes('\n')
	if err != nil {
		return redisFaultFrame{}, err
	}
	if len(line) < 3 || line[len(line)-2] != '\r' {
		return redisFaultFrame{}, fmt.Errorf("invalid RESP line")
	}
	f := redisFaultFrame{raw: append([]byte(nil), line...), text: string(line[1 : len(line)-2])}
	switch line[0] {
	case '$', '!', '=':
		n, err := strconv.Atoi(f.text)
		if err != nil || n < -1 || n > 64<<20 {
			return f, fmt.Errorf("invalid RESP payload size")
		}
		if n == -1 {
			return f, nil
		}
		payload := make([]byte, n+2)
		if _, err := io.ReadFull(r, payload); err != nil {
			return f, err
		}
		if !bytes.HasSuffix(payload, []byte("\r\n")) {
			return f, fmt.Errorf("invalid RESP payload terminator")
		}
		f.raw = append(f.raw, payload...)
		f.text = string(payload[:n])
	case '*', '~', '>', '%', '|':
		n, err := strconv.Atoi(f.text)
		if err != nil || n < -1 || n > 100000 {
			return f, fmt.Errorf("invalid RESP collection size")
		}
		if line[0] == '%' || line[0] == '|' {
			n *= 2
		}
		for i := 0; i < n; i++ {
			part, err := readRedisFaultFrame(r, depth+1)
			if err != nil {
				return f, err
			}
			f.raw = append(f.raw, part.raw...)
			f.parts = append(f.parts, part)
		}
	case '+', '-', ':', '_', '#', ',', '(':
	default:
		return f, fmt.Errorf("unsupported RESP type")
	}
	return f, nil
}

type redisFaultGate struct {
	addr      string
	mode      atomic.Int32 // 0 forward; 1 blackhole; 2 disconnect before forwarding; 3 lose committed CAS reply; 4 stall committed CAS reply
	hits      atomic.Int32
	casWrites atomic.Int32
	committed atomic.Int32
	stop      chan struct{}
	listener  net.Listener
	mu        sync.Mutex
	conns     map[net.Conn]bool
	wg        sync.WaitGroup
}

func newRedisFaultGate(t *testing.T, upstream string) *redisFaultGate {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := &redisFaultGate{addr: listener.Addr().String(), listener: listener, stop: make(chan struct{}), conns: map[net.Conn]bool{}}
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			g.mu.Lock()
			g.conns[client] = true
			g.mu.Unlock()
			g.wg.Add(1)
			go func() {
				defer g.wg.Done()
				defer client.Close()
				defer func() { g.mu.Lock(); delete(g.conns, client); g.mu.Unlock() }()
				server, err := net.DialTimeout("tcp", upstream, time.Second)
				if err != nil {
					return
				}
				defer server.Close()
				requests, responses := bufio.NewReader(client), bufio.NewReader(server)
				for {
					command, err := readRedisFaultFrame(requests, 0)
					if err != nil {
						return
					}
					mode := g.mode.Load()
					if mode == 1 {
						g.hits.Add(1)
						<-g.stop
						return
					}
					if mode == 2 {
						g.hits.Add(1)
						return
					}
					isCAS := false
					if len(command.parts) >= 4 && strings.EqualFold(command.parts[0].text, "SET") {
						for _, part := range command.parts[3:] {
							isCAS = isCAS || strings.EqualFold(part.text, "NX") || strings.EqualFold(part.text, "IFEQ")
						}
					}
					if isCAS {
						g.casWrites.Add(1)
					}
					if _, err := server.Write(command.raw); err != nil {
						return
					}
					reply, err := readRedisFaultFrame(responses, 0)
					if err != nil {
						return
					}
					if mode == 3 && isCAS && string(reply.raw) == "+OK\r\n" {
						// Redis has executed and acknowledged SET. Only its response
						// to the calling application is lost; no fake state is used.
						g.committed.Add(1)
						return
					}
					if mode == 4 && isCAS && string(reply.raw) == "+OK\r\n" {
						g.hits.Add(1)
						<-g.stop
						return
					}
					if _, err := client.Write(reply.raw); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		close(g.stop)
		g.listener.Close()
		g.mu.Lock()
		for conn := range g.conns {
			conn.Close()
		}
		g.mu.Unlock()
		g.wg.Wait()
	})
	return g
}

func assertRedisFault(t *testing.T, err error) {
	t.Helper()
	if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrConflict) {
		t.Fatalf("transport fault must remain an error, not absence or CAS conflict: %v", err)
	}
}

func TestRedisRealTransportFaultRecovery(t *testing.T) {
	addr := redisFaultAddress(t)
	prefix := fmt.Sprintf("transport-fault/%d/", time.Now().UnixNano())
	direct := redisFaultStorage(t, addr, prefix)
	gate := newRedisFaultGate(t, addr)
	client := redisFaultStorage(t, gate.addr, prefix)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.Put(ctx, "durable", []byte("before-fault")); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []int32{1, 2} {
		gate.mode.Store(mode)
		bounded, stop := context.WithTimeout(ctx, 150*time.Millisecond)
		started := time.Now()
		err := client.Update(bounded, "durable", func([]byte) ([]byte, error) { return []byte("corrupt"), nil })
		stop()
		assertRedisFault(t, err)
		if time.Since(started) > time.Second {
			t.Fatalf("fault mode %d ignored operation deadline", mode)
		}
	}
	gate.mode.Store(0)
	reopened := redisFaultStorage(t, gate.addr, prefix)
	for _, store := range []*RedisStorage{client, reopened, direct} {
		data, err := store.Get(ctx, "durable")
		if err != nil || string(data) != "before-fault" {
			t.Fatalf("reconnected data=%q err=%v", data, err)
		}
	}
	if gate.hits.Load() < 2 {
		t.Fatal("faults did not reach the real transport gate")
	}
	// Concurrent clients resume optimistic updates after restoring the network.
	var wg sync.WaitGroup
	errs := make(chan error, 40)
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store := client
			if i%2 != 0 {
				store = reopened
			}
			errs <- store.ListPush(ctx, "reconnected-list", i)
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	items, err := direct.ListGet(ctx, "reconnected-list", 0, 0)
	if err != nil || len(items) != 40 {
		t.Fatalf("reconnected list length=%d err=%v", len(items), err)
	}
	seen := map[float64]bool{}
	for _, item := range items {
		n, ok := item.(float64)
		if !ok || n < 0 || n >= 40 || seen[n] {
			t.Fatalf("duplicate/invalid item after reconnect: %v", item)
		}
		seen[n] = true
	}
}

func TestRedisRealCommittedResponseLossRecovery(t *testing.T) {
	addr := redisFaultAddress(t)
	prefix := fmt.Sprintf("response-loss/%d/", time.Now().UnixNano())
	direct := redisFaultStorage(t, addr, prefix)
	gate := newRedisFaultGate(t, addr)
	client := redisFaultStorage(t, gate.addr, prefix)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := client.Put(ctx, "counter", []byte("0")); err != nil {
		t.Fatal(err)
	}
	gate.casWrites.Store(0)
	gate.mode.Store(3)
	var callbacks atomic.Int32
	bounded, stop := context.WithTimeout(ctx, time.Second)
	err := client.Update(bounded, "counter", func(raw []byte) ([]byte, error) {
		callbacks.Add(1)
		n, err := strconv.Atoi(string(raw))
		return []byte(strconv.Itoa(n + 1)), err
	})
	stop()
	assertRedisFault(t, err)
	if callbacks.Load() != 1 || gate.casWrites.Load() != 1 || gate.committed.Load() != 1 {
		t.Fatalf("ambiguous CAS replayed: callbacks=%d commands=%d commits=%d", callbacks.Load(), gate.casWrites.Load(), gate.committed.Load())
	}
	raw, err := direct.Get(ctx, "counter")
	if err != nil || string(raw) != "1" {
		t.Fatalf("real committed counter=%q err=%v", raw, err)
	}
	gate.mode.Store(0)
	if err := client.Put(ctx, "after-loss", []byte("usable")); err != nil {
		t.Fatal(err)
	}
}

func TestRedisRealServerSIGKILLRecovery(t *testing.T) {
	addr := redisFaultAddress(t)
	container := os.Getenv("OWLCRON_TEST_REDIS_CONTAINER")
	if container == "" {
		t.Skip("set OWLCRON_TEST_REDIS_CONTAINER for a labeled disposable Redis with persistent AOF always")
	}
	control := newRemoteContainerControl(t, container)
	prefix := fmt.Sprintf("server-fault/%d/", time.Now().UnixNano())
	a, b := redisFaultStorage(t, addr, prefix), redisFaultStorage(t, addr, prefix)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := a.Put(ctx, "durable", []byte("confirmed-before-sigkill")); err != nil {
		t.Fatal(err)
	}
	_, oldVersion, err := b.read(ctx, "durable")
	if err != nil {
		t.Fatal(err)
	}
	control(t, "pause")
	paused := true
	needsStart := false
	t.Cleanup(func() {
		if paused {
			control(t, "unpause")
		}
		if needsStart {
			control(t, "start")
		}
	})
	// Exercise concurrent reads and writes against a paused real server. None
	// may report absence/conflict or succeed on an unconfirmed mutation.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			bounded, stop := context.WithTimeout(ctx, 150*time.Millisecond)
			defer stop()
			started := time.Now()
			var err error
			if i%2 == 0 {
				_, err = a.Get(bounded, "durable")
			} else {
				err = b.Put(bounded, fmt.Sprintf("unconfirmed-%d", i), []byte("must not report success"))
			}
			assertRedisFault(t, err)
			if time.Since(started) > time.Second {
				t.Error("paused Redis ignored independent deadline")
			}
		}(i)
	}
	wg.Wait()
	for _, store := range []*RedisStorage{a, b} {
		bounded, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { _, err := store.Get(bounded, "durable"); done <- err }()
		time.Sleep(50 * time.Millisecond)
		stop()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("paused read cancellation=%v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Error("paused read ignored cancellation")
		}
	}
	control(t, "unpause")
	paused = false
	control(t, "kill") // Docker kill --signal KILL, never a graceful restart.
	needsStart = true
	bounded, stop := context.WithTimeout(ctx, 200*time.Millisecond)
	_, err = a.Get(bounded, "durable")
	stop()
	assertRedisFault(t, err)
	control(t, "start")
	needsStart = false
	deadline := time.Now().Add(30 * time.Second)
	for {
		bounded, stop := context.WithTimeout(ctx, time.Second)
		data, err := a.Get(bounded, "durable")
		stop()
		if err == nil && string(data) == "confirmed-before-sigkill" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("AOF-confirmed data lost after SIGKILL: %q %v", data, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	reopened := redisFaultStorage(t, addr, prefix)
	if err := reopened.Put(ctx, "durable", []byte("new-owner")); err != nil {
		t.Fatal(err)
	}
	stale, _ := json.Marshal(remoteDocument{Data: []byte("stale-owner"), Revision: "stale"})
	if err := b.compareAndSwap(ctx, "durable", stale, oldVersion); !errors.Is(err, ErrConflict) {
		t.Fatalf("old version accepted after SIGKILL and owner change: %v", err)
	}
	data, err := a.Get(ctx, "durable")
	if err != nil || string(data) != "new-owner" {
		t.Fatalf("stale owner corrupted recovered data=%q err=%v", data, err)
	}
	var pushed sync.WaitGroup
	failures := make(chan error, 40)
	for i := 0; i < 40; i++ {
		pushed.Add(1)
		go func(i int) {
			defer pushed.Done()
			store := b
			if i%2 != 0 {
				store = reopened
			}
			failures <- store.ListPush(ctx, "post-crash-list", i)
		}(i)
	}
	pushed.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	seen := map[float64]bool{}
	for i := 0; i < 40; i++ {
		var item interface{}
		if i%2 == 0 {
			item, err = b.ListShift(ctx, "post-crash-list")
		} else {
			item, err = reopened.ListPop(ctx, "post-crash-list")
		}
		n, ok := item.(float64)
		if err != nil || !ok || n < 0 || n >= 40 || seen[n] {
			t.Fatalf("post-crash list duplicate/loss: item=%v err=%v", item, err)
		}
		seen[n] = true
	}
}
