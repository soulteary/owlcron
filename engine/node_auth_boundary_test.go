package engine

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
	ws "github.com/soulteary/owlcron/websocket"
	"go.uber.org/zap"
)

func nodeIdentityFixture(t *testing.T) (*Engine, *Engine) {
	t.Helper()
	sender, receiver := securityEngine(t), securityEngine(t)
	sender.config.Hostname = "sender"
	receiver.config.Hostname = "receiver"
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sender.nodeAuth = &nodeAuthConfig{private: priv, peers: map[string]ed25519.PublicKey{"sender": pub}}
	receiver.nodeAuth = &nodeAuthConfig{peers: map[string]ed25519.PublicKey{"sender": pub}}
	raw, _ := json.Marshal(types.Server{Hostname: "sender"})
	if err := receiver.storage.Put(receiver.ctx, "servers:sender", raw); err != nil {
		t.Fatal(err)
	}
	own, _ := json.Marshal(types.Server{Hostname: "receiver"})
	if err := receiver.storage.Put(receiver.ctx, "servers:receiver", own); err != nil {
		t.Fatal(err)
	}
	return sender, receiver
}
func TestNodeProofBindsReceiverChallengeAndRegisteredIdentity(t *testing.T) {
	sender, receiver := nodeIdentityFixture(t)
	challenge := newNodeChallenge()
	p := sender.MakeNodeAuthFor("sender", false, "receiver", challenge)
	if _, _, err := receiver.VerifyNodeAuthFor(p, "receiver", newNodeChallenge()); err == nil {
		t.Fatal("cross connection proof accepted")
	}
	if _, _, err := receiver.VerifyNodeAuthFor(p, "other", challenge); err == nil {
		t.Fatal("cross receiver proof accepted")
	}
	tampered := p
	tampered.Hostname = "another"
	if _, _, err := receiver.VerifyNodeAuthFor(tampered, "receiver", challenge); err == nil {
		t.Fatal("identity forgery accepted")
	}
	if host, master, err := receiver.VerifyNodeAuthFor(p, "receiver", challenge); err != nil || host != "sender" || master {
		t.Fatalf("valid proof rejected: %v", err)
	}
	if _, _, err := receiver.VerifyNodeAuthFor(p, "receiver", challenge); err == nil {
		t.Fatal("replay accepted")
	}
	p = sender.MakeNodeAuthFor("sender", false, "receiver", challenge)
	raw, _ := json.Marshal(types.Server{Hostname: "sender", Disabled: true})
	receiver.storage.Put(receiver.ctx, "servers:sender", raw)
	if _, _, err := receiver.VerifyNodeAuthFor(p, "receiver", challenge); err == nil {
		t.Fatal("disabled peer accepted")
	}
}
func TestNodeProofConcurrentClaimHasOneWinner(t *testing.T) {
	sender, receiver := nodeIdentityFixture(t)
	challenge := newNodeChallenge()
	p := sender.MakeNodeAuthFor("sender", false, "receiver", challenge)
	var wg sync.WaitGroup
	var won atomic.Int32
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := receiver.VerifyNodeAuthFor(p, "receiver", challenge); err == nil {
				won.Add(1)
			}
		}()
	}
	wg.Wait()
	if won.Load() != 1 {
		t.Fatalf("proof claims: %d", won.Load())
	}
}
func TestNodeProofRejectsForgedRoleAndExpiredSignature(t *testing.T) {
	sender, receiver := nodeIdentityFixture(t)
	challenge := newNodeChallenge()
	p := sender.MakeNodeAuthFor("sender", false, "receiver", challenge)
	p.Master = true
	if _, _, err := receiver.VerifyNodeAuthFor(p, "receiver", challenge); err == nil {
		t.Fatal("role tamper accepted")
	}
	p = sender.MakeNodeAuthFor("sender", true, "receiver", challenge)
	if _, _, err := receiver.VerifyNodeAuthFor(p, "receiver", challenge); err == nil {
		t.Fatal("nonleader master accepted")
	}
	p = sender.MakeNodeAuthFor("sender", false, "receiver", challenge)
	p.Timestamp = time.Now().Unix() - 61
	p.Signature = sender.nodeAuthSignature(p)
	if _, _, err := receiver.VerifyNodeAuthFor(p, "receiver", challenge); err == nil {
		t.Fatal("expired proof accepted")
	}
	// Possessing a receiver public verification key does not permit signing for sender.
	p = receiver.MakeNodeAuthFor("sender", false, "receiver", challenge)
	if p.Signature != "" {
		t.Fatal("receiver signed another node identity")
	}
}
func TestNodeAuthenticationHasNoProductionSharedSecretFallback(t *testing.T) {
	sender, receiver := nodeIdentityFixture(t)
	sender.nodeAuth = nil
	sender.config.ClusterAllowInsecure = true
	receiver.config.ClusterAllowInsecure = true
	challenge := newNodeChallenge()
	p := sender.MakeNodeAuthFor("sender", false, "receiver", challenge)
	if _, _, err := receiver.VerifyNodeAuthFor(p, "receiver", challenge); err == nil {
		t.Fatal("shared secret bypassed registered public identity")
	}
	receiver.config.ClusterAllowInsecure = false
	if _, _, err := receiver.VerifyNodeAuth(p); err == nil {
		t.Fatal("unbound proof API accepted in production")
	}
}
func TestNodeKeyConfigurationRequiresPrivateFileAndMatchingRegistration(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	config := &types.Config{Hostname: "node", NodeAuthPrivateKey: base64.StdEncoding.EncodeToString(priv.Seed()), NodeAuthPeers: map[string]string{"node": base64.StdEncoding.EncodeToString(pub)}}
	if _, err := loadNodeAuthConfig(config); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "node.key")
	os.WriteFile(file, []byte(config.NodeAuthPrivateKey), 0644)
	config.NodeAuthPrivateKey = ""
	config.NodeAuthKeyFile = file
	if _, err := loadNodeAuthConfig(config); err == nil {
		t.Fatal("publicly readable key file accepted")
	}
	os.Chmod(file, 0600)
	if _, err := loadNodeAuthConfig(config); err != nil {
		t.Fatal(err)
	}
	config.NodeAuthPeers = nil
	if _, err := loadNodeAuthConfig(config); err == nil {
		t.Fatal("unregistered local identity accepted")
	}
}
func TestNodeAndBrowserCredentialSurfacesAreSeparated(t *testing.T) {
	sender, receiver := nodeIdentityFixture(t)
	h := NewWebSocketHandler(receiver, zap.NewNop())
	challenge := newNodeChallenge()
	p := sender.MakeNodeAuthFor("sender", false, "receiver", challenge)
	raw, _ := json.Marshal(p)
	browser := &ws.Client{ID: "browser"}
	h.handleAuth(browser, &ws.Message{Type: "auth", Data: raw})
	if browser.IsAuthenticated() {
		t.Fatal("node proof entered browser surface")
	}
	node := &ws.Client{ID: "node", NodeEndpoint: true}
	h.challenges[node.ID] = challenge
	h.handleAuth(node, &ws.Message{Type: "auth", Data: json.RawMessage(`{"session_id":"session"}`)})
	if node.IsAuthenticated() {
		t.Fatal("browser credential entered node surface")
	}
	node.SetAuthentication("sender", true, true, nil, "node", "lease", 0)
	if _, allowed := h.FilterBroadcast(node, "update", map[string]string{"secret": "x"}); allowed {
		t.Fatal("node received browser data")
	}
}

func TestNodeNonceCapacityFailsClosedAndExpiredEntriesRecover(t *testing.T) {
	sender, receiver := nodeIdentityFixture(t)
	receiver.nodeAuth.seen = make(map[string]int64, maxNodeAuthNonces)
	for i := 0; i < maxNodeAuthNonces; i++ {
		receiver.nodeAuth.seen[fmt.Sprintf("reserved-%d", i)] = time.Now().Unix() + 120
	}
	challenge := newNodeChallenge()
	p := sender.MakeNodeAuthFor("sender", false, "receiver", challenge)
	if _, _, err := receiver.VerifyNodeAuthFor(p, "receiver", challenge); err == nil {
		t.Fatal("full nonce table allowed a proof without replay protection")
	}
	if len(receiver.nodeAuth.seen) != maxNodeAuthNonces {
		t.Fatal("nonce bound exceeded")
	}
	for key := range receiver.nodeAuth.seen {
		receiver.nodeAuth.seen[key] = time.Now().Unix() - 1
	}
	if _, _, err := receiver.VerifyNodeAuthFor(p, "receiver", challenge); err != nil {
		t.Fatalf("expired capacity did not recover: %v", err)
	}
}
func TestNodeMasterProofFencedAfterFailoverAndStorageRecovery(t *testing.T) {
	sender, receiver := nodeIdentityFixture(t)
	lease := leadershipLease{Hostname: "sender", Owner: "first-owner", Expires: time.Now().Add(time.Minute).UnixNano()}
	putLease := func() {
		raw, _ := json.Marshal(lease)
		if err := receiver.storage.Put(receiver.ctx, "global/master_lease", raw); err != nil {
			t.Fatal(err)
		}
	}
	putLease()
	makeProof := func() NodeAuthData {
		p := sender.MakeNodeAuthFor("sender", true, "receiver", newNodeChallenge())
		p.LeaseOwner = "first-owner"
		p.Signature = sender.nodeAuthSignature(p)
		return p
	}
	first := makeProof()
	if _, _, err := receiver.VerifyNodeAuthFor(first, "receiver", first.Challenge); err != nil {
		t.Fatal(err)
	}
	client := &ws.Client{ID: "master", NodeEndpoint: true}
	client.SetAuthentication("sender", true, true, nil, "node", "first-owner", 0)
	receiver.mu.Lock()
	receiver.multi.Master = false
	receiver.multi.MasterHostname = "sender"
	receiver.mu.Unlock()
	h := NewWebSocketHandler(receiver, zap.NewNop())
	if !h.AllowMaster(client) {
		t.Fatal("current master rejected")
	}
	lease.Owner = "replacement-owner"
	putLease()
	stale := makeProof()
	if _, _, err := receiver.VerifyNodeAuthFor(stale, "receiver", stale.Challenge); err == nil {
		t.Fatal("stale lease proof accepted")
	}
	if h.AllowMaster(client) {
		t.Fatal("old authenticated connection survived failover")
	}
	receiver.mu.Lock()
	receiver.multi.LastPingReceived = 123
	receiver.mu.Unlock()
	h.OnMessage(client, &ws.Message{Type: "master_ping", Data: json.RawMessage(`{"epoch":1}`)})
	if receiver.GetMulti().LastPingReceived != 123 {
		t.Fatal("stale authenticated connection refreshed leader heartbeat")
	}

	receiver.storage.Delete(receiver.ctx, "global/master_lease")
	p := makeProof()
	if _, _, err := receiver.VerifyNodeAuthFor(p, "receiver", p.Challenge); err == nil {
		t.Fatal("missing lease accepted")
	}
	lease.Owner = "first-owner"
	putLease()
	if _, _, err := receiver.VerifyNodeAuthFor(p, "receiver", p.Challenge); err != nil {
		t.Fatalf("lease recovery failed: %v", err)
	}
}

func TestDisabledLocalWorkerRejectsExistingMasterConnection(t *testing.T) {
	sender, receiver := nodeIdentityFixture(t)
	lease := leadershipLease{Hostname: "sender", Owner: "current-owner", Expires: time.Now().Add(time.Minute).UnixNano()}
	raw, _ := json.Marshal(lease)
	receiver.storage.Put(receiver.ctx, "global/master_lease", raw)
	receiver.mu.Lock()
	receiver.multi.Master = false
	receiver.multi.MasterHostname = "sender"
	receiver.mu.Unlock()
	client := &ws.Client{ID: "master", NodeEndpoint: true}
	client.SetAuthentication(sender.Hostname(), true, true, nil, "node", "current-owner", 0)
	h := NewWebSocketHandler(receiver, zap.NewNop())
	if !h.AllowMaster(client) {
		t.Fatal("registered local worker rejected")
	}
	raw, _ = json.Marshal(types.Server{Hostname: "receiver", Disabled: true})
	receiver.storage.Put(receiver.ctx, "servers:receiver", raw)
	h.OnMessage(client, &ws.Message{Type: "shutdown_server", Data: json.RawMessage(`{}`)})
	if receiver.ctx.Err() != nil {
		t.Fatal("disabled local worker still accepted master command")
	}
	if h.AllowMaster(client) {
		t.Fatal("disabled local worker retained command channel")
	}
}

func TestNodePrivateIdentityRedactedFromWebHookAndDiagnostics(t *testing.T) {
	sender, _ := nodeIdentityFixture(t)
	private := base64.StdEncoding.EncodeToString(sender.nodeAuth.private)
	seed := base64.StdEncoding.EncodeToString(sender.nodeAuth.private.Seed())
	raw, err := json.Marshal(map[string]interface{}{"description": private + " " + seed, "node_auth_private_key": private})
	if err != nil {
		t.Fatal(err)
	}
	cleaned, err := sender.cleanWebHookBody(raw)
	if err != nil {
		t.Fatal(err)
	}
	diagnostic := sender.RedactSensitiveText("private=" + private + " seed=" + seed)
	for _, key := range []string{private, seed} {
		if strings.Contains(string(cleaned), key) || strings.Contains(diagnostic, key) {
			t.Fatal("loaded node private identity escaped redaction")
		}
	}
}

func TestNodePeerRegistrationRequiresSigningIdentity(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	if _, err := loadNodeAuthConfig(&types.Config{Hostname: "worker", NodeAuthPeers: map[string]string{"master": base64.StdEncoding.EncodeToString(pub)}}); err == nil {
		t.Fatal("peer-only config cannot complete mutual authentication")
	}
	if _, err := loadNodeAuthConfig(&types.Config{}); err != nil {
		t.Fatalf("standalone default rejected: %v", err)
	}
}
func TestRevokedMasterConnectionDoesNotReviveWhenLeaseReturns(t *testing.T) {
	sender, receiver := nodeIdentityFixture(t)
	owner := "stable-process-owner"
	putLease := func(current string) {
		raw, _ := json.Marshal(leadershipLease{Hostname: "sender", Owner: current, Expires: time.Now().Add(time.Minute).UnixNano()})
		if err := receiver.storage.Put(receiver.ctx, "global/master_lease", raw); err != nil {
			t.Fatal(err)
		}
	}
	receiver.mu.Lock()
	receiver.multi.Master = false
	receiver.multi.MasterHostname = "sender"
	receiver.mu.Unlock()
	client := &ws.Client{ID: "master", NodeEndpoint: true}
	client.SetAuthentication(sender.Hostname(), true, true, nil, "node", owner, 0)
	handler := NewWebSocketHandler(receiver, zap.NewNop())
	putLease(owner)
	if !handler.AllowMaster(client) {
		t.Fatal("current connection rejected")
	}
	putLease("replacement-owner")
	if handler.AllowMaster(client) {
		t.Fatal("replaced connection accepted")
	}
	if client.IsAuthenticated() {
		t.Fatal("revoked connection retained authentication")
	}
	putLease(owner)
	if handler.AllowMaster(client) {
		t.Fatal("revoked connection revived with reused lease token")
	}
	handler.OnMessage(client, &ws.Message{Type: "shutdown_server", Data: json.RawMessage(`{}`)})
	if receiver.ctx.Err() != nil {
		t.Fatal("revoked connection issued command after re-election")
	}
}
