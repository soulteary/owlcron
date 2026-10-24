package engine

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/soulteary/owlcron/types"
)

type NodeAuthData struct {
	Hostname   string `json:"hostname"`
	Master     bool   `json:"master"`
	Timestamp  int64  `json:"timestamp"`
	Nonce      string `json:"nonce"`
	Signature  string `json:"signature"`
	LeaseOwner string `json:"lease_owner,omitempty"`
	Receiver   string `json:"receiver,omitempty"`
	Challenge  string `json:"challenge,omitempty"`
}

type nodeAuthConfig struct {
	private ed25519.PrivateKey
	peers   map[string]ed25519.PublicKey
	mu      sync.Mutex
	seen    map[string]int64
}

func loadNodeAuthConfig(config *types.Config) (*nodeAuthConfig, error) {
	c := &nodeAuthConfig{peers: make(map[string]ed25519.PublicKey)}
	encoded := config.NodeAuthPrivateKey
	if config.NodeAuthKeyFile != "" {
		if encoded != "" {
			return nil, fmt.Errorf("configure only one node private key source")
		}
		secret, err := readWebHookSigningSecret(config.NodeAuthKeyFile)
		if err != nil {
			return nil, fmt.Errorf("cannot load node private key: require a private regular file with mode 0600 and bounded content")
		}
		encoded = strings.TrimSpace(secret)
	}
	if encoded != "" {
		key, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("invalid node private key")
		}
		switch len(key) {
		case ed25519.SeedSize:
			c.private = ed25519.NewKeyFromSeed(key)
		case ed25519.PrivateKeySize:
			c.private = ed25519.NewKeyFromSeed(key[:ed25519.SeedSize])
			if !hmac.Equal(c.private, key) {
				return nil, fmt.Errorf("invalid node private key")
			}
		default:
			return nil, fmt.Errorf("invalid node private key length")
		}
	}
	for host, encoded := range config.NodeAuthPeers {
		key, err := base64.StdEncoding.DecodeString(encoded)
		if !validNodeName(host) || err != nil || len(key) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("invalid node peer public key")
		}
		c.peers[host] = ed25519.PublicKey(append([]byte(nil), key...))
	}
	if len(c.peers) > 0 && len(c.private) == 0 {
		return nil, fmt.Errorf("node peer registration requires a local private key")
	}
	if len(c.private) > 0 {
		own, ok := c.peers[config.Hostname]
		if !ok || !hmac.Equal(own, c.private.Public().(ed25519.PublicKey)) {
			return nil, fmt.Errorf("local node public key must match registered identity")
		}
	}
	return c, nil
}

const maxNodeAuthNonces = 16384

func validNodeName(s string) bool {
	if s == "" || len(s) > 253 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '-' || c == '_' || c == ':') {
			return false
		}
	}
	return true
}
func newNodeChallenge() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}
func nodeAuthPayload(data NodeAuthData) []byte {
	data.Signature = ""
	raw, _ := json.Marshal(struct {
		Domain string
		Proof  NodeAuthData
	}{"owlcron-node-auth-v2", data})
	return raw
}
func (e *Engine) MakeNodeAuthFor(hostname string, master bool, receiver, challenge string) NodeAuthData {
	d := NodeAuthData{Hostname: hostname, Master: master, Timestamp: time.Now().Unix(), Receiver: receiver, Challenge: challenge, Nonce: newNodeChallenge()}
	if master {
		d.LeaseOwner = e.LeadershipToken()
	}
	if hostname != e.Hostname() || !validNodeName(receiver) || len(challenge) != 64 || d.Nonce == "" {
		return d
	}
	d.Signature = e.nodeAuthSignature(d)
	return d
}
func (e *Engine) MakeNodeAuth(hostname string, master bool) NodeAuthData {
	if !e.config.ClusterAllowInsecure {
		return NodeAuthData{}
	}
	return e.MakeNodeAuthFor(hostname, master, e.Hostname(), strings.Repeat("0", 64))
}
func (e *Engine) nodeAuthSignature(data NodeAuthData) string {
	if e.nodeAuth != nil && len(e.nodeAuth.private) > 0 {
		return hex.EncodeToString(ed25519.Sign(e.nodeAuth.private, nodeAuthPayload(data)))
	}
	if !e.config.ClusterAllowInsecure {
		return ""
	}
	h := hmac.New(sha256.New, []byte(e.config.SecretKey))
	h.Write(nodeAuthPayload(data))
	return hex.EncodeToString(h.Sum(nil))
}
func (e *Engine) VerifyNodeAuth(data NodeAuthData) (string, bool, error) {
	if !e.config.ClusterAllowInsecure {
		return "", false, fmt.Errorf("connection-bound authentication required")
	}
	return e.VerifyNodeAuthFor(data, e.Hostname(), strings.Repeat("0", 64))
}
func (e *Engine) isRegisteredNode(hostname string) bool {
	if e.config.ClusterAllowInsecure {
		return true
	}
	if e.storage == nil {
		return false
	}
	raw, err := e.storage.Get(e.ctx, "servers:"+hostname)
	var server types.Server
	return err == nil && json.Unmarshal(raw, &server) == nil && server.Hostname == hostname && !server.Disabled
}

func (e *Engine) VerifyNodeAuthFor(data NodeAuthData, receiver, challenge string) (string, bool, error) {
	fail := func() (string, bool, error) { return "", false, fmt.Errorf("node authentication rejected") }
	now := time.Now().Unix()
	if !validNodeName(data.Hostname) || receiver != e.Hostname() || data.Receiver != receiver || len(challenge) != 64 || data.Challenge != challenge || data.Timestamp < now-60 || data.Timestamp > now+60 {
		return fail()
	}
	nonce, err := hex.DecodeString(data.Nonce)
	if err != nil || len(nonce) != 32 {
		return fail()
	}
	actual, err := hex.DecodeString(data.Signature)
	if err != nil {
		return fail()
	}
	verified := false
	if e.nodeAuth != nil {
		if key, ok := e.nodeAuth.peers[data.Hostname]; ok {
			verified = ed25519.Verify(key, nodeAuthPayload(data), actual)
		}
	}
	if !verified && (e.nodeAuth == nil || len(e.nodeAuth.peers) == 0) && e.config.ClusterAllowInsecure && len(e.config.SecretKey) >= 32 && !strings.HasPrefix(e.config.SecretKey, "CHANGE_ME") {
		h := hmac.New(sha256.New, []byte(e.config.SecretKey))
		h.Write(nodeAuthPayload(data))
		verified = hmac.Equal(actual, h.Sum(nil))
	}
	if !verified {
		return fail()
	}
	if !e.isRegisteredNode(data.Hostname) {
		return fail()
	}
	if data.Master && !e.ValidateNodeLeadership(data.Hostname, data.LeaseOwner) {
		return fail()
	}
	digest := sha256.Sum256(append(nodeAuthPayload(data), actual...))
	key := hex.EncodeToString(digest[:])
	if e.nodeAuth == nil {
		return fail()
	}
	e.nodeAuth.mu.Lock()
	defer e.nodeAuth.mu.Unlock()
	if e.nodeAuth.seen == nil {
		e.nodeAuth.seen = make(map[string]int64)
	}
	for key, expires := range e.nodeAuth.seen {
		if expires < now {
			delete(e.nodeAuth.seen, key)
		}
	}
	if _, ok := e.nodeAuth.seen[key]; ok {
		return fail()
	}
	if len(e.nodeAuth.seen) >= maxNodeAuthNonces {
		return fail()
	}
	e.nodeAuth.seen[key] = now + 120
	return data.Hostname, data.Master, nil
}
