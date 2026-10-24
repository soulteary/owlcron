package engine

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
)

func TestClusterProductionWSSMTLSDispatchReconnectAndTakeover(t *testing.T) {
	a := newSchedulingTestEngine(t, "node-a")
	b := newSchedulingTestEngine(t, "node-b")
	shareSchedulingStore(t, a, b)
	caPub, caKey, _ := ed25519.GenerateKey(rand.Reader)
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "cluster test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, caPub, caKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	if err = os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0600); err != nil {
		t.Fatal(err)
	}
	keys := map[string]string{}
	private := map[string]string{}
	for _, node := range []*Engine{a, b} {
		pub, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		keys[node.Hostname()] = base64.StdEncoding.EncodeToString(pub)
		private[node.Hostname()] = base64.StdEncoding.EncodeToString(key)
	}
	for index, node := range []*Engine{a, b} {
		pub, key, _ := ed25519.GenerateKey(rand.Reader)
		cert := &x509.Certificate{SerialNumber: big.NewInt(int64(index + 2)), Subject: pkix.Name{CommonName: node.Hostname()}, DNSNames: []string{node.Hostname()}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		der, err := x509.CreateCertificate(rand.Reader, cert, ca, pub, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		certPath := filepath.Join(dir, node.Hostname()+".pem")
		keyPath := filepath.Join(dir, node.Hostname()+".key")
		if err = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600); err != nil {
			t.Fatal(err)
		}
		node.config.ClusterAllowInsecure = false
		node.config.ClusterTLSCAFile = caPath
		node.config.ClusterTLSCertFile = certPath
		node.config.ClusterTLSKeyFile = keyPath
		node.config.ClusterTLSRequireClientCert = true
		node.config.NodeAuthPrivateKey = private[node.Hostname()]
		node.config.NodeAuthPeers = keys
		node.nodeAuth, err = loadNodeAuthConfig(node.config)
		if err != nil {
			t.Fatal(err)
		}
		node.clusterTLS, err = initClusterTLS(node.config)
		if err != nil {
			t.Fatal(err)
		}
	}
	servers := map[string]*httptest.Server{}
	for _, node := range []*Engine{a, b} {
		mux := http.NewServeMux()
		node.comm.RegisterRoutes(mux)
		server := httptest.NewUnstartedServer(mux)
		server.TLS = node.ClusterServerTLSConfig()
		server.StartTLS()
		defer server.Close()
		servers[node.Hostname()] = server
		if err = a.comm.SaveServerToStorage(&types.Server{Hostname: node.Hostname(), IP: strings.TrimPrefix(server.URL, "https://")}); err != nil {
			t.Fatal(err)
		}
	}
	setMasterForTest(a)
	if err = a.comm.ConnectToSlave(b.Hostname(), strings.TrimPrefix(servers[b.Hostname()].URL, "https://")); err != nil {
		t.Fatal(err)
	}
	a.comm.SendMasterPings()
	waitForCondition(t, time.Second, func() bool { return b.GetMulti().MasterHostname == a.Hostname() })
	event := saveSchedulingFixture(t, a, "production-wss-event")
	event.Target = b.Hostname()
	if err = a.scheduler.AddEvent(event); err != nil {
		t.Fatal(err)
	}
	if err = a.jobManager.LaunchJob(event, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, 3*time.Second, func() bool { return len(a.GetDeadJobs()) == 1 })
	peer, _ := a.comm.GetSlaveConnection(b.Hostname())
	first := peer.Socket
	first.Close()
	waitForCondition(t, 4*time.Second, func() bool {
		peer, ok := a.comm.GetSlaveConnection(b.Hostname())
		return ok && peer.Socket != nil && peer.Socket != first
	})
	a.comm.SendMasterPings()
	if err = a.jobManager.LaunchJob(event, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, 3*time.Second, func() bool { return len(a.GetDeadJobs()) == 2 })
	// Simulate old leader process suspension: close its links and expire its
	// durable lease while its local Master bit remains stale.
	a.comm.ResetCluster()
	raw, err := a.storage.Get(context.Background(), "global/master_lease")
	if err != nil {
		t.Fatal(err)
	}
	var lease leadershipLease
	if err = json.Unmarshal(raw, &lease); err != nil {
		t.Fatal(err)
	}
	lease.Expires = time.Now().Add(-time.Second).UnixNano()
	raw, _ = json.Marshal(lease)
	if err = a.storage.Put(a.ctx, "global/master_lease", raw); err != nil {
		t.Fatal(err)
	}
	b.goMaster()
	if !b.IsMaster() {
		t.Fatal("replacement did not acquire leadership")
	}
	a.goSlave()
	waitForCondition(t, 4*time.Second, func() bool { p, ok := b.comm.GetSlaveConnection(a.Hostname()); return ok && p.Socket != nil })
	b.comm.SendMasterPings()
	waitForCondition(t, time.Second, func() bool { return a.GetMulti().MasterHostname == b.Hostname() })
	// Reverse the job direction after takeover. Both nodes must still authenticate
	// with their own key and certificate, rather than reusing the old master role.
	event = saveSchedulingFixture(t, b, "post-takeover-wss")
	event.Target = a.Hostname()
	if err = b.scheduler.AddEvent(event); err != nil {
		t.Fatal(err)
	}
	if err = b.jobManager.LaunchJob(event, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, 3*time.Second, func() bool {
		for _, job := range b.GetDeadJobs() {
			if job.Event == event.ID && job.Hostname == a.Hostname() && job.Code == 0 {
				return true
			}
		}
		return false
	})
	if a.ValidateNodeLeadership(a.Hostname(), a.LeadershipToken()) {
		t.Fatal("old leader remained authoritative")
	}
}
