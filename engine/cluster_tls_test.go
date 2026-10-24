package engine

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
)

func TestClusterEndpointSecurityBoundary(t *testing.T) {
	for _, address := range []string{"ws://node/ws", "http://node/", "wss://user:password@node/ws", "wss://node/cluster/ws?token=secret", "wss://node/cluster/ws#fragment", "wss://node/evil", "wss://node/ws", "node/path", "node\nheader", "wss://node:0/", "ftp://node/"} {
		if endpoint, err := clusterNodeEndpoint("node", address, 3012, false); err == nil {
			t.Errorf("accepted endpoint %q -> %q", address, endpoint)
		}
	}
	for _, test := range []struct {
		address, want string
		insecure      bool
	}{
		{"10.0.0.1", "wss://10.0.0.1:3012/cluster/ws", false},
		{"[::1]:3013", "wss://[::1]:3013/cluster/ws", false},
		{"https://node:443/", "wss://node:443/cluster/ws", false},
		{"http://node:3012/ws", "ws://node:3012/cluster/ws", true},
	} {
		actual, err := clusterNodeEndpoint("node", test.address, 3012, test.insecure)
		if err != nil || actual != test.want {
			t.Errorf("endpoint=%q err=%v want=%q", actual, err, test.want)
		}
	}
}

func clusterTestCA(t *testing.T, server *httptest.Server) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	cert := server.TLS.Certificates[0].Certificate[0]
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert}), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestClusterTLSCertificateAndIdentity(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	trusted, err := initClusterTLS(&types.Config{ClusterTLSCAFile: clusterTestCA(t, server)})
	if err != nil {
		t.Fatal(err)
	}
	dial := func(policy *clusterTLS, identity string) error {
		cfg, err := policy.clientConfig(identity)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		d := tls.Dialer{Config: cfg}
		conn, err := d.DialContext(ctx, "tcp", strings.TrimPrefix(server.URL, "https://"))
		if conn != nil {
			conn.Close()
		}
		return err
	}
	if err := dial(trusted, "example.com"); err != nil {
		t.Fatalf("dedicated CA failed: %v", err)
	}
	if err := dial(trusted, "wrong.example"); err == nil {
		t.Fatal("accepted wrong node certificate identity")
	}
	untrusted, err := initClusterTLS(&types.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := dial(untrusted, "example.com"); err == nil {
		t.Fatal("accepted untrusted certificate")
	}
}

func TestClusterTLSConfigurationAndConcurrentClones(t *testing.T) {
	for _, config := range []*types.Config{{ClusterTLSCAFile: "/missing/cluster-ca"}, {ClusterTLSCertFile: "cert"}, {ClusterTLSKeyFile: "key"}, {ClusterTLSRequireClientCert: true}} {
		if _, err := initClusterTLS(config); err == nil {
			t.Fatal("accepted incomplete TLS configuration")
		}
	}
	policy, err := initClusterTLS(&types.Config{})
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for i := 0; i < 64; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			cfg, err := policy.clientConfig("node.example:3012")
			if err != nil || cfg.ServerName != "node.example" || cfg.InsecureSkipVerify || cfg.MinVersion < tls.VersionTLS12 {
				t.Errorf("invalid TLS config: %v", err)
			}
			cfg.ServerName = "mutated"
		}()
	}
	group.Wait()
	if policy.client.ServerName != "" {
		t.Fatal("connection mutated shared config")
	}
	for _, identity := range []string{"", "https://node", "node\r\nheader", "node@other"} {
		if _, err := policy.clientConfig(identity); err == nil {
			t.Errorf("accepted identity %q", identity)
		}
	}
}

func TestClusterEndpointIPv6DefaultPort(t *testing.T) {
	endpoint, err := clusterNodeEndpoint("::1", "", 3012, false)
	if err != nil || endpoint != "wss://[::1]:3012/cluster/ws" {
		t.Fatalf("%q %v", endpoint, err)
	}
	if _, _, err := net.SplitHostPort("[::1]:3012"); err != nil {
		t.Fatal(err)
	}
}

func TestClusterTLSMutualCertificateConfiguration(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	cert := server.TLS.Certificates[0]
	directory := t.TempDir()
	certFile, keyFile := filepath.Join(directory, "cert.pem"), filepath.Join(directory, "key.pem")
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600); err != nil {
		t.Fatal(err)
	}
	policy, err := initClusterTLS(&types.Config{ClusterTLSCAFile: certFile, ClusterTLSCertFile: certFile, ClusterTLSKeyFile: keyFile, ClusterTLSRequireClientCert: true})
	if err != nil {
		t.Fatal(err)
	}
	serverConfig := policy.ServerConfig()
	if serverConfig == nil || serverConfig.ClientAuth != tls.VerifyClientCertIfGiven || serverConfig.ClientCAs == nil || len(policy.client.Certificates) != 1 {
		t.Fatal("mutual TLS certificate policy missing")
	}
	serverConfig.MinVersion = tls.VersionTLS10
	if policy.ServerConfig().MinVersion != tls.VersionTLS12 {
		t.Fatal("server config mutation leaked")
	}
}

func TestClusterTLSMaterialFileBoundaries(t *testing.T) {
	directory := t.TempDir()
	file := filepath.Join(directory, "material")
	if err := os.WriteFile(file, []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []os.FileMode{0400, 0600} {
		if err := os.Chmod(file, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := readClusterTLSFile(file, true); err != nil {
			t.Fatalf("private mode %o: %v", mode, err)
		}
	}
	for _, mode := range []os.FileMode{0644, 0660, 0700} {
		if err := os.Chmod(file, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := readClusterTLSFile(file, true); err == nil {
			t.Fatalf("accepted private mode %o", mode)
		}
	}
	if _, err := readClusterTLSFile(file, false); err != nil {
		t.Fatal("public certificate modes should not require private permissions")
	}
	link := filepath.Join(directory, "link")
	if err := os.Symlink(file, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readClusterTLSFile(link, false); err == nil {
		t.Fatal("accepted symlink")
	}
	if err := os.WriteFile(file, make([]byte, (64<<10)+1), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(file, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readClusterTLSFile(file, true); err == nil {
		t.Fatal("accepted oversized private key")
	}
	if err := os.WriteFile(file, make([]byte, (1<<20)+1), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readClusterTLSFile(file, false); err == nil {
		t.Fatal("accepted oversized public material")
	}
	// A Unix socket exercises non-regular-file rejection without mknod, which
	// unprivileged container seccomp profiles may forbid even for a FIFO.
	// Keep the path short enough for macOS sockaddr_un as well as Linux.
	socketDirectory, err := os.MkdirTemp("/tmp", "owlcron-tls-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDirectory) })
	socket := filepath.Join(socketDirectory, "socket")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	for _, private := range []bool{false, true} {
		if _, err := readClusterTLSFile(socket, private); err == nil {
			t.Fatal("accepted Unix socket")
		}
		if _, err := readClusterTLSFile(directory, private); err == nil {
			t.Fatal("accepted directory")
		}
	}
	t.Run("FIFO", func(t *testing.T) {
		fifo := filepath.Join(directory, "fifo")
		if err := syscall.Mkfifo(fifo, 0600); err != nil {
			if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EOPNOTSUPP) {
				t.Skipf("host/container disallows FIFO creation: %v; Unix-socket and directory rejection remains verified", err)
			}
			t.Fatal(err)
		}
		for _, private := range []bool{false, true} {
			if _, err := readClusterTLSFile(fifo, private); err == nil {
				t.Fatal("accepted FIFO")
			}
		}
	})
}

func TestClusterTLSExplicitCAReplacesSystemTrust(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	policy, err := initClusterTLS(&types.Config{ClusterTLSCAFile: clusterTestCA(t, server)})
	if err != nil {
		t.Fatal(err)
	}
	subjects := policy.client.RootCAs.Subjects()
	if len(subjects) != 1 {
		t.Fatalf("dedicated CA retained other roots: %d", len(subjects))
	}
	cert, err := x509.ParseCertificate(server.TLS.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(subjects[0]) != string(cert.RawSubject) {
		t.Fatal("dedicated trust root differs")
	}
}
