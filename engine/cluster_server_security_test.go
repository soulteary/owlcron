package engine

import (
	"crypto/x509"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestClusterMTLSRequirementDoesNotBlockBrowserHTTPS(t *testing.T) {
	// Use a CA-trusted fixture certificate to exercise the real HTTPS handshake,
	// rather than accepting a forged Request.TLS field in a unit test.
	seed := httptest.NewTLSServer(http.NotFoundHandler())
	defer seed.Close()
	certificate := seed.TLS.Certificates[0]
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]})
	keyDER, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	if err = os.WriteFile(certPath, certPEM, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600); err != nil {
		t.Fatal(err)
	}
	e := newSchedulingTestEngine(t, "example.com")
	e.config.ClusterAllowInsecure = false
	e.config.ClusterTLSCertFile = certPath
	e.config.ClusterTLSKeyFile = keyPath
	e.config.ClusterTLSCAFile = certPath
	e.config.ClusterTLSRequireClientCert = true
	e.clusterTLS, err = initClusterTLS(e.config)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	e.comm.RegisterRoutes(mux)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	server := httptest.NewUnstartedServer(mux)
	server.TLS = e.ClusterServerTLSConfig()
	server.StartTLS()
	defer server.Close()
	client := server.Client()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, tc := range []struct {
				path string
				code int
			}{{"/", 204}, {"/cluster/ws", 403}} {
				response, err := client.Get(server.URL + tc.path)
				if err != nil {
					t.Error(err)
					return
				}
				io.Copy(io.Discard, response.Body)
				response.Body.Close()
				if response.StatusCode != tc.code {
					t.Errorf("%s status=%d", tc.path, response.StatusCode)
				}
			}
		}()
	}
	wg.Wait()
	// An HTTPS request carrying a browser Origin cannot enter the node route.
	request, _ := http.NewRequest("GET", server.URL+"/cluster/ws", nil)
	request.Header.Set("Origin", server.URL)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("browser origin entered node route: %d", response.StatusCode)
	}
}
