package engine

import (
	"crypto/tls"
	"net/http"
)

// ClusterServerTLSConfig returns an isolated server configuration. Browser
// clients do not need certificates; required mTLS is enforced at the node route.
func (e *Engine) ClusterServerTLSConfig() *tls.Config {
	if e.clusterTLS == nil {
		return nil
	}
	return e.clusterTLS.ServerConfig()
}

func (c *Communication) serveNodeWebSocket(w http.ResponseWriter, r *http.Request) {
	cfg := c.engine.config
	if !cfg.ClusterAllowInsecure && r.TLS == nil {
		http.Error(w, "TLS required for node connections", http.StatusForbidden)
		return
	}
	if cfg.ClusterTLSRequireClientCert && (r.TLS == nil || len(r.TLS.VerifiedChains) == 0) {
		http.Error(w, "Verified client certificate required", http.StatusForbidden)
		return
	}
	// Browser-origin requests cannot enter the privileged node protocol.
	if r.Header.Get("Origin") != "" {
		http.Error(w, "Browser node connections are prohibited", http.StatusForbidden)
		return
	}
	c.mu.RLock()
	server, closed := c.wsServer, c.closed
	c.mu.RUnlock()
	if closed || server == nil {
		http.Error(w, "Node communication unavailable", http.StatusServiceUnavailable)
		return
	}
	server.HandleNodeWebSocket(w, r)
}
