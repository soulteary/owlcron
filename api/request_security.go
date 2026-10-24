package api

import (
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"
)

func (s *Server) trustedProxy(ip netip.Addr) bool {
	if !ip.IsValid() {
		return false
	}
	for _, raw := range s.engine.GetConfig().TrustedProxyCIDRs {
		prefix, err := netip.ParsePrefix(raw)
		if err == nil && prefix.Contains(ip.Unmap()) {
			return true
		}
	}
	return false
}

func requestPeer(r *http.Request) netip.Addr {
	raw := r.RemoteAddr
	if host, _, err := net.SplitHostPort(raw); err == nil {
		raw = host
	}
	ip, err := netip.ParseAddr(raw)
	if err != nil || ip.Zone() != "" {
		return netip.Addr{}
	}
	return ip.Unmap()
}

// clientIP walks from the socket peer towards the client. Once an untrusted
// hop is reached, every address supplied to its left is ignored.
func (s *Server) clientIP(r *http.Request) string {
	peer := requestPeer(r)
	if !peer.IsValid() {
		return "unknown"
	}
	fallback := peer.String()
	if !s.trustedProxy(peer) {
		return fallback
	}
	values := r.Header.Values("X-Forwarded-For")
	if len(values) != 1 || len(values[0]) > 4096 {
		return fallback
	}
	parts := strings.Split(values[0], ",")
	if len(parts) > 32 {
		return fallback
	}
	chain := make([]netip.Addr, len(parts))
	for i, raw := range parts {
		ip, err := netip.ParseAddr(strings.TrimSpace(raw))
		if err != nil || ip.Zone() != "" {
			return fallback
		}
		chain[i] = ip.Unmap()
	}
	current := peer
	for i := len(chain) - 1; i >= 0 && s.trustedProxy(current); i-- {
		current = chain[i]
	}
	return current.String()
}

func (s *Server) secureCookie(r *http.Request) bool {
	if r.TLS != nil || s.engine.GetConfig().SessionCookieSecure {
		return true
	}
	if !s.trustedProxy(requestPeer(r)) {
		return false
	}
	values := r.Header.Values("X-Forwarded-Proto")
	return len(values) == 1 && values[0] == "https"
}

func (s *Server) setSessionCookie(w http.ResponseWriter, r *http.Request, value string) {
	cookie := &http.Cookie{Name: "session_id", Value: value, Path: "/", HttpOnly: true,
		SameSite: http.SameSiteLaxMode, Secure: s.secureCookie(r)}
	if value == "" {
		cookie.MaxAge = -1
		cookie.Expires = time.Unix(1, 0).UTC()
	} else {
		cookie.MaxAge = sessionExpireDays * 24 * 60 * 60
		cookie.Expires = time.Now().Add(time.Duration(cookie.MaxAge) * time.Second).UTC()
	}
	http.SetCookie(w, cookie)
}
