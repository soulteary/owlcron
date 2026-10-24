package api

import (
	"net/http"
	"net/url"
)

// Cookie-authenticated browser requests must come from the serving origin.
// Explicit header credentials remain usable by external API clients.
func CORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			parsed, err := url.Parse(origin)
			explicit := r.Header.Get("X-API-Key") != "" || r.Header.Get("Authorization") != "" || r.Header.Get("X-Session-ID") != ""
			if err != nil || (parsed.Host != r.Host && !explicit) {
				http.Error(w, "Cross-origin request denied", http.StatusForbidden)
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			if parsed.Host == r.Host {
				w.Header().Set("Access-Control-Allow-Credentials", "true")
			}
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key, X-Session-ID")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
