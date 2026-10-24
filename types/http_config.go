package types

import (
	"errors"
	"net/netip"
	"net/url"
	"regexp"
	"strings"
)

// ValidateHTTPConfig keeps zero as the safe default; negative values never
// silently disable a protection. Shared by the engine and executable server.
func ValidateHTTPConfig(c *Config) error {
	if c == nil {
		return errors.New("configuration is required")
	}
	if c.BasePath != "" {
		if !regexp.MustCompile(`^(/[A-Za-z0-9_-]+)+$`).MatchString(c.BasePath) || strings.HasPrefix(c.BasePath, "/cluster") {
			return errors.New("base_path must contain nonempty alphanumeric, underscore or hyphen segments and must not use /cluster")
		}
		if c.BaseAppURL != "" {
			u, err := url.Parse(c.BaseAppURL)
			if err != nil || strings.TrimSuffix(u.Path, "/") != c.BasePath {
				return errors.New("base_app_url path must match base_path")
			}
		}
	}
	for _, seconds := range []int{c.HTTPReadHeaderTimeoutSeconds, c.HTTPReadTimeoutSeconds, c.HTTPWriteTimeoutSeconds, c.HTTPIdleTimeoutSeconds, c.HTTPRequestTimeoutSeconds, c.HTTPStreamTimeoutSeconds} {
		if seconds < 0 || seconds > 3600 {
			return errors.New("HTTP timeouts must be between 0 and 3600 seconds")
		}
	}
	if c.LoginRateLimit < 0 || c.LoginRateLimit > 100000 || c.LoginAccountRateLimit < 0 || c.LoginAccountRateLimit > 100000 || c.LoginRateWindowSeconds < 0 || c.LoginRateWindowSeconds > 86400 || c.LoginMaxConcurrent < 0 || c.LoginMaxConcurrent > 128 || c.LoginLimiterMaxEntries < 0 || c.LoginLimiterMaxEntries > 1000000 {
		return errors.New("invalid login protection limits")
	}
	if c.LoginLimiterMaxEntries > 0 && c.LoginLimiterMaxEntries < 3 {
		return errors.New("login limiter capacity must allow at least three buckets")
	}
	if len(c.TrustedProxyCIDRs) > 1024 {
		return errors.New("too many trusted proxy networks")
	}
	for _, raw := range c.TrustedProxyCIDRs {
		prefix, err := netip.ParsePrefix(raw)
		if err != nil || prefix.Addr().Is4In6() {
			return errors.New("invalid trusted proxy CIDR")
		}
	}
	return ValidateAccountEmailConfig(c)
}
