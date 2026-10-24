package types

import "testing"

func TestHTTPProtectionConfigurationValidation(t *testing.T) {
	for _, cfg := range []Config{
		{HTTPReadHeaderTimeoutSeconds: -1}, {HTTPReadTimeoutSeconds: -1}, {HTTPWriteTimeoutSeconds: -1}, {HTTPIdleTimeoutSeconds: -1}, {HTTPRequestTimeoutSeconds: -1}, {HTTPStreamTimeoutSeconds: -1}, {HTTPStreamTimeoutSeconds: 3601},
		{LoginRateLimit: -1}, {LoginAccountRateLimit: -1}, {LoginRateWindowSeconds: -1}, {LoginMaxConcurrent: -1}, {LoginLimiterMaxEntries: -1}, {LoginLimiterMaxEntries: 2},
		{LoginMaxConcurrent: 129}, {LoginLimiterMaxEntries: 1000001}, {TrustedProxyCIDRs: []string{"127.0.0.1"}}, {TrustedProxyCIDRs: []string{"::ffff:127.0.0.1/128"}},
	} {
		if err := ValidateHTTPConfig(&cfg); err == nil {
			t.Fatalf("invalid protection config accepted: %+v", cfg)
		}
	}
	for _, cfg := range []Config{{}, {TrustedProxyCIDRs: []string{"127.0.0.1/32", "::1/128"}}, {SessionCookieSecure: true, HTTPStreamTimeoutSeconds: 300, LoginLimiterMaxEntries: 3}} {
		if err := ValidateHTTPConfig(&cfg); err != nil {
			t.Fatalf("safe config rejected: %v", err)
		}
	}
}
