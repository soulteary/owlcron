package types

import (
	"errors"
	"net/mail"
	"net/url"
	"strings"
)

// ValidateAccountEmailConfig validates the opt-in account recovery policy.
// Zero limits select the documented safe defaults; they never disable limits.
func ValidateAccountEmailConfig(c *Config) error {
	if c == nil {
		return errors.New("configuration is required")
	}
	if c.AccountTokenTTLSeconds != 0 && (c.AccountTokenTTLSeconds < 60 || c.AccountTokenTTLSeconds > 86400) {
		return errors.New("account_token_ttl_seconds must be zero or between 60 and 86400")
	}
	for _, n := range []int{c.AccountEmailCooldownSeconds, c.AccountEmailRateWindowSeconds} {
		if n < 0 || n > 86400 {
			return errors.New("account email time limits must be between 0 and 86400 seconds")
		}
	}
	for _, n := range []int{c.AccountEmailRateLimit, c.AccountEmailIPRateLimit} {
		if n < 0 || n > 100000 {
			return errors.New("account email request limits must be between 0 and 100000")
		}
	}
	if !c.AccountEmailEnabled {
		return nil
	}
	if strings.TrimSpace(c.SMTPHostname) == "" {
		return errors.New("account email requires an explicit smtp_hostname")
	}
	if strings.EqualFold(strings.TrimSpace(c.SMTPTLSMode), "none") && !c.AccountEmailAllowInsecure {
		return errors.New("account email requires SMTP TLS (plaintext requires account_email_allow_insecure for local testing)")
	}
	if _, err := mail.ParseAddress(c.EmailFrom); err != nil {
		return errors.New("account email requires a valid explicit email_from")
	}
	u, err := url.Parse(c.BaseAppURL)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(c.BaseAppURL, "#") || u.Opaque != "" {
		return errors.New("account email requires an absolute base_app_url without credentials, query or fragment")
	}
	if u.Scheme != "https" && !(c.AccountEmailAllowInsecure && u.Scheme == "http") {
		return errors.New("account email requires an HTTPS base_app_url (HTTP requires account_email_allow_insecure for local testing)")
	}
	if u.RawPath != "" || strings.TrimSuffix(u.Path, "/") != c.BasePath {
		return errors.New("account email base_app_url path must match base_path")
	}
	return nil
}
