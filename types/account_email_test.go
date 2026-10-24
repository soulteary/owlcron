package types

import (
	"encoding/json"
	"testing"
)

func TestAccountEmailConfiguration(t *testing.T) {
	valid := Config{AccountEmailEnabled: true, SMTPHostname: "smtp.example.test", EmailFrom: "OwlCron <noreply@example.test>", BaseAppURL: "https://cron.example.test/owl", BasePath: "/owl"}
	if err := ValidateHTTPConfig(&valid); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Config){
		"missing SMTP":              func(c *Config) { c.SMTPHostname = "" },
		"plaintext SMTP":            func(c *Config) { c.SMTPTLSMode = "none" },
		"plaintext SMTP normalized": func(c *Config) { c.SMTPTLSMode = " NONE " },
		"missing sender":            func(c *Config) { c.EmailFrom = "" },
		"multiple senders":          func(c *Config) { c.EmailFrom = "a@example.test,b@example.test" },
		"HTTP":                      func(c *Config) { c.BaseAppURL = "http://cron.example.test/owl" },
		"relative URL":              func(c *Config) { c.BaseAppURL = "/owl" },
		"credentials":               func(c *Config) { c.BaseAppURL = "https://user:pass@cron.example.test/owl" },
		"query":                     func(c *Config) { c.BaseAppURL += "?x=1" },
		"empty query":               func(c *Config) { c.BaseAppURL += "?" },
		"fragment":                  func(c *Config) { c.BaseAppURL += "#token" },
		"empty fragment":            func(c *Config) { c.BaseAppURL += "#" },
		"path mismatch":             func(c *Config) { c.BasePath = "" },
		"short lifetime":            func(c *Config) { c.AccountTokenTTLSeconds = 59 },
		"long lifetime":             func(c *Config) { c.AccountTokenTTLSeconds = 86401 },
		"negative cooldown":         func(c *Config) { c.AccountEmailCooldownSeconds = -1 },
		"large cooldown":            func(c *Config) { c.AccountEmailCooldownSeconds = 86401 },
		"negative rate":             func(c *Config) { c.AccountEmailRateLimit = -1 },
		"large IP rate":             func(c *Config) { c.AccountEmailIPRateLimit = 100001 },
		"large window":              func(c *Config) { c.AccountEmailRateWindowSeconds = 86401 },
	} {
		t.Run(name, func(t *testing.T) {
			c := valid
			change(&c)
			if ValidateHTTPConfig(&c) == nil {
				t.Fatal("unsafe configuration accepted")
			}
		})
	}
	local := valid
	local.BaseAppURL = "http://localhost:3012/owl"
	local.SMTPTLSMode = "none"
	local.AccountEmailAllowInsecure = true
	if err := ValidateHTTPConfig(&local); err != nil {
		t.Fatal(err)
	}
	if err := ValidateHTTPConfig(&Config{}); err != nil {
		t.Fatal("legacy configuration rejected", err)
	}
	if err := ValidateHTTPConfig(&Config{SMTPTLSMode: "none"}); err != nil {
		t.Fatal("disabled legacy plaintext SMTP configuration rejected", err)
	}
	for _, mode := range []string{"", "starttls", "implicit"} {
		c := valid
		c.SMTPTLSMode = mode
		if err := ValidateHTTPConfig(&c); err != nil {
			t.Fatal("secure SMTP configuration rejected", err)
		}
	}
}

func TestVerifiedEmailStorageAndPublicStatus(t *testing.T) {
	var legacy User
	if err := json.Unmarshal([]byte(`{"id":"legacy","email":"user@example.test"}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.SafeMap()["email_verified"] != false {
		t.Fatal("legacy email trusted without verification")
	}
	legacy.VerifiedEmail = legacy.Email
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	var loaded User
	if err = json.Unmarshal(raw, &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.VerifiedEmail != legacy.Email || loaded.SafeMap()["email_verified"] != true {
		t.Fatal("verified address not preserved")
	}
	if _, ok := loaded.SafeMap()["verified_email"]; ok {
		t.Fatal("raw verification state disclosed")
	}
	loaded.Email = "changed@example.test"
	if loaded.SafeMap()["email_verified"] != false {
		t.Fatal("changed email remained verified")
	}
}
