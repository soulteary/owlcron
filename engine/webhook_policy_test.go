package engine

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/soulteary/owlcron/types"
)

func TestWebHookPolicyRejectsUnsafeDestinations(t *testing.T) {
	p := newWebHookPolicy(nil)
	p.lookup = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
	}
	for _, target := range []string{
		"http://8.8.8.8/hook", "https://user:secret@8.8.8.8/hook", "https://8.8.8.8/hook#fragment", "https://8.8.8.8:0/hook", "https://8.8.8.8:65536/hook", "https://8.8.8.8:0443/hook", "https://8.8.8.8:/hook", "ftp://8.8.8.8/hook", "https://localhost./hook", "https://[fe80::1%25eth0]/hook",
		"https://127.0.0.1/hook", "https://10.0.0.1/hook", "https://172.16.1.1/hook", "https://192.168.1.1/hook", "https://169.254.169.254/latest/meta-data", "https://100.100.100.200/hook", "https://168.63.129.16/hook", "https://0.0.0.0/hook", "https://224.0.0.1/hook", "https://198.18.0.1/hook", "https://192.0.2.1/hook", "https://[::1]/hook", "https://[::ffff:127.0.0.1]/hook", "https://[fc00::1]/hook", "https://[2001:db8::1]/hook", "https://[64:ff9b::a00:1]/hook", "https://2130706433/hook", "https://127.1/hook",
	} {
		t.Run(target, func(t *testing.T) {
			if err := p.validate(context.Background(), target); err == nil {
				t.Fatal("unsafe destination accepted")
			}
		})
	}
	for _, target := range []string{"https://8.8.8.8/hook", "https://[2606:4700:4700::1111]/hook"} {
		if err := p.validate(context.Background(), target); err != nil {
			t.Fatalf("public address rejected: %v", err)
		}
	}
}
func TestWebHookPolicyExplicitTrustAndNeverMetadata(t *testing.T) {
	p := newWebHookPolicy(&types.Config{WebHookAllowedHosts: []string{"127.0.0.1"}, WebHookAllowedCIDRs: []string{"10.2.0.0/16", "169.254.0.0/16", "100.100.100.200/32", "168.63.129.16/32"}})
	for _, target := range []string{"http://127.0.0.1:8080/hook", "http://10.2.3.4/hook"} {
		if err := p.validate(context.Background(), target); err != nil {
			t.Fatal(err)
		}
	}
	for _, target := range []string{"http://127.0.0.2/hook", "http://10.3.0.1/hook", "http://169.254.169.254/hook", "https://100.100.100.200/hook", "https://168.63.129.16/hook"} {
		if err := p.validate(context.Background(), target); err == nil {
			t.Fatal("unexpected trust", target)
		}
	}
}
func TestWebHookPolicyDNSMixedAnswersAndRebinding(t *testing.T) {
	p := newWebHookPolicy(nil)
	var lookups atomic.Int32
	p.lookup = func(context.Context, string) ([]net.IPAddr, error) {
		if lookups.Add(1) == 1 {
			return []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}}, nil
		}
		return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
	}
	var dials atomic.Int32
	p.dial = func(context.Context, string, string) (net.Conn, error) { dials.Add(1); return nil, errors.New("test") }
	if err := p.validate(context.Background(), "https://webhook.example/hook"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.dialContext(context.Background(), "tcp", "webhook.example:443"); err == nil {
		t.Fatal("rebound DNS accepted")
	}
	if dials.Load() != 0 {
		t.Fatal("private address was dialed")
	}
	p.lookup = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}, {IP: net.ParseIP("10.0.0.1")}}, nil
	}
	if err := p.validate(context.Background(), "https://webhook.example/hook"); err == nil {
		t.Fatal("mixed DNS answers accepted")
	}
}
func TestWebHookPolicyPinsDialAndConcurrentValidation(t *testing.T) {
	p := newWebHookPolicy(nil)
	p.lookup = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}}, nil
	}
	p.dial = func(_ context.Context, network, address string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			t.Error(err)
		}
		if _, err := netip.ParseAddr(host); err != nil {
			t.Error("dial did not pin numeric IP")
		}
		return nil, errors.New("test dial")
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.validate(context.Background(), "https://webhook.example/hook"); err != nil {
				t.Error(err)
			}
			_, _ = p.dialContext(context.Background(), "tcp", "webhook.example:443")
		}()
	}
	wg.Wait()
	if p.client().Transport.(*http.Transport).Proxy != nil {
		t.Fatal("environment proxy enabled")
	}
}
func TestWebHookPolicyHTTPRebindingRequiresExplicitTrust(t *testing.T) {
	p := newWebHookPolicy(&types.Config{WebHookAllowedCIDRs: []string{"10.0.0.0/8"}})
	var n atomic.Int32
	p.lookup = func(context.Context, string) ([]net.IPAddr, error) {
		if n.Add(1) == 1 {
			return []net.IPAddr{{IP: net.ParseIP("10.1.2.3")}}, nil
		}
		return []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}}, nil
	}
	if err := p.validate(context.Background(), "http://trusted.example/hook"); err != nil {
		t.Fatal(err)
	}
	p.dial = func(context.Context, string, string) (net.Conn, error) {
		t.Error("HTTP dial escaped trusted CIDR")
		return nil, errors.New("test")
	}
	ctx := context.WithValue(context.Background(), webHookHTTPContextKey{}, true)
	if _, err := p.dialContext(ctx, "tcp", "trusted.example:80"); err == nil {
		t.Fatal("HTTP trust changed")
	}
}
func TestWebHookPolicyConfigurationValidation(t *testing.T) {
	for _, c := range []*types.Config{{WebHookAllowedHosts: []string{"*.example.com"}}, {WebHookAllowedHosts: []string{"https://example.com"}}, {WebHookAllowedHosts: []string{"example.com:443"}}, {WebHookAllowedCIDRs: []string{"bad"}}} {
		if err := validateWebHookPolicy(c); err == nil {
			t.Fatal("invalid policy accepted")
		}
	}
}

func TestWebHookPolicyExplicitTrustCannotBypassMetadataViaIPv6(t *testing.T) {
	targets := []string{
		"fd00:ec2::254", "64:ff9b::a9fe:a9fe", "64:ff9b:1::a9fe:a9fe",
		"2002:a9fe:a9fe::1", "2001:0:4136:e378:8000:63bf:5601:5601", "::a9fe:a9fe",
		"0.1.2.3", "255.255.255.255", "fec0::1",
	}
	for _, target := range targets {
		t.Run(target, func(t *testing.T) {
			for _, config := range []*types.Config{
				{WebHookAllowedHosts: []string{target}},
				{WebHookAllowedCIDRs: []string{"::/0", "0.0.0.0/0"}},
			} {
				policy := newWebHookPolicy(config)
				addr := netip.MustParseAddr(target)
				if allowed, _ := policy.allowedAddress(target, addr); allowed {
					t.Fatal("explicit trust bypassed metadata protection")
				}
			}
		})
	}
	// Ordinary trusted IPv6 private and loopback destinations remain supported.
	p := newWebHookPolicy(&types.Config{WebHookAllowedCIDRs: []string{"fc00::/7", "::1/128"}})
	for _, target := range []string{"fd12::1", "::1"} {
		if allowed, _ := p.allowedAddress(target, netip.MustParseAddr(target)); !allowed {
			t.Fatal("ordinary trusted IPv6 address denied", target)
		}
	}
}
