package engine

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/soulteary/owlcron/types"
)

type webHookPolicy struct {
	hosts    map[string]bool
	networks []netip.Prefix
	lookup   func(context.Context, string) ([]net.IPAddr, error)
	dial     func(context.Context, string, string) (net.Conn, error)
	err      error
}

type webHookPolicyError struct{}

func (*webHookPolicyError) Error() string {
	return "webhook destination is prohibited by security policy"
}
func blockedWebHook() error { return &webHookPolicyError{} }

func newWebHookPolicy(config *types.Config) *webHookPolicy {
	p := &webHookPolicy{hosts: make(map[string]bool), lookup: net.DefaultResolver.LookupIPAddr, dial: (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext}
	if config == nil {
		return p
	}
	for _, host := range config.WebHookAllowedHosts {
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" || strings.ContainsAny(host, "/@?#*%\\ \t\r\n") || strings.HasSuffix(host, ".") {
			p.err = errors.New("invalid webhook allowed host")
			return p
		}
		if ip, err := netip.ParseAddr(host); err == nil {
			host = ip.Unmap().String()
		} else if strings.Contains(host, ":") || !validWebHookDNSHost(host) {
			p.err = errors.New("invalid webhook allowed host")
			return p
		}
		p.hosts[host] = true
	}
	for _, raw := range config.WebHookAllowedCIDRs {
		prefix, err := netip.ParsePrefix(raw)
		if err != nil || prefix.Addr().Is4In6() {
			p.err = errors.New("invalid webhook allowed CIDR")
			return p
		}
		p.networks = append(p.networks, prefix.Masked())
	}
	return p
}
func validateWebHookPolicy(config *types.Config) error { return newWebHookPolicy(config).err }

func validWebHookDNSHost(host string) bool {
	if len(host) > 253 || host == "" {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, ch := range label {
			if !(ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '-') {
				return false
			}
		}
	}
	return true
}
func parseWebHookURL(target string) (*url.URL, error) {
	u, err := url.Parse(target)
	if err != nil || u.Opaque != "" || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil || u.Fragment != "" || strings.Contains(target, "#") || strings.ContainsAny(u.Host, "%\\ \t\r\n") {
		return nil, blockedWebHook()
	}
	host := strings.ToLower(u.Hostname())
	if host == "" || strings.HasSuffix(host, ".") {
		return nil, blockedWebHook()
	}
	if _, err := netip.ParseAddr(host); err != nil && !validWebHookDNSHost(host) {
		return nil, blockedWebHook()
	}
	if strings.HasSuffix(u.Host, ":") {
		return nil, blockedWebHook()
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 || strconv.Itoa(n) != port {
			return nil, blockedWebHook()
		}
	}
	return u, nil
}

// Special-use ranges are denied by default, including metadata and transition networks.
var webHookSpecialNetworks = func() []netip.Prefix {
	var result []netip.Prefix
	for _, s := range []string{"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24", "192.88.99.0/24", "192.168.0.0/16", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4", "::/96", "::ffff:0:0/96", "64:ff9b::/96", "64:ff9b:1::/48", "100::/64", "2001::/23", "2001:db8::/32", "2002::/16", "3fff::/20", "fc00::/7", "fe80::/10", "ff00::/8"} {
		result = append(result, netip.MustParsePrefix(s))
	}
	return result
}()

// Transition and translation ranges can conceal an otherwise forbidden IPv4 target.
// Explicit trust never enables these ranges or cloud metadata service endpoints.
var webHookAlwaysDeniedNetworks = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("2001::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("fec0::/10"),
}

func (p *webHookPolicy) allowedAddress(host string, addr netip.Addr) (bool, bool) {
	addr = addr.Unmap()
	// These destinations remain forbidden even when a broad allowlist is configured.
	if !addr.IsValid() || addr.IsUnspecified() || addr.IsMulticast() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() || (addr == netip.MustParseAddr("100.100.100.200") || addr == netip.MustParseAddr("168.63.129.16") || addr == netip.MustParseAddr("fd00:ec2::254")) {
		return false, false
	}
	for _, prefix := range webHookAlwaysDeniedNetworks {
		if prefix.Contains(addr) {
			return false, false
		}
	}
	if addr.Is6() && netip.MustParsePrefix("::/96").Contains(addr) && !addr.IsLoopback() {
		return false, false
	}
	explicit := p.hosts[host]
	for _, prefix := range p.networks {
		if prefix.Contains(addr) {
			explicit = true
		}
	}
	if explicit {
		return true, true
	}
	if !addr.IsGlobalUnicast() || (addr.Is6() && !netip.MustParsePrefix("2000::/3").Contains(addr)) {
		return false, false
	}
	for _, prefix := range webHookSpecialNetworks {
		if prefix.Contains(addr) {
			return false, false
		}
	}
	return true, false
}
func (p *webHookPolicy) resolve(ctx context.Context, host string) ([]netip.Addr, bool, error) {
	if p.err != nil {
		return nil, false, blockedWebHook()
	}
	host = strings.ToLower(host)
	var addresses []net.IPAddr
	if addr, err := netip.ParseAddr(host); err == nil {
		addresses = []net.IPAddr{{IP: net.IP(addr.AsSlice())}}
	} else {
		var err error
		addresses, err = p.lookup(ctx, host)
		if err != nil {
			return nil, false, errors.New("webhook DNS resolution failed")
		}
	}
	if len(addresses) == 0 {
		return nil, false, errors.New("webhook DNS returned no addresses")
	}
	result := make([]netip.Addr, 0, len(addresses))
	allExplicit := true
	for _, address := range addresses {
		ip, ok := netip.AddrFromSlice(address.IP)
		if !ok || address.Zone != "" {
			return nil, false, blockedWebHook()
		}
		allowed, explicit := p.allowedAddress(host, ip)
		if !allowed {
			return nil, false, blockedWebHook()
		}
		allExplicit = allExplicit && explicit
		result = append(result, ip.Unmap())
	}
	return result, allExplicit, nil
}
func (p *webHookPolicy) validate(ctx context.Context, target string) error {
	u, err := parseWebHookURL(target)
	if err != nil {
		return err
	}
	_, explicit, err := p.resolve(ctx, u.Hostname())
	if err != nil {
		return err
	}
	if u.Scheme != "https" && !explicit {
		return blockedWebHook()
	}
	return nil
}

type webHookHTTPContextKey struct{}

func (p *webHookPolicy) dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, blockedWebHook()
	}
	ips, explicit, err := p.resolve(ctx, host)
	if err != nil {
		return nil, err
	}
	if required, _ := ctx.Value(webHookHTTPContextKey{}).(bool); required && !explicit {
		return nil, blockedWebHook()
	}
	for _, ip := range ips {
		conn, dialErr := p.dial(ctx, network, net.JoinHostPort(ip.String(), port))
		if dialErr == nil {
			return conn, nil
		}
		err = dialErr
	}
	return nil, err
}
func (p *webHookPolicy) client() *http.Client {
	return &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Transport: &http.Transport{Proxy: nil, DialContext: p.dialContext, ForceAttemptHTTP2: true, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 10 * time.Second, MaxIdleConns: 32, MaxIdleConnsPerHost: 4, IdleConnTimeout: 30 * time.Second}}
}
func (whm *WebHookManager) ValidateWebHookURL(ctx context.Context, target string) error {
	return whm.policy.validate(ctx, target)
}
