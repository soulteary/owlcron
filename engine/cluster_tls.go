package engine

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/soulteary/owlcron/types"
)

// clusterTLS holds immutable configuration; each connection receives a clone.
type clusterTLS struct {
	client        *tls.Config
	server        *tls.Config
	allowInsecure bool
}

func initClusterTLS(config *types.Config) (*clusterTLS, error) {
	var roots *x509.CertPool
	if config.ClusterTLSCAFile != "" {
		pem, err := readClusterTLSFile(config.ClusterTLSCAFile, false)
		if err != nil {
			return nil, fmt.Errorf("cannot read cluster TLS CA file")
		}
		roots = x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("cluster TLS CA file contains no certificates")
		}
	} else {
		var err error
		roots, err = x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
	}
	result := &clusterTLS{client: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, allowInsecure: config.ClusterAllowInsecure}
	if (config.ClusterTLSCertFile == "") != (config.ClusterTLSKeyFile == "") {
		return nil, fmt.Errorf("cluster TLS certificate and key must be configured together")
	}
	if config.ClusterTLSCertFile != "" {
		certPEM, certErr := readClusterTLSFile(config.ClusterTLSCertFile, false)
		keyPEM, keyErr := readClusterTLSFile(config.ClusterTLSKeyFile, true)
		if certErr != nil || keyErr != nil {
			return nil, fmt.Errorf("cannot read cluster TLS certificate and private key")
		}
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, fmt.Errorf("cannot load cluster TLS certificate and key")
		}
		result.client.Certificates = []tls.Certificate{cert}
		result.server = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}
	}
	if config.ClusterTLSRequireClientCert {
		if result.server == nil || config.ClusterTLSCAFile == "" {
			return nil, fmt.Errorf("cluster TLS client authentication requires a server certificate and explicit CA")
		}
		result.server.ClientAuth = tls.VerifyClientCertIfGiven
		result.server.ClientCAs = roots
	}
	return result, nil
}

func (c *clusterTLS) ServerConfig() *tls.Config {
	if c == nil || c.server == nil {
		return nil
	}
	return c.server.Clone()
}

func (c *clusterTLS) clientConfig(hostname string) (*tls.Config, error) {
	if c == nil {
		return nil, fmt.Errorf("cluster TLS is not initialized")
	}
	identity := hostname
	if strings.ContainsAny(identity, "/?#@\\ \t\r\n") {
		return nil, fmt.Errorf("invalid cluster node identity")
	}
	if h, port, err := net.SplitHostPort(identity); err == nil {
		value, err := strconv.Atoi(port)
		if err != nil || value < 1 || value > 65535 {
			return nil, fmt.Errorf("invalid cluster node identity")
		}
		identity = h
	}
	identity = strings.Trim(identity, "[]")
	if identity == "" || strings.ContainsAny(identity, "/?#@\\ \t\r\n") {
		return nil, fmt.Errorf("invalid cluster node identity")
	}
	config := c.client.Clone()
	config.ServerName = identity
	return config, nil
}

func clusterNodeEndpoint(hostname, address string, port int, allowInsecure bool) (string, error) {
	if address == "" {
		address = hostname
	}
	if address == "" || strings.ContainsAny(address, "\\ \t\r\n") {
		return "", fmt.Errorf("invalid cluster endpoint")
	}
	var endpoint *url.URL
	if strings.Contains(address, "://") {
		parsed, err := url.Parse(address)
		if err != nil {
			return "", fmt.Errorf("invalid cluster endpoint")
		}
		endpoint = parsed
		switch endpoint.Scheme {
		case "https":
			endpoint.Scheme = "wss"
		case "http":
			endpoint.Scheme = "ws"
		case "ws", "wss":
		default:
			return "", fmt.Errorf("unsupported cluster endpoint scheme")
		}
	} else {
		host := address
		if _, _, err := net.SplitHostPort(host); err != nil {
			host = strings.Trim(host, "[]")
			if strings.Contains(host, ":") && net.ParseIP(host) == nil {
				return "", fmt.Errorf("invalid cluster endpoint")
			}
			host = net.JoinHostPort(host, strconv.Itoa(port))
		}
		endpoint = &url.URL{Scheme: "wss", Host: host}
		if allowInsecure {
			endpoint.Scheme = "ws"
		}
	}
	if strings.ContainsAny(endpoint.Hostname(), "/?#@\\ \t\r\n") || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.RawFragment != "" || endpoint.Opaque != "" {
		return "", fmt.Errorf("invalid cluster endpoint")
	}
	if endpoint.Path != "" && endpoint.Path != "/" && endpoint.Path != "/cluster/ws" && !(allowInsecure && endpoint.Path == "/ws") {
		return "", fmt.Errorf("cluster endpoint must use /cluster/ws")
	}
	if endpoint.RawPath != "" {
		return "", fmt.Errorf("invalid cluster endpoint path")
	}
	if endpoint.Scheme == "ws" && !allowInsecure {
		return "", fmt.Errorf("unencrypted cluster transport is disabled")
	}
	if endpoint.Port() != "" {
		value, err := strconv.Atoi(endpoint.Port())
		if err != nil || value < 1 || value > 65535 {
			return "", fmt.Errorf("invalid cluster endpoint port")
		}
	}
	endpoint.Path = "/cluster/ws"
	return endpoint.String(), nil
}

// Read regular files through a nonblocking, no-follow descriptor. The inode and
// mode checks occur before and after opening to reject replacement races.
func readClusterTLSFile(path string, private bool) ([]byte, error) {
	invalid := fmt.Errorf("invalid cluster TLS material file")
	limit := int64(1 << 20)
	if private {
		limit = 64 << 10
	}
	valid := func(info os.FileInfo) bool {
		if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > limit {
			return false
		}
		return !private || info.Mode().Perm() == 0400 || info.Mode().Perm() == 0600
	}
	before, err := os.Lstat(path)
	if err != nil || !valid(before) {
		return nil, invalid
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, invalid
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !valid(after) || !os.SameFile(before, after) {
		return nil, invalid
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || len(data) == 0 || int64(len(data)) > limit {
		return nil, invalid
	}
	return data, nil
}
