package engine

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/soulteary/owlcron/storage"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPrepareEmailAddressValidation(t *testing.T) {
	mm := &MailManager{config: &MailConfig{From: "Sender <sender@example.com>"}}
	payload, err := mm.prepareEmail(`Alice <alice@example.com>, bob@example.com, alice@example.com`, "你好", "Hello {{.Name}}", map[string]interface{}{"Name": "World"})
	if err != nil || len(payload.To) != 2 || payload.Body != "Hello World" {
		t.Fatalf("payload=%+v err=%v", payload, err)
	}
	for _, to := range []string{"victim@example.com\r\nBcc: other@example.com", "bad-address"} {
		if _, err = mm.prepareEmail(to, "subject", "body", nil); err == nil {
			t.Fatalf("accepted %q", to)
		}
	}
	if _, err = mm.prepareEmail("a@example.com", "subject\nBcc: x", "body", nil); err == nil {
		t.Fatal("accepted header injection")
	}
}

// The test SMTP server handles the actual wire protocol, including TLS and authentication.
func smtpTestServer(t *testing.T, mode string, rejectAuth bool) (*MailManager, <-chan []string) {
	t.Helper()
	https := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	cert := https.TLS.Certificates[0]
	https.Close()
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	result := make(chan []string, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if mode == "implicit" {
			secure := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
			if secure.Handshake() != nil {
				return
			}
			conn = secure
		}
		reader := bufio.NewReader(conn)
		fmt.Fprint(conn, "220 test SMTP\r\n")
		var commands []string
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimSpace(line)
			commands = append(commands, line)
			switch {
			case strings.HasPrefix(line, "EHLO"):
				if mode == "starttls" {
					fmt.Fprint(conn, "250-test\r\n250-STARTTLS\r\n250 AUTH PLAIN\r\n")
				} else {
					fmt.Fprint(conn, "250-test\r\n250 AUTH PLAIN\r\n")
				}
			case line == "STARTTLS":
				fmt.Fprint(conn, "220 Go ahead\r\n")
				secure := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
				if secure.Handshake() != nil {
					return
				}
				conn = secure
				reader = bufio.NewReader(conn)
			case strings.HasPrefix(line, "AUTH"):
				if rejectAuth {
					fmt.Fprint(conn, "535 Invalid credentials\r\n")
				} else {
					fmt.Fprint(conn, "235 Authenticated\r\n")
				}
			case line == "DATA":
				fmt.Fprint(conn, "354 Send message\r\n")
				for {
					bodyLine, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					commands = append(commands, strings.TrimSpace(bodyLine))
					if bodyLine == ".\r\n" {
						break
					}
				}
				fmt.Fprint(conn, "250 Accepted\r\n")
				result <- commands
			case line == "QUIT":
				fmt.Fprint(conn, "221 Bye\r\n")
				return
			default:
				fmt.Fprint(conn, "250 OK\r\n")
			}
		}
	}()
	port := listener.Addr().(*net.TCPAddr).Port
	return &MailManager{config: &MailConfig{SMTPHostname: "127.0.0.1", SMTPPort: port, TLSMode: mode, TLSCAFile: caFile, TLSServerName: "example.com", Username: "user", Password: "secret", TimeoutSeconds: 3}}, result
}

func TestSMTPAuthenticatedTLSAndMultipleRecipients(t *testing.T) {
	for _, mode := range []string{"starttls", "implicit"} {
		t.Run(mode, func(t *testing.T) {
			mm, result := smtpTestServer(t, mode, false)
			err := mm.deliverEmail(context.Background(), EmailPayload{From: "sender@example.com", To: []string{"alice@example.com", "bob@example.com"}, Subject: "subject", Body: "body", MessageID: "stable@owlcron"})
			if err != nil {
				t.Fatal(err)
			}
			commands := strings.Join(<-result, "\n")
			for _, want := range []string{"AUTH PLAIN", "RCPT TO:<alice@example.com>", "RCPT TO:<bob@example.com>", "Message-ID: <stable@owlcron>"} {
				if !strings.Contains(commands, want) {
					t.Errorf("missing %s in %s", want, commands)
				}
			}
		})
	}
}
func TestSMTPAuthenticationFailure(t *testing.T) {
	mm, _ := smtpTestServer(t, "starttls", true)
	if err := mm.deliverEmail(context.Background(), EmailPayload{From: "sender@example.com", To: []string{"a@example.com"}}); err == nil || !strings.Contains(err.Error(), "authentication") {
		t.Fatalf("err=%v", err)
	}
}
func TestSMTPRejectsPlaintextAuthentication(t *testing.T) {
	mm := &MailManager{config: &MailConfig{TLSMode: "none", Username: "user", Password: "password"}}
	if err := mm.deliverEmail(context.Background(), EmailPayload{From: "s@example.com", To: []string{"a@example.com"}}); err == nil || !strings.Contains(err.Error(), "requires TLS") {
		t.Fatalf("err=%v", err)
	}
}
func TestSMTPTimeoutAndConcurrentCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var serverWG sync.WaitGroup
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			serverWG.Add(1)
			go func() { defer serverWG.Done(); defer conn.Close(); _, _ = bufio.NewReader(conn).ReadString('\n') }()
		}
	}()
	mm := &MailManager{config: &MailConfig{SMTPHostname: "127.0.0.1", SMTPPort: listener.Addr().(*net.TCPAddr).Port, TLSMode: "none", TimeoutSeconds: 1}}
	var wg sync.WaitGroup
	start := time.Now()
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			if err := mm.deliverEmail(ctx, EmailPayload{From: "s@example.com", To: []string{"a@example.com"}}); err == nil {
				t.Error("stalled SMTP succeeded")
			}
		}()
	}
	wg.Wait()
	listener.Close()
	serverWG.Wait()
	if time.Since(start) > 2*time.Second {
		t.Fatal("SMTP cancellation did not interrupt greeting")
	}
}

func TestSMTPDailyQuotaConcurrentAndRestart(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.NewFilesystemStorage(&storage.StorageConfig{Engine: "Filesystem", BaseDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	mm := &MailManager{config: &MailConfig{MaxPerDay: 5}, engine: &Engine{storage: store}}
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for range 30 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := mm.reserveDailyAttempt(context.Background(), now)
			if err == nil {
				accepted.Add(1)
				return
			}
			var deferred *NotificationDeliveryError
			if !errors.As(err, &deferred) || !deferred.Deferred || deferred.RetryAfter != 12*time.Hour {
				t.Errorf("unexpected limit error: %v", err)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 5 {
		t.Fatalf("accepted %d", accepted.Load())
	}
	restarted, err := storage.NewFilesystemStorage(&storage.StorageConfig{Engine: "Filesystem", BaseDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	mm.engine = &Engine{storage: restarted}
	if err = mm.reserveDailyAttempt(context.Background(), now); err == nil {
		t.Fatal("restart lost quota")
	}
	if err = mm.reserveDailyAttempt(context.Background(), now.Add(24*time.Hour)); err != nil {
		t.Fatalf("next day did not reset: %v", err)
	}
}

func TestSMTPRequiresAdvertisedSTARTTLS(t *testing.T) {
	mm, _ := smtpTestServer(t, "none", false)
	mm.config.TLSMode = "starttls"
	if err := mm.deliverEmail(context.Background(), EmailPayload{From: "s@example.com", To: []string{"a@example.com"}}); err == nil || !strings.Contains(err.Error(), "required STARTTLS") {
		t.Fatalf("err=%v", err)
	}
}

func TestSMTPRejectsUntrustedTLSCertificate(t *testing.T) {
	mm, _ := smtpTestServer(t, "implicit", false)
	mm.config.TLSCAFile = ""
	if err := mm.deliverEmail(context.Background(), EmailPayload{From: "s@example.com", To: []string{"a@example.com"}}); err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("err=%v", err)
	}
}
