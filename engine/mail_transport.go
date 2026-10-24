package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/soulteary/owlcron/storage"
	"mime"
	"net"
	"net/mail"
	"net/smtp"
	"os"
	"strings"
	"text/template"
	"time"
)

// EmailPayload is the immutable delivery snapshot. SMTP credentials are never persisted.
type EmailPayload struct {
	From      string   `json:"from"`
	To        []string `json:"to"`
	Subject   string   `json:"subject"`
	Body      string   `json:"body"`
	MessageID string   `json:"message_id,omitempty"`
}

func parseEmailAddresses(value string) ([]*mail.Address, error) {
	if strings.ContainsAny(value, "\r\n") {
		return nil, errors.New("email address contains a newline")
	}
	addresses, err := mail.ParseAddressList(value)
	if err != nil {
		return nil, fmt.Errorf("invalid email address: %w", err)
	}
	seen := map[string]bool{}
	result := make([]*mail.Address, 0, len(addresses))
	for _, address := range addresses {
		key := strings.ToLower(address.Address)
		if !seen[key] {
			seen[key] = true
			result = append(result, address)
		}
	}
	if len(result) == 0 || len(result) > 100 {
		return nil, errors.New("email must have between 1 and 100 recipients")
	}
	return result, nil
}

func (mm *MailManager) prepareEmail(to, subject, templateStr string, data map[string]interface{}) (EmailPayload, error) {
	var result EmailPayload
	if strings.ContainsAny(subject, "\r\n") {
		return result, errors.New("email subject contains a newline")
	}
	from, err := parseEmailAddresses(mm.config.From)
	if err != nil {
		return result, err
	}
	if len(from) != 1 {
		return result, errors.New("email sender must be one address")
	}
	recipients, err := parseEmailAddresses(to)
	if err != nil {
		return result, err
	}
	tmpl, err := template.New("email").Parse(templateStr)
	if err != nil {
		return result, err
	}
	var body strings.Builder
	if err = tmpl.Execute(&body, data); err != nil {
		return result, err
	}
	if body.Len() > 1024*1024 {
		return result, errors.New("email body exceeds 1 MiB")
	}
	result = EmailPayload{From: from[0].String(), Subject: subject, Body: body.String()}
	for _, recipient := range recipients {
		result.To = append(result.To, recipient.String())
	}
	return result, nil
}

func (mm *MailManager) deliverEmail(ctx context.Context, payload EmailPayload) error {
	timeout := time.Duration(mm.config.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	from, err := parseEmailAddresses(payload.From)
	if err != nil {
		return err
	}
	if len(from) != 1 {
		return errors.New("email sender must be one address")
	}
	recipients, err := parseEmailAddresses(strings.Join(payload.To, ","))
	if err != nil {
		return err
	}
	if strings.ContainsAny(payload.Subject+payload.MessageID, "\r\n") {
		return errors.New("email header contains a newline")
	}
	mode := mm.config.TLSMode
	if mode == "" {
		mode = "starttls"
	}
	if mode != "starttls" && mode != "implicit" && mode != "none" {
		return errors.New("invalid SMTP TLS mode")
	}
	if (mm.config.Username == "") != (mm.config.Password == "") {
		return errors.New("SMTP username and password must both be configured")
	}
	if mode == "none" && mm.config.Username != "" {
		return errors.New("SMTP authentication requires TLS")
	}
	serverName := mm.config.TLSServerName
	if serverName == "" {
		serverName = mm.config.SMTPHostname
	}
	tlsConfig := &tls.Config{ServerName: serverName, MinVersion: tls.VersionTLS12}
	if mm.config.TLSCAFile != "" {
		pem, err := os.ReadFile(mm.config.TLSCAFile)
		if err != nil {
			return fmt.Errorf("read SMTP CA: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return errors.New("SMTP CA file contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	if err = mm.reserveDailyAttempt(ctx, time.Now()); err != nil {
		return err
	}
	address := net.JoinHostPort(mm.config.SMTPHostname, fmt.Sprint(mm.config.SMTPPort))
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("SMTP connect: %w", err)
	}
	defer conn.Close()
	rawConn := conn
	stop := context.AfterFunc(ctx, func() { _ = rawConn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	if err = conn.SetDeadline(deadline); err != nil {
		return err
	}
	if mode == "implicit" {
		secure := tls.Client(conn, tlsConfig)
		if err = secure.HandshakeContext(ctx); err != nil {
			return fmt.Errorf("SMTP TLS: %w", err)
		}
		conn = secure
	}
	client, err := smtp.NewClient(conn, mm.config.SMTPHostname)
	if err != nil {
		return fmt.Errorf("SMTP greeting: %w", err)
	}
	defer client.Close()
	if mode == "starttls" {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			return errors.New("SMTP server does not support required STARTTLS")
		}
		if err = client.StartTLS(tlsConfig); err != nil {
			return fmt.Errorf("SMTP STARTTLS: %w", err)
		}
	}
	if mm.config.Username != "" {
		if err = client.Auth(smtp.PlainAuth("", mm.config.Username, mm.config.Password, mm.config.SMTPHostname)); err != nil {
			return errors.New("SMTP authentication failed")
		}
	}
	if err = client.Mail(from[0].Address); err != nil {
		return fmt.Errorf("SMTP sender: %w", err)
	}
	headers := make([]string, 0, len(recipients))
	for _, recipient := range recipients {
		if err = client.Rcpt(recipient.Address); err != nil {
			return fmt.Errorf("SMTP recipient: %w", err)
		}
		headers = append(headers, recipient.String())
	}
	writer, err := client.Data()
	if err != nil {
		return fmt.Errorf("SMTP DATA: %w", err)
	}
	message := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n", from[0].String(), strings.Join(headers, ", "), mime.QEncoding.Encode("UTF-8", payload.Subject))
	if payload.MessageID != "" {
		message += "Message-ID: <" + payload.MessageID + ">\r\n"
	}
	message += "\r\n" + payload.Body
	if _, err = writer.Write([]byte(message)); err != nil {
		return fmt.Errorf("SMTP body: %w", err)
	}
	if err = writer.Close(); err != nil {
		return fmt.Errorf("SMTP acceptance: %w", err)
	}
	// DATA acceptance is authoritative; a failed QUIT must not cause duplicate delivery.
	_ = client.Quit()
	if mm.engine != nil {
		mm.engine.mu.Lock()
		if mm.engine.state != nil {
			if mm.engine.state.Stats == nil {
				mm.engine.state.Stats = make(map[string]float64)
			}
			mm.engine.state.Stats["emails_sent"]++
		}
		mm.engine.mu.Unlock()
	}
	return nil
}

// sendEmail queues each envelope independently so one recipient failure cannot replay accepted recipients.
func (mm *MailManager) sendEmail(to, subject, templateStr string, data map[string]interface{}) error {
	payload, err := mm.prepareEmail(to, subject, templateStr, data)
	if err != nil {
		return err
	}
	identity, _ := data["ID"].(string)
	if identity == "" {
		var token [16]byte
		if _, err = rand.Read(token[:]); err != nil {
			return err
		}
		identity = hex.EncodeToString(token[:])
	}
	var enqueueErrors []error
	for _, recipient := range payload.To {
		copyPayload := payload
		copyPayload.To = []string{recipient}
		digest := sha256.Sum256([]byte(identity + "\x00" + subject + "\x00" + recipient))
		id := "email-" + hex.EncodeToString(digest[:])
		copyPayload.MessageID = id + "@owlcron"
		if err = mm.engine.enqueueNotification(mm.engine.ctx, id, "email", copyPayload); err != nil {
			enqueueErrors = append(enqueueErrors, err)
		}
	}
	return errors.Join(enqueueErrors...)
}

// Count attempts conservatively: failures still consume quota, avoiding send storms.
// The current UTC day replaces yesterday in the same record, so counters do not accumulate.
func (mm *MailManager) reserveDailyAttempt(ctx context.Context, now time.Time) error {
	if mm.config.MaxPerDay <= 0 {
		return nil
	}
	if mm.engine == nil || mm.engine.storage == nil {
		return errors.New("SMTP daily quota storage unavailable")
	}
	atomic, ok := mm.engine.storage.(storage.AtomicStorage)
	if !ok {
		return errors.New("SMTP daily quota requires atomic storage")
	}
	day := now.UTC().Format("2006-01-02")
	return atomic.Update(ctx, "notification_email_daily_quota", func(raw []byte) ([]byte, error) {
		var quota struct {
			Day      string `json:"day"`
			Attempts int    `json:"attempts"`
		}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &quota); err != nil {
				return nil, err
			}
		}
		if quota.Day != day {
			quota.Day = day
			quota.Attempts = 0
		}
		if quota.Attempts >= mm.config.MaxPerDay {
			tomorrow := time.Date(now.UTC().Year(), now.UTC().Month(), now.UTC().Day()+1, 0, 0, 0, 0, time.UTC)
			return nil, &NotificationDeliveryError{Err: errors.New("daily SMTP attempt limit reached"), RetryAfter: tomorrow.Sub(now), Deferred: true}
		}
		quota.Attempts++
		return json.Marshal(quota)
	})
}
