package engine

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/soulteary/owlcron/types"
)

const maxWebHookSigningSecretBytes = 4096

var webHookKeyIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// webHookSigner is immutable after construction and safe for concurrent attempts.
// Credentials remain in process memory, never in persisted notification payloads.
type webHookSigner struct {
	keyID  string
	secret []byte
}

func loadWebHookSigner(config *types.Config) (*webHookSigner, error) {
	if config == nil {
		return nil, nil
	}
	id, secret, path := config.WebHookSigningKeyID, config.WebHookSigningSecret, config.WebHookSigningSecretFile
	if id == "" && secret == "" && path == "" {
		return nil, nil
	}
	if !webHookKeyIDPattern.MatchString(id) {
		return nil, errors.New("invalid webhook signing key ID")
	}
	if (secret == "") == (path == "") {
		return nil, errors.New("configure exactly one webhook signing secret source")
	}
	if path != "" {
		var err error
		secret, err = readWebHookSigningSecret(path)
		if err != nil {
			return nil, err
		}
	}
	if len(secret) < 32 || len(secret) > maxWebHookSigningSecretBytes || strings.ContainsAny(secret, "\r\n\x00") {
		return nil, errors.New("webhook signing secret must contain 32 to 4096 bytes without line breaks or NUL")
	}
	if secret == config.SecretKey || secret == config.SMTPPassword {
		return nil, errors.New("webhook signing requires an independent secret")
	}
	return &webHookSigner{keyID: id, secret: []byte(secret)}, nil
}

func readWebHookSigningSecret(path string) (string, error) {
	const message = "cannot load webhook signing secret: require a regular file with mode 0600 containing at most 4096 bytes"
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm() != 0600 {
		return "", errors.New(message)
	}
	// Reject final-component symlinks atomically and avoid blocking if the path
	// is replaced with a FIFO between Lstat and open. Recheck inode and mode below.
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", errors.New(message)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || !os.SameFile(before, info) {
		return "", errors.New(message)
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxWebHookSigningSecretBytes+3))
	if err != nil {
		return "", errors.New(message)
	}
	secret := strings.TrimSuffix(strings.TrimSuffix(string(raw), "\n"), "\r")
	if len(secret) > maxWebHookSigningSecretBytes {
		return "", errors.New(message)
	}
	return secret, nil
}

// sign authenticates the exact outbound body and target. Receivers must reject
// timestamps outside their allowed clock window and deduplicate Idempotency-Key.
// Canonical bytes: v1\n + keyID\n + timestamp\n + ID\n + method\n + scheme\n + host\n + requestURI\n + body.
func (s *webHookSigner) sign(req *http.Request, body []byte, id string) error {
	if s == nil {
		return nil
	}
	if req == nil || req.URL == nil || id == "" || strings.ContainsAny(id, "\r\n\x00") {
		return errors.New("invalid webhook signing request")
	}
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	canonical := strings.Join([]string{"v1", s.keyID, timestamp, id, req.Method, req.URL.Scheme, req.URL.Host, req.URL.RequestURI(), ""}, "\n")
	mac := hmac.New(sha256.New, s.secret)
	_, _ = mac.Write([]byte(canonical))
	_, _ = mac.Write(body)
	req.Header.Set("Idempotency-Key", id)
	req.Header.Set("X-Owlcron-Timestamp", timestamp)
	req.Header.Set("X-Owlcron-Key-ID", s.keyID)
	req.Header.Set("X-Owlcron-Signature", "v1="+hex.EncodeToString(mac.Sum(nil)))
	return nil
}
