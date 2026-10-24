package engine

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/mail"
	"net/url"
	"strings"
	"time"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

var ErrAccountActionInvalid = errors.New("account link is invalid or expired")
var ErrAccountEmailDisabled = errors.New("account email is unavailable")
var ErrAccountEmailCooldown = errors.New("please wait before requesting another account email")

const verifyEmailAction = "verify_email"
const resetPasswordAction = "reset_password"

type accountEmailReference struct {
	UserID    string `json:"user_id"`
	Purpose   string `json:"purpose"`
	RequestID string `json:"request_id"`
}

func validAccountAddress(email string) bool {
	address, err := mail.ParseAddress(email)
	return err == nil && address.Address == email && len(email) <= 254 && !strings.ContainsAny(email, "\r\n")
}

func (e *Engine) accountEmailAvailable() bool {
	return e.config.AccountEmailEnabled && e.config.SecretKey != "" && e.notifications != nil
}

// mutateAccountAction commits the action and account change together in the
// canonical record. The index is only a discovery aid, never an authority.
func (e *Engine) mutateAccountAction(ctx context.Context, id string, change func(*types.User) error) error {
	if !authIDPattern.MatchString(id) {
		return ErrAccountActionInvalid
	}
	atomicStore, ok := e.storage.(storage.AtomicStorage)
	if !ok {
		return storage.ErrUnsupported
	}
	authStoreMu.Lock()
	defer authStoreMu.Unlock()
	var committed []byte
	if err := atomicStore.Update(ctx, "users:"+id, func(raw []byte) ([]byte, error) {
		var user types.User
		if len(raw) == 0 || json.Unmarshal(raw, &user) != nil || !user.Enabled {
			return nil, ErrAccountActionInvalid
		}
		if err := change(&user); err != nil {
			return nil, err
		}
		user.Modified = time.Now().Unix()
		var err error
		committed, err = json.Marshal(&user)
		return committed, err
	}); err != nil {
		return err
	}
	return e.updateAuthIndex(ctx, "users", id, committed, false)
}

func (e *Engine) accountActionToken(id, purpose string, action types.AccountAction) string {
	mac := hmac.New(sha256.New, []byte(e.config.SecretKey))
	// JSON provides unambiguous domain-separated encoding of every binding.
	raw, _ := json.Marshal([]any{"owlcron/account-action/v1", id, purpose, action.RequestID, action.Email, action.OriginalEmail, action.PasswordRevision, action.Expires})
	_, _ = mac.Write(raw)
	return hex.EncodeToString(mac.Sum(nil))
}
func accountTokenDigest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (e *Engine) issueAccountAction(ctx context.Context, id, purpose, email, expectedRevision string) error {
	if !e.accountEmailAvailable() {
		return ErrAccountEmailDisabled
	}
	if !validAccountAddress(email) {
		return errors.New("invalid email address")
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	requestID := hex.EncodeToString(random[:])
	now := time.Now().Unix()
	ttl := e.config.AccountTokenTTLSeconds
	if ttl == 0 {
		ttl = 1800
	}
	cooldown := e.config.AccountEmailCooldownSeconds
	if cooldown == 0 {
		cooldown = 60
	}
	err := e.mutateAccountAction(ctx, id, func(user *types.User) error {
		if expectedRevision != "" && expectedRevision != PasswordRevision(user) {
			return ErrAccountActionInvalid
		}
		if purpose == resetPasswordAction && (user.Email != email || user.VerifiedEmail != email) {
			return ErrAccountActionInvalid
		}
		if now-user.AccountEmailRequestedAt < int64(cooldown) {
			return ErrAccountEmailCooldown
		}
		action := types.AccountAction{RequestID: requestID, Email: email, OriginalEmail: user.Email, PasswordRevision: PasswordRevision(user), Expires: now + int64(ttl)}
		action.Digest = accountTokenDigest(e.accountActionToken(id, purpose, action))
		if user.AccountActions == nil {
			user.AccountActions = map[string]types.AccountAction{}
		}
		user.AccountActions[purpose] = action
		user.AccountEmailRequestedAt = now
		return nil
	})
	if err != nil {
		return err
	}
	return e.enqueueNotification(ctx, "account/"+requestID, "account_email", accountEmailReference{id, purpose, requestID})
}

func (e *Engine) IssueEmailVerification(ctx context.Context, userID, email string) error {
	return e.issueAccountAction(ctx, userID, verifyEmailAction, email, "")
}

// IssueEmailVerificationForRevision binds the authenticated request to the
// credential checked by the caller, fencing a concurrent password change.
func (e *Engine) IssueEmailVerificationForRevision(ctx context.Context, userID, email, passwordRevision string) error {
	if passwordRevision == "" {
		return ErrAccountActionInvalid
	}
	return e.issueAccountAction(ctx, userID, verifyEmailAction, email, passwordRevision)
}

// RequestPasswordRecovery intentionally does not distinguish missing, disabled,
// unverified accounts or requests suppressed by the persistent cooldown.
func (e *Engine) RequestPasswordRecovery(ctx context.Context, username, email string) error {
	if !e.accountEmailAvailable() {
		return ErrAccountEmailDisabled
	}
	user, err := e.LoadUserByUsernameFromStorage(username)
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if !user.Enabled || user.Email != email || user.VerifiedEmail != email || !validAccountAddress(email) {
		return nil
	}
	err = e.issueAccountAction(ctx, user.ID, resetPasswordAction, email, "")
	if errors.Is(err, ErrAccountActionInvalid) || errors.Is(err, ErrAccountEmailCooldown) {
		return nil
	}
	return err
}

func (e *Engine) validAccountAction(user *types.User, purpose, token string) (types.AccountAction, bool) {
	action, ok := user.AccountActions[purpose]
	if !ok || !user.Enabled || action.Expires <= time.Now().Unix() || action.OriginalEmail != user.Email || action.PasswordRevision != PasswordRevision(user) {
		return action, false
	}
	if purpose == resetPasswordAction && (user.VerifiedEmail != user.Email || action.Email != user.Email) {
		return action, false
	}
	if purpose != verifyEmailAction && purpose != resetPasswordAction {
		return action, false
	}
	expected := e.accountActionToken(user.ID, purpose, action)
	return action, len(token) == 64 && subtle.ConstantTimeCompare([]byte(accountTokenDigest(token)), []byte(action.Digest)) == 1 && subtle.ConstantTimeCompare([]byte(expected), []byte(token)) == 1
}

func (e *Engine) ConsumeEmailVerification(ctx context.Context, userID, token string) error {
	if !e.accountEmailAvailable() {
		return ErrAccountEmailDisabled
	}
	var previous string
	err := e.mutateAccountAction(ctx, userID, func(user *types.User) error {
		action, ok := e.validAccountAction(user, verifyEmailAction, token)
		if !ok {
			return ErrAccountActionInvalid
		}
		previous = user.Email
		user.Email = action.Email
		user.VerifiedEmail = action.Email
		user.AccountActions = nil
		return nil
	})
	if err != nil {
		return err
	}
	if previous != "" && validAccountAddress(previous) {
		user, loadErr := e.LoadUserFromStorage(userID)
		if loadErr == nil && user.Email != previous {
			// No secret or recovery link is included in this immutable change notice.
			p, prepareErr := e.jobManager.mailManager.prepareEmail(previous, "OwlCron account email changed", "The email address for your OwlCron account has changed. If you did not request this change, contact your administrator immediately.", nil)
			if prepareErr == nil {
				_ = e.enqueueNotification(ctx, "account-email-changed/"+accountTokenDigest(token), "email", p)
			}
		}
	}
	return nil
}

func (e *Engine) ConsumePasswordRecovery(ctx context.Context, userID, token, newPassword string) error {
	if !e.accountEmailAvailable() {
		return ErrAccountEmailDisabled
	}
	if !authIDPattern.MatchString(userID) {
		return ErrAccountActionInvalid
	}
	if len(newPassword) < 12 || len(newPassword) > 72 {
		return errors.New("password must contain 12 to 72 bytes")
	}
	// Reject obviously invalid tokens before doing costly password hashing.
	user, err := e.LoadUserFromStorage(userID)
	if errors.Is(err, storage.ErrNotFound) {
		return ErrAccountActionInvalid
	}
	if err != nil {
		return err
	}
	if _, ok := e.validAccountAction(user, resetPasswordAction, token); !ok {
		return ErrAccountActionInvalid
	}
	hash, err := e.HashPassword(newPassword)
	if err != nil {
		return err
	}
	return e.mutateAccountAction(ctx, userID, func(user *types.User) error {
		if _, ok := e.validAccountAction(user, resetPasswordAction, token); !ok {
			return ErrAccountActionInvalid
		}
		user.Password = hash
		user.AccountActions = nil
		return nil
	})
}

func (e *Engine) prepareAccountEmail(ctx context.Context, n *Notification) (EmailPayload, error) {
	invalid := func() (EmailPayload, error) {
		return EmailPayload{}, &NotificationDeliveryError{Err: ErrAccountActionInvalid, Permanent: true}
	}
	if !e.accountEmailAvailable() {
		return invalid()
	}
	var ref accountEmailReference
	if json.Unmarshal(n.Payload, &ref) != nil || !authIDPattern.MatchString(ref.UserID) {
		return invalid()
	}
	user, err := e.LoadUserFromStorage(ref.UserID)
	if errors.Is(err, storage.ErrNotFound) {
		return invalid()
	}
	if err != nil {
		return EmailPayload{}, err
	}
	action, exists := user.AccountActions[ref.Purpose]
	if !exists || action.RequestID != ref.RequestID {
		return invalid()
	}
	token := e.accountActionToken(user.ID, ref.Purpose, action)
	if _, ok := e.validAccountAction(user, ref.Purpose, token); !ok {
		return invalid()
	}
	base, err := url.Parse(e.config.BaseAppURL)
	if err != nil || base.Host == "" || (base.Scheme != "https" && !(e.config.AccountEmailAllowInsecure && base.Scheme == "http")) {
		return invalid()
	}
	mode := "verify"
	subject := "Verify your OwlCron email address"
	if ref.Purpose == resetPasswordAction {
		mode = "reset"
		subject = "Reset your OwlCron password"
	}
	values := url.Values{mode: {"1"}, "u": {user.ID}, "token": {token}}
	base.Fragment = "Login?" + values.Encode()
	body := fmt.Sprintf("%s\n\n%s\n\nThis link expires at %s and can be used once. If you did not request it, ignore this message.\n", subject, base.String(), time.Unix(action.Expires, 0).UTC().Format(time.RFC3339))
	p, err := e.jobManager.mailManager.prepareEmail(action.Email, subject, "{{.Body}}", map[string]interface{}{"Body": body})
	if err != nil {
		return EmailPayload{}, &NotificationDeliveryError{Err: errors.New("invalid account email configuration"), Permanent: true}
	}
	p.MessageID = n.ID
	return p, nil
}

func (e *Engine) deliverAccountEmail(ctx context.Context, n *Notification) error {
	payload, err := e.prepareAccountEmail(ctx, n)
	if err != nil {
		return err
	}
	// SMTP errors may echo message content. Never persist an account token through diagnostics.
	err = e.jobManager.mailManager.deliverEmail(ctx, payload)
	if err == nil {
		return nil
	}
	var delivery *NotificationDeliveryError
	if errors.As(err, &delivery) {
		return &NotificationDeliveryError{Err: errors.New("account email delivery failed"), Permanent: delivery.Permanent, Deferred: delivery.Deferred, RetryAfter: delivery.RetryAfter}
	}
	return errors.New("account email delivery failed")
}
