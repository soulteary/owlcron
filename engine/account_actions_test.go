package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
)

func accountActionTestEngine(t *testing.T) (*Engine, *types.User) {
	t.Helper()
	e := jobTestEngine(t)
	e.config.AccountEmailEnabled = true
	e.config.BaseAppURL = "https://owlcron.example.test/prefix"
	e.config.AccountEmailCooldownSeconds = 1
	user := &types.User{ID: "test-user", Username: "tester", Email: "old@example.test", VerifiedEmail: "old@example.test", Password: "existing-password-hash", Enabled: true, Privileges: types.Privileges{"run_events": true}}
	if err := e.SaveUserToStorage(user); err != nil {
		t.Fatal(err)
	}
	return e, user
}
func actionToken(t *testing.T, e *Engine, id, purpose string) string {
	t.Helper()
	user, err := e.LoadUserFromStorage(id)
	if err != nil {
		t.Fatal(err)
	}
	action, ok := user.AccountActions[purpose]
	if !ok {
		t.Fatal("missing action")
	}
	return e.accountActionToken(id, purpose, action)
}
func TestAccountEmailVerificationAtomicAndReplay(t *testing.T) {
	e, user := accountActionTestEngine(t)
	ctx := context.Background()
	if err := e.IssueEmailVerification(ctx, user.ID, "new@example.test"); err != nil {
		t.Fatal(err)
	}
	pending, _ := e.LoadUserFromStorage(user.ID)
	if pending.Email != user.Email || pending.VerifiedEmail != user.Email {
		t.Fatal("request changed recovery address")
	}
	token := actionToken(t, e, user.ID, verifyEmailAction)
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := e.ConsumeEmailVerification(ctx, user.ID, token)
			if err == nil {
				successes.Add(1)
			} else if !errors.Is(err, ErrAccountActionInvalid) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("successful consumptions %d", successes.Load())
	}
	got, _ := e.LoadUserFromStorage(user.ID)
	if got.Email != "new@example.test" || got.VerifiedEmail != got.Email || len(got.AccountActions) != 0 {
		t.Fatalf("bad verified user: %+v", got.SafeMap())
	}
	if err := e.ConsumeEmailVerification(ctx, user.ID, token); !errors.Is(err, ErrAccountActionInvalid) {
		t.Fatal(err)
	}
}
func TestAccountRecoveryChangesPasswordAndInvalidatesSession(t *testing.T) {
	e, user := accountActionTestEngine(t)
	ctx := context.Background()
	session := &types.Session{ID: "old-session", UserID: user.ID, Expires: time.Now().Add(time.Hour).Unix()}
	if err := e.SaveSessionToStorage(session); err != nil {
		t.Fatal(err)
	}
	if err := e.RequestPasswordRecovery(ctx, user.Username, user.Email); err != nil {
		t.Fatal(err)
	}
	token := actionToken(t, e, user.ID, resetPasswordAction)
	if err := e.ConsumePasswordRecovery(ctx, user.ID, token, "a-strong-new-password"); err != nil {
		t.Fatal(err)
	}
	got, _ := e.LoadUserFromStorage(user.ID)
	if !e.VerifyUserPassword(got, "a-strong-new-password") || got.Enabled != user.Enabled || !got.Privileges["run_events"] {
		t.Fatal("reset changed account access or failed password")
	}
	if _, err := e.ValidateUserSession(session); err == nil {
		t.Fatal("old session survived reset")
	}
	if err := e.ConsumePasswordRecovery(ctx, user.ID, token, "another-password"); !errors.Is(err, ErrAccountActionInvalid) {
		t.Fatal(err)
	}
}
func TestAccountActionsRejectStaleBindings(t *testing.T) {
	for _, scenario := range []string{"expired", "password", "email", "disabled", "deleted", "wrong-purpose", "wrong-user", "wrong-token"} {
		t.Run(scenario, func(t *testing.T) {
			e, user := accountActionTestEngine(t)
			ctx := context.Background()
			if err := e.IssueEmailVerification(ctx, user.ID, "new@example.test"); err != nil {
				t.Fatal(err)
			}
			token := actionToken(t, e, user.ID, verifyEmailAction)
			current, _ := e.LoadUserFromStorage(user.ID)
			switch scenario {
			case "expired":
				a := current.AccountActions[verifyEmailAction]
				a.Expires = time.Now().Unix() - 1
				current.AccountActions[verifyEmailAction] = a
			case "password":
				current.Password = "changed"
			case "email":
				current.Email = "changed@example.test"
			case "disabled":
				current.Enabled = false
			case "deleted":
				if err := e.DeleteUserFromStorage(user.ID); err != nil {
					t.Fatal(err)
				}
			case "wrong-purpose":
				err := e.ConsumePasswordRecovery(ctx, user.ID, token, "a-strong-password")
				if !errors.Is(err, ErrAccountActionInvalid) {
					t.Fatal(err)
				}
				return
			case "wrong-user":
				user.ID = "someone-else"
			case "wrong-token":
				token = strings.Repeat("0", 64)
			}
			if scenario != "deleted" && scenario != "wrong-user" {
				if err := e.SaveUserToStorage(current); err != nil {
					t.Fatal(err)
				}
			}
			if err := e.ConsumeEmailVerification(ctx, user.ID, token); !errors.Is(err, ErrAccountActionInvalid) {
				t.Fatalf("accepted %s: %v", scenario, err)
			}
		})
	}
}
func TestAccountRecoveryGenericAndCooldown(t *testing.T) {
	e, user := accountActionTestEngine(t)
	ctx := context.Background()
	for _, name := range []string{"absent", user.Username} {
		if err := e.RequestPasswordRecovery(ctx, name, "wrong@example.test"); err != nil {
			t.Fatal(err)
		}
	}
	rows, _ := e.notifications.List(ctx, "", 100)
	if len(rows) != 0 {
		t.Fatal("invalid request queued mail")
	}
	if err := e.RequestPasswordRecovery(ctx, user.Username, user.Email); err != nil {
		t.Fatal(err)
	}
	first := actionToken(t, e, user.ID, resetPasswordAction)
	if err := e.RequestPasswordRecovery(ctx, user.Username, user.Email); err != nil {
		t.Fatal(err)
	}
	if first != actionToken(t, e, user.ID, resetPasswordAction) {
		t.Fatal("cooldown rotated token")
	}
	rows, _ = e.notifications.List(ctx, "", 100)
	if len(rows) != 1 {
		t.Fatalf("cooldown queued %d mail", len(rows))
	}
}
func TestAccountQueueContainsNoTokenAndRechecksDelivery(t *testing.T) {
	e, user := accountActionTestEngine(t)
	ctx := context.Background()
	if err := e.IssueEmailVerification(ctx, user.ID, "new@example.test"); err != nil {
		t.Fatal(err)
	}
	token := actionToken(t, e, user.ID, verifyEmailAction)
	rows, _ := e.notifications.List(ctx, "", 100)
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
	raw, _ := json.Marshal(rows[0])
	if strings.Contains(string(raw), token) || strings.Contains(string(raw), "https://") {
		t.Fatal("queue leaked recovery link")
	}
	// A reconstructed engine uses the same durable bindings after a restart.
	restart := &Engine{config: e.config, storage: e.storage, notifications: e.notifications, jobManager: e.jobManager}
	payload, err := restart.prepareAccountEmail(ctx, &rows[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(payload.Body, "https://owlcron.example.test/prefix#Login?") || !strings.Contains(payload.Body, token) {
		t.Fatalf("bad link: %s", payload.Body)
	}
	if err := e.ConsumeEmailVerification(ctx, user.ID, token); err != nil {
		t.Fatal(err)
	}
	_, err = e.prepareAccountEmail(ctx, &rows[0])
	var delivery *NotificationDeliveryError
	if !errors.As(err, &delivery) || !delivery.Permanent {
		t.Fatalf("stale delivery retriable: %v", err)
	}
	e.notifications.deliver = e.deliverNotification
	if err := e.notifications.process(ctx, notificationKey(rows[0].ID)); err != nil {
		t.Fatal(err)
	}
	dead, err := e.notifications.Get(ctx, rows[0].ID)
	if err != nil || dead.State != "dead" {
		t.Fatalf("stale link sent: %v %v", dead, err)
	}
	if strings.Contains(dead.LastError, token) {
		t.Fatal("error leaked token")
	}
}

func TestAccountRecoveryUnverifiedAndDisabledAreSilent(t *testing.T) {
	for _, state := range []string{"unverified", "disabled"} {
		t.Run(state, func(t *testing.T) {
			e, user := accountActionTestEngine(t)
			if state == "unverified" {
				user.VerifiedEmail = ""
			} else {
				user.Enabled = false
			}
			if err := e.SaveUserToStorage(user); err != nil {
				t.Fatal(err)
			}
			if err := e.RequestPasswordRecovery(context.Background(), user.Username, user.Email); err != nil {
				t.Fatal(err)
			}
			rows, err := e.notifications.List(context.Background(), "", 10)
			if err != nil || len(rows) != 0 {
				t.Fatalf("ineligible recovery enqueued: %v %v", rows, err)
			}
		})
	}
}

func TestAccountTokenRotationAndUnsafeURL(t *testing.T) {
	e, user := accountActionTestEngine(t)
	ctx := context.Background()
	if err := e.IssueEmailVerification(ctx, user.ID, "new@example.test"); err != nil {
		t.Fatal(err)
	}
	token := actionToken(t, e, user.ID, verifyEmailAction)
	rows, _ := e.notifications.List(ctx, "", 10)
	e.config.BaseAppURL = "http://untrusted.example.test/"
	if _, err := e.prepareAccountEmail(ctx, &rows[0]); err == nil {
		t.Fatal("plaintext URL allowed")
	}
	e.config.BaseAppURL = "https://owlcron.example.test/"
	e.config.SecretKey = "rotated-secret-for-test"
	if err := e.ConsumeEmailVerification(ctx, user.ID, token); !errors.Is(err, ErrAccountActionInvalid) {
		t.Fatal("old key token accepted")
	}
	if _, err := e.prepareAccountEmail(ctx, &rows[0]); err == nil {
		t.Fatal("old key action regenerated")
	}
}

func TestAccountActionsStayPrivate(t *testing.T) {
	e, user := accountActionTestEngine(t)
	if err := e.IssueEmailVerification(context.Background(), user.ID, "new@example.test"); err != nil {
		t.Fatal(err)
	}
	user, _ = e.LoadUserFromStorage(user.ID)
	raw, _ := json.Marshal(user.SafeMap())
	for _, secret := range []string{"account_actions", "request_id", "digest", user.AccountActions[verifyEmailAction].RequestID} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("SafeMap leaked %s", secret)
		}
	}
}

func TestAccountActionsRevokedByOrdinaryAccountEdit(t *testing.T) {
	for _, kind := range []string{"email", "password", "disable"} {
		t.Run(kind, func(t *testing.T) {
			e, user := accountActionTestEngine(t)
			if err := e.IssueEmailVerification(context.Background(), user.ID, "new@example.test"); err != nil {
				t.Fatal(err)
			}
			before, _ := e.LoadUserFromStorage(user.ID)
			after := *before
			switch kind {
			case "email":
				after.Email = "admin-change@example.test"
			case "password":
				after.Password = "admin-reset"
			case "disable":
				after.Enabled = false
			}
			if err := e.ChangeUserInStorage(before, &after); err != nil {
				t.Fatal(err)
			}
			got, _ := e.LoadUserFromStorage(user.ID)
			if len(got.AccountActions) != 0 {
				t.Fatal("account change retained action")
			}
			if got.AccountEmailRequestedAt != before.AccountEmailRequestedAt {
				t.Fatal("account edit erased cooldown")
			}
		})
	}
}

func TestAccountRecoveryConcurrentConsumption(t *testing.T) {
	e, user := accountActionTestEngine(t)
	ctx := context.Background()
	if err := e.RequestPasswordRecovery(ctx, user.Username, user.Email); err != nil {
		t.Fatal(err)
	}
	token := actionToken(t, e, user.ID, resetPasswordAction)
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := e.ConsumePasswordRecovery(ctx, user.ID, token, "a-strong-new-password")
			if err == nil {
				successes.Add(1)
			} else if !errors.Is(err, ErrAccountActionInvalid) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("successful password resets %d", successes.Load())
	}
}
