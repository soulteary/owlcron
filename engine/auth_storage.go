package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"os"
	"path/filepath"
	"regexp"
	"sync"

	"encoding/json"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"time"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
	"golang.org/x/crypto/bcrypt"
)

// LoadAPIKeyFromStorage 从存储加载 API Key
func (e *Engine) LoadAPIKeyFromStorage(apiKeyID string) (*types.APIKey, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage not initialized")
	}

	ctx := context.Background()
	key := fmt.Sprintf("api_keys:%s", apiKeyID)

	data, err := e.storage.Get(ctx, key)
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		// 兼容通过 key 字段查找（X-API-Key 常传真实密钥）
		items, listErr := e.storage.ListGet(ctx, "api_keys", 0, 10000)
		if listErr != nil {
			return nil, err
		}
		for _, item := range items {
			raw, marshalErr := json.Marshal(item)
			if marshalErr != nil {
				continue
			}
			var keyItem types.APIKey
			if unmarshalErr := json.Unmarshal(raw, &keyItem); unmarshalErr != nil {
				continue
			}
			if keyItem.Key != "" && subtle.ConstantTimeCompare([]byte(keyItem.Key), []byte(apiKeyID)) == 1 {
				canonical, err := e.storage.Get(ctx, "api_keys:"+keyItem.ID)
				if err != nil {
					return nil, err
				}
				var key types.APIKey
				if err := json.Unmarshal(canonical, &key); err != nil {
					return nil, err
				}
				return &key, nil
			}
		}
		return nil, err
	}

	var apiKey types.APIKey
	if err := json.Unmarshal(data, &apiKey); err != nil {
		return nil, fmt.Errorf("failed to unmarshal API key: %w", err)
	}

	return &apiKey, nil
}

// ListAPIKeysFromStorage 列出 API Key
func (e *Engine) ListAPIKeysFromStorage() ([]*types.APIKey, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage not initialized")
	}
	items, err := e.storage.ListGet(context.Background(), "api_keys", 0, 10000)
	if err != nil {
		return nil, err
	}
	result := make([]*types.APIKey, 0, len(items))
	seen := map[string]bool{}
	for _, item := range items {
		raw, err := json.Marshal(item)
		if err != nil {
			continue
		}
		var key types.APIKey
		if err := json.Unmarshal(raw, &key); err != nil {
			continue
		}
		if key.ID == "" || seen[key.ID] {
			continue
		}
		canonical, err := e.LoadAPIKeyFromStorage(key.ID)
		if err != nil {
			if !errors.Is(err, storage.ErrNotFound) {
				return nil, err
			}
			continue
		}
		seen[key.ID] = true
		result = append(result, canonical)
	}
	return result, nil
}

// SaveAPIKeyToStorage 保存 API Key
func (e *Engine) SaveAPIKeyToStorage(apiKey *types.APIKey) error {
	if e.storage == nil {
		return fmt.Errorf("storage not initialized")
	}
	if apiKey.Created == 0 {
		apiKey.Created = time.Now().Unix()
	}
	apiKey.Modified = time.Now().Unix()
	raw, err := json.Marshal(apiKey)
	if err != nil {
		return err
	}
	return e.saveAuthRecord("api_keys", apiKey.ID, raw, false)
}

// DeleteAPIKeyFromStorage 删除 API Key
func (e *Engine) DeleteAPIKeyFromStorage(id string) error {
	if e.storage == nil {
		return fmt.Errorf("storage not initialized")
	}
	return e.saveAuthRecord("api_keys", id, nil, true)
}

// LoadSessionFromStorage 从存储加载 Session
func (e *Engine) LoadSessionFromStorage(sessionID string) (*types.Session, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage not initialized")
	}

	ctx := context.Background()
	key := fmt.Sprintf("sessions:%s", sessionID)

	data, err := e.storage.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	var session types.Session
	if err := json.Unmarshal(data, &session); err != nil {
		return nil, fmt.Errorf("failed to unmarshal session: %w", err)
	}

	return &session, nil
}

// LoadUserFromStorage 从存储加载用户
func (e *Engine) LoadUserFromStorage(userID string) (*types.User, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage not initialized")
	}

	ctx := context.Background()
	key := fmt.Sprintf("users:%s", userID)

	data, err := e.storage.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	var user types.User
	if err := json.Unmarshal(data, &user); err != nil {
		return nil, fmt.Errorf("failed to unmarshal user: %w", err)
	}

	return &user, nil
}

// LoadUserByUsernameFromStorage 从存储根据用户名加载用户
func (e *Engine) LoadUserByUsernameFromStorage(username string) (*types.User, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage not initialized")
	}

	ctx := context.Background()

	// 从用户列表查找
	items, err := e.storage.ListGet(ctx, "users", 0, 10000)
	if err != nil {
		return nil, err
	}

	for _, item := range items {
		itemData, err := json.Marshal(item)
		if err != nil {
			continue
		}

		var user types.User
		if err := json.Unmarshal(itemData, &user); err != nil {
			continue
		}

		if user.Username == username {
			canonical, err := e.LoadUserFromStorage(user.ID)
			if err == nil && canonical.Username == username {
				return canonical, nil
			}
			if err != nil && !errors.Is(err, storage.ErrNotFound) {
				return nil, err
			}
		}
	}

	return nil, fmt.Errorf("user not found: %s: %w", username, storage.ErrNotFound)
}

// SaveSessionToStorage 保存 Session
func (e *Engine) SaveSessionToStorage(session *types.Session) error {
	if e.storage == nil {
		return fmt.Errorf("storage not initialized")
	}
	user, err := e.LoadUserFromStorage(session.UserID)
	if err != nil {
		return err
	}
	if session.Data == nil {
		session.Data = make(map[string]interface{})
	}
	if !user.Enabled {
		return fmt.Errorf("user is disabled")
	}
	revision := PasswordRevision(user)
	if supplied, exists := session.Data["password_revision"]; exists {
		verified, ok := supplied.(string)
		if !ok || subtle.ConstantTimeCompare([]byte(verified), []byte(revision)) != 1 {
			return fmt.Errorf("password changed while creating session")
		}
	} else {
		session.Data["password_revision"] = revision
	}
	raw, err := json.Marshal(session)
	if err != nil {
		return err
	}
	return e.storage.Put(context.Background(), fmt.Sprintf("sessions:%s", session.ID), raw)
}

func PasswordRevision(user *types.User) string {
	sum := sha256.Sum256([]byte(user.Password))
	return hex.EncodeToString(sum[:])
}

// ValidateUserSession rejects expired, revoked, legacy and password-stale sessions.
func (e *Engine) ValidateUserSession(session *types.Session) (*types.User, error) {
	if session == nil || time.Now().Unix() >= session.Expires {
		return nil, fmt.Errorf("session expired")
	}
	user, err := e.LoadUserFromStorage(session.UserID)
	if err != nil || !user.Enabled {
		return nil, fmt.Errorf("account unavailable")
	}
	revision, _ := session.Data["password_revision"].(string)
	if revision == "" || subtle.ConstantTimeCompare([]byte(revision), []byte(PasswordRevision(user))) != 1 {
		return nil, fmt.Errorf("session revoked")
	}
	return user, nil
}

func (e *Engine) DeleteUserFromStorage(id string) error {
	if e.storage == nil {
		return fmt.Errorf("storage not initialized")
	}
	return e.saveAuthRecord("users", id, nil, true)
}

// DeleteSessionFromStorage 删除 Session
func (e *Engine) DeleteSessionFromStorage(sessionID string) error {
	if e.storage == nil {
		return fmt.Errorf("storage not initialized")
	}
	return e.storage.Delete(context.Background(), fmt.Sprintf("sessions:%s", sessionID))
}

// SaveUserToStorage 保存用户
func (e *Engine) SaveUserToStorage(user *types.User) error {
	if e.storage == nil {
		return fmt.Errorf("storage not initialized")
	}
	if user.Created == 0 {
		user.Created = time.Now().Unix()
	}
	user.Modified = time.Now().Unix()

	raw, err := json.Marshal(user)
	if err != nil {
		return err
	}
	return e.saveAuthRecord("users", user.ID, raw, false)
}

// ChangeUserInStorage rejects stale account edits instead of overwriting a
// concurrent password change, revocation or deletion on another node.
func (e *Engine) ChangeUserInStorage(expected, replacement *types.User) error {
	if expected == nil || !authIDPattern.MatchString(expected.ID) {
		return fmt.Errorf("invalid expected user")
	}
	atomic, ok := e.storage.(storage.AtomicStorage)
	if !ok {
		return fmt.Errorf("account changes require atomic storage")
	}
	before, err := json.Marshal(expected)
	if err != nil {
		return err
	}
	remove := replacement == nil
	if remove {
		// A disabled tombstone fences stale writers before the physical delete.
		replacement = &types.User{ID: expected.ID, Username: expected.Username, Enabled: false}
	} else if replacement.ID != expected.ID || replacement.Username != expected.Username {
		return fmt.Errorf("account identity cannot change")
	}
	if replacement.Email != expected.Email || replacement.Password != expected.Password || !replacement.Enabled {
		replacement.AccountActions = nil
	}
	if replacement.Email != expected.Email {
		replacement.VerifiedEmail = ""
	}
	if replacement.Created == 0 {
		replacement.Created = expected.Created
	}
	replacement.Modified = time.Now().Unix()
	raw, err := json.Marshal(replacement)
	if err != nil {
		return err
	}
	authStoreMu.Lock()
	defer authStoreMu.Unlock()
	ctx := context.Background()
	key := "users:" + expected.ID
	if err := atomic.Update(ctx, key, func(current []byte) ([]byte, error) {
		var latest types.User
		if len(current) == 0 || json.Unmarshal(current, &latest) != nil {
			return nil, fmt.Errorf("account changed; reload and retry")
		}
		canonical, err := json.Marshal(&latest)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(before, canonical) {
			return nil, fmt.Errorf("account changed; reload and retry")
		}
		return raw, nil
	}); err != nil {
		return err
	}
	if err := e.updateAuthIndex(ctx, "users", expected.ID, raw, remove); err != nil {
		return err
	}
	if remove {
		return e.storage.Delete(ctx, key)
	}
	return nil
}

// ListUsersFromStorage 列出所有用户
func (e *Engine) ListUsersFromStorage() ([]*types.User, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage not initialized")
	}
	items, err := e.storage.ListGet(context.Background(), "users", 0, 10000)
	if err != nil {
		return nil, err
	}
	result := make([]*types.User, 0, len(items))
	seen := map[string]bool{}
	for _, item := range items {
		raw, err := json.Marshal(item)
		if err != nil {
			continue
		}
		var user types.User
		if err := json.Unmarshal(raw, &user); err != nil {
			continue
		}
		if user.ID == "" || seen[user.ID] {
			continue
		}
		canonical, err := e.LoadUserFromStorage(user.ID)
		if err != nil {
			if !errors.Is(err, storage.ErrNotFound) {
				return nil, err
			}
			continue
		}
		seen[user.ID] = true
		result = append(result, canonical)
	}
	return result, nil
}

// VerifyUserPassword 使用 bcrypt 验证用户密码
func (e *Engine) VerifyUserPassword(user *types.User, password string) bool {
	if user.Password == "" {
		return false
	}
	err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(password))
	return err == nil
}

// HashPassword 使用 bcrypt 生成密码哈希
func (e *Engine) HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

var authStoreMu sync.Mutex
var authIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_.@-]+$`)

// Canonical credentials are updated first, followed by a CAS index update.
// Authentication never trusts stale duplicate index entries.
func (e *Engine) saveAuthRecord(list, id string, raw []byte, remove bool) error {
	if !authIDPattern.MatchString(id) {
		return fmt.Errorf("invalid credential identifier")
	}
	authStoreMu.Lock()
	defer authStoreMu.Unlock()
	ctx := context.Background()
	key := list + ":" + id
	// Authentication always reloads the canonical record. A stale index can never
	// restore a disabled/deleted credential, including after an interrupted write.
	if remove {
		if err := e.storage.Delete(ctx, key); err != nil {
			return err
		}
	} else if err := e.storage.Put(ctx, key, raw); err != nil {
		return err
	}
	return e.updateAuthIndex(ctx, list, id, raw, remove)
}

func (e *Engine) updateAuthIndex(ctx context.Context, list, id string, raw []byte, remove bool) error {
	return storage.UpdateList(ctx, e.storage, list, func(items []interface{}) ([]interface{}, error) {
		var incoming types.User
		if list == "users" && !remove {
			if err := json.Unmarshal(raw, &incoming); err != nil {
				return nil, err
			}
		}
		updated := make([]interface{}, 0, len(items)+1)
		for _, item := range items {
			data, err := json.Marshal(item)
			if err != nil {
				return nil, err
			}
			var entry struct {
				ID       string `json:"id"`
				Username string `json:"username"`
			}
			if err := json.Unmarshal(data, &entry); err != nil {
				return nil, err
			}
			if entry.ID != id {
				if list == "users" && !remove && entry.Username == incoming.Username {
					return nil, fmt.Errorf("username already exists")
				}
				updated = append(updated, item)
			}
		}
		if !remove {
			var item interface{}
			if err := json.Unmarshal(raw, &item); err != nil {
				return nil, err
			}
			updated = append(updated, item)
		}
		return updated, nil
	})
}

// A fresh installation has no shared default password. A generated credential is
// written once to a private local file; an operator may supply one via environment.
func (e *Engine) ensureDefaultAdmin() error {
	users, err := e.ListUsersFromStorage()
	if err != nil {
		return err
	}
	if len(users) > 0 {
		return nil
	}
	username := os.Getenv("OWLCRON_ADMIN_USERNAME")
	if username == "" {
		username = "admin"
	}
	if !authIDPattern.MatchString(username) {
		return fmt.Errorf("invalid bootstrap username")
	}
	password := os.Getenv("OWLCRON_ADMIN_PASSWORD")
	generated := password == ""
	base := e.config.StorageBaseDir
	if base == "" {
		base = "data"
	}
	credentialPath := filepath.Join(base, "bootstrap-admin.json")
	if generated {
		var credential struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if raw, err := os.ReadFile(credentialPath); err == nil {
			if err := json.Unmarshal(raw, &credential); err != nil {
				return err
			}
			username, password = credential.Username, credential.Password
			if !authIDPattern.MatchString(username) {
				return fmt.Errorf("invalid stored bootstrap username")
			}
			if err := os.Chmod(credentialPath, 0600); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		} else {
			random := make([]byte, 24)
			if _, err := rand.Read(random); err != nil {
				return err
			}
			password = hex.EncodeToString(random)
			credential.Username, credential.Password = username, password
			if err := os.MkdirAll(base, 0700); err != nil {
				return err
			}
			raw, err := json.Marshal(credential)
			if err != nil {
				return err
			}
			file, err := os.OpenFile(credentialPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				return err
			}
			_, writeErr := file.Write(raw)
			closeErr := file.Close()
			if writeErr != nil {
				return writeErr
			}
			if closeErr != nil {
				return closeErr
			}
		}
	}
	if len(password) < 12 || len(password) > 72 {
		return fmt.Errorf("bootstrap password must contain 12 to 72 bytes")
	}
	hashed, err := e.HashPassword(password)
	if err != nil {
		return err
	}
	user := &types.User{ID: username, Username: username, Password: hashed, Enabled: true, Privileges: types.Privileges{"admin": true}}
	user.Created = time.Now().Unix()
	user.Modified = user.Created
	atomic, ok := e.storage.(storage.AtomicStorage)
	if !ok {
		return storage.ErrUnsupported
	}
	created := false
	if err := atomic.Update(context.Background(), "users:"+username, func(current []byte) ([]byte, error) {
		created = len(current) == 0
		if !created {
			if err := json.Unmarshal(current, user); err != nil {
				return nil, err
			}
			if !user.Enabled || !user.Privileges["admin"] {
				return nil, fmt.Errorf("bootstrap account exists without administrator access")
			}
			return current, nil
		}
		return json.Marshal(user)
	}); err != nil {
		return err
	}
	raw, err := json.Marshal(user)
	if err != nil {
		return err
	}
	if err := e.updateAuthIndex(context.Background(), "users", user.ID, raw, false); err != nil {
		return err
	}
	if generated && e.VerifyUserPassword(user, password) {
		e.logger.Info("Created administrator; read the private bootstrap credential file", zap.String("path", credentialPath))
	} else if !created {
		if generated {
			if err := os.Remove(credentialPath); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		e.logger.Info("Administrator was initialized by another node; obtain its bootstrap credential there")
	}
	return nil
}
