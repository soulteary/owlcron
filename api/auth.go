package api

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/soulteary/owlcron/engine"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

// AuthContext 认证上下文
type AuthContext struct {
	User       *types.User
	APIKey     *types.APIKey
	Session    *types.Session
	IsAdmin    bool
	Privileges map[string]bool
	LogJobID   string
}

// authContextKey 认证上下文键
type authContextKey struct{}

// GetAuthContext 从请求中获取认证上下文
func GetAuthContext(r *http.Request) *AuthContext {
	ctx := r.Context()
	if authCtx, ok := ctx.Value(authContextKey{}).(*AuthContext); ok {
		return authCtx
	}
	return nil
}

// SetAuthContext 设置认证上下文
func SetAuthContext(r *http.Request, authCtx *AuthContext) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), authContextKey{}, authCtx))
}

// AuthMiddleware 认证中间件
func AuthMiddleware(eng *engine.Engine, logger *zap.Logger) func(http.Handler) http.Handler {
	s := NewServer(eng, logger)
	return s.AuthenticationMiddleware
}

// AuthenticationMiddleware shares password admission limits with JSON login.
func (s *Server) AuthenticationMiddleware(next http.Handler) http.Handler {
	eng, logger := s.engine, s.logger
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
			if origin := r.Header.Get("Origin"); origin != "" {
				u, err := url.Parse(origin)
				if err != nil || u.Host != r.Host || (u.Scheme != "http" && u.Scheme != "https") {
					sendErrorResponse(w, 1, "Cross-origin write denied")
					return
				}
			}
		}
		if GetAuthContext(r) != nil {
			next.ServeHTTP(w, r)
			return
		}
		// 跳过认证的路径
		if skipAuth(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		authCtx, err := authenticateLimited(eng, r, logger, s.loginLimiter, s.clientIP(r))
		if err != nil {
			var limited *loginLimitError
			if errors.As(err, &limited) {
				sendLoginLimited(w, limited.retry)
				return
			}
			sendErrorResponse(w, 1, "Authentication failed: "+err.Error())
			return
		}

		// 设置认证上下文
		r = SetAuthContext(r, authCtx)
		next.ServeHTTP(w, r)
	})
}

// skipAuth 检查是否跳过认证
func skipAuth(path string) bool {
	publicPaths := []string{
		"/api/app/ping",
		"/api/app/status",
		"/api/app/echo",
		"/api/app/config",
		"/api/user/login",
		"/api/user/logout",
		"/api/user/resume_session",
	}
	for _, p := range publicPaths {
		if path == p || path == p+"/v1" {
			return true
		}
	}

	if !strings.HasPrefix(path, "/api/") {
		return true
	}

	return false
}

// authenticate 认证请求
type loginLimitError struct{ retry time.Duration }

func (e *loginLimitError) Error() string { return "too many login attempts" }

func authenticate(eng *engine.Engine, r *http.Request, logger *zap.Logger) (*AuthContext, error) {
	return authenticateLimited(eng, r, logger, nil, "")
}

func authenticateLimited(eng *engine.Engine, r *http.Request, logger *zap.Logger, limiter *loginLimiter, ip string) (*AuthContext, error) {
	if r.URL.Query().Get("t") != "" {
		return authenticateLogToken(eng, r)
	}
	params, err := requestParameters(r)
	if err != nil {
		return nil, err
	}
	// 尝试 API Key 认证
	apiKey := r.Header.Get("X-API-Key")
	if apiKey == "" {
		apiKey = stringParam(params, "api_key")
	}
	if apiKey != "" {
		return authenticateAPIKey(eng, apiKey, logger)
	}

	// 尝试 Session 认证
	sessionID := getSessionID(r)
	if sessionID == "" {
		sessionID = stringParam(params, "session_id")
	}
	if sessionID != "" {
		return authenticateSession(eng, sessionID, logger)
	}

	// 尝试 Basic Auth
	username, password, ok := r.BasicAuth()
	if ok {
		if limiter != nil {
			if retry := limiter.allowIP(ip); retry > 0 {
				return nil, &loginLimitError{retry}
			}
			if username == "" || len(username) > 50 || password == "" || len(password) > 72 {
				return nil, fmt.Errorf("invalid credentials")
			}
			release, retry := limiter.acquire(ip, username)
			if retry > 0 {
				return nil, &loginLimitError{retry}
			}
			defer release()
		}
		if err := r.Context().Err(); err != nil {
			return nil, err
		}
		auth, err := authenticateBasic(eng, username, password, logger)
		if contextErr := r.Context().Err(); contextErr != nil {
			return nil, contextErr
		}
		return auth, err
	}

	return nil, fmt.Errorf("no authentication provided")
}

// authenticateAPIKey API Key 认证
func authenticateAPIKey(eng *engine.Engine, apiKey string, logger *zap.Logger) (*AuthContext, error) {
	// 从存储加载 API Key
	key, err := eng.LoadAPIKeyFromStorage(apiKey)
	if err != nil {
		logger.Debug("API key not found in storage", zap.Error(err))
		return nil, fmt.Errorf("invalid API key")
	}

	if !key.Enabled {
		return nil, fmt.Errorf("API key is disabled")
	}
	if key.Key == "" || subtle.ConstantTimeCompare([]byte(key.Key), []byte(apiKey)) != 1 {
		return nil, fmt.Errorf("invalid API key")
	}

	// 更新最后使用时间并异步持久化
	key.LastUsed = time.Now().Unix()
	// Persisting a stale key here can re-enable a concurrently disabled credential.
	// LastUsed is exposed on this request only; security state is never overwritten.

	authCtx := &AuthContext{
		APIKey:     key,
		IsAdmin:    key.Privileges["admin"],
		Privileges: key.Privileges,
	}

	return authCtx, nil
}

// authenticateSession Session 认证
func authenticateSession(eng *engine.Engine, sessionID string, logger *zap.Logger) (*AuthContext, error) {
	// 从存储加载 Session
	session, err := eng.LoadSessionFromStorage(sessionID)
	if err != nil {
		logger.Debug("Session not found in storage", zap.Error(err))
		return nil, fmt.Errorf("invalid session")
	}

	user, err := eng.ValidateUserSession(session)
	if err != nil {
		return nil, err
	}

	authCtx := &AuthContext{
		User:       user,
		Session:    session,
		IsAdmin:    user.Privileges["admin"],
		Privileges: user.Privileges,
	}

	return authCtx, nil
}

// authenticateBasic Basic Auth 认证
func authenticateBasic(eng *engine.Engine, username, password string, logger *zap.Logger) (*AuthContext, error) {
	if username == "" || len(username) > 50 || password == "" || len(password) > 72 {
		return nil, fmt.Errorf("invalid credentials")
	}
	user, err := eng.LoadUserByUsernameFromStorage(username)
	if err != nil || user == nil || user.Password == "" {
		dummyPasswordCheck(password)
		return nil, fmt.Errorf("invalid credentials")
	}
	valid := eng.VerifyUserPassword(user, password)
	if !valid || !user.Enabled {
		return nil, fmt.Errorf("invalid credentials")
	}

	authCtx := &AuthContext{
		User:       user,
		IsAdmin:    user.Privileges["admin"],
		Privileges: user.Privileges,
	}

	return authCtx, nil
}

// getSessionID 从请求中获取 Session ID
func getSessionID(r *http.Request) string {
	// 从 Cookie 获取
	cookie, err := r.Cookie("session_id")
	if err == nil && cookie.Value != "" {
		return cookie.Value
	}

	// 从 Header 获取
	return r.Header.Get("X-Session-ID")
}

// RequireAdmin 要求管理员权限
func RequireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authCtx := GetAuthContext(r)
		if authCtx == nil || !authCtx.IsAdmin {
			sendErrorResponse(w, 1, "Admin privileges required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// RequirePrivilege 要求特定权限
func RequirePrivilege(privilege string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authCtx := GetAuthContext(r)
			if authCtx == nil {
				sendErrorResponse(w, 1, "Authentication required")
				return
			}

			if !authCtx.Privileges[privilege] && !authCtx.IsAdmin {
				sendErrorResponse(w, 1, fmt.Sprintf("Privilege '%s' required", privilege))
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// RequireMaster 要求主服务器
func RequireMaster(eng *engine.Engine) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !eng.EnsureLeadership() {
				sendErrorResponse(w, 1, "Current master server required")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// ValidateSignature 验证请求签名
func ValidateSignature(secretKey string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			signature := r.Header.Get("X-Signature")
			if signature == "" {
				sendErrorResponse(w, 1, "Missing signature")
				return
			}

			// 构建签名消息（方法 + 路径 + 查询字符串 + 时间戳）
			timestamp := r.Header.Get("X-Timestamp")
			if timestamp == "" {
				sendErrorResponse(w, 1, "Missing timestamp")
				return
			}

			message := fmt.Sprintf("%s%s%s%s", r.Method, r.URL.Path, r.URL.RawQuery, timestamp)
			expectedSignature := computeHMAC(secretKey, message)

			if !hmac.Equal([]byte(signature), []byte(expectedSignature)) {
				sendErrorResponse(w, 1, "Invalid signature")
				return
			}

			ts, err := strconv.ParseInt(timestamp, 10, 64)
			if err != nil {
				sendErrorResponse(w, 1, "Invalid timestamp format")
				return
			}
			const maxDriftSeconds = 300 // 5 分钟
			if math.Abs(float64(time.Now().Unix()-ts)) > maxDriftSeconds {
				sendErrorResponse(w, 1, "Request timestamp expired (anti-replay)")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// computeHMAC 计算 HMAC
func computeHMAC(secret, message string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(message))
	return hex.EncodeToString(h.Sum(nil))
}

// sendErrorResponse 发送错误响应（独立函数版本，用于中间件）
func sendErrorResponse(w http.ResponseWriter, code int, description string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	resp := map[string]interface{}{
		"code":        code,
		"description": description,
	}

	json.NewEncoder(w).Encode(resp)
}
