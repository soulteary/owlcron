package api

import (
	"container/heap"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/soulteary/owlcron/types"
	"golang.org/x/crypto/bcrypt"
)

type loginBucket struct {
	key     string
	count   int
	expires time.Time
}
type loginExpiryHeap []*loginBucket

func (h loginExpiryHeap) Len() int            { return len(h) }
func (h loginExpiryHeap) Less(i, j int) bool  { return h[i].expires.Before(h[j].expires) }
func (h loginExpiryHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *loginExpiryHeap) Push(v interface{}) { *h = append(*h, v.(*loginBucket)) }
func (h *loginExpiryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	b := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return b
}

// loginLimiter counts attempts, including successful logins. Active buckets are
// never evicted to make room: random account/IP spraying cannot reset a victim's
// counter. Capacity exhaustion fails closed until a bucket expires.
type loginLimiter struct {
	mu                                sync.Mutex
	buckets                           map[string]*loginBucket
	expiry                            loginExpiryHeap
	window                            time.Duration
	ipLimit, accountLimit, maxEntries int
	slots                             chan struct{}
	now                               func() time.Time
}

func newLoginLimiter(config *types.Config) *loginLimiter {
	positive := func(v, fallback int) int {
		if v > 0 {
			return v
		}
		return fallback
	}
	return &loginLimiter{
		buckets:      make(map[string]*loginBucket),
		window:       time.Duration(positive(config.LoginRateWindowSeconds, 60)) * time.Second,
		ipLimit:      positive(config.LoginRateLimit, 20),
		accountLimit: positive(config.LoginAccountRateLimit, 10),
		maxEntries:   positive(config.LoginLimiterMaxEntries, 10000),
		slots:        make(chan struct{}, positive(config.LoginMaxConcurrent, 4)),
		now:          time.Now,
	}
}

func loginKey(kind, value string) string {
	h := sha256.Sum256([]byte(value))
	return kind + hex.EncodeToString(h[:])
}

func (l *loginLimiter) reserve(keys []string, limits []int) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	for len(l.expiry) > 0 && !l.expiry[0].expires.After(now) {
		b := heap.Pop(&l.expiry).(*loginBucket)
		delete(l.buckets, b.key)
	}
	missing := 0
	var retry time.Duration
	for i, key := range keys {
		b := l.buckets[key]
		if b == nil {
			missing++
			continue
		}
		if b.count >= limits[i] && b.expires.Sub(now) > retry {
			retry = b.expires.Sub(now)
		}
	}
	if retry > 0 {
		return retry
	}
	if len(l.buckets)+missing > l.maxEntries {
		if len(l.expiry) > 0 {
			return l.expiry[0].expires.Sub(now)
		}
		return l.window
	}
	for _, key := range keys {
		b := l.buckets[key]
		if b == nil {
			b = &loginBucket{key: key, expires: now.Add(l.window)}
			l.buckets[key] = b
			heap.Push(&l.expiry, b)
		}
		b.count++
	}
	return 0
}

func (l *loginLimiter) allowIP(ip string) time.Duration {
	return l.reserve([]string{loginKey("ip:", ip)}, []int{l.ipLimit})
}

func (l *loginLimiter) acquire(ip, username string) (func(), time.Duration) {
	if retry := l.reserve([]string{loginKey("account:", username), loginKey("pair:", ip+"\x00"+username)}, []int{l.accountLimit, l.accountLimit}); retry > 0 {
		return nil, retry
	}
	select {
	case l.slots <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-l.slots }) }, 0
	default:
		return nil, time.Second
	}
}

func sendLoginLimited(w http.ResponseWriter, retry time.Duration) {
	seconds := int64((retry + time.Second - 1) / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
	w.WriteHeader(http.StatusTooManyRequests)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 1, "description": "Too many login attempts. Please retry later."})
}

func dummyPasswordCheck(password string) {
	_ = bcrypt.CompareHashAndPassword([]byte("$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"), []byte(password))
}
