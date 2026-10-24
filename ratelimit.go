package fasthttp

import (
	"fmt"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// RateLimitPolicy is a per-source request allowance expressed as a token
// bucket: Rate tokens are added per second, up to a maximum bank of Burst.
//
// A source may spend one token per request. It may burst up to Burst requests
// after being idle, and sustain Rate requests per second on average. Refill
// is continuous, so unused allowance is restored smoothly over time.
type RateLimitPolicy struct {
	// Rate is the sustained number of requests per second permitted for one
	// source. It must be greater than zero and must be finite.
	Rate float64

	// Burst is the maximum number of requests a source may make in a single
	// burst. It must be greater than zero.
	Burst int
}

// RateLimitConfig configures per-source request rate limiting on a Server.
//
// A source is the peer address the connection comes from (the same key
// Server.MaxConnsPerIP uses). All connections from one source share a single
// allowance, even when the requests arrive concurrently over different
// connections. Requests over the allowance are rejected on their own with
// 429 Too Many Requests and a Retry-After hint; the connection and requests
// from other sources are not affected.
//
// PerPath may relax or tighten the allowance for an individual request path
// (matched exactly against the request URI path). Each configured path keeps
// a bucket of its own per source, while every other path shares the default
// bucket configured by Rate and Burst.
//
// A nil *RateLimitConfig (the default) disables rate limiting completely.
// Rate limiting must not be enabled on a zero-valued config: invalid values
// are reported as an error when the server starts instead of silently
// turning into a zero allowance.
type RateLimitConfig struct {
	// Rate is the default sustained request rate per source, in requests
	// per second. It must be greater than zero and must be finite.
	Rate float64

	// Burst is the default maximum burst size per source. It must be
	// greater than zero.
	Burst int

	// PerPath optionally overrides the default Rate/Burst for individual
	// request paths. Paths are matched exactly, e.g. "/login". Empty path
	// keys are rejected.
	PerPath map[string]RateLimitPolicy
}

// Validate reports an error when the policy cannot be used. Zero or negative
// values, NaN and infinite rates are all rejected so that a misconfigured
// limit can never silently become a zero allowance.
func (p RateLimitPolicy) Validate() error {
	switch {
	case math.IsNaN(p.Rate) || math.IsInf(p.Rate, 0):
		return fmt.Errorf("rate must be a finite number, got %v", p.Rate)
	case p.Rate <= 0:
		return fmt.Errorf("rate must be greater than zero, got %v", p.Rate)
	case p.Burst <= 0:
		return fmt.Errorf("burst must be greater than zero, got %d", p.Burst)
	}
	return nil
}

// Validate reports an error when the configuration cannot be enabled,
// including any invalid PerPath entry.
func (c *RateLimitConfig) Validate() error {
	if c == nil {
		return nil
	}
	if err := (RateLimitPolicy{Rate: c.Rate, Burst: c.Burst}).Validate(); err != nil {
		return fmt.Errorf("fasthttp: invalid default rate limit: %w", err)
	}
	for path, p := range c.PerPath {
		if path == "" {
			return fmt.Errorf("fasthttp: invalid rate limit for path %q: path must not be empty", path)
		}
		if err := p.Validate(); err != nil {
			return fmt.Errorf("fasthttp: invalid rate limit for path %q: %w", path, err)
		}
	}
	return nil
}

// RateLimitRejectionReasonDefault is the reason recorded when a request is
// rejected by the server-wide per-source limit (a path without a PerPath
// override).
const RateLimitRejectionReasonDefault = "default"

const (
	// rateLimitGCInterval bounds how often the bucket table is swept.
	rateLimitGCInterval = time.Minute
	// rateLimitBucketIdle is how long a source bucket may stay untouched
	// before it is evicted.
	rateLimitBucketIdle = 5 * time.Minute
)

// rateLimitSource identifies the source a request comes from. Peers with an
// IP address key on the address itself; other transports fall back to their
// remote address string.
type rateLimitSource struct {
	ip   perIPKey
	ipOK bool
	addr string
}

func connRateLimitSource(c net.Conn) rateLimitSource {
	if key, ok := getPerIPKey(c); ok {
		return rateLimitSource{ip: key, ipOK: true}
	}
	if a := c.RemoteAddr(); a != nil {
		return rateLimitSource{addr: a.String()}
	}
	return rateLimitSource{}
}

type rateLimitBucketKey struct {
	source rateLimitSource
	// policy is the overridden request path, or "" for the default policy.
	policy string
}

// rateLimitBucket is one source's token balance for one policy. The mutex
// makes the refill-then-deduct decision atomic: concurrent requests over
// several connections can never each observe the same token as available.
type rateLimitBucket struct {
	mu sync.Mutex

	tokens float64
	last   time.Time

	// lastUsed drives idle eviction. It is read by the sweeper without
	// taking mu, hence atomic.
	lastUsed atomic.Int64
}

// allow refills the bucket according to the elapsed time and then atomically
// deducts one token. When no token is available it reports the time the
// caller has to wait before the next token.
func (b *rateLimitBucket) allow(now time.Time, rate float64, burst int) (bool, time.Duration) {
	b.mu.Lock()

	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		tokens := b.tokens + elapsed*rate
		if cap := float64(burst); tokens > cap {
			tokens = cap
		}
		b.tokens = tokens
		b.last = now
	}

	allowed := b.tokens >= 1
	if allowed {
		b.tokens--
	}
	wait := time.Duration(0)
	if !allowed {
		wait = time.Duration((1 - b.tokens) / rate * float64(time.Second))
	}

	b.mu.Unlock()
	return allowed, wait
}

// rateLimiter holds the immutable policies and the mutable per-source
// buckets of one Server.
type rateLimiter struct {
	defaultPolicy rateLimitPolicy
	pathPolicies  map[string]rateLimitPolicy

	mu      sync.Mutex
	buckets map[rateLimitBucketKey]*rateLimitBucket
	lastGC  time.Time

	totalRejected atomic.Uint64
	// rejectedByReason counts rejections separately: the default policy
	// records RateLimitRejectionReasonDefault, an overridden path records
	// the path itself.
	rejectedByReason map[string]*atomic.Uint64
}

type rateLimitPolicy struct {
	rate  float64
	burst int
}

func newRateLimiter(cfg *RateLimitConfig) (*rateLimiter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	rl := &rateLimiter{
		defaultPolicy: rateLimitPolicy{rate: cfg.Rate, burst: cfg.Burst},
		pathPolicies:  make(map[string]rateLimitPolicy, len(cfg.PerPath)),
		buckets:       make(map[rateLimitBucketKey]*rateLimitBucket),
		lastGC:        time.Now(),
		rejectedByReason: map[string]*atomic.Uint64{
			RateLimitRejectionReasonDefault: {},
		},
	}
	for path, p := range cfg.PerPath {
		rl.pathPolicies[path] = rateLimitPolicy{rate: p.Rate, burst: p.Burst}
		rl.rejectedByReason[path] = &atomic.Uint64{}
	}
	return rl, nil
}

// allowRequest atomically checks and consumes one token of the source's
// allowance for the given request path. On rejection it reports the suggested
// Retry-After value in whole seconds (always at least one) and records the
// rejection under its reason.
func (rl *rateLimiter) allowRequest(src rateLimitSource, path []byte) (allowed bool, retryAfterSeconds int) {
	policyPath, p := rl.policyFor(path)
	reason := RateLimitRejectionReasonDefault
	if policyPath != "" {
		reason = policyPath
	}

	now := time.Now()
	key := rateLimitBucketKey{source: src, policy: policyPath}
	b := rl.acquireBucket(key, now)

	ok, wait := b.allow(now, p.rate, p.burst)
	if ok {
		return true, 0
	}

	secs := int((wait + time.Second - 1) / time.Second) // ceil
	if secs < 1 {
		secs = 1
	}
	rl.totalRejected.Add(1)
	rl.rejectedByReason[reason].Add(1)
	return false, secs
}

func (rl *rateLimiter) policyFor(path []byte) (policyPath string, p rateLimitPolicy) {
	// The table is small (it comes from configuration), so a direct string
	// lookup beats hashing the bytes into another structure.
	policyPath = string(path)
	if pp, ok := rl.pathPolicies[policyPath]; ok {
		return policyPath, pp
	}
	return "", rl.defaultPolicy
}

func (rl *rateLimiter) acquireBucket(key rateLimitBucketKey, now time.Time) *rateLimitBucket {
	rl.mu.Lock()

	if b := rl.buckets[key]; b != nil {
		b.lastUsed.Store(now.UnixNano())
		rl.mu.Unlock()
		return b
	}

	b := &rateLimitBucket{
		tokens: float64(rl.burstFor(key)),
		last:   now,
	}
	b.lastUsed.Store(now.UnixNano())
	rl.buckets[key] = b

	rl.gcLocked(now)

	rl.mu.Unlock()
	return b
}

func (rl *rateLimiter) burstFor(key rateLimitBucketKey) int {
	if key.policy != "" {
		return rl.pathPolicies[key.policy].burst
	}
	return rl.defaultPolicy.burst
}

// gcLocked drops buckets of sources that have not sent a request within
// rateLimitBucketIdle, so the table cannot grow with the number of distinct
// sources ever seen. In-flight requests already hold their own bucket
// pointer, so an eviction only resets that source's balance if it becomes
// active again later.
func (rl *rateLimiter) gcLocked(now time.Time) {
	if now.Sub(rl.lastGC) < rateLimitGCInterval {
		return
	}
	rl.lastGC = now
	deadline := now.Add(-rateLimitBucketIdle).UnixNano()
	for key, b := range rl.buckets {
		if b.lastUsed.Load() < deadline {
			delete(rl.buckets, key)
		}
	}
}

func (rl *rateLimiter) rejectedCount() uint64 {
	return rl.totalRejected.Load()
}

func (rl *rateLimiter) rejectedCounts() map[string]uint64 {
	m := make(map[string]uint64, len(rl.rejectedByReason))
	for reason, n := range rl.rejectedByReason {
		m[reason] = n.Load()
	}
	return m
}
