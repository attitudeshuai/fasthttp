package fasthttp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Dial dials the given TCP addr using tcp4.
//
// This function has the following additional features comparing to net.Dial:
//
//   - It reduces load on DNS resolver by caching resolved TCP addressed
//     for DNSCacheDuration.
//   - It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//   - It returns ErrDialTimeout if connection cannot be established during
//     DefaultDialTimeout seconds. Use DialTimeout for customizing dial timeout.
//
// This dialer is intended for custom code wrapping before passing
// to Client.Dial or HostClient.Dial.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//   - foobar.baz:443
//   - foo.bar:80
//   - aaa.com:8080
func Dial(addr string) (net.Conn, error) {
	return defaultDialer.Dial(addr)
}

// DialTimeout dials the given TCP addr using tcp4 using the given timeout.
//
// This function has the following additional features comparing to net.Dial:
//
//   - It reduces load on DNS resolver by caching resolved TCP addressed
//     for DNSCacheDuration.
//   - It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//
// This dialer is intended for custom code wrapping before passing
// to Client.DialTimeout or HostClient.DialTimeout.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//   - foobar.baz:443
//   - foo.bar:80
//   - aaa.com:8080
func DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return defaultDialer.DialTimeout(addr, timeout)
}

// DialDualStack dials the given TCP addr using both tcp4 and tcp6.
//
// This function has the following additional features comparing to net.Dial:
//
//   - It reduces load on DNS resolver by caching resolved TCP addressed
//     for DNSCacheDuration.
//   - It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//   - It returns ErrDialTimeout if connection cannot be established during
//     DefaultDialTimeout seconds. Use DialDualStackTimeout for custom dial
//     timeout.
//
// This dialer is intended for custom code wrapping before passing
// to Client.Dial or HostClient.Dial.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//   - foobar.baz:443
//   - foo.bar:80
//   - aaa.com:8080
func DialDualStack(addr string) (net.Conn, error) {
	return defaultDialer.DialDualStack(addr)
}

// DialDualStackTimeout dials the given TCP addr using both tcp4 and tcp6
// using the given timeout.
//
// This function has the following additional features comparing to net.Dial:
//
//   - It reduces load on DNS resolver by caching resolved TCP addressed
//     for DNSCacheDuration.
//   - It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//
// This dialer is intended for custom code wrapping before passing
// to Client.DialTimeout or HostClient.DialTimeout.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//   - foobar.baz:443
//   - foo.bar:80
//   - aaa.com:8080
func DialDualStackTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return defaultDialer.DialDualStackTimeout(addr, timeout)
}

var defaultDialer = &TCPDialer{Concurrency: 1000}

// Resolver represents interface of the tcp resolver.
type Resolver interface {
	LookupIPAddr(context.Context, string) (names []net.IPAddr, err error)
}

// TCPDialer contains options to control a group of Dial calls.
type TCPDialer struct {
	// This may be used to override DNS resolving policy, like this:
	// var dialer = &fasthttp.TCPDialer{
	// 	Resolver: &net.Resolver{
	// 		PreferGo:     true,
	// 		StrictErrors: false,
	// 		Dial: func (ctx context.Context, network, address string) (net.Conn, error) {
	// 			d := net.Dialer{}
	// 			return d.DialContext(ctx, "udp", "8.8.8.8:53")
	// 		},
	// 	},
	// }
	Resolver Resolver

	// LocalAddr is the local address to use when dialing an
	// address.
	// If nil, a local address is automatically chosen.
	LocalAddr *net.TCPAddr

	concurrencyCh chan struct{}

	tcpAddrsMap    sync.Map
	cleanerRunning atomic.Bool

	// Concurrency controls the maximum number of concurrent Dials
	// that can be performed using this object.
	// Setting this to 0 means unlimited.
	//
	// WARNING: This can only be changed before the first Dial.
	// Changes made after the first Dial will not affect anything.
	Concurrency int

	// DNSCacheDuration may be used to override the default DNS cache duration (DefaultDNSCacheDuration)
	DNSCacheDuration time.Duration

	// KeepAlive specifies the keep-alive period for an active network connection.
	// If zero, keep-alives are enabled if supported by the protocol and operating system.
	// If negative, keep-alives are disabled.
	KeepAlive time.Duration

	// AddrFailureThreshold is the number of consecutive dial failures of a
	// single resolved address after which the address is put into cooldown
	// and skipped (moved to the end of the candidate list) by subsequent
	// dials until AddrCooldownDuration has passed.
	//
	// Address-level failure tracking is enabled only when
	// AddrCooldownDuration is greater than zero. When tracking is enabled
	// and AddrFailureThreshold is zero, defaultAddrFailureThreshold is used.
	//
	// Failure counters and cooldown state are shared between all concurrent
	// dials to the same host, per resolved address.
	//
	// WARNING: This can only be changed before the first Dial, like Concurrency.
	AddrFailureThreshold uint32

	// AddrCooldownDuration is the duration for which a resolved address is
	// skipped after AddrFailureThreshold consecutive dial failures.
	//
	// Zero disables address-level failure tracking: the dialer keeps the
	// historical round-robin dial order and error behavior, including
	// aborting the whole round on the first dial timeout.
	//
	// When the cooldown expires, the address is allowed back into the
	// candidate list as a single half-open probe: concurrent dials do not
	// pile on the same address, only one probe runs while the others keep
	// using healthy addresses. A successful probe immediately restores the
	// address to normal traffic; a failed probe puts it back into cooldown
	// and doubles the cooldown duration, up to 64x AddrCooldownDuration.
	// Cooldown is never a permanent ban.
	//
	// When every resolved address is in cooldown (or already being probed),
	// the dial returns an error wrapping ErrAllAddrsInCooldown, which
	// callers may use to decide on degradation.
	//
	// WARNING: This can only be changed before the first Dial, like Concurrency.
	AddrCooldownDuration time.Duration

	once sync.Once

	// DisableDNSResolution may be used to disable DNS resolution
	DisableDNSResolution bool

	// dialConn optionally overrides the low-level network dial. It is meant
	// for tests; nil uses a net.Dialer honoring LocalAddr and KeepAlive.
	dialConn func(ctx context.Context, network, addr string) (net.Conn, error)
}

// Dial dials the given TCP addr using tcp4.
//
// This function has the following additional features comparing to net.Dial:
//
//   - It reduces load on DNS resolver by caching resolved TCP addressed
//     for DNSCacheDuration.
//   - It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//   - It returns ErrDialTimeout if connection cannot be established during
//     DefaultDialTimeout seconds. Use DialTimeout for customizing dial timeout.
//
// This dialer is intended for custom code wrapping before passing
// to Client.Dial or HostClient.Dial.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//   - foobar.baz:443
//   - foo.bar:80
//   - aaa.com:8080
func (d *TCPDialer) Dial(addr string) (net.Conn, error) {
	return d.dial(addr, false, DefaultDialTimeout)
}

// DialTimeout dials the given TCP addr using tcp4 using the given timeout.
//
// This function has the following additional features comparing to net.Dial:
//
//   - It reduces load on DNS resolver by caching resolved TCP addressed
//     for DNSCacheDuration.
//   - It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//
// This dialer is intended for custom code wrapping before passing
// to Client.DialTimeout or HostClient.DialTimeout.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//   - foobar.baz:443
//   - foo.bar:80
//   - aaa.com:8080
func (d *TCPDialer) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return d.dial(addr, false, timeout)
}

// DialDualStack dials the given TCP addr using both tcp4 and tcp6.
//
// This function has the following additional features comparing to net.Dial:
//
//   - It reduces load on DNS resolver by caching resolved TCP addressed
//     for DNSCacheDuration.
//   - It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//   - It returns ErrDialTimeout if connection cannot be established during
//     DefaultDialTimeout seconds. Use DialDualStackTimeout for custom dial
//     timeout.
//
// This dialer is intended for custom code wrapping before passing
// to Client.Dial or HostClient.Dial.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//   - foobar.baz:443
//   - foo.bar:80
//   - aaa.com:8080
func (d *TCPDialer) DialDualStack(addr string) (net.Conn, error) {
	return d.dial(addr, true, DefaultDialTimeout)
}

// DialDualStackTimeout dials the given TCP addr using both tcp4 and tcp6
// using the given timeout.
//
// This function has the following additional features comparing to net.Dial:
//
//   - It reduces load on DNS resolver by caching resolved TCP addressed
//     for DNSCacheDuration.
//   - It dials all the resolved TCP addresses in round-robin manner until
//     connection is established. This may be useful if certain addresses
//     are temporarily unreachable.
//
// This dialer is intended for custom code wrapping before passing
// to Client.DialTimeout or HostClient.DialTimeout.
//
// For instance, per-host counters and/or limits may be implemented
// by such wrappers.
//
// The addr passed to the function must contain port. Example addr values:
//
//   - foobar.baz:443
//   - foo.bar:80
//   - aaa.com:8080
func (d *TCPDialer) DialDualStackTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return d.dial(addr, true, timeout)
}

// FlushDNSCache clears all cached DNS entries, forcing fresh DNS lookups on subsequent dials.
// This is useful when you want to ensure fresh DNS resolution, for example after network changes.
func (d *TCPDialer) FlushDNSCache() {
	d.tcpAddrsMap.Range(func(k, v any) bool {
		d.tcpAddrsMap.Delete(k)
		return true
	})
}

// FlushDNSCache clears all cached DNS entries for the default dialer,
// forcing fresh DNS lookups on subsequent Dial* calls.
// This is useful when you want to ensure fresh DNS resolution, for example after network changes.
func FlushDNSCache() {
	defaultDialer.FlushDNSCache()
}

func (d *TCPDialer) dial(addr string, dualStack bool, timeout time.Duration) (net.Conn, error) {
	d.once.Do(func() {
		if d.Concurrency > 0 {
			d.concurrencyCh = make(chan struct{}, d.Concurrency)
		}

		if d.DNSCacheDuration == 0 {
			d.DNSCacheDuration = DefaultDNSCacheDuration
		}
	})
	deadline := time.Now().Add(timeout)
	network := "tcp4"
	if dualStack {
		network = "tcp"
	}
	if d.DisableDNSResolution {
		return d.tryDial(network, addr, deadline, d.concurrencyCh)
	}
	e, idx, err := d.getTCPAddrs(addr, dualStack, deadline)
	if err != nil {
		return nil, err
	}
	d.startTCPAddrsClean()
	if d.AddrCooldownDuration > 0 {
		return d.dialTracked(network, e, idx, deadline)
	}
	addrs := e.addrs
	var conn net.Conn
	n := uint32(len(addrs)) // #nosec G115
	for range n {
		conn, err = d.tryDial(network, addrs[idx%n].String(), deadline, d.concurrencyCh)
		if err == nil {
			return conn, nil
		}
		if errors.Is(err, ErrDialTimeout) {
			return nil, err
		}
		idx++
	}
	return nil, err
}

func (d *TCPDialer) tryDial(
	network string, addr string, deadline time.Time, concurrencyCh chan struct{},
) (net.Conn, error) {
	timeout := time.Until(deadline)
	if timeout <= 0 {
		return nil, wrapDialWithUpstream(ErrDialTimeout, addr)
	}

	if concurrencyCh != nil {
		select {
		case concurrencyCh <- struct{}{}:
		default:
			tc := AcquireTimer(timeout)
			isTimeout := false
			select {
			case concurrencyCh <- struct{}{}:
			case <-tc.C:
				isTimeout = true
			}
			ReleaseTimer(tc)
			if isTimeout {
				return nil, wrapDialWithUpstream(ErrDialTimeout, addr)
			}
		}
		defer func() { <-concurrencyCh }()
	}

	ctx, cancelCtx := context.WithDeadline(context.Background(), deadline)
	defer cancelCtx()
	var conn net.Conn
	var err error
	if d.dialConn != nil {
		conn, err = d.dialConn(ctx, network, addr)
	} else {
		dialer := net.Dialer{}
		if d.LocalAddr != nil {
			dialer.LocalAddr = d.LocalAddr
		}
		if d.KeepAlive != 0 {
			dialer.KeepAlive = d.KeepAlive
		}
		conn, err = dialer.DialContext(ctx, network, addr)
	}
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return nil, wrapDialWithUpstream(ErrDialTimeout, addr)
		}
		return nil, wrapDialWithUpstream(err, addr)
	}
	return conn, nil
}

// ErrDialTimeout is returned when TCP dialing is timed out.
var ErrDialTimeout = errors.New("fasthttp: dialing to the given tcp address timed out")

// ErrDialWithUpstream wraps dial error with upstream info.
//
// Use errors.AsType to get upstream information from the error:
//
//	hc := fasthttp.HostClient{Addr: "foo.com,bar.com"}
//	err := hc.Do(req, res)
//
//	if dialErr, ok := errors.AsType[*fasthttp.ErrDialWithUpstream](err); ok {
//		upstream = dialErr.Upstream // 34.206.39.153:80
//	}
type ErrDialWithUpstream struct {
	wrapErr  error
	Upstream string
}

func (e *ErrDialWithUpstream) Error() string {
	return fmt.Sprintf("error when dialing %s: %s", e.Upstream, e.wrapErr.Error())
}

func (e *ErrDialWithUpstream) Unwrap() error {
	return e.wrapErr
}

func wrapDialWithUpstream(err error, upstream string) error {
	return &ErrDialWithUpstream{
		Upstream: upstream,
		wrapErr:  err,
	}
}

// ErrAllAddrsInCooldown is returned (wrapped) when address-level failure
// tracking is enabled and every resolved address of a host is temporarily
// skipped because of repeated dial failures, or because the only address
// whose cooldown has expired is already being probed by another dial.
//
// Use errors.Is to detect it and decide on degradation:
//
//	if errors.Is(err, fasthttp.ErrAllAddrsInCooldown) {
//	    // every upstream address is cooling down; degrade somehow
//	}
//
// The wrapped error also joins the most recent dial error of every address.
var ErrAllAddrsInCooldown = errors.New("fasthttp: all resolved tcp addresses are in cooldown")

// defaultAddrFailureThreshold is used when failure tracking is enabled
// (AddrCooldownDuration > 0) but AddrFailureThreshold is left zero.
const defaultAddrFailureThreshold uint32 = 3

// maxAddrCooldownMultiplier bounds the exponential cooldown backoff:
// each failed half-open probe doubles the cooldown, up to this multiple
// of AddrCooldownDuration.
const maxAddrCooldownMultiplier = 64

// Per-address dial state, shared between all concurrent dials of the host
// the address belongs to.
type tcpAddrHealth struct {
	mu sync.Mutex

	// failures is the number of consecutive dial failures since the
	// last successful dial.
	failures uint32

	// cooldownUntil in the future means the address is cooling down and
	// must be skipped. The zero value means it has never cooled down.
	cooldownUntil time.Time

	// cooldown is the currently effective cooldown duration; it doubles
	// on every failed half-open probe up to the configured cap.
	cooldown time.Duration

	// lastErr is the most recent dial failure reason for the address.
	lastErr error

	// probing marks an in-flight half-open probe after cooldown expiry,
	// so concurrent dials skip the address instead of all probing it.
	probing bool
}

// tcpAddrState is the result of classifying an address for a dial.
type tcpAddrState uint8

const (
	// addrHealthy: below the failure threshold, may be dialed normally.
	addrHealthy tcpAddrState = iota
	// addrProbe: cooldown expired and the caller wins the single
	// half-open probe slot; it must dial the address now.
	addrProbe
	// addrCooling: inside cooldown, or a probe is already in flight;
	// the caller must skip this address.
	addrCooling
)

// peek classifies the address without claiming a probe slot.
func (h *tcpAddrHealth) peek(now time.Time) tcpAddrState {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cooldownUntil.After(now) || h.probing {
		return addrCooling
	}
	if !h.cooldownUntil.IsZero() {
		return addrProbe
	}
	return addrHealthy
}

// acquire classifies the address and, when it is due for a half-open
// probe, atomically claims the single probe slot. The caller must dial
// the address right away and then call either recordSuccess or
// recordFailure, which releases the slot.
func (h *tcpAddrHealth) acquire(now time.Time) tcpAddrState {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cooldownUntil.After(now) || h.probing {
		return addrCooling
	}
	if !h.cooldownUntil.IsZero() {
		h.probing = true
		return addrProbe
	}
	return addrHealthy
}

// recordSuccess clears all failure state: a working address immediately
// receives normal traffic again.
func (h *tcpAddrHealth) recordSuccess() {
	h.mu.Lock()
	h.failures = 0
	h.cooldownUntil = time.Time{}
	h.cooldown = 0
	h.lastErr = nil
	h.probing = false
	h.mu.Unlock()
}

// recordFailure remembers the failure reason and, once the consecutive
// failure threshold is reached, (re)arms the cooldown. A failed half-open
// probe doubles the cooldown duration up to baseCooldown*maxAddrCooldownMultiplier.
func (h *tcpAddrHealth) recordFailure(now time.Time, threshold uint32, baseCooldown time.Duration, err error) {
	h.mu.Lock()
	h.failures++
	h.lastErr = err
	h.probing = false
	if h.failures >= threshold {
		switch {
		case h.cooldown <= 0:
			h.cooldown = baseCooldown
		default:
			h.cooldown *= 2
			if max := baseCooldown * maxAddrCooldownMultiplier; h.cooldown > max {
				h.cooldown = max
			}
		}
		h.cooldownUntil = now.Add(h.cooldown)
	}
	h.mu.Unlock()
}

// lastFailure returns the most recent dial failure reason, if any.
func (h *tcpAddrHealth) lastFailure() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastErr
}

// dialTracked dials resolved addresses with per-address failure tracking.
//
// Addresses are visited in the same round-robin order starting at startIdx,
// but cooling addresses (and addresses already being half-open probed by a
// concurrent dial) are skipped while the remaining addresses keep being
// tried. The overall deadline is split across the addresses still eligible
// in this round, so one address timing out does not abort the round.
//
// It returns a connection as soon as one address accepts the dial. If no
// address can be dialed it returns an error joining the most recent failure
// reason of every address; when every address is cooling down the error
// wraps ErrAllAddrsInCooldown.
func (d *TCPDialer) dialTracked(
	network string, e *tcpAddrEntry, startIdx uint32, deadline time.Time,
) (net.Conn, error) {
	addrs := e.addrs
	health := e.health
	n := len(addrs)
	if n == 0 || health == nil {
		// Should never happen; fall back to the legacy behavior.
		var conn net.Conn
		var err error
		for i := range n {
			conn, err = d.tryDial(network, addrs[(int(startIdx)+i)%n].String(), deadline, d.concurrencyCh)
			if err == nil {
				return conn, nil
			}
		}
		return nil, err
	}

	threshold := d.AddrFailureThreshold
	if threshold == 0 {
		threshold = defaultAddrFailureThreshold
	}
	baseCooldown := d.AddrCooldownDuration

	// Rotation order, identical to the legacy path: startIdx, startIdx+1, ...
	order := make([]int, n)
	for i := range n {
		order[i] = (int(startIdx) + i) % n
	}

	if time.Until(deadline) <= 0 {
		return nil, wrapDialWithUpstream(ErrDialTimeout, addrs[order[0]].String())
	}

	attempted := false
	for i, pos := range order {
		switch health[pos].acquire(time.Now()) {
		case addrHealthy, addrProbe:
			// Eligible: dial it.
		default:
			// Cooling down, or a concurrent probe is already running.
			continue
		}
		attempted = true

		// Split the remaining time across the addresses still eligible
		// from here on, so a slow/dead address cannot eat the whole
		// deadline and starve every remaining address.
		eligible := 1
		now := time.Now()
		for j := i + 1; j < n; j++ {
			if health[order[j]].peek(now) != addrCooling {
				eligible++
			}
		}
		attemptDeadline := deadline
		if remaining := time.Until(deadline); remaining > 0 {
			if split := now.Add(remaining / time.Duration(eligible)); split.Before(attemptDeadline) {
				attemptDeadline = split
			}
		}

		addr := addrs[pos].String()
		conn, err := d.tryDial(network, addr, attemptDeadline, d.concurrencyCh)
		if err == nil {
			health[pos].recordSuccess()
			return conn, nil
		}
		health[pos].recordFailure(time.Now(), threshold, baseCooldown, err)
	}

	// Nothing usable: report each address's most recent failure reason.
	errs := make([]error, 0, n)
	for _, pos := range order {
		if err := health[pos].lastFailure(); err != nil {
			errs = append(errs, err)
		}
	}
	joined := errors.Join(errs...)
	if !attempted {
		// Every address was cooling (or being probed by other dials).
		if joined != nil {
			return nil, fmt.Errorf("%w: %d address(es) unavailable; last errors: %w",
				ErrAllAddrsInCooldown, n, joined)
		}
		return nil, fmt.Errorf("%w: %d address(es) unavailable", ErrAllAddrsInCooldown, n)
	}
	if joined == nil {
		// Deadline exhausted before any failure got recorded.
		return nil, wrapDialWithUpstream(ErrDialTimeout, addrs[order[0]].String())
	}
	return nil, joined
}

// DefaultDialTimeout is timeout used by Dial and DialDualStack
// for establishing TCP connections.
const DefaultDialTimeout = 3 * time.Second

type tcpAddrEntry struct {
	resolveTime time.Time
	addrs       []net.TCPAddr
	addrsIdx    uint32

	pending int32

	// health holds one tcpAddrHealth per addrs entry when address-level
	// failure tracking is enabled (TCPDialer.AddrCooldownDuration > 0).
	// It is nil otherwise, keeping the default path allocation-free.
	health []*tcpAddrHealth
}

// DefaultDNSCacheDuration is the duration for caching resolved TCP addresses
// by Dial* functions.
const DefaultDNSCacheDuration = time.Minute

// cleanExpiredDNSEntries removes expired DNS cache entries based on DNSCacheDuration.
// This is the core cleanup logic used by both the background cleaner and manual cleanup.
func (d *TCPDialer) cleanExpiredDNSEntries() bool {
	expireDuration := 2 * d.DNSCacheDuration

	t := time.Now()
	hasEntries := false
	d.tcpAddrsMap.Range(func(k, v any) bool {
		if e, ok := v.(*tcpAddrEntry); ok && t.Sub(e.resolveTime) > expireDuration {
			d.tcpAddrsMap.Delete(k)
		} else {
			hasEntries = true
		}
		return true
	})
	return hasEntries
}

func (d *TCPDialer) startTCPAddrsClean() {
	if d.cleanerRunning.CompareAndSwap(false, true) {
		go d.tcpAddrsClean()
	}
}

func (d *TCPDialer) hasTCPAddrsEntries() bool {
	hasEntries := false
	d.tcpAddrsMap.Range(func(k, v any) bool {
		hasEntries = true
		return false
	})
	return hasEntries
}

var tcpAddrsCleanInterval = int64(time.Second)

func (d *TCPDialer) tcpAddrsClean() {
	t := time.NewTicker(time.Duration(atomic.LoadInt64(&tcpAddrsCleanInterval)))
	defer t.Stop()

	for range t.C {
		if !d.cleanExpiredDNSEntries() {
			d.cleanerRunning.Store(false)
			// A dial may store a new DNS entry while cleanerRunning is still
			// true and skip starting a cleaner. Reclaim ownership in that case.
			if !d.hasTCPAddrsEntries() {
				return
			}
			if d.cleanerRunning.CompareAndSwap(false, true) {
				continue
			}
			// Another dial already claimed ownership and started a cleaner.
			return
		}
	}
}

func (d *TCPDialer) getTCPAddrs(addr string, dualStack bool, deadline time.Time) (*tcpAddrEntry, uint32, error) {
	item, exist := d.tcpAddrsMap.Load(addr)
	e, ok := item.(*tcpAddrEntry)
	if exist && ok && e != nil && time.Since(e.resolveTime) > d.DNSCacheDuration {
		// Only let one goroutine re-resolve at a time.
		if atomic.SwapInt32(&e.pending, 1) == 0 {
			e = nil
		}
	}

	if e == nil {
		addrs, err := resolveTCPAddrs(addr, dualStack, d.Resolver, deadline)
		if err != nil {
			item, exist := d.tcpAddrsMap.Load(addr)
			e, ok = item.(*tcpAddrEntry)
			if exist && ok && e != nil {
				// Set pending to 0 so another goroutine can retry.
				atomic.StoreInt32(&e.pending, 0)
			}
			return nil, 0, err
		}

		e = &tcpAddrEntry{
			addrs:       addrs,
			resolveTime: time.Now(),
		}
		if d.AddrCooldownDuration > 0 {
			e.health = make([]*tcpAddrHealth, len(addrs))
			for i := range e.health {
				e.health[i] = &tcpAddrHealth{}
			}
		}
		d.tcpAddrsMap.Store(addr, e)
	}

	idx := atomic.AddUint32(&e.addrsIdx, 1)
	return e, idx, nil
}

func resolveTCPAddrs(addr string, dualStack bool, resolver Resolver, deadline time.Time) ([]net.TCPAddr, error) {
	host, portS, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portS)
	if err != nil {
		return nil, err
	}

	if resolver == nil {
		resolver = net.DefaultResolver
	}

	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	ipaddrs, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}

	n := len(ipaddrs)
	addrs := make([]net.TCPAddr, 0, n)
	for i := range n {
		ip := ipaddrs[i]
		if !dualStack && ip.IP.To4() == nil {
			continue
		}
		addrs = append(addrs, net.TCPAddr{
			IP:   ip.IP,
			Port: port,
			Zone: ip.Zone,
		})
	}
	if len(addrs) == 0 {
		return nil, errNoDNSEntries
	}
	return addrs, nil
}

var errNoDNSEntries = errors.New("couldn't find dns entries for the given domain: try using dual-stack dialing")
