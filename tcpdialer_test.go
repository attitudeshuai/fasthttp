package fasthttp

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// dialHook is a deterministic TCPDialer.dialConn replacement: addresses in
// failAddrs fail, addresses in blockAddrs block until their channel is
// closed (or until the dial context expires), and goodAddr is dialed for
// real against a local listener. It also records call order and the maximum
// number of concurrent dials per address.
type dialHook struct {
	mu        sync.Mutex
	calls     []string
	active    map[string]int
	maxActive map[string]int

	goodAddr string

	mu2       sync.Mutex
	failAddrs map[string]error
	block     map[string]chan struct{}

	// recoveredAddrs succeed by dialing goodAddr's listener.
	recoveredAddrs map[string]bool
}

func newDialHook(goodAddr string) *dialHook {
	return &dialHook{
		active:         map[string]int{},
		maxActive:      map[string]int{},
		goodAddr:       goodAddr,
		failAddrs:      map[string]error{},
		block:          map[string]chan struct{}{},
		recoveredAddrs: map[string]bool{},
	}
}

func (h *dialHook) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	h.mu.Lock()
	h.calls = append(h.calls, addr)
	h.active[addr]++
	if h.active[addr] > h.maxActive[addr] {
		h.maxActive[addr] = h.active[addr]
	}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		h.active[addr]--
		h.mu.Unlock()
	}()

	h.mu2.Lock()
	release := h.block[addr]
	failErr := h.failAddrs[addr]
	recovered := h.recoveredAddrs[addr]
	h.mu2.Unlock()

	if release != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
		}
	}
	if failErr != nil {
		return nil, failErr
	}
	if recovered || addr == h.goodAddr {
		var dl net.Dialer
		return dl.DialContext(ctx, network, h.goodAddr)
	}
	return nil, errors.New("dial hook: address not configured")
}

func (h *dialHook) setFail(addr string, err error) {
	h.mu2.Lock()
	h.failAddrs[addr] = err
	h.mu2.Unlock()
}

func (h *dialHook) setBlock(addr string, release chan struct{}) {
	h.mu2.Lock()
	h.block[addr] = release
	h.mu2.Unlock()
}

func (h *dialHook) setRecovered(addr string) {
	h.mu2.Lock()
	delete(h.failAddrs, addr)
	h.recoveredAddrs[addr] = true
	h.mu2.Unlock()
}

func (h *dialHook) countCalls(addr string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, a := range h.calls {
		if a == addr {
			n++
		}
	}
	return n
}

func (h *dialHook) totalCalls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.calls)
}

// newTrackedDialer builds a TCPDialer with address failure tracking and the
// given hook/static resolver. The host always resolves to ips, all using
// the listener's port; only 127.0.0.1 (the good address) is reachable.
func newTrackedDialer(host string, ips []string, hook *dialHook, threshold uint32, cooldown time.Duration) *TCPDialer {
	ipaddrs := make([]net.IPAddr, len(ips))
	for i, ip := range ips {
		ipaddrs[i] = net.IPAddr{IP: net.ParseIP(ip)}
	}
	return &TCPDialer{
		Resolver:             &staticResolver{addrs: ipaddrs},
		AddrFailureThreshold: threshold,
		AddrCooldownDuration: cooldown,
		dialConn:             hook.dial,
	}
}

func mustDialerEntry(t *testing.T, d *TCPDialer, host string) *tcpAddrEntry {
	t.Helper()
	item, ok := d.tcpAddrsMap.Load(host)
	if !ok {
		t.Fatalf("no cached DNS entry for %s", host)
	}
	e, ok := item.(*tcpAddrEntry)
	if !ok || e == nil {
		t.Fatalf("unexpected DNS entry type for %s", host)
	}
	return e
}

func healthSnapshot(h *tcpAddrHealth) (failures uint32, cooldownUntil time.Time, cooldown time.Duration, lastErrSnippet string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	snippet := ""
	if h.lastErr != nil {
		snippet = h.lastErr.Error()
	}
	return h.failures, h.cooldownUntil, h.cooldown, snippet
}

func startDrainingListener(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go c.Close()
		}
	}()
	return ln
}

// Reaching the failure threshold cools the address down: subsequent dials
// whose rotation slot points at it skip it entirely and keep succeeding via
// the healthy addresses.
func TestDialTrackedSkipsCoolingAddress(t *testing.T) {
	ln := startDrainingListener(t)
	goodAddr := ln.Addr().String()
	host := "example.com:" + goodAddr[strings.LastIndex(goodAddr, ":")+1:]
	badAddr := "10.0.0.1" + goodAddr[strings.LastIndex(goodAddr, ":"):]

	hook := newDialHook(goodAddr)
	hook.setFail(badAddr, errors.New("connection refused"))
	// pos0 is the bad address, pos1/pos2 the healthy one (duplicated IP is
	// fine: both slots hit the same listener).
	d := newTrackedDialer(host, []string{"10.0.0.1", "127.0.0.1", "127.0.0.1"}, hook, 2, 10*time.Second)

	// Rotation starts at pos1 and advances one slot per dial, so the bad
	// address is naturally reached on dials #3 and #6; without cooldown it
	// would be reached again on dial #9.
	for range 9 {
		conn, err := d.DialTimeout(host, time.Second)
		if err != nil {
			t.Fatalf("unexpected dial error: %v", err)
		}
		conn.Close()
	}

	if got := hook.countCalls(badAddr); got != 2 {
		t.Fatalf("bad address calls = %d, want 2 (third hit should be skipped while cooling)", got)
	}
	if got := hook.countCalls(goodAddr); got != 9 {
		t.Fatalf("good address calls = %d, want 9", got)
	}

	e := mustDialerEntry(t, d, host)
	failures, cooldownUntil, cooldown, _ := healthSnapshot(e.health[0])
	if failures != 2 {
		t.Fatalf("bad failures = %d, want 2", failures)
	}
	if cooldown != 10*time.Second || time.Until(cooldownUntil) <= 0 {
		t.Fatalf("bad address not cooling: cooldown=%v until=%v", cooldown, cooldownUntil)
	}
}

// When every address fails, the returned error carries the most recent
// failure of each address; once all are cooling, later dials return a
// recognizable ErrAllAddrsInCooldown and perform no network attempts.
func TestDialTrackedAllCoolingError(t *testing.T) {
	ln := startDrainingListener(t)
	listenAddr := ln.Addr().String()
	port := listenAddr[strings.LastIndex(listenAddr, ":")+1:]
	host := "example.com:" + port
	bad1, bad2 := "10.0.0.1:"+port, "10.0.0.2:"+port

	hook := newDialHook(listenAddr)
	hook.setFail(bad1, errors.New("boom-one"))
	hook.setFail(bad2, errors.New("boom-two"))
	d := newTrackedDialer(host, []string{"10.0.0.1", "10.0.0.2"}, hook, 1, time.Minute)

	_, err := d.DialTimeout(host, time.Second)
	if err == nil {
		t.Fatal("expected error when all addresses fail")
	}
	if !strings.Contains(err.Error(), bad1) || !strings.Contains(err.Error(), bad2) {
		t.Fatalf("error must mention every address, got: %v", err)
	}
	if !strings.Contains(err.Error(), "boom-one") || !strings.Contains(err.Error(), "boom-two") {
		t.Fatalf("error must carry each address failure reason, got: %v", err)
	}
	var upstreamErr *ErrDialWithUpstream
	if !errors.As(err, &upstreamErr) {
		t.Fatalf("joined error should contain *ErrDialWithUpstream, got: %v", err)
	}

	// Second round: everything is cooling.
	before := hook.totalCalls()
	_, err = d.DialTimeout(host, time.Second)
	if !errors.Is(err, ErrAllAddrsInCooldown) {
		t.Fatalf("want ErrAllAddrsInCooldown, got: %v", err)
	}
	if !strings.Contains(err.Error(), "boom-one") || !strings.Contains(err.Error(), "boom-two") {
		t.Fatalf("cooldown error should still carry last reasons, got: %v", err)
	}
	if hook.totalCalls() != before {
		t.Fatalf("cooling addresses must not be dialed, calls before=%d after=%d", before, hook.totalCalls())
	}
}

// A timing-out address no longer aborts the whole round: the remaining
// healthy address gets its share of the deadline and is dialed.
func TestDialTrackedTimeoutDoesNotAbortRound(t *testing.T) {
	ln := startDrainingListener(t)
	goodAddr := ln.Addr().String()
	port := goodAddr[strings.LastIndex(goodAddr, ":")+1:]
	host := "example.com:" + port
	badAddr := "10.0.0.1:" + port

	hook := newDialHook(goodAddr)
	hook.setBlock(badAddr, make(chan struct{})) // never released: only ctx deadline unblocks
	// First dial starts at pos1, so put the dead address there.
	d := newTrackedDialer(host, []string{"127.0.0.1", "10.0.0.1"}, hook, 3, 10*time.Second)

	start := time.Now()
	conn, err := d.DialTimeout(host, 400*time.Millisecond)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("expected success via the second address, got: %v", err)
	}
	conn.Close()
	if hook.countCalls(badAddr) != 1 || hook.countCalls(goodAddr) != 1 {
		t.Fatalf("want one call per address, got bad=%d good=%d", hook.countCalls(badAddr), hook.countCalls(goodAddr))
	}
	if elapsed >= 400*time.Millisecond {
		t.Fatalf("deadline was not split across addresses: elapsed=%v", elapsed)
	}
}

// When the cooldown expires only a single half-open probe runs; concurrent
// dials skip the probing address and keep using healthy ones instead of
// all piling onto the same bad address. A failed probe re-arms an extended
// cooldown; a later successful probe restores full traffic.
func TestDialTrackedHalfOpenProbeSingleflightAndRecovery(t *testing.T) {
	ln := startDrainingListener(t)
	goodAddr := ln.Addr().String()
	port := goodAddr[strings.LastIndex(goodAddr, ":")+1:]
	host := "example.com:" + port
	badAddr := "10.0.0.1:" + port

	const baseCooldown = 60 * time.Millisecond
	hook := newDialHook(goodAddr)
	hook.setFail(badAddr, errors.New("still dead"))
	d := newTrackedDialer(host, []string{"10.0.0.1", "127.0.0.1"}, hook, 1, baseCooldown)

	// Arm the cooldown. Rotation starts at pos1 (the good address), so two
	// dials are needed before the bad address at pos0 gets hit.
	for range 2 {
		conn, err := d.DialTimeout(host, time.Second)
		if err != nil {
			t.Fatalf("arming dial: %v", err)
		}
		conn.Close()
	}
	e := mustDialerEntry(t, d, host)
	if _, until, cd, _ := healthSnapshot(e.health[0]); cd != baseCooldown || time.Until(until) <= 0 {
		t.Fatalf("bad address should be cooling for %v, got cd=%v until=%v", baseCooldown, cd, until)
	}

	// Wait for cooldown expiry, then block the probe and fire a concurrent wave.
	time.Sleep(baseCooldown + 40*time.Millisecond)
	release := make(chan struct{})
	hook.setBlock(badAddr, release)

	const goroutines = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(goroutines)
	errs := make([]error, goroutines)
	for i := range goroutines {
		go func(i int) {
			defer wg.Done()
			<-start
			c, err := d.DialTimeout(host, 5*time.Second)
			if c != nil {
				c.Close()
			}
			errs[i] = err
		}(i)
	}
	close(start)

	// Wait for exactly one in-flight probe of the bad address.
	deadline := time.Now().Add(2 * time.Second)
	for {
		hook.mu.Lock()
		maxBad := hook.maxActive[badAddr]
		badCalls := hook.countCallsLocked(badAddr)
		hook.mu.Unlock()
		if badCalls >= 1 {
			if maxBad != 1 {
				close(release)
				t.Fatalf("concurrent probes of the same bad address: max in-flight = %d, want 1", maxBad)
			}
			break
		}
		if time.Now().After(deadline) {
			close(release)
			t.Fatal("half-open probe never started")
		}
		time.Sleep(time.Millisecond)
	}

	// Give the other goroutines a moment to route around the probe, then let
	// the probe fail.
	time.Sleep(30 * time.Millisecond)
	close(release)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d should succeed via the healthy address: %v", i, err)
		}
	}
	if got := hook.countCalls(badAddr); got != 2 {
		t.Fatalf("bad address calls = %d, want 2 (arming failure + one probe)", got)
	}
	hook.mu.Lock()
	maxBad := hook.maxActive[badAddr]
	hook.mu.Unlock()
	if maxBad != 1 {
		t.Fatalf("max concurrent bad-address dials = %d, want 1", maxBad)
	}

	// Failed probe must extend the cooldown (doubled), not ban forever.
	_, until, cd, _ := healthSnapshot(e.health[0])
	if cd != 2*baseCooldown {
		t.Fatalf("cooldown after failed probe = %v, want %v", cd, 2*baseCooldown)
	}
	if time.Until(until) <= 0 {
		t.Fatal("failed probe did not re-arm the cooldown")
	}

	// After the extended cooldown, a successful probe restores the address.
	// Only every other dial rotates into the bad address's slot, so try a
	// couple of times.
	time.Sleep(cd + 40*time.Millisecond)
	hook.setRecovered(badAddr)
	recovered := false
	for range 4 {
		conn, err := d.DialTimeout(host, time.Second)
		if err != nil {
			t.Fatalf("recovery probe dial: %v", err)
		}
		conn.Close()
		failures, until2, cd2, _ := healthSnapshot(e.health[0])
		if failures == 0 && cd2 == 0 && until2.IsZero() {
			recovered = true
			break
		}
	}
	if !recovered {
		t.Fatal("address was not restored after a successful half-open probe")
	}

	// Restored address receives normal traffic again: a later dial whose
	// rotation slot points at it dials it as a healthy candidate.
	before := hook.countCalls(badAddr)
	found := false
	for range 4 {
		c, err := d.DialTimeout(host, time.Second)
		if err != nil {
			t.Fatalf("post-recovery dial: %v", err)
		}
		c.Close()
		if hook.countCalls(badAddr) > before {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("restored address was never dialed as a healthy candidate")
	}
}

func (h *dialHook) countCallsLocked(addr string) int {
	n := 0
	for _, a := range h.calls {
		if a == addr {
			n++
		}
	}
	return n
}

// With default options (no cooldown configured) behavior stays historical:
// rotation starts at idx, and the first dial-timeout aborts the whole round
// without touching remaining addresses.
func TestDialDefaultBehaviorUnchanged(t *testing.T) {
	ln := startDrainingListener(t)
	goodAddr := ln.Addr().String()
	port := goodAddr[strings.LastIndex(goodAddr, ":")+1:]
	host := "example.com:" + port
	badAddr := "10.0.0.1:" + port

	hook := newDialHook(goodAddr)
	hook.setBlock(badAddr, make(chan struct{}))
	d := &TCPDialer{
		Resolver: &staticResolver{addrs: []net.IPAddr{
			{IP: net.ParseIP("127.0.0.1")},
			{IP: net.ParseIP("10.0.0.1")}, // first dial starts at this slot (pos1)
		}},
		dialConn: hook.dial,
	}

	_, err := d.DialTimeout(host, 200*time.Millisecond)
	if !errors.Is(err, ErrDialTimeout) {
		t.Fatalf("want ErrDialTimeout on first-address timeout, got: %v", err)
	}
	if got := hook.countCalls(badAddr); got != 1 {
		t.Fatalf("bad address calls = %d, want 1", got)
	}
	if got := hook.countCalls(goodAddr); got != 0 {
		t.Fatalf("remaining addresses must not be tried after timeout in default mode, good calls = %d", got)
	}
}
