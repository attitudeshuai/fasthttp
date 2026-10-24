package fasthttp

import (
	"bufio"
	"fmt"
	"math"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/fasthttp/fasthttputil"
)

// startRateLimitTestServer serves ln with a handler that counts how many
// times it actually ran. The returned channel receives Serve's result.
func startRateLimitTestServer(t *testing.T, cfg *RateLimitConfig, served *atomic.Int64) (*fasthttputil.InmemoryListener, <-chan error) {
	t.Helper()

	ln := fasthttputil.NewInmemoryListener()
	s := &Server{
		Handler: func(ctx *RequestCtx) {
			served.Add(1)
			ctx.SetStatusCode(StatusOK)
		},
		RateLimit: cfg,
		Logger:    &testLogger{},
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- s.Serve(ln)
	}()
	return ln, serveErr
}

// rateLimitClient is one keep-alive connection whose remote address looks
// like the given source IP, so the server keys it on that source.
type rateLimitClient struct {
	c  net.Conn
	br *bufio.Reader
}

func dialRateLimitClient(t *testing.T, ln *fasthttputil.InmemoryListener, sourceIP string) *rateLimitClient {
	t.Helper()

	c, err := ln.DialWithLocalAddr(&net.TCPAddr{IP: net.ParseIP(sourceIP)})
	if err != nil {
		t.Fatalf("dial from %s: %v", sourceIP, err)
	}
	return &rateLimitClient{c: c, br: bufio.NewReader(c)}
}

func (rc *rateLimitClient) close() {
	_ = rc.c.Close()
}

func (rc *rateLimitClient) get(t *testing.T, path string) *Response {
	t.Helper()

	if _, err := fmt.Fprintf(rc.c, "GET %s HTTP/1.1\r\nHost: example.com\r\n\r\n", path); err != nil {
		t.Fatalf("write request: %v", err)
	}
	var resp Response
	if err := resp.Read(rc.br); err != nil {
		t.Fatalf("read response: %v", err)
	}
	return &resp
}

func TestRateLimitPolicyValidate(t *testing.T) {
	t.Parallel()

	valid := []RateLimitPolicy{
		{Rate: 1, Burst: 1},
		{Rate: 0.001, Burst: 1000},
		{Rate: 1e9, Burst: 1},
	}
	for _, p := range valid {
		if err := p.Validate(); err != nil {
			t.Errorf("policy %+v unexpectedly rejected: %v", p, err)
		}
	}

	invalid := []RateLimitPolicy{
		{Rate: 0, Burst: 1},
		{Rate: -1, Burst: 1},
		{Rate: 1, Burst: 0},
		{Rate: 1, Burst: -3},
		{Rate: math.NaN(), Burst: 1},
		{Rate: math.Inf(1), Burst: 1},
		{Rate: math.Inf(-1), Burst: 1},
	}
	for _, p := range invalid {
		if err := p.Validate(); err == nil {
			t.Errorf("policy %+v unexpectedly accepted", p)
		}
	}

	var nilCfg *RateLimitConfig
	if err := nilCfg.Validate(); err != nil {
		t.Errorf("nil config must be valid (means disabled): %v", err)
	}

	badConfigs := []*RateLimitConfig{
		{}, // zero values must not silently become a zero allowance
		{Rate: 10, Burst: 0},
		{Rate: 0, Burst: 10},
		{Rate: math.NaN(), Burst: 10},
		{Rate: 10, Burst: 10, PerPath: map[string]RateLimitPolicy{"": {Rate: 1, Burst: 1}}},
		{Rate: 10, Burst: 10, PerPath: map[string]RateLimitPolicy{"/x": {Rate: 0, Burst: 1}}},
		{Rate: 10, Burst: 10, PerPath: map[string]RateLimitPolicy{"/x": {Rate: 1, Burst: 0}}},
	}
	for _, cfg := range badConfigs {
		if err := cfg.Validate(); err == nil {
			t.Errorf("config %+v unexpectedly accepted", cfg)
		}
	}
}

func TestRateLimitInvalidConfigServedOnTheSpot(t *testing.T) {
	t.Parallel()

	t.Run("Serve", func(t *testing.T) {
		ln := fasthttputil.NewInmemoryListener()
		s := &Server{
			Handler:   func(ctx *RequestCtx) {},
			RateLimit: &RateLimitConfig{Rate: 1, Burst: 0},
			Logger:    &testLogger{},
		}
		err := s.Serve(ln)
		if err == nil {
			t.Fatal("Serve with an invalid rate limit config must return an error")
		}
		_ = ln.Close()
	})

	t.Run("ServeConn", func(t *testing.T) {
		s := &Server{
			Handler:   func(ctx *RequestCtx) {},
			RateLimit: &RateLimitConfig{},
			Logger:    &testLogger{},
		}
		c1, c2 := net.Pipe()
		defer c1.Close()
		defer c2.Close()
		if err := s.ServeConn(c1); err == nil {
			t.Fatal("ServeConn with an invalid rate limit config must return an error")
		}
	})
}

func TestRateLimitDisabledByDefault(t *testing.T) {
	t.Parallel()

	var served atomic.Int64
	ln, serveErr := startRateLimitTestServer(t, nil, &served)
	defer ln.Close()

	rc := dialRateLimitClient(t, ln, "10.0.0.1")
	defer rc.close()

	for i := 0; i < 10; i++ {
		resp := rc.get(t, "/")
		if resp.StatusCode() != StatusOK {
			t.Fatalf("request %d: status %d, want %d", i, resp.StatusCode(), StatusOK)
		}
	}
	if served.Load() != 10 {
		t.Fatalf("handler ran %d times, want 10", served.Load())
	}

	if got := (&Server{}).GetRateLimitRejectedCount(); got != 0 {
		t.Fatalf("disabled limiter rejection count = %d, want 0", got)
	}
	if counts := (&Server{}).GetRateLimitRejectedCounts(); counts != nil {
		t.Fatalf("disabled limiter rejection counts = %v, want nil", counts)
	}

	_ = ln.Close()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(time.Second):
	}
}

func TestRateLimitBurstRejectAndKeepAlive(t *testing.T) {
	t.Parallel()

	var served atomic.Int64
	cfg := &RateLimitConfig{Rate: 0.0001, Burst: 3}
	ln, serveErr := startRateLimitTestServer(t, cfg, &served)
	defer ln.Close()

	rc := dialRateLimitClient(t, ln, "10.0.0.1")
	defer rc.close()

	// The whole burst goes through on a single keep-alive connection.
	for i := 0; i < cfg.Burst; i++ {
		if resp := rc.get(t, "/"); resp.StatusCode() != StatusOK {
			t.Fatalf("burst request %d: status %d, want %d", i, resp.StatusCode(), StatusOK)
		}
	}

	// The next request is rejected on its own, with a retry hint.
	resp := rc.get(t, "/")
	if resp.StatusCode() != StatusTooManyRequests {
		t.Fatalf("over-burst status %d, want %d", resp.StatusCode(), StatusTooManyRequests)
	}
	retryAfter := string(resp.Header.Peek(HeaderRetryAfter))
	secs, err := strconv.Atoi(retryAfter)
	if err != nil || secs < 1 {
		t.Fatalf("Retry-After = %q, want an integer >= 1", retryAfter)
	}
	if resp.Header.ConnectionClose() {
		t.Fatal("rejected request must not close the connection")
	}

	// The connection is still alive: the following request is answered too.
	if resp := rc.get(t, "/"); resp.StatusCode() != StatusTooManyRequests {
		t.Fatalf("second over-burst status %d, want %d", resp.StatusCode(), StatusTooManyRequests)
	}

	if served.Load() != int64(cfg.Burst) {
		t.Fatalf("handler ran %d times, want %d", served.Load(), cfg.Burst)
	}

	_ = ln.Close()
	select {
	case <-serveErr:
	case <-time.After(time.Second):
	}
}

func TestRateLimitSmoothRefill(t *testing.T) {
	t.Parallel()

	var served atomic.Int64
	// 200 tokens/s, bank of 2: one token accrues every 5ms.
	cfg := &RateLimitConfig{Rate: 200, Burst: 2}
	ln, serveErr := startRateLimitTestServer(t, cfg, &served)
	defer ln.Close()

	rc := dialRateLimitClient(t, ln, "10.0.0.1")
	defer rc.close()

	if resp := rc.get(t, "/"); resp.StatusCode() != StatusOK {
		t.Fatalf("first status %d, want 200", resp.StatusCode())
	}
	if resp := rc.get(t, "/"); resp.StatusCode() != StatusOK {
		t.Fatalf("second status %d, want 200", resp.StatusCode())
	}
	if resp := rc.get(t, "/"); resp.StatusCode() != StatusTooManyRequests {
		t.Fatalf("third status %d, want 429", resp.StatusCode())
	}

	// Wait long enough for the bank to refill smoothly (100ms >= 20 tokens,
	// capped at Burst=2), then two requests pass again.
	time.Sleep(100 * time.Millisecond)

	if resp := rc.get(t, "/"); resp.StatusCode() != StatusOK {
		t.Fatalf("post-refill status %d, want 200", resp.StatusCode())
	}
	if resp := rc.get(t, "/"); resp.StatusCode() != StatusOK {
		t.Fatalf("post-refill status %d, want 200", resp.StatusCode())
	}
	if resp := rc.get(t, "/"); resp.StatusCode() != StatusTooManyRequests {
		t.Fatalf("post-refill status %d, want 429", resp.StatusCode())
	}

	_ = ln.Close()
	select {
	case <-serveErr:
	case <-time.After(time.Second):
	}
}

func TestRateLimitSharedAndAtomicAcrossConnections(t *testing.T) {
	t.Parallel()

	var served atomic.Int64
	const burst = 50
	// Effectively no refill during the test: any admit over the burst would
	// prove the check-and-deduct was not atomic.
	cfg := &RateLimitConfig{Rate: 0.0001, Burst: burst}
	srv := &Server{
		Handler: func(ctx *RequestCtx) {
			served.Add(1)
			ctx.SetStatusCode(StatusOK)
		},
		RateLimit: cfg,
		Logger:    &testLogger{},
	}
	ln := fasthttputil.NewInmemoryListener()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ln)
	}()
	defer ln.Close()

	const clients = burst * 4
	var wg sync.WaitGroup
	statuses := make(chan int, clients)
	start := make(chan struct{})
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Every connection reports the same source IP.
			rc := dialRateLimitClient(t, ln, "10.0.0.1")
			defer rc.close()
			<-start
			statuses <- rc.get(t, "/").StatusCode()
		}()
	}
	close(start)
	wg.Wait()
	close(statuses)

	var admitted, rejected int
	for code := range statuses {
		switch code {
		case StatusOK:
			admitted++
		case StatusTooManyRequests:
			rejected++
		default:
			t.Fatalf("unexpected status %d", code)
		}
	}
	if admitted != burst {
		t.Fatalf("admitted %d requests from one source over many connections, want exactly %d", admitted, burst)
	}
	if admitted+rejected != clients {
		t.Fatalf("accounted %d of %d responses", admitted+rejected, clients)
	}
	if served.Load() != burst {
		t.Fatalf("handler ran %d times, want %d", served.Load(), burst)
	}
	if got := srv.GetRateLimitRejectedCount(); got != clients-burst {
		t.Fatalf("rejection count %d, want %d", got, clients-burst)
	}

	_ = ln.Close()
	select {
	case <-serveErr:
	case <-time.After(time.Second):
	}
}

func TestRateLimitOtherSourcesUnaffected(t *testing.T) {
	t.Parallel()

	var served atomic.Int64
	cfg := &RateLimitConfig{Rate: 0.0001, Burst: 2}
	srv := &Server{
		Handler: func(ctx *RequestCtx) {
			served.Add(1)
			ctx.SetStatusCode(StatusOK)
		},
		RateLimit: cfg,
		Logger:    &testLogger{},
	}
	ln := fasthttputil.NewInmemoryListener()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ln)
	}()
	defer ln.Close()

	a := dialRateLimitClient(t, ln, "10.0.0.1")
	defer a.close()
	b := dialRateLimitClient(t, ln, "10.0.0.2")
	defer b.close()

	// Exhaust source A.
	if resp := a.get(t, "/"); resp.StatusCode() != StatusOK {
		t.Fatalf("A1 status %d, want 200", resp.StatusCode())
	}
	if resp := a.get(t, "/"); resp.StatusCode() != StatusOK {
		t.Fatalf("A2 status %d, want 200", resp.StatusCode())
	}
	if resp := a.get(t, "/"); resp.StatusCode() != StatusTooManyRequests {
		t.Fatalf("A3 status %d, want 429", resp.StatusCode())
	}

	// Source B has its own untouched allowance.
	for i := 0; i < cfg.Burst; i++ {
		if resp := b.get(t, "/"); resp.StatusCode() != StatusOK {
			t.Fatalf("B request %d: status %d, want 200", i, resp.StatusCode())
		}
	}
	if resp := b.get(t, "/"); resp.StatusCode() != StatusTooManyRequests {
		t.Fatalf("B over-burst status %d, want 429", resp.StatusCode())
	}

	// A is still limited; B being limited did not reset it.
	if resp := a.get(t, "/"); resp.StatusCode() != StatusTooManyRequests {
		t.Fatalf("A again status %d, want 429", resp.StatusCode())
	}

	if served.Load() != int64(cfg.Burst*2) {
		t.Fatalf("handler ran %d times, want %d", served.Load(), cfg.Burst*2)
	}

	_ = ln.Close()
	select {
	case <-serveErr:
	case <-time.After(time.Second):
	}
}

func TestRateLimitPerPathOverrides(t *testing.T) {
	t.Parallel()

	var served atomic.Int64
	cfg := &RateLimitConfig{
		Rate:  0.0001,
		Burst: 2,
		PerPath: map[string]RateLimitPolicy{
			"/relaxed": {Rate: 0.0001, Burst: 5},
			"/tight":   {Rate: 0.0001, Burst: 1},
		},
	}
	srv := &Server{
		Handler: func(ctx *RequestCtx) {
			served.Add(1)
			ctx.SetStatusCode(StatusOK)
		},
		RateLimit: cfg,
		Logger:    &testLogger{},
	}
	ln := fasthttputil.NewInmemoryListener()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ln)
	}()
	defer ln.Close()

	rc := dialRateLimitClient(t, ln, "10.0.0.1")
	defer rc.close()

	// Default bucket: two admits, then rejects on every non-overridden path.
	expectStatus := func(path string, want int) {
		t.Helper()
		if resp := rc.get(t, path); resp.StatusCode() != want {
			t.Fatalf("%s status %d, want %d", path, resp.StatusCode(), want)
		}
	}
	expectStatus("/", StatusOK)
	expectStatus("/other", StatusOK)
	expectStatus("/", StatusTooManyRequests)
	expectStatus("/other", StatusTooManyRequests)

	// Relaxed path keeps an independent, larger bucket.
	for i := 0; i < 5; i++ {
		expectStatus("/relaxed", StatusOK)
	}
	expectStatus("/relaxed", StatusTooManyRequests)

	// Tight path keeps an independent, smaller bucket.
	expectStatus("/tight", StatusOK)
	expectStatus("/tight", StatusTooManyRequests)

	// Rejections are recorded separately by reason.
	counts := srv.GetRateLimitRejectedCounts()
	if counts[RateLimitRejectionReasonDefault] != 2 {
		t.Errorf("default rejections = %d, want 2", counts[RateLimitRejectionReasonDefault])
	}
	if counts["/relaxed"] != 1 {
		t.Errorf("/relaxed rejections = %d, want 1", counts["/relaxed"])
	}
	if counts["/tight"] != 1 {
		t.Errorf("/tight rejections = %d, want 1", counts["/tight"])
	}
	if total := srv.GetRateLimitRejectedCount(); total != 4 {
		t.Errorf("total rejections = %d, want 4", total)
	}
	if served.Load() != 8 {
		t.Errorf("handler ran %d times, want 8", served.Load())
	}

	_ = ln.Close()
	select {
	case <-serveErr:
	case <-time.After(time.Second):
	}
}
