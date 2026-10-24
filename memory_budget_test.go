package fasthttp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/valyala/fasthttp/fasthttputil"
)

// startTestServer runs s on a fresh in-memory listener and returns it.
func startTestServer(t *testing.T, s *Server) *fasthttputil.InmemoryListener {
	t.Helper()
	ln := fasthttputil.NewInmemoryListener()
	go func() {
		_ = s.Serve(ln)
	}()
	t.Cleanup(func() {
		ln.Close()
	})
	return ln
}

// rawRoundTrip writes req on a fresh connection and parses one response.
func rawRoundTrip(t *testing.T, ln *fasthttputil.InmemoryListener, req string) (*Response, error) {
	t.Helper()
	c, err := ln.Dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write([]byte(req)); err != nil {
		return nil, err
	}
	var resp Response
	if err := resp.Read(bufio.NewReader(c)); err != nil {
		return nil, err
	}
	return &resp, nil
}

func fixedPostRequest(body []byte) string {
	return fmt.Sprintf("POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
}

func waitBudgetUsage(t *testing.T, s *Server, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s.GetMemoryBudgetUsage() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("budget usage = %d, want %d (rejected so far: %d)",
		s.GetMemoryBudgetUsage(), want, s.GetMemoryBudgetRejectedCount())
}

// withSmallBodyPoolLimit makes request/response body buffers return to the GC
// instead of being retained, so body growth charges in a test are
// deterministic regardless of what other tests left in the package pools.
func withSmallBodyPoolLimit(t *testing.T) {
	SetBodySizePoolLimit(0, 0)
	t.Cleanup(func() {
		SetBodySizePoolLimit(-1, -1)
	})
}

func echoHandler(ctx *RequestCtx) {
	_, _ = ctx.Write(ctx.PostBody())
}

// Without a configured budget everything behaves as before and no usage is
// ever reported.
func TestMemoryBudgetDisabled(t *testing.T) {
	s := &Server{Handler: echoHandler}
	ln := startTestServer(t, s)

	body := bytes.Repeat([]byte("A"), 100*1024)
	resp, err := rawRoundTrip(t, ln, fixedPostRequest(body))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode() != StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode())
	}
	if !bytes.Equal(resp.Body(), body) {
		t.Fatalf("body mismatch")
	}
	if s.GetMemoryBudgetLimit() != 0 || s.GetMemoryBudgetUsage() != 0 || s.GetMemoryBudgetRejectedCount() != 0 {
		t.Fatalf("budget must be inactive, got limit=%d usage=%d rejected=%d",
			s.GetMemoryBudgetLimit(), s.GetMemoryBudgetUsage(), s.GetMemoryBudgetRejectedCount())
	}
}

// A fixed-length body whose growth does not fit the budget is rejected before
// the buffer is allocated, with a recognizable 503, and everything reserved is
// released afterwards.
func TestMemoryBudgetBodyDenial(t *testing.T) {
	withSmallBodyPoolLimit(t)

	s := &Server{
		Handler:            echoHandler,
		MemoryBudgetBytes:  64 * 1024,
		MaxRequestBodySize: 4 * 1024 * 1024,
		ErrorHandler: func(ctx *RequestCtx, err error) {
			if !errors.Is(err, ErrMemoryBudgetExceeded) {
				t.Errorf("error handler got %v, want ErrMemoryBudgetExceeded", err)
			}
			ctx.Error("budget", StatusServiceUnavailable)
		},
	}
	ln := startTestServer(t, s)

	rejectedBefore := s.GetMemoryBudgetRejectedCount()
	body := bytes.Repeat([]byte("B"), 200*1024)
	resp, err := rawRoundTrip(t, ln, fixedPostRequest(body))
	if err != nil {
		t.Fatalf("expected an HTTP rejection response, got read error: %v", err)
	}
	if resp.StatusCode() != StatusServiceUnavailable {
		t.Fatalf("unexpected status: %d, want 503", resp.StatusCode())
	}
	if s.GetMemoryBudgetRejectedCount() <= rejectedBefore {
		t.Fatalf("rejection counter not incremented: %d", rejectedBefore)
	}
	waitBudgetUsage(t, s, 0)
}

// While a handler is in flight the body bytes it buffered are reserved; the
// reservation is gone as soon as the request finishes.
func TestMemoryBudgetInFlightUsage(t *testing.T) {
	withSmallBodyPoolLimit(t)

	const bodySize = 128 * 1024 // power of two, exact grow size from cap 0
	started := make(chan struct{})
	release := make(chan struct{})
	s := &Server{
		MemoryBudgetBytes: 4 * 1024 * 1024,
		Handler: func(ctx *RequestCtx) {
			if len(ctx.PostBody()) != bodySize {
				t.Errorf("unexpected body size %d", len(ctx.PostBody()))
			}
			close(started)
			<-release
			ctx.SetStatusCode(StatusOK)
		},
	}
	ln := startTestServer(t, s)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := rawRoundTrip(t, ln, fixedPostRequest(bytes.Repeat([]byte("C"), bodySize)))
		if err != nil {
			t.Errorf("round trip: %v", err)
		}
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	if usage := s.GetMemoryBudgetUsage(); usage < bodySize {
		t.Fatalf("in-flight usage = %d, want at least %d", usage, bodySize)
	}
	if usage := s.GetMemoryBudgetUsage(); usage > s.GetMemoryBudgetLimit() {
		t.Fatalf("usage %d exceeds limit %d", usage, s.GetMemoryBudgetLimit())
	}

	close(release)
	<-done
	waitBudgetUsage(t, s, 0)
}

// Under a concurrent wave of big bodies some requests are admitted and the
// rest get the recognizable rejection; the usage never exceeds the limit and
// falls to zero afterwards, with the server serving traffic again without a
// restart.
func TestMemoryBudgetConcurrentRejection(t *testing.T) {
	withSmallBodyPoolLimit(t)

	const (
		bodySize = 512 * 1024
		clients  = 40
		budget   = 600 * 1024
	)
	s := &Server{
		Handler:            echoHandler,
		MemoryBudgetBytes:  budget,
		MaxRequestBodySize: 4 * 1024 * 1024,
	}
	ln := startTestServer(t, s)

	var (
		wg          sync.WaitGroup
		ok200       int
		denied      int
		other       int
		maxObserved int64
		mu          sync.Mutex
	)
	stopSampling := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopSampling:
				return
			default:
				u := s.GetMemoryBudgetUsage()
				mu.Lock()
				if u > maxObserved {
					maxObserved = u
				}
				mu.Unlock()
				time.Sleep(time.Millisecond)
			}
		}
	}()

	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func() {
			defer wg.Done()
			c, err := ln.Dial()
			if err != nil {
				return
			}
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(10 * time.Second))
			req := fixedPostRequest(bytes.Repeat([]byte("D"), bodySize))
			go func() {
				_, _ = c.Write([]byte(req)) // may error once the server closes
			}()
			var resp Response
			if err := resp.Read(bufio.NewReader(c)); err != nil {
				// Connection closed while rejecting; counts as denial.
				mu.Lock()
				denied++
				mu.Unlock()
				return
			}
			mu.Lock()
			switch resp.StatusCode() {
			case StatusOK:
				ok200++
			case StatusServiceUnavailable:
				denied++
			default:
				other++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(stopSampling)

	if ok200 == 0 {
		t.Fatalf("expected at least one admitted request")
	}
	if denied == 0 {
		t.Fatalf("expected at least one budget rejection")
	}
	if other != 0 {
		t.Fatalf("got %d responses that were neither 200 nor 503", other)
	}
	if maxObserved > budget {
		t.Fatalf("observed usage %d exceeded budget %d", maxObserved, budget)
	}
	if s.GetMemoryBudgetRejectedCount() == 0 {
		t.Fatalf("rejection counter stayed zero")
	}
	waitBudgetUsage(t, s, 0)

	// The server keeps serving after the wave without a restart.
	resp, err := rawRoundTrip(t, ln, "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
	if err != nil {
		t.Fatalf("server did not recover: %v", err)
	}
	if resp.StatusCode() != StatusOK {
		t.Fatalf("post-wave request status = %d", resp.StatusCode())
	}
	waitBudgetUsage(t, s, 0)
}

// A chunked body whose chunks exceed the budget mid-stream is rejected at the
// grow point, not after buffering everything.
func TestMemoryBudgetChunkedDenial(t *testing.T) {
	withSmallBodyPoolLimit(t)

	s := &Server{
		Handler:            echoHandler,
		MemoryBudgetBytes:  64 * 1024,
		MaxRequestBodySize: 4 * 1024 * 1024,
	}
	ln := startTestServer(t, s)

	var req bytes.Buffer
	req.WriteString("POST / HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: chunked\r\n\r\n")
	for i := 0; i < 6; i++ {
		chunk := bytes.Repeat([]byte("E"), 40*1024)
		fmt.Fprintf(&req, "%x\r\n", len(chunk))
		req.Write(chunk)
		req.WriteString("\r\n")
	}
	req.WriteString("0\r\n\r\n")

	resp, err := rawRoundTrip(t, ln, req.String())
	if err != nil {
		t.Fatalf("expected rejection response, got %v", err)
	}
	if resp.StatusCode() != StatusServiceUnavailable {
		t.Fatalf("unexpected status: %d, want 503", resp.StatusCode())
	}
	waitBudgetUsage(t, s, 0)
}

// When even the 4KiB response write buffer cannot be reserved, the request is
// answered with a directly written 503 and the counter moves.
func TestMemoryBudgetWriterDenial(t *testing.T) {
	s := &Server{
		Handler:            echoHandler,
		MemoryBudgetBytes:  100, // below the default 4096 byte write buffer
		MaxRequestBodySize: 4 * 1024 * 1024,
	}
	ln := startTestServer(t, s)

	resp, err := rawRoundTrip(t, ln, "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
	if err != nil {
		t.Fatalf("expected direct rejection response, got %v", err)
	}
	if resp.StatusCode() != StatusServiceUnavailable {
		t.Fatalf("unexpected status: %d, want 503", resp.StatusCode())
	}
	if s.GetMemoryBudgetRejectedCount() == 0 {
		t.Fatalf("rejection counter stayed zero")
	}
	waitBudgetUsage(t, s, 0)
}

// Expect: 100-continue requests are rejected before the 100 interim response
// and before the body is read.
func TestMemoryBudgetExpectContinueDenial(t *testing.T) {
	s := &Server{
		Handler:            echoHandler,
		MemoryBudgetBytes:  100,
		MaxRequestBodySize: 4 * 1024 * 1024,
	}
	ln := startTestServer(t, s)

	c, err := ln.Dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))

	head := "POST /upload HTTP/1.1\r\nHost: example.com\r\nContent-Length: 4096\r\nExpect: 100-continue\r\n\r\n"
	if _, err := c.Write([]byte(head)); err != nil {
		t.Fatalf("write head: %v", err)
	}
	// The client may be blocked sending the body while the server rejects.
	go func() {
		_, _ = c.Write(make([]byte, 4096))
	}()

	var resp Response
	if err := resp.Read(bufio.NewReader(c)); err != nil {
		t.Fatalf("expected rejection response, got %v", err)
	}
	if resp.StatusCode() != StatusServiceUnavailable {
		t.Fatalf("unexpected status: %d, want 503", resp.StatusCode())
	}
	waitBudgetUsage(t, s, 0)
}

// After a normal keep-alive exchange the reservation is returned even while
// the connection stays open, and it stays at zero across more requests.
func TestMemoryBudgetReleaseNormalPath(t *testing.T) {
	withSmallBodyPoolLimit(t)

	s := &Server{
		Handler:            echoHandler,
		MemoryBudgetBytes:  4 * 1024 * 1024,
		MaxRequestBodySize: 4 * 1024 * 1024,
	}
	ln := startTestServer(t, s)

	c, err := ln.Dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(c)

	for i := 0; i < 2; i++ {
		req := fixedPostRequest(bytes.Repeat([]byte("F"), 64*1024))
		if _, err := c.Write([]byte(req)); err != nil {
			t.Fatalf("write: %v", err)
		}
		var resp Response
		if err := resp.Read(br); err != nil {
			t.Fatalf("read response: %v", err)
		}
		if resp.StatusCode() != StatusOK {
			t.Fatalf("unexpected status: %d", resp.StatusCode())
		}
		// The connection is idle now; its quota must already be back.
		waitBudgetUsage(t, s, 0)
	}
}

// A handler that returns early without reading a streamed body: the server
// drains what it can, closes the connection, and still returns every byte.
func TestMemoryBudgetReleaseEarlyReturn(t *testing.T) {
	s := &Server{
		Handler: func(ctx *RequestCtx) {
			// Deliberately do not touch ctx.Request.BodyStream().
			ctx.SetStatusCode(StatusOK)
		},
		StreamRequestBody:  true,
		MemoryBudgetBytes:  4 * 1024 * 1024,
		MaxRequestBodySize: 4 * 1024 * 1024,
		DisableKeepalive:   true,
	}
	ln := startTestServer(t, s)

	c, err := ln.Dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))

	body := bytes.Repeat([]byte("G"), 1024*1024)
	go func() {
		_, _ = c.Write([]byte(fixedPostRequest(body)))
	}()

	var resp Response
	if err := resp.Read(bufio.NewReader(c)); err != nil {
		t.Fatalf("expected a response before close, got %v", err)
	}
	if resp.StatusCode() != StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode())
	}
	waitBudgetUsage(t, s, 0)
}

// Hijacking must release the request's reservation once the hijack handler
// returns and the context is put back.
func TestMemoryBudgetReleaseHijack(t *testing.T) {
	withSmallBodyPoolLimit(t)

	hijackDone := make(chan struct{})
	s := &Server{
		MemoryBudgetBytes:  4 * 1024 * 1024,
		MaxRequestBodySize: 4 * 1024 * 1024,
		Handler: func(ctx *RequestCtx) {
			ctx.Hijack(func(net.Conn) {
				close(hijackDone)
			})
		},
	}
	ln := startTestServer(t, s)

	c, err := ln.Dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))

	if _, err := c.Write([]byte(fixedPostRequest(bytes.Repeat([]byte("H"), 64*1024)))); err != nil {
		t.Fatalf("write: %v", err)
	}
	var resp Response
	if err := resp.Read(bufio.NewReader(c)); err != nil {
		t.Fatalf("read hijack response: %v", err)
	}
	if resp.StatusCode() != StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode())
	}
	select {
	case <-hijackDone:
	case <-time.After(5 * time.Second):
		t.Fatal("hijack handler did not run")
	}
	waitBudgetUsage(t, s, 0)
}

// With ReduceMemoryUsage the keep-alive first-byte wait swaps the ctx before
// every request; the budget account must be rebound so body growth on later
// requests is still metered.
func TestMemoryBudgetReduceMemoryUsageKeepalive(t *testing.T) {
	withSmallBodyPoolLimit(t)

	const bodySize = 64 * 1024
	var (
		mu     sync.Mutex
		gates  = map[uint64]chan struct{}{}
		starts = map[uint64]chan struct{}{}
	)
	getGate := func(n uint64) (chan struct{}, chan struct{}) {
		mu.Lock()
		defer mu.Unlock()
		if gates[n] == nil {
			gates[n] = make(chan struct{})
			starts[n] = make(chan struct{})
		}
		return starts[n], gates[n]
	}
	s := &Server{
		ReduceMemoryUsage:  true,
		MemoryBudgetBytes:  4 * 1024 * 1024,
		MaxRequestBodySize: 4 * 1024 * 1024,
		Handler: func(ctx *RequestCtx) {
			n := ctx.ConnRequestNum()
			if len(ctx.PostBody()) != bodySize {
				t.Errorf("request %d unexpected body size %d", n, len(ctx.PostBody()))
			}
			started, release := getGate(n)
			close(started)
			<-release
			ctx.SetStatusCode(StatusOK)
		},
	}
	ln := startTestServer(t, s)

	c, err := ln.Dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(15 * time.Second))
	br := bufio.NewReader(c)

	for reqNum := uint64(1); reqNum <= 2; reqNum++ {
		started, release := getGate(reqNum)
		go func() {
			_, _ = c.Write([]byte(fixedPostRequest(bytes.Repeat([]byte("M"), bodySize))))
		}()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatalf("request %d handler never started", reqNum)
		}
		if usage := s.GetMemoryBudgetUsage(); usage < bodySize {
			t.Fatalf("request %d in-flight usage = %d, want at least %d", reqNum, usage, bodySize)
		}
		close(release)
		var resp Response
		if err := resp.Read(br); err != nil {
			t.Fatalf("request %d response: %v", reqNum, err)
		}
		if resp.StatusCode() != StatusOK {
			t.Fatalf("request %d status = %d", reqNum, resp.StatusCode())
		}
		waitBudgetUsage(t, s, 0)
	}
}

// A handler that recovers from its own panic still returns all quota through
// the normal path.
func TestMemoryBudgetReleaseRecoveredPanic(t *testing.T) {
	withSmallBodyPoolLimit(t)

	s := &Server{
		MemoryBudgetBytes:  4 * 1024 * 1024,
		MaxRequestBodySize: 4 * 1024 * 1024,
		Handler: func(ctx *RequestCtx) {
			defer func() {
				_ = recover()
			}()
			ctx.SetStatusCode(StatusInternalServerError)
			panic("boom")
		},
	}
	ln := startTestServer(t, s)

	resp, err := rawRoundTrip(t, ln, fixedPostRequest(bytes.Repeat([]byte("I"), 64*1024)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode() != StatusInternalServerError {
		t.Fatalf("unexpected status: %d, want 500", resp.StatusCode())
	}
	waitBudgetUsage(t, s, 0)
}

// Per-request MaxRequestBodySize and the header-limiting ReadBufferSize keep
// working independently of the budget, with no budget rejections recorded.
func TestMemoryBudgetIndependentLimits(t *testing.T) {
	t.Run("max_request_body_size", func(t *testing.T) {
		rejected := uint64(0)
		run := func(memBudget int) int {
			s := &Server{
				Handler:            echoHandler,
				MemoryBudgetBytes:  memBudget,
				MaxRequestBodySize: 1024,
			}
			ln := startTestServer(t, s)
			resp, err := rawRoundTrip(t, ln, fixedPostRequest(bytes.Repeat([]byte("J"), 4096)))
			if err != nil {
				t.Fatalf("unexpected transport error: %v", err)
			}
			rejected = s.GetMemoryBudgetRejectedCount()
			return resp.StatusCode()
		}
		statusBudgeted := run(16 * 1024 * 1024)
		statusPlain := run(0)
		if statusBudgeted != StatusBadRequest || statusBudgeted != statusPlain {
			t.Fatalf("MaxRequestBodySize not enforced independently: %d vs %d",
				statusBudgeted, statusPlain)
		}
		if rejected != 0 {
			t.Fatalf("per-request limit must not consume budget rejections, got %d", rejected)
		}
	})

	t.Run("read_buffer_size", func(t *testing.T) {
		run := func(memBudget int) (int, *Server) {
			s := &Server{
				Handler:           echoHandler,
				MemoryBudgetBytes: memBudget,
				ReadBufferSize:    128,
			}
			ln := startTestServer(t, s)
			bigHeader := fmt.Sprintf("GET / HTTP/1.1\r\nHost: example.com\r\nX-Big: %s\r\n\r\n",
				bytes.Repeat([]byte("K"), 8*1024))
			resp, _ := rawRoundTrip(t, ln, bigHeader)
			status := 0
			if resp != nil {
				status = resp.StatusCode()
			}
			return status, s
		}
		statusBudgeted, sb := run(16 * 1024 * 1024)
		statusPlain, _ := run(0)
		if statusBudgeted != StatusRequestHeaderFieldsTooLarge || statusBudgeted != statusPlain {
			t.Fatalf("ReadBufferSize header limit not enforced independently: %d vs %d",
				statusBudgeted, statusPlain)
		}
		if sb.GetMemoryBudgetUsage() != 0 {
			t.Fatalf("header rejection must leave no reserved bytes, got %d",
				sb.GetMemoryBudgetUsage())
		}
	})
}

// --- unit tests for the budget primitives ---

func TestMemBudgetAcquireRelease(t *testing.T) {
	var b memBudget
	b.setLimit(10)

	if !b.tryAcquire(6) {
		t.Fatal("first acquire must succeed")
	}
	if b.tryAcquire(5) {
		t.Fatal("acquire over the limit must fail")
	}
	if got := b.rejectedCount(); got != 1 {
		t.Fatalf("rejected = %d, want 1", got)
	}
	if got := b.inUseBytes(); got != 6 {
		t.Fatalf("inUse = %d, want 6", got)
	}
	b.release(6)
	if got := b.inUseBytes(); got != 0 {
		t.Fatalf("inUse = %d after release, want 0", got)
	}
	// Over-release must never drive the counter below zero.
	b.release(100)
	if got := b.inUseBytes(); got != 0 {
		t.Fatalf("inUse = %d after over-release, want 0", got)
	}
}

func TestMemBudgetDisabledAlwaysAdmits(t *testing.T) {
	var b memBudget // limit zero
	if !b.tryAcquire(1 << 40) {
		t.Fatal("disabled budget must admit everything")
	}
	if b.inUseBytes() != 0 {
		t.Fatalf("disabled budget must not account, got %d", b.inUseBytes())
	}
	if b.enabled() {
		t.Fatal("zero limit must be disabled")
	}
}

func TestMemAccountDenialReleasesReservation(t *testing.T) {
	var b memBudget
	b.setLimit(100)
	a := &memAccount{budget: &b}

	if err := a.growBody(0, 60); err != nil {
		t.Fatalf("first grow: %v", err)
	}
	if err := a.growBody(60, 120); !errors.Is(err, ErrMemoryBudgetExceeded) {
		t.Fatalf("want ErrMemoryBudgetExceeded, got %v", err)
	}
	// The rejected read releases everything it had reserved.
	if a.body != 0 || b.inUseBytes() != 0 {
		t.Fatalf("reservation leaked after denial: account=%d budget=%d", a.body, b.inUseBytes())
	}
	// Repeated releases are harmless.
	a.releaseBody()
	a.releaseAll()
}

// releaseAll deferred around the handler boundary returns the bytes even when
// the handler unwinds with a panic.
func TestMemAccountReleasesOnPanicUnwind(t *testing.T) {
	var b memBudget
	b.setLimit(100000)

	func() {
		a := &memAccount{budget: &b}
		defer func() {
			if r := recover(); r != nil {
				// emulate the connection loop deferred release
				a.releaseAll()
			}
		}()
		if err := a.growBody(0, 4096); err != nil {
			t.Fatalf("grow: %v", err)
		}
		if b.inUseBytes() != 4096 {
			t.Fatalf("inUse = %d, want 4096", b.inUseBytes())
		}
		panic("handler panic")
	}()

	if b.inUseBytes() != 0 {
		t.Fatalf("quota not returned after panic unwind: %d", b.inUseBytes())
	}
}
