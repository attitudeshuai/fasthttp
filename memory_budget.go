package fasthttp

import (
	"errors"
	"sync/atomic"
)

// ErrMemoryBudgetExceeded is returned when the server-wide byte budget
// configured via Server.MemoryBudgetBytes is exhausted.
//
// Request body reads fail with this error before allocating a larger body
// buffer, and response write buffers cannot be acquired while the budget is
// exhausted. The server answers the affected request with
// StatusServiceUnavailable (custom ErrorHandler implementations may detect
// the cause with errors.Is) and closes the connection. Bytes reserved by the
// rejected read are released immediately.
var ErrMemoryBudgetExceeded = errors.New("fasthttp: memory budget exceeded")

// memBudget is the process-wide (per Server) byte budget shared by all
// connections. Zero limit means budgeting is disabled and every acquisition
// succeeds, which keeps the unconfigured server behavior unchanged.
//
// All fields are accessed atomically; a single memBudget value lives inside
// the Server and must not be copied after first use.
type memBudget struct {
	// limit is the maximum number of bytes that may be reserved at once.
	// A value <= 0 disables budgeting.
	limit atomic.Int64

	// inUse is the number of bytes currently reserved.
	inUse atomic.Int64

	// rejected is the cumulative number of acquisition attempts that failed
	// because the budget was exhausted. It never decreases.
	rejected atomic.Uint64
}

func (b *memBudget) setLimit(limit int) {
	if limit < 0 {
		limit = 0
	}
	b.limit.Store(int64(limit))
}

func (b *memBudget) enabled() bool {
	return b.limit.Load() > 0
}

func (b *memBudget) limitBytes() int64 {
	return b.limit.Load()
}

func (b *memBudget) inUseBytes() int64 {
	return b.inUse.Load()
}

func (b *memBudget) rejectedCount() uint64 {
	return b.rejected.Load()
}

// tryAcquire reserves n bytes as an all-or-nothing operation. It returns
// false (and counts a rejection) when the reservation would exceed the
// limit. A disabled budget or n <= 0 always succeeds.
func (b *memBudget) tryAcquire(n int) bool {
	if n <= 0 {
		return true
	}
	if b.limit.Load() <= 0 {
		return true
	}
	for {
		cur := b.inUse.Load()
		if cur+int64(n) > b.limit.Load() {
			b.rejected.Add(1)
			return false
		}
		if b.inUse.CompareAndSwap(cur, cur+int64(n)) {
			return true
		}
	}
}

// release returns n bytes to the budget. Releasing more than the amount
// currently reserved is a framework accounting bug; instead of driving the
// counter below zero (which would let the budget over-admit forever), the
// counter is clamped at zero so that overload recovery never requires a
// server restart.
func (b *memBudget) release(n int64) {
	if n <= 0 {
		return
	}
	for {
		cur := b.inUse.Load()
		next := cur - n
		if next < 0 {
			next = 0
		}
		if b.inUse.CompareAndSwap(cur, next) {
			return
		}
	}
}

// memAccount tracks the budget reservations of a single connection.
//
// A connection is served by one worker goroutine, so the counters are plain
// fields. The response write buffer reservations are balanced directly
// against the budget (acquireWriter/releaseWriter), while the body bucket is
// released once per request and unconditionally when the connection ends.
//
// A memAccount is created in serveConnCounted, which defers releaseAll: that
// guarantees the reserved bytes are returned on every connection teardown -
// normal request end, handler early return, hijack and a panicking handler.
type memAccount struct {
	budget *memBudget

	// body is the number of bytes reserved for the current request's body
	// buffer growth. Zeroed at request end and connection teardown.
	body int64
}

// growBody reserves the extra capacity a body buffer needs to grow from
// oldCap to newCap. On rejection the whole body reservation of the failed
// request is released immediately: the read is aborted and the bytes it had
// reserved so far must not wait for connection cleanup.
func (a *memAccount) growBody(oldCap, newCap int) error {
	if a == nil || a.budget == nil {
		return nil
	}
	delta := newCap - oldCap
	if delta <= 0 || !a.budget.enabled() {
		return nil
	}
	if !a.budget.tryAcquire(delta) {
		// "release the already-acquired part": the read fails fatally, so
		// return everything this request had reserved for its body.
		a.releaseBody()
		return ErrMemoryBudgetExceeded
	}
	a.body += int64(delta)
	return nil
}

// releaseBody returns the whole current request body reservation. It is safe
// to call repeatedly.
func (a *memAccount) releaseBody() {
	if a == nil || a.body == 0 {
		return
	}
	a.budget.release(a.body)
	a.body = 0
}

// releaseAll returns every reservation still held by the connection. It is
// deferred from serveConnCounted so the quota is returned even when the
// handler panics or the connection is hijacked or closed after an error.
func (a *memAccount) releaseAll() {
	a.releaseBody()
}

// shrinkBuffersWhenIdle reports whether the response write buffer should be
// returned to its pool after each response instead of being kept on the
// keep-alive connection. ReduceMemoryUsage asks for this, and so does an
// active budget: returning the buffer releases its reservation immediately,
// so the measured usage falls back once connections go idle instead of
// waiting for every keep-alive connection to close.
func (s *Server) shrinkBuffersWhenIdle() bool {
	return s.ReduceMemoryUsage || s.memBudget.enabled()
}

// acquireMemAccount returns a recycled per-connection account bound to this
// server's budget. Pooled accounts keep ServeConn allocation-free in steady
// state.
func (s *Server) acquireMemAccount() *memAccount {
	v := s.memAcctPool.Get()
	if v == nil {
		v = &memAccount{}
	}
	a := v.(*memAccount) //nolint:forcetypeassert
	a.budget = &s.memBudget
	return a
}

// releaseMemAccount returns everything the account still holds and puts it
// back in the pool. It is deferred from serveConnCounted, so a handler
// panic still releases the quota.
func (s *Server) releaseMemAccount(a *memAccount) {
	if a == nil {
		return
	}
	a.releaseAll()
	a.budget = nil
	s.memAcctPool.Put(a)
}
