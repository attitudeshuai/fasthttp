package fasthttp

import (
	"errors"
	"io"
	"sync"
)

// ErrBodyStreamNotReplayable is returned when a retry or a body-preserving
// redirect needs to send a streamed request body again but the bytes already
// consumed from the stream exceeded the configured retention limit.
//
// The retained prefix alone cannot rebuild the body, so fasthttp fails the
// request with this error instead of silently replaying a truncated or empty
// body. Raise the configured limit or buffer the body before sending it to
// allow the request to be replayed.
var ErrBodyStreamNotReplayable = errors.New("fasthttp: request body stream cannot be replayed: retained body limit exceeded")

// maxReplayBufReuseCap bounds the capacity of retention buffers returned to
// the pool. A body that needed a bigger buffer lets its backing array be
// collected instead of pinning it for the lifetime of the process.
const maxReplayBufReuseCap = 1 << 20 // 1MiB

var replayBodyPool sync.Pool

// bodyReplay wraps a streaming request body and retains every byte consumed
// from it, up to maxBytes, so a send can be rolled back and replayed from the
// first byte on a brand-new connection.
//
// The state lives on the Request rather than on a connection, which is what
// lets it survive connection replacement, a TLS re-handshake, or an upstream
// address change between attempts and redirect hops.
//
// Reads serve the retained prefix first and then the wrapped stream, so a
// replay re-sends the retained bytes before touching the stream's unread tail.
// Close is a no-op on purpose: the request writer closes the installed reader
// after every attempt, but a retry or redirect may still need the stream's
// tail. The wrapped stream is closed exactly once in finish, when the logical
// request (or redirect chain) ends.
type bodyReplay struct {
	src io.Reader

	// buf holds the retained prefix: every byte consumed from src while the
	// limit had room. pos is the current read position within buf for the
	// ongoing send attempt.
	buf []byte
	pos int

	maxBytes int

	// overflow is set as soon as a consumed byte could not be retained. From
	// that point on the consumed range has a hole and the body can never be
	// reconstructed, so every later replay attempt must fail.
	overflow bool

	finished bool
	closeErr error
}

func acquireBodyReplay(src io.Reader, maxBytes int) *bodyReplay {
	r, _ := replayBodyPool.Get().(*bodyReplay)
	if r == nil {
		r = &bodyReplay{}
	}
	buf := r.buf
	if cap(buf) > maxReplayBufReuseCap {
		buf = nil
	}
	*r = bodyReplay{
		src:      src,
		buf:      buf[:0],
		maxBytes: maxBytes,
	}
	return r
}

// installBodyReplay wraps req.bodyStream with a replay buffer bounded by
// maxBytes. The limit must be positive.
func installBodyReplay(req *Request, maxBytes int) *bodyReplay {
	src := req.bodyStream
	r := acquireBodyReplay(src, maxBytes)
	// writeBodyStream derives a fixed content length from an *io.LimitedReader
	// before reading the stream. The wrapper is no longer a *LimitedReader,
	// so perform the same framing discovery up front to keep the wire framing
	// identical and stable across every replay attempt.
	if req.Header.ContentLength() < 0 && !req.Header.disableSpecialHeader {
		if size := limitedReaderSize(src); size >= 0 && size == int64(int(size)) {
			req.Header.SetContentLength(int(size))
		}
	}
	req.bodyStream = r
	req.bodyReplay = r
	return r
}

// releaseBodyReplay closes the wrapped stream, returns the retention buffer to
// the pool, and detaches the replay state from the request. It is safe to call
// when no replay state is installed.
func releaseBodyReplay(req *Request) error {
	r := req.bodyReplay
	if r == nil {
		return nil
	}
	req.bodyReplay = nil
	return r.finish()
}

// Read implements io.Reader: bytes already retained are served from the
// buffer, everything else is read from the wrapped stream and retained until
// the retention limit is reached.
func (r *bodyReplay) Read(p []byte) (int, error) {
	if r.pos < len(r.buf) {
		n := copy(p, r.buf[r.pos:])
		r.pos += n
		return n, nil
	}

	n, err := r.src.Read(p)
	if n > 0 {
		r.pos += n
		if !r.overflow {
			remaining := r.maxBytes - len(r.buf)
			switch {
			case remaining >= n:
				r.buf = append(r.buf, p[:n]...)
			default:
				if remaining > 0 {
					r.buf = append(r.buf, p[:remaining]...)
				}
				// The stream ran past the retained prefix: the gap between
				// the buffer and the stream position can never be refilled.
				r.overflow = true
			}
		}
	}
	return n, err
}

// Close is invoked by the request writer at the end of every send attempt. It
// must not close the wrapped stream, because a retry or body-preserving
// redirect still has to read its tail. finish performs the real close once the
// logical request ends.
func (r *bodyReplay) Close() error {
	return nil
}

// rewind rolls the reader back to the first retained byte for another send.
// It returns false when consumed bytes exceeded the retention limit, in which
// case the body is not replayable and the caller must fail rather than resend
// a partial or empty body.
func (r *bodyReplay) rewind() bool {
	r.pos = 0
	return !r.overflow
}

// finish ends the logical request: the wrapped stream is closed once and the
// retention buffer is returned to the pool.
func (r *bodyReplay) finish() error {
	if r.finished {
		return r.closeErr
	}
	r.finished = true

	src := r.src
	if closer, ok := src.(io.Closer); ok {
		r.closeErr = closer.Close()
	}

	closeErr := r.closeErr
	r.src = nil
	r.pos = 0
	r.overflow = false
	r.maxBytes = 0
	if cap(r.buf) > maxReplayBufReuseCap {
		r.buf = nil
	} else if r.buf != nil {
		r.buf = r.buf[:0]
	}
	replayBodyPool.Put(r)
	return closeErr
}
