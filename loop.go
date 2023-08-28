package iol

import (
	"context"
	"log/slog"
	"math"
	"os"
	"syscall"
	"time"
	"unsafe"

	"github.com/pawelgaczynski/giouring"
	"golang.org/x/sys/unix"
)

const (
	batchSize      = 128
	buffersGroupID = 0 // currently using only 1 provided buffer group
)

type completionCallback = func(res int32, flags uint32, errno syscall.Errno)

type Loop struct {
	ring      *giouring.Ring
	callbacks callbacks
	buffers   providedBuffers
}

type Options struct {
	RingEntries      uint32
	RecvBuffersCount uint32
	RecvBufferLen    uint32
}

var DefaultOptions = Options{
	RingEntries:      1024,
	RecvBuffersCount: 256,
	RecvBufferLen:    4 * 1024,
}

func New(opt Options) (*Loop, error) {
	ring, err := giouring.CreateRing(opt.RingEntries)
	if err != nil {
		return nil, err
	}
	l := &Loop{ring: ring}
	l.callbacks.init()
	if err := l.buffers.setup(ring, opt.RecvBuffersCount, opt.RecvBufferLen); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Loop) RunOnce() error {
	if err := l.submitAndWait(1); err != nil {
		return err
	}
	_ = l.flushCompletions()
	return nil
}

// Run until all prepared operations are finished.
func (l *Loop) RunUntilDone() error {
	for {
		if l.callbacks.count() == 0 {
			return nil
		}
		if err := l.RunOnce(); err != nil {
			return err
		}
	}
}

// Run until context is canceled.
// Check context every timeout.
func (l *Loop) Run(ctx context.Context, timeout time.Duration) error {
	ts := syscall.NsecToTimespec(int64(timeout))
	done := func() bool {
		select {
		case <-ctx.Done():
			return true
		default:
		}
		return false
	}
	for {
		if err := l.submitAndWait(0); err != nil {
			return err
		}
		if _, err := l.ring.WaitCQEs(1, &ts, nil); err != nil && !TemporaryErr(err) {
			return err
		}
		_ = l.flushCompletions()
		if done() {
			break
		}
	}
	return nil
}

// is this error temporary
func TemporaryErr(err error) bool {
	if errno, ok := err.(syscall.Errno); ok {
		return TemporaryErrno(errno)
	}
	if os.IsTimeout(err) {
		return true
	}
	return false
}

func TemporaryErrno(errno syscall.Errno) bool {
	return errno.Temporary() || errno == unix.ETIME || errno == syscall.ENOBUFS
}

// retry on temporary errors
func (l *Loop) submitAndWait(waitNr int) error {
	for {
		_, err := l.ring.SubmitAndWait(0)
		if err != nil && TemporaryErr(err) {
			continue
		}
		return err
	}
}

func (l *Loop) flushCompletions() uint32 {
	var cqes [batchSize]*giouring.CompletionQueueEvent
	var noCompleted uint32 = 0
	for {
		peeked := l.ring.PeekBatchCQE(cqes[:])
		for _, cqe := range cqes[:peeked] {
			errno := cqeErrno(cqe)
			if cqe.UserData == 0 {
				slog.Debug("ceq without userdata", "res", cqe.Res, "flags", cqe.Flags, "errno", errno)
				continue
			}
			cb := l.callbacks.get(cqe)
			cb(cqe.Res, cqe.Flags, errno)

		}
		l.ring.CQAdvance(peeked)
		noCompleted += peeked
		if peeked < uint32(len(cqes)) {
			return noCompleted
		}
	}
}

func (l *Loop) getSQE() (*giouring.SubmissionQueueEntry, error) {
	for {
		sqe := l.ring.GetSQE()
		if sqe == nil {
			if _, err := l.ring.Submit(); err != nil {
				return nil, err
			}
			continue
		}
		return sqe, nil
	}
}

func (l *Loop) Close() {
	l.ring.QueueExit()
}

func (l *Loop) PrepareMultishotAccept(fd int, cb completionCallback) error {
	sqe, err := l.getSQE()
	if err != nil {
		return err
	}
	sqe.PrepareMultishotAccept(fd, 0, 0, 0)
	l.callbacks.set(sqe, cb)
	return nil
}

func (l *Loop) PrepareCancelFd(fd int, cb completionCallback) error {
	sqe, err := l.getSQE()
	if err != nil {
		return err
	}
	sqe.PrepareCancelFd(fd, 0)
	l.callbacks.set(sqe, cb)
	return nil
}

func (l *Loop) PrepareShutdown(fd int, cb completionCallback) error {
	sqe, err := l.getSQE()
	if err != nil {
		return err
	}
	const SHUT_RDWR = 2
	sqe.PrepareShutdown(fd, SHUT_RDWR)
	l.callbacks.set(sqe, cb)
	return nil
}

func (l *Loop) PrepareClose(fd int, cb completionCallback) error {
	sqe, err := l.getSQE()
	if err != nil {
		return err
	}
	sqe.PrepareClose(fd)
	l.callbacks.set(sqe, cb)
	return nil
}

func (l *Loop) PrepareSend(fd int, buf []byte, cb completionCallback) error {
	sqe, err := l.getSQE()
	if err != nil {
		return err
	}
	sqe.PrepareSend(fd, uintptr(unsafe.Pointer(&buf[0])), uint32(len(buf)), 0)
	l.callbacks.set(sqe, cb)
	return nil
}

// Multishot, provided buffers recv
func (l *Loop) PrepareRecv(fd int, cb completionCallback) error {
	sqe, err := l.getSQE()
	if err != nil {
		return err
	}
	sqe.PrepareRecvMultishot(fd, 0, 0, 0)
	sqe.Flags = giouring.SqeBufferSelect
	sqe.BufIG = buffersGroupID
	l.callbacks.set(sqe, cb)
	return nil
}

func cqeErrno(c *giouring.CompletionQueueEvent) syscall.Errno {
	if c.Res > -4096 && c.Res < 0 {
		return syscall.Errno(-c.Res)
	}
	return 0
}

// #region providedBuffers

type providedBuffers struct {
	br      *giouring.BufAndRing
	data    []byte
	entries uint32
	bufLen  uint32
}

func (b *providedBuffers) setup(ring *giouring.Ring, entries uint32, bufLen uint32) error {
	b.entries = entries
	b.bufLen = bufLen
	b.data = make([]byte, b.entries*b.bufLen)
	var err error
	b.br, err = ring.SetupBufRing(b.entries, buffersGroupID, 0)
	if err != nil {
		return err
	}
	for i := uint32(0); i < b.entries; i++ {
		b.br.BufRingAdd(
			uintptr(unsafe.Pointer(&b.data[b.bufLen*i])),
			b.bufLen,
			uint16(i),
			giouring.BufRingMask(b.entries),
			int(i),
		)
	}
	b.br.BufRingAdvance(int(b.entries))
	return nil
}

// get provided buffer from cqe res, flags
func (b *providedBuffers) get(res int32, flags uint32) ([]byte, uint16) {
	isProvidedBuffer := flags&giouring.CQEFBuffer > 0
	if !isProvidedBuffer {
		panic("missing buffer flag")
	}
	bufferID := uint16(flags >> giouring.CQEBufferShift)
	start := uint32(bufferID) * b.bufLen
	n := uint32(res)
	return b.data[start : start+n], bufferID
}

// return provided buffer to the kernel
func (b *providedBuffers) release(buf []byte, bufferID uint16) {
	b.br.BufRingAdd(
		uintptr(unsafe.Pointer(&buf[0])),
		b.bufLen,
		uint16(bufferID),
		giouring.BufRingMask(b.entries),
		0,
	)
	b.br.BufRingAdvance(1)
}

//#endregion providedBuffers

// #region callbacks

type callbacks struct {
	m    map[uint64]completionCallback
	next uint64
}

func (c *callbacks) init() {
	c.m = make(map[uint64]completionCallback)
	c.next = math.MaxUint16 // reserve first few userdata values for internal use
}

func (c *callbacks) set(sqe *giouring.SubmissionQueueEntry, cb completionCallback) {
	c.next++
	key := c.next
	c.m[key] = cb
	sqe.UserData = key
}

func (c *callbacks) get(cqe *giouring.CompletionQueueEvent) completionCallback {
	isMultiShot := (cqe.Flags & giouring.CQEFMore) > 0
	cb := c.m[cqe.UserData]
	if !isMultiShot {
		delete(c.m, cqe.UserData)
	}
	return cb
}

func (c *callbacks) count() int {
	return len(c.m)
}

// #endregion
