package loop

import (
	"context"
	"log/slog"
	"syscall"
	"time"
	"unsafe"

	"github.com/pawelgaczynski/giouring"
	"golang.org/x/sys/unix"
)

const (
	ringSize  = 32
	batchSize = 32
)

type completionCallback = func(res int32, flags uint32, errno syscall.Errno)

// currently using only 1 provided buffer group
const buffersGroupID = 0

type Loop struct {
	ring             *giouring.Ring
	inKernel         uint
	callbacks        map[uint64]completionCallback
	callbacksCounter uint64
	buffers          providedBuffers
}

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

func New(ringSize uint32) (*Loop, error) {
	ring, err := giouring.CreateRing(ringSize)
	if err != nil {
		return nil, err
	}
	l := &Loop{
		ring:      ring,
		callbacks: make(map[uint64]completionCallback),
	}
	//  TODO: remove constants, make reasonable defaults
	if err := l.buffers.setup(ring, 2, 4096); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Loop) Tick() error {
	submitted, err := l.ring.SubmitAndWait(1)
	if err != nil {
		return err
	}
	l.inKernel += submitted
	_ = l.flushCompletions()
	return nil
}

func (l *Loop) RunUntilDone() error {
	for {
		slog.Debug("run until done", "in-kernel", l.inKernel)
		if l.inKernel == 0 {
			return nil
		}
		l.Tick()
	}
}

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
		submitted, err := l.ring.SubmitAndWait(0)
		if err != nil {
			return err
		}
		l.inKernel += submitted
		if _, err := l.ring.WaitCQEs(1, &ts, nil); err != nil {
			if errno, ok := err.(syscall.Errno); ok {
				if errno.Temporary() || errno == unix.ETIME {
					err = nil
				}
			}
			if err != nil {
				return err
			}
		}
		_ = l.flushCompletions()
		if done() {
			break
		}
	}
	return nil
}

func (l *Loop) flushCompletions() uint32 {
	var cqes [batchSize]*giouring.CompletionQueueEvent
	var noCompleted uint32 = 0
	for {
		peeked := l.ring.PeekBatchCQE(cqes[:])
		l.inKernel -= uint(peeked)
		for _, cqe := range cqes[:peeked] {
			isMultiShot := (cqe.Flags & giouring.CQEFMore) > 0
			if isMultiShot {
				l.inKernel++
			}
			if cqe.UserData == 0 {
				slog.Debug("ceq with userdata 0", "flags", cqe.Flags, "res", cqe.Res, "error", cqeErrno(cqe))
				continue
			}
			cb := l.getCallback(cqe, !isMultiShot)
			cb(cqe.Res, cqe.Flags, cqeErrno(cqe))

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
			if err := l.Tick(); err != nil {
				return nil, err
			}
			continue
		}
		return sqe, nil
	}
}

func (l *Loop) setCallback(sqe *giouring.SubmissionQueueEntry, cb completionCallback) {
	l.callbacksCounter++
	l.callbacks[l.callbacksCounter] = cb
	sqe.UserData = l.callbacksCounter
}

func (l *Loop) getCallback(cqe *giouring.CompletionQueueEvent, remove bool) completionCallback {
	cb := l.callbacks[cqe.UserData]
	if remove {
		delete(l.callbacks, cqe.UserData)
	}
	return cb
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
	l.setCallback(sqe, cb)
	return nil
}

func (l *Loop) PrepareCancelFd(fd int, cb completionCallback) error {
	sqe, err := l.getSQE()
	if err != nil {
		return err
	}
	sqe.PrepareCancelFd(fd, 0)
	l.setCallback(sqe, cb)
	return nil
}

func (l *Loop) PrepareShutdown(fd int, cb completionCallback) error {
	sqe, err := l.getSQE()
	if err != nil {
		return err
	}
	const SHUT_RDWR = 2
	sqe.PrepareShutdown(fd, SHUT_RDWR)
	l.setCallback(sqe, cb)
	return nil
}

func (l *Loop) PrepareSend(fd int, buf []byte, cb completionCallback) error {
	sqe, err := l.getSQE()
	if err != nil {
		return err
	}
	sqe.PrepareSend(fd, uintptr(unsafe.Pointer(&buf[0])), uint32(len(buf)), 0)
	l.setCallback(sqe, cb)
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
	l.setCallback(sqe, cb)
	return nil
}

func cqeErrno(c *giouring.CompletionQueueEvent) syscall.Errno {
	if c.Res > -4096 && c.Res < 0 {
		return syscall.Errno(-c.Res)
	}
	return 0
}

// func cqeErr(c *giouring.CompletionQueueEvent) error {
// 	if c.Res > -4096 && c.Res < 0 {
// 		return syscall.Errno(-c.Res)
// 	}
// 	return nil
// }
