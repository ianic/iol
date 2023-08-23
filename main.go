package main

import (
	"io"
	"log"
	"log/slog"
	"os"
	"syscall"
	"unsafe"

	"github.com/pawelgaczynski/giouring"
	"golang.org/x/sys/unix"
)

const (
	ringSize  = 32
	batchSize = 32
)

func main() {
	// var programLevel = new(slog.LevelVar) // Info by default
	// h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: programLevel})
	// slog.SetDefault(slog.New(h))
	// programLevel.Set(slog.LevelDebug)
	slog.SetDefault(slog.New(
		slog.NewTextHandler(
			os.Stderr,
			&slog.HandlerOptions{
				Level:     slog.LevelDebug,
				AddSource: true,
			})))

	if err := run(); err != nil {
		log.Panic(err)
	}

}

func run() error {
	io, err := NewIO(ringSize)
	if err != nil {
		return err
	}
	defer io.Close()
	l, err := NewListener(io, 4245)
	if err != nil {
		return err
	}
	defer l.Close()
	for {
		io.tick()
	}
	return nil
}

type completionCallback = func(res int32, flags uint32, err error)

type Loop struct {
	ring             *giouring.Ring
	inKernel         uint
	callbacks        map[uint64]completionCallback
	callbacksCounter uint64
	br               *giouring.BufAndRing
	buffers          []byte
}

func NewIO(ringSize uint32) (*Loop, error) {
	ring, err := giouring.CreateRing(ringSize)
	if err != nil {
		return nil, err
	}
	const entries = uint32(16)
	const bufLen = uint32(4096)
	l := &Loop{
		ring:      ring,
		callbacks: make(map[uint64]completionCallback),
		buffers:   make([]byte, entries*bufLen),
	}

	l.br, err = ring.SetupBufRing(entries, 1, 0)
	if err != nil {
		return nil, err
	}
	for i := uint32(0); i < entries; i++ {
		l.br.BufRingAdd(
			uintptr(unsafe.Pointer(&l.buffers[bufLen*i])),
			bufLen,
			uint16(i),
			giouring.BufRingMask(entries),
			int(i),
		)
	}
	l.br.BufRingAdvance(int(entries))

	return l, nil
}

func (l *Loop) tick() error {
	submitted, err := l.ring.SubmitAndWait(1)
	if err != nil {
		return err
	}
	l.inKernel += submitted
	_ = l.flushCompletions()
	return nil
}

func (l *Loop) flushCompletions() uint32 {
	var cqes [batchSize]*giouring.CompletionQueueEvent
	var noCompleted uint32 = 0
	for {
		peeked := l.ring.PeekBatchCQE(cqes[:])
		l.inKernel -= uint(peeked)
		for _, cqe := range cqes[:peeked] {
			if cqe.UserData == 0 {
				continue
			}
			l.getCallback(cqe)(cqe.Res, cqe.Flags, cqeErr(cqe))
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
			if err := l.tick(); err != nil {
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

func (l *Loop) getCallback(cqe *giouring.CompletionQueueEvent) completionCallback {
	cb := l.callbacks[cqe.UserData]
	isMultishot := (cqe.Flags & giouring.CQEFMore) > 0
	if !isMultishot {
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
	sqe.BufIG = 1 // l.br.Bid
	l.setCallback(sqe, cb)
	return nil
}

type Listener struct {
	loop *Loop
	fd   int
	s    Socket
}

func NewListener(loop *Loop, port int) (*Listener, error) {
	l := &Listener{loop: loop}
	if err := l.bind(port); err != nil {
		return nil, err
	}
	if err := l.accept(); err != nil {
		_ = l.Close()
		return nil, err
	}
	return l, nil
}
func (l *Listener) Close() error {
	return syscall.Close(l.fd)
}

func (l *Listener) bind(port int) error {
	addr := syscall.SockaddrInet4{Port: port}
	fd, err := bind(&addr)
	if err != nil {
		return err
	}
	l.fd = fd
	return nil
}

func (l *Listener) accept() error {
	return l.loop.PrepareMultishotAccept(l.fd, func(res int32, flags uint32, err error) {
		if err == nil {
			l.onAccept(int(res))
		}
	})
}

func (l *Listener) onAccept(fd int) {
	slog.Info("listener accept", "fd", fd)
	l.s = Socket{
		loop: l.loop,
		fd:   fd,
	}
	l.s.write([]byte("iso medo u ducan\n"), func(n int, err error) {
		slog.Info("write", "bytes", n, "err", err)
		syscall.Close(fd)
	})
	if err := l.s.read(func(data []byte, err error) {
		slog.Info("read", "data", data, "err", err)
	}); err != nil {
		slog.Error("prepare read", "err", err)
	}
}

type Socket struct {
	loop *Loop
	fd   int
}

// Prepares Send. Ensures that whole buffer is sent. Write could be partial only
// in case of error. In that case it returns number of bytes written and error.
// When error is nil, number of bytes written is len(buf).
func (s *Socket) write(buf []byte, onWrite func(int, error)) error {
	nn := 0
	var cb completionCallback
	cb = func(res int32, flags uint32, err error) {
		nn += int(res) // bytes written
		if err != nil {
			onWrite(nn, err)
			return
		}
		if nn >= len(buf) {
			onWrite(nn, nil)
			return
		}
		if err := s.loop.PrepareSend(s.fd, buf[nn:], cb); err != nil {
			onWrite(nn, err)
		}
		// new send prepared
	}
	return s.loop.PrepareSend(s.fd, buf, cb)
}

func (s *Socket) read(onRead func([]byte, error)) error {
	s.loop.PrepareRecv(s.fd, func(res int32, flags uint32, err error) {
		if err != nil {
			onRead(nil, err)
			return
		}
		if res == 0 {
			onRead(nil, io.EOF)
			return
		}
		isProvidedBuffer := flags&giouring.CQEFBuffer > 0
		if !isProvidedBuffer {
			panic("missing buffer flag")
		}
		bufferID := flags >> giouring.CQEBufferShift
		start := bufferID * 4096
		n := uint32(res)
		buf := s.loop.buffers[start : start+n]
		onRead(buf, nil)
		s.loop.br.BufRingAdd(
			uintptr(unsafe.Pointer(&buf[0])),
			4096,
			uint16(bufferID),
			giouring.BufRingMask(16),
			0,
		)
		s.loop.br.BufRingAdvance(1)
		slog.Debug("onRead", "bufferID", bufferID, "len", n)
	})
	return nil
}

func (s *Socket) onWrite(noBytes int32, err error) {
	slog.Info("onWrite", "noBytes", noBytes, "err", err)
}

func bind(addr *syscall.SockaddrInet4) (int, error) {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return 0, err
	}
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		return 0, err
	}
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
		return 0, err
	}
	if err := syscall.Bind(fd, addr); err != nil {
		return 0, err
	}
	if err := syscall.SetNonblock(fd, false); err != nil {
		return 0, err
	}
	if err := syscall.Listen(fd, 128); err != nil {
		return 0, err
	}
	return fd, nil
}

func cqeErr(c *giouring.CompletionQueueEvent) error {
	if c.Res > -4096 && c.Res < 0 {
		return syscall.Errno(-c.Res)
	}
	return nil
}
