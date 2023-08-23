package main

import (
	"fmt"
	"log"
	"log/slog"
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
	if err := run2(); err != nil {
		log.Panic(err)
	}

}

func run2() error {
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

type IO struct {
	ring             *giouring.Ring
	inKernel         uint
	callbacks        map[uint64]completionCallback
	callbacksCounter uint64
}

func NewIO(ringSize uint32) (*IO, error) {
	ring, err := giouring.CreateRing(ringSize)
	if err != nil {
		return nil, err
	}
	return &IO{
		ring:      ring,
		callbacks: make(map[uint64]completionCallback),
	}, nil
}

func (io *IO) tick() error {
	submitted, err := io.ring.SubmitAndWait(1)
	if err != nil {
		return err
	}
	io.inKernel += submitted
	_ = io.flushCompletions()
	return nil
}

func (io *IO) flushCompletions() uint32 {
	var cqes [batchSize]*giouring.CompletionQueueEvent
	var noCompleted uint32 = 0
	for {
		peeked := io.ring.PeekBatchCQE(cqes[:])
		io.inKernel -= uint(peeked)
		for _, cqe := range cqes[:peeked] {
			if cqe.UserData == 0 {
				continue
			}
			io.getCallback(cqe)(cqe.Res, cqe.Flags, cqeErr(cqe))
		}
		io.ring.CQAdvance(peeked)
		noCompleted += peeked
		if peeked < uint32(len(cqes)) {
			return noCompleted
		}
	}
}

func (io *IO) getSQE() (*giouring.SubmissionQueueEntry, error) {
	for {
		sqe := io.ring.GetSQE()
		if sqe == nil {
			if err := io.tick(); err != nil {
				return nil, err
			}
			continue
		}
		return sqe, nil
	}
}

func (io *IO) setCallback(sqe *giouring.SubmissionQueueEntry, cb completionCallback) {
	io.callbacksCounter++
	io.callbacks[io.callbacksCounter] = cb
	sqe.UserData = io.callbacksCounter
}

func (io *IO) getCallback(cqe *giouring.CompletionQueueEvent) completionCallback {
	cb := io.callbacks[cqe.UserData]
	isMultishot := (cqe.Flags & giouring.CQEFMore) > 0
	if !isMultishot {
		delete(io.callbacks, cqe.UserData)
	}
	return cb
}

func (io *IO) Close() {
	io.ring.QueueExit()
}

func (io *IO) PrepareMultishotAccept(fd int, cb completionCallback) error {
	sqe, err := io.getSQE()
	if err != nil {
		return err
	}
	sqe.PrepareMultishotAccept(fd, 0, 0, 0)
	io.setCallback(sqe, cb)
	return nil
}

func (io *IO) PrepareSend(fd int, buf []byte, cb completionCallback) error {
	sqe, err := io.getSQE()
	if err != nil {
		return err
	}
	sqe.PrepareSend(fd, uintptr(unsafe.Pointer(&buf[0])), uint32(len(buf)), 0)
	io.setCallback(sqe, cb)
	return nil
}

type Listener struct {
	io *IO
	fd int
	s  Socket
}

func NewListener(io *IO, port int) (*Listener, error) {
	l := &Listener{io: io}
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
	return l.io.PrepareMultishotAccept(l.fd, func(res int32, flags uint32, err error) {
		if err == nil {
			l.onAccept(int(res))
		}
	})
}

func (l *Listener) onAccept(fd int) {
	slog.Info("listener accept", "fd", fd)
	l.s = Socket{
		io: l.io,
		fd: fd,
	}
	l.s.write([]byte("iso medo u ducan\n"), func(n int, err error) {
		slog.Info("write", "bytes", n, "err", err)
		syscall.Close(fd)
	})
}

type Socket struct {
	io *IO
	fd int
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
		if err := s.io.PrepareSend(s.fd, buf[nn:], cb); err != nil {
			onWrite(nn, err)
		}
		// new send prepared
	}
	return s.io.PrepareSend(s.fd, buf, cb)
}

func (s *Socket) onWrite(noBytes int32, err error) {
	slog.Info("onWrite", "noBytes", noBytes, "err", err)
}

type completionCallback = func(res int32, flags uint32, err error)

func run() error {
	ring, err := giouring.CreateRing(ringSize)
	if err != nil {
		return err
	}
	defer ring.QueueExit()

	cqeBuff := make([]*giouring.CompletionQueueEvent, batchSize)

	addr := syscall.SockaddrInet4{Port: 4242}
	fd, err := bind(&addr)
	if err != nil {
		return err
	}
	defer syscall.Close(fd)

	slog.Info("bind", "fd", fd)

	//for {
	entry := ring.GetSQE()
	if entry == nil {
		log.Panic()
	}

	entry.PrepareMultishotAccept(fd, 0, 0, 0)
	entry.UserData = 0xdead_beef

	submitted, err := ring.SubmitAndWait(1)
	if err != nil {
		log.Panic(err)
	}
	slog.Info("zavrsio accept", "submitted", submitted)

	peeked := ring.PeekBatchCQE(cqeBuff)
	cqe := cqeBuff[0]
	cqe.GetData()
	fmt.Printf("cqe %d %d %x %v", cqe.Flags, cqe.Res, cqe.UserData, cqeErr(cqe))
	ring.CQAdvance(uint32(peeked))

	//}
	return nil
}

func cqeErr(c *giouring.CompletionQueueEvent) error {
	if c.Res > -4096 && c.Res < 0 {
		return syscall.Errno(-c.Res)
	}
	return nil
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
