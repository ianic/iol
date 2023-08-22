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
	ring     *giouring.Ring
	inKernel uint
}

func NewIO(ringSize uint32) (*IO, error) {
	ring, err := giouring.CreateRing(ringSize)
	if err != nil {
		return nil, err
	}
	return &IO{
		ring: ring,
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
			err := cqeErr(cqe)
			cb := (*completionCallback)(cqe.GetData())
			(*cb)(cqe.Res, cqe.Flags, err)
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
			panic("nema")
			continue
		}
		return sqe, nil
	}
}

func (io *IO) Close() {
	io.ring.QueueExit()
}

type Listener struct {
	io *IO
	fd int
	cb completionCallback
	s  Socket
}

func NewListener(io *IO, port int) (*Listener, error) {
	l := Listener{io: io}
	if err := l.bind(port); err != nil {
		return nil, err
	}
	if err := l.accept(); err != nil {
		_ = l.Close()
		return nil, err
	}
	return &l, nil
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
	sqe, err := l.io.getSQE()
	if err != nil {
		return err
	}
	l.cb = func(res int32, flags uint32, err error) {
		if err == nil {
			l.onAccept(int(res))
		}
	}
	sqe.PrepareMultishotAccept(l.fd, 0, 0, 0)
	sqe.UserData = uint64(uintptr(unsafe.Pointer(&l.cb)))
	return nil
}

func (l *Listener) onAccept2(res int32, flags uint32, err error) {
	slog.Info("listener onAccept2", "fd", res)
}

func (l *Listener) onAccept(fd int) {
	slog.Info("listener accept", "fd", fd)
	l.s = Socket{
		io: l.io,
		fd: fd,
	}
	l.s.write(data)
	//syscall.Close(fd)
}

var data = []byte("iso medo u ducan")

type Socket struct {
	io *IO
	fd int

	cb completionCallback
}

func (s *Socket) write(buf []byte) error {
	sqe, err := s.io.getSQE()
	if err != nil {
		return err
	}
	s.cb = func(res int32, flags uint32, err error) {
		if err == nil {
			s.onWrite(res, err)
		}
	}
	sqe.PrepareSend(s.fd, uintptr(unsafe.Pointer(&buf[0])), uint32(len(buf)), 0)
	sqe.UserData = uint64(uintptr(unsafe.Pointer(&s.cb)))
	return nil
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
