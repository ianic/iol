package loop

import (
	"log/slog"
	"syscall"

	"golang.org/x/sys/unix"
)

type Sender interface {
	Send(fd int, data []byte) error
	// Close(fd int)
}

type Server interface {
	OnStart(Sender)
	OnConnect(fd int)
	OnDisconnect(fd int, err error)
	OnRecv(fd int, data []byte)
	OnSend(fd int, err error)
}

type Listener struct {
	loop *Loop
	fd   int
	srv  Server
}

func NewListener(loop *Loop, port int, srv Server) (*Listener, error) {
	l := &Listener{loop: loop, srv: srv}
	if err := l.bind(port); err != nil {
		return nil, err
	}
	if err := l.accept(); err != nil {
		syscall.Close(l.fd)
		return nil, err
	}
	srv.OnStart(l)
	return l, nil
}

func (l *Listener) Send(fd int, data []byte) error {
	nn := 0
	var cb completionCallback
	cb = func(res int32, flags uint32, errno syscall.Errno) {
		nn += int(res) // bytes written so far
		if errno > 0 {
			// TODO: here nn could be > 0
			// usually senders report that situation, but I don't see usage of that
			l.srv.OnSend(fd, errno)
			return
		}
		if nn >= len(data) {
			// all sent call callback
			l.srv.OnSend(fd, nil)
			return
		}
		if err := l.loop.PrepareSend(fd, data[nn:], cb); err != nil {
			// here also nn could be > 0
			l.srv.OnSend(fd, err)
		}
		// new send prepared
	}
	return l.loop.PrepareSend(fd, data, cb)
}

func (l *Listener) Close() error {
	// TODO clean close, close connections, wait ...
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
	return l.loop.PrepareMultishotAccept(l.fd, func(res int32, flags uint32, errno syscall.Errno) {
		if errno == 0 {
			l.onAccept(int(res))
		}
	})
}

func (l *Listener) onAccept(fd int) {
	l.srv.OnConnect(fd)
	l.recvLoop(fd)
}

// recvLoop starts multishot recv on fd
// Will receive on fd until error occures.
func (l *Listener) recvLoop(fd int) error {
	var cb completionCallback
	cb = func(res int32, flags uint32, errno syscall.Errno) {
		if errno > 0 {
			if errno == syscall.ENOBUFS || errno.Temporary() {
				slog.Debug("read temporary error", "error", errno.Error(), "errno", uint(errno), "flags", flags)
				l.loop.PrepareRecv(fd, cb)
				return
			}
			slog.Warn("read error", "error", errno.Error(), "errno", uint(errno), "flags", flags)
			l.srv.OnDisconnect(fd, errno)
			return
		}
		if res == 0 {
			l.srv.OnDisconnect(fd, nil)
			return
		}
		buf, id := l.loop.buffers.get(res, flags)
		slog.Debug("read", "bufferID", id, "len", len(buf))
		l.srv.OnRecv(fd, buf)
		l.loop.buffers.release(buf, id)
	}
	return l.loop.PrepareRecv(fd, cb)
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
