package iol

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
	loop  *Loop
	fd    int
	srv   Server
	conns map[int]struct{}
}

func NewListener(loop *Loop, port int, srv Server) (*Listener, error) {
	l := &Listener{
		loop:  loop,
		srv:   srv,
		conns: make(map[int]struct{}),
	}
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
	if _, ok := l.conns[fd]; !ok {
		l.srv.OnSend(fd, syscall.ENOTCONN)
		return nil
	}
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
			fd := int(res)
			l.srv.OnConnect(fd)
			l.recvLoop(fd)
			return
		}
		if errno != syscall.ECANCELED {
			slog.Debug("accept", "errno", errno, "res", res, "flags", flags)
		}
	})
}

// func (l *Listener) Close() error {
// 	// TODO clean close, close connections, wait ...
// 	return syscall.Close(l.fd)
// }

func (l *Listener) Close() error {
	return l.loop.PrepareCancelFd(l.fd, func(res int32, flags uint32, errno syscall.Errno) {
		slog.Debug("listener close", "errno", errno, "res", res, "flags", flags)
		for fd := range l.conns {
			l.shutdown(fd, nil)
		}
	})
}

// recvLoop starts multishot recv on fd
// Will receive on fd until error occures.
func (l *Listener) recvLoop(fd int) error {
	l.conns[fd] = struct{}{}

	var cb completionCallback
	cb = func(res int32, flags uint32, errno syscall.Errno) {
		if errno > 0 {
			if TemporaryErrno(errno) {
				slog.Debug("read temporary error", "error", errno.Error(), "errno", uint(errno), "flags", flags)
				l.loop.PrepareRecv(fd, cb)
				return
			}
			if errno != syscall.ECONNRESET {
				slog.Warn("read error", "error", errno.Error(), "errno", uint(errno), "flags", flags)
			}
			l.shutdown(fd, errno)
			return
		}
		if res == 0 {
			l.shutdown(fd, nil)
			return
		}
		buf, id := l.loop.buffers.get(res, flags)
		//slog.Debug("read", "bufferID", id, "len", len(buf))
		l.srv.OnRecv(fd, buf)
		l.loop.buffers.release(buf, id)
	}
	return l.loop.PrepareRecv(fd, cb)
}

func (l *Listener) shutdown(fd int, err error) {
	if _, ok := l.conns[fd]; ok {
		l.srv.OnDisconnect(fd, err)
		delete(l.conns, fd)

		l.loop.PrepareShutdown(fd, func(res int32, flags uint32, errno syscall.Errno) {
			if !(errno == 0 || errno == syscall.ENOTCONN) {
				slog.Debug("shutdown", "fd", fd, "errno", errno, "res", res, "flags", flags)
			}

			if errno != 0 {
				return
			}
			l.loop.PrepareClose(fd, func(res int32, flags uint32, errno syscall.Errno) {
				if errno != 0 {
					slog.Debug("close", "fd", fd, "errno", errno, "res", res, "flags", flags)
				}
			})
		})
	}
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
