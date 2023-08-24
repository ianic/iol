package main

import (
	"log"
	"log/slog"
	"loop"
)

func main() {
	// slog.SetDefault(slog.New(
	// 	slog.NewTextHandler(
	// 		os.Stderr,
	// 		&slog.HandlerOptions{
	// 			Level:     slog.LevelDebug,
	// 			AddSource: true,
	// 		})))
	if err := run(4242); err != nil {
		log.Panic(err)
	}

}

const ringSize = 32

func run(port int) error {
	slog.Debug("starting server", "port", port)
	lp, err := loop.NewLoop(ringSize)
	if err != nil {
		return err
	}
	defer lp.Close()
	srv := server{}
	l, err := loop.NewListener(lp, port, &srv)
	if err != nil {
		return err
	}
	defer l.Close()
	for {
		lp.Tick()
	}
	return nil
}

type server struct {
	sender loop.Sender
}

func (s *server) OnStart(writer loop.Sender) {
	s.sender = writer
}

func (s *server) OnConnect(fd int) {
	slog.Debug("connect", "fd", fd)
}

func (s *server) OnDisconnect(fd int, err error) {
	slog.Debug("disconnect", "fd", fd)
}

func (s *server) OnRecv(fd int, data []byte) {
	slog.Debug("received", "fd", fd, "data", data)

	dst := make([]byte, len(data))
	copy(dst, data)
	s.sender.Send(fd, dst)
}

func (s *server) OnSend(fd int, err error) {
	slog.Debug("sent", "fd", fd)
}
