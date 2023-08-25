package main

import (
	"context"
	"log/slog"
	"loop"
	"os"
	"os/signal"
	"syscall"
	"time"
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
		slog.Error("run", "error", err)
	}

}

const ringSize = 32

func run(port int) error {
	slog.Debug("starting server", "port", port)
	lp, err := loop.New(ringSize)
	if err != nil {
		return err
	}
	defer lp.Close()
	srv := server{}
	ln, err := loop.NewListener(lp, port, &srv)
	if err != nil {
		return err
	}

	ctx := interuptContext()
	if err := lp.Run(ctx, time.Second); err != nil {
		slog.Error("run", "error", err)
	}
	ln.Close()
	if err := lp.RunUntilDone(); err != nil {
		slog.Error("run", "error", err)
	}

	return nil
}

func waitForInterupt() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Debug("interrupt")
}

// interuptContext returns context which will be closed on application interupt
func interuptContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		waitForInterupt()
		cancel()
	}()
	return ctx
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
