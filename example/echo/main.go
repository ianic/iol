package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ianic/iol"
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

func run(port int) error {
	slog.Debug("starting server", "port", port)
	lp, err := iol.New(iol.Options{
		RingEntries:      128,
		RecvBuffersCount: 256,
		RecvBufferLen:    1024,
	})
	if err != nil {
		return err
	}
	defer lp.Close()
	srv := server{}
	ln, err := iol.NewListener(lp, port, &srv)
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
	sender iol.Sender
}

func (s *server) OnStart(writer iol.Sender) {
	s.sender = writer
}

func (s *server) OnConnect(fd int) {
	slog.Debug("connect", "fd", fd)
}

func (s *server) OnDisconnect(fd int, err error) {
	slog.Debug("disconnect", "fd", fd)
}

func (s *server) OnRecv(fd int, data []byte) {
	slog.Debug("received", "fd", fd, "len", len(data))

	dst := make([]byte, len(data))
	copy(dst, data)
	s.sender.Send(fd, dst)
}

func (s *server) OnSend(fd int, err error) {
	slog.Debug("sent", "fd", fd)
}
