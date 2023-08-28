package signal

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

func WaitForInterupt() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
}

// interuptContext returns context which will be closed on application interupt
func InteruptContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		WaitForInterupt()
		cancel()
	}()
	return ctx
}
