package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
)

func main() {
	buf, err := randomBuf(4096)
	if err != nil {
		log.Fatal(err)
	}
	ch := make(chan struct{}, 256)
	for {
		ch <- struct{}{}
		go func() {
			n := rand.Intn(len(buf)) + 1
			if err := echoTest(buf[:n]); err != nil {
				fmt.Printf("e")
			} else {
				fmt.Printf(".")
			}
			<-ch
		}()
	}
}

func randomBuf(size int) ([]byte, error) {
	f, err := os.Open("/dev/random")
	if err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func echoTest(buf []byte) error {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:4242")
	if err != nil {
		return err
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return err
	}
	if _, err := conn.Write(buf); err != nil {
		return err
	}
	rbuf := make([]byte, len(buf))
	if _, err := io.ReadFull(conn, rbuf); err != nil {
		return err
	}
	if err := conn.Close(); err != nil {
		return err
	}
	if !bytes.Equal(buf, rbuf) {
		return errors.New("not equal buffers")
	}
	return nil
}
