package tcp

import (
	"context"
	"go-redis/interface/tcp"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Config struct {
	Address string
}


func ListenAndServeWithSignal(cfg Config, handler tcp.Handler) error {
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	closeChan := make(chan struct{})
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		switch <- sigChan {
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			closeChan <- struct{}{}
		}
	}()

	ListenAndServe(listener, handler, closeChan)
	return nil
}


func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// 程序被主动kill掉时会执行这一步
	go func() {
		<-closeChan
		_ = listener.Close()
		_ = handler.Close()
	}()
	// 程序自动执行完成后会主动close掉listener和handler
	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()

	var wg sync.WaitGroup
	ctx := context.Background()
	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			handler.Handle(ctx, conn)
		}()
	}
	wg.Wait()
}


