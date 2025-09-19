package tcp

import (
	"bufio"
	"context"
	"fmt"
	"go-redis/lib/sync/wait"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type EchoClient struct {
	conn net.Conn
	waiting wait.Wait
}

func (e *EchoClient) Close() error {
	e.waiting.WaitWithTimeout(10 * time.Second)
	return e.conn.Close()
}


type EchoHandler struct {
	activeConn sync.Map
	closing atomic.Bool
}

func NewEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

func (e *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	// 如果服务器已经关闭，关闭链接
	if e.closing.Load() {
		_ = conn.Close()
	}
	// 将新的连接包装成EchoClient，并存放到服务器中
	client := &EchoClient{
		conn: conn,
	}
	e.activeConn.Store(client, struct{}{})

	// 读取客户端的信息
	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection close")
				e.activeConn.Delete(client)
				return
			}
			fmt.Println("读取客户端数据出错")
			return
		}
		// 业务开始，
		client.waiting.Add(1)
		data := []byte(msg)
		_, _ = conn.Write(data)
		client.waiting.Done()
	}
}

func (e *EchoHandler) Close() error {
	fmt.Println("handler shut down")
	e.closing.Store(true)
	e.activeConn.Range(func(key, value any) bool {
		conn := key.(*EchoClient)
		_ = conn.Close()
		return true
	})
	return nil
}




