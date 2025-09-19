package handler

import (
	"context"
	"errors"
	"go-redis/cluster"
	"go-redis/config"
	"go-redis/database"
	databaseface "go-redis/interface/database"
	"go-redis/logger"
	"go-redis/resp/connection"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

type RespHandler struct {
	activeConn sync.Map
	closing atomic.Bool
	db databaseface.Database

}

func NewRespHandler() *RespHandler {
	var db databaseface.Database
	// db = database.NewStandaloneDatabase()
	if config.Properties.Self != "" && len(config.Properties.Peers) > 0{  // cluster
		db = cluster.NewClusterDatabase()
	} else {
		db = database.NewStandaloneDatabase()
	}
	return &RespHandler{
		db:db,
	}
}

func (r *RespHandler) closeClient (client *connection.Connection) error {
	_ = client.Close()
	_ = r.db.AfterClientClose(client)
	r.activeConn.Delete(client)
	return nil
}


func (r *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	if r.closing.Load() {
		_ = conn.Close()
	}
	client := connection.NewConnection(conn)
	r.activeConn.Store(client, struct{}{})

	ch := parser.ParseStream(conn)

	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF || errors.Is(payload.Err, io.ErrUnexpectedEOF) || strings.Contains(payload.Err.Error(), "use of closed network connection") {
				_ = r.closeClient(client)
				logger.Errorf("connection closed: %v\n", client.RemoteAddr().String())
				return
			}
			// protocol error
			errReply := reply.NewErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				_ = r.closeClient(client)
				logger.Errorf("connection closed: %v\n", client.RemoteAddr().String())
				return
			}
			continue
		}
		if payload.Data == nil {
			continue
		}
		rep, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply error")
			continue
		}
		exec := r.db.Exec(client, rep.Args)
		if exec != nil {
			_ = client.Write(exec.ToBytes())
		} else {
			_ = client.Write(unknownErrReplyBytes)
		}
	}

}

func (r *RespHandler) Close() error {
	logger.Info("handler shutting down!")
	r.closing.Store(true)
	r.activeConn.Range(func(key, value any) bool {
		_ =  key.(*connection.Connection).Close()
		return true
	})
	r.db.Close()
	return nil
}



