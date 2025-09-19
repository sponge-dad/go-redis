package database

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

type EchoDatabase struct {

}

func NewEchoDatabase() *EchoDatabase {
	return &EchoDatabase{}
}

func (e EchoDatabase) Exec(client resp.Connection, args [][]byte) resp.Reply {
	return reply.NewMultiBulkReply(args)
}

func (e EchoDatabase) Close() error {
	// TODO implement me
	panic("implement me")
}

func (e EchoDatabase) AfterClientClose(c resp.Connection) error {
	// TODO implement me
	panic("implement me")
}


