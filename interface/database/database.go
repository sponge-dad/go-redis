package database

import (
	"go-redis/interface/resp"
)

type CmdLine = [][]byte

type Database interface {
	Exec(client resp.Connection, args CmdLine) resp.Reply
	Close() error
	AfterClientClose(c resp.Connection) error
}

type DataEntity struct {
	Data interface{}
}
