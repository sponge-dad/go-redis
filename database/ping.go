package database

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

func init() {
	RegisterCommand("ping", Ping, 1)
}

func Ping (db *DB, args [][]byte) resp.Reply {
	return reply.NewPongReply()
}
