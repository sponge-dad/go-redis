package database

import (
	databaseface "go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

func init() {
	RegisterCommand("get", execGet, 2)
	RegisterCommand("set", execSet, 3)
	RegisterCommand("setnx", execSetNX, 3)
	RegisterCommand("getset", execGetSet, 3)
	RegisterCommand("getstrlen", execStrlen, 2)
}

// GET
func execGet(db *DB, args[][]byte) resp.Reply {
	entity, ok := db.GetEntity(string(args[0]))
	if !ok {
		return reply.NewNullBulkReply()
	}
	return reply.NewBulkReply(entity.Data.([]byte))
}

// SET key value
func execSet(db *DB, args [][]byte) resp.Reply {
	key, value := string(args[0]), args[1]
	entity := &databaseface.DataEntity{
		Data: value,
	}
	db.PutEntity(key, entity)
	db.addAof(utils.ToCmdLine3("set", args...))
	return reply.NewOkReply()
}

// SETNX
func execSetNX(db *DB, args[][]byte) resp.Reply {
	key, value := string(args[0]), args[1]
	entity := &databaseface.DataEntity {
		Data: value,
	}
	res := db.PutEntityIfAbsent(key, entity)
	db.addAof(utils.ToCmdLine3("setnx", args...))
	return reply.NewIntReply(int64(res))
}

// GETSET key value
func execGetSet(db *DB, args[][]byte) resp.Reply {
	key, value := string(args[0]), args[1]
	entity, ok := db.GetEntity(key)
	db.PutEntity(key, &databaseface.DataEntity{
		Data: value,
	})
	db.addAof(utils.ToCmdLine3("getset", args...))
	if !ok {
		return reply.NewNullBulkReply()
	}
	return reply.NewBulkReply(entity.Data.([]byte))
}

// STRLEN
func execStrlen(db *DB, args[][]byte) resp.Reply {
	entity, exist := db.GetEntity(string(args[0]))
	if !exist {
		return reply.NewNullBulkReply()
	}
	return reply.NewIntReply(int64(len(entity.Data.([]byte))))
}


