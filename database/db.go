package database

import (
	"go-redis/datastruct/dict"
	"go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
	"strings"
)

type DB struct {
	index int
	data dict.Dict
	addAof func(CmdLine)
}


type ExecFunc func(db *DB, args [][]byte) resp.Reply

type CmdLine = [][]byte

func newDB() *DB {
	return &DB{
		data: dict.NewSyncDict(),
		addAof: func(CmdLine) {},
	}
}

func (db *DB) Exec(c resp.Connection, cmdLine CmdLine) resp.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.NewErrReply("-Err unknown command\n" + cmdName)
	}
	if !validateArity(cmd.arity, cmdLine) {
		return reply.NewArgNumErrReply(cmdName)
	}
	return cmd.exector(db, cmdLine[1:])
}

func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return arity == argNum
	}
	return argNum >= -argNum
}

func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	value, exit := db.data.Get(key)
	if !exit {
		return nil, false
	}
	return &database.DataEntity{
		Data: value,
	}, exit
}

func (db *DB) PutEntity(key string, value *database.DataEntity) int {
	return db.data.Put(key, value.Data)
}

func (db *DB) PutEntityIfExist(key string, value *database.DataEntity) int {
	return db.data.PutIfExist(key, value.Data)
}

func (db *DB) PutEntityIfAbsent(key string, value *database.DataEntity) int {
	return db.data.PutIfAbsent(key, value.Data)
}
func (db *DB) RemoveEntity(key string) int {
	return db.data.Remove(key)
}

func (db *DB) Removes(keys ...string) int {
	deleted := 0
	for _, key := range keys {
		deleted += db.data.Remove(key)
	}
	return deleted
}

func (db *DB) Flush() {
	db.data.Clear()
}
