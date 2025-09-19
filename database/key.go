package database

import (
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/lib/wildcard"
	"go-redis/resp/reply"
)

func init() {
	RegisterCommand("del", execDel, -2)
	RegisterCommand("exists", execExist, -2)
	RegisterCommand("flushdb", execFlushDB, -1)
	RegisterCommand("type", execType, 2)
	RegisterCommand("rename", execRename, 3)
	RegisterCommand("renamenx", execRenameNX, 3)
	RegisterCommand("keys", execKeys, 2)
}

// DEL k1 k2 k3
func execDel(db *DB, args [][]byte) resp.Reply {
	keys := make([]string, len(args))
	for i := range args {
		keys[i] = string(args[i])
	}
	deleted := int64(db.Removes(keys...))

	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("del", args...))
	}

	return reply.NewIntReply(deleted)

}

// EXIST k1 k2 k3 ...
func execExist (db *DB, args[][]byte) resp.Reply {
	var res int64
	for i := range args {
		_, t := db.GetEntity(string(args[i]))
		if t {
			res ++
		}
	}
	return reply.NewIntReply(res)
}

// FLUSHDB
func execFlushDB (db *DB, args[][]byte) resp.Reply {
	db.Flush()
	// db.addAof(utils.ToCmdLine3("flushdb", args...))
	return reply.NewOkReply()
}
// TYPE
func execType(db *DB, args[][]byte) resp.Reply {
	entity, ok := db.GetEntity(string(args[0]))
	if !ok {
		return reply.NewStatusReply("none")
	}
	switch entity.Data.(type) {
	case int:
		return reply.NewStatusReply("int")
	case []byte:
		return reply.NewStatusReply("string")
	}
	// TODO: 别的数据结构
	return reply.NewUnknownErrReply()
}

// RENAME old_name new_name
func execRename (db *DB, args[][]byte) resp.Reply {
	oldName, newName := string(args[0]), string(args[1])
	entity, exist := db.GetEntity(oldName)
	if !exist {
		return reply.NewStatusReply("no such key")
	}
	db.PutEntity(newName, entity)
	db.RemoveEntity(oldName)
	db.addAof(utils.ToCmdLine3("rename", args...))
	return reply.NewOkReply()
}

// RENAMENX
func execRenameNX (db *DB, args[][]byte) resp.Reply {
	oldName, newName := string(args[0]), string(args[1])
	_, exist := db.GetEntity(newName)
	if exist {
		return reply.NewIntReply(0)
	}
	entity, exist := db.GetEntity(oldName)
	if !exist {
		return reply.NewStatusReply("no such key")
	}
	db.PutEntity(newName, entity)
	db.RemoveEntity(oldName)
	db.addAof(utils.ToCmdLine3("renamenx", args...))
	return reply.NewOkReply()
}

// KEYS pattern
func execKeys(db *DB, args[][]byte) resp.Reply {
	p, err := wildcard.CompilePattern(string(args[0]))
	if err != nil {
		return reply.NewErrReply("pattern error")
	}

	res := make([][]byte, 0)
	db.data.ForEach(func(key string, value interface{}) bool {
		if p.IsMatch(key) {
			res = append(res, []byte(key))
		}
		return true
	})
	return reply.NewMultiBulkReply(res)
}



