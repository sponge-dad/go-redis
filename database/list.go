package database

import (
	List "go-redis/datastruct/list"
	databaseface "go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
	"strconv"
)

func init() {
	RegisterCommand("LPush", LPush, -3)
	RegisterCommand("LPushX", LPushX, -3)
	RegisterCommand("RPush", RPush, -3)
	RegisterCommand("RPushX", RPushX, -3)
	RegisterCommand("LPop", LPop, -2)
	RegisterCommand("RPop", RPop, -2)
	RegisterCommand("RPopLPush", RPopLPush, 3)
	RegisterCommand("LRem", LRem, 4)
	RegisterCommand("LLen", LLen, 2)
	RegisterCommand("LIndex", LIndex, 3)
	RegisterCommand("LSet", LSet, 4)
	RegisterCommand("LRange", LRange, 4)
}


func (db *DB) getAsList(key string) (*List.LinkList, reply.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	bytes, ok := entity.Data.(*List.LinkList)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return bytes, nil
}

func (db *DB) getOrInitList(key string) (list *List.LinkList, isNew bool, errReply reply.ErrorReply) {
	list, errReply = db.getAsList(key)
	if errReply != nil {
		return nil, false, errReply
	}
	isNew = false
	if list == nil {
		list = &List.LinkList{}
		db.PutEntity(key, &databaseface.DataEntity{
			Data: list,
		})
		isNew = true
	}
	return list, isNew, nil
}

// LIndex gets element of list at given list
func LIndex(db *DB, args [][]byte) resp.Reply {
	// parse args
	if len(args) != 2 {
		return reply.NewErrReply("ERR wrong number of arguments for 'lindex' command")
	}
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.NewErrReply("ERR value is not an integer or out of range")
	}
	index := int(index64)


	// get entity
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return &reply.NullBulkReply{}
	}

	size := list.Len() // assert: size > 0
	if index < -1*size {
		return &reply.NullBulkReply{}
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return &reply.NullBulkReply{}
	}

	val, _ := list.Get(index).([]byte)
	return reply.NewBulkReply(val)
}

// LLen gets length of list
func LLen(db *DB, args [][]byte) resp.Reply {
	// parse args
	if len(args) != 1 {
		return reply.NewErrReply("ERR wrong number of arguments for 'llen' command")
	}
	key := string(args[0])

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.NewIntReply(0)
	}

	size := int64(list.Len())
	return reply.NewIntReply(size)
}

// LPop removes the first element of list, and return it
func LPop(db *DB, args [][]byte) resp.Reply {
	// parse args
	if len(args) != 1 {
		return reply.NewErrReply("ERR wrong number of arguments for 'lindex' command")
	}
	key := string(args[0])


	// get data
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return &reply.NullBulkReply{}
	}

	val, _ := list.Remove(0).([]byte)
	if list.Len() == 0 {
		db.RemoveEntity(key)
	}
	db.addAof(utils.ToCmdLine3("lpop", args...))

	return reply.NewBulkReply(val)
}

// LPush inserts element at head of list
func LPush(db *DB, args [][]byte) resp.Reply {
	if len(args) < 2 {
		return reply.NewErrReply("ERR wrong number of arguments for 'lpush' command")
	}
	key := string(args[0])
	values := args[1:]

	// lock

	// get or init entity
	list, _, errReply := db.getOrInitList(key)
	if errReply != nil {
		return errReply
	}

	// insert
	for _, value := range values {
		list.Insert(0, value)
	}

	db.addAof(utils.ToCmdLine3("lpush", args...))
	return reply.NewIntReply(int64(list.Len()))
}

// LPushX inserts element at head of list, only if list exists
func LPushX(db *DB, args [][]byte) resp.Reply {
	if len(args) < 2 {
		return reply.NewErrReply("ERR wrong number of arguments for 'lpushx' command")
	}
	key := string(args[0])
	values := args[1:]

	// lock

	// get or init entity
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.NewIntReply(0)
	}

	// insert
	for _, value := range values {
		list.Insert(0, value)
	}
	db.addAof(utils.ToCmdLine3("lpushx", args...))
	return reply.NewIntReply(int64(list.Len()))
}

// LRange gets elements of list in given range
func LRange(db *DB, args [][]byte) resp.Reply {
	// parse args
	if len(args) != 3 {
		return reply.NewErrReply("ERR wrong number of arguments for 'lrange' command")
	}
	key := string(args[0])
	start64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.NewErrReply("ERR value is not an integer or out of range")
	}
	start := int(start64)
	stop64, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return reply.NewErrReply("ERR value is not an integer or out of range")
	}
	stop := int(stop64)

	// lock key

	// get data
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return &reply.NullMultiBulkReply{}
	}

	// compute index
	size := list.Len() // assert: size > 0
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return &reply.NullMultiBulkReply{}
	}
	if stop < -1*size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop + 1
	} else if stop < size {
		stop = stop + 1
	} else {
		stop = size
	}
	if stop < start {
		stop = start
	}

	// assert: start in [0, size - 1], stop in [start, size]
	slice := list.Range(start, stop)
	result := make([][]byte, len(slice))
	for i, raw := range slice {
		bytes, _ := raw.([]byte)
		result[i] = bytes
	}
	return reply.NewMultiBulkReply(result)
}

// LRem removes element of list at specified index
func LRem(db *DB, args [][]byte) resp.Reply {
	// parse args
	if len(args) != 3 {
		return reply.NewErrReply("ERR wrong number of arguments for 'lrem' command")
	}
	key := string(args[0])
	count64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.NewErrReply("ERR value is not an integer or out of range")
	}
	count := int(count64)
	value := args[2]

	// lock

	// get data entity
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.NewIntReply(0)
	}

	var removed int
	if count == 0 {
		removed = list.RemoveAllByVal(value)
	} else if count > 0 {
		removed = list.RemoveByVal(value, count)
	} else {
		removed = list.ReverseRemoveByVal(value, -count)
	}

	if list.Len() == 0 {
		db.RemoveEntity(key)
	}
	if removed > 0 {
		db.addAof(utils.ToCmdLine3("lrem", args...))
	}

	return reply.NewIntReply(int64(removed))
}

// LSet puts element at specified index of list
func LSet(db *DB, args [][]byte) resp.Reply {
	// parse args
	if len(args) != 3 {
		return reply.NewErrReply("ERR wrong number of arguments for 'lset' command")
	}
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.NewErrReply("ERR value is not an integer or out of range")
	}
	index := int(index64)
	value := args[2]

	// lock

	// get data
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.NewErrReply("ERR no such key")
	}

	size := list.Len() // assert: size > 0
	if index < -1*size {
		return reply.NewErrReply("ERR index out of range")
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return reply.NewErrReply("ERR index out of range")
	}

	list.Set(index, value)
	db.addAof(utils.ToCmdLine3("lset", args...))
	return &reply.OkReply{}
}

// RPop removes last element of list then return it
func RPop(db *DB, args [][]byte) resp.Reply {
	// parse args
	if len(args) != 1 {
		return reply.NewErrReply("ERR wrong number of arguments for 'rpop' command")
	}
	key := string(args[0])

	// lock

	// get data
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return &reply.NullBulkReply{}
	}

	val, _ := list.RemoveLast().([]byte)
	if list.Len() == 0 {
		db.RemoveEntity(key)
	}
	db.addAof(utils.ToCmdLine3("rpop", args...))
	return reply.NewBulkReply(val)
}

// RPopLPush pops last element of list-A then insert it to the head of list-B
func RPopLPush(db *DB, args [][]byte) resp.Reply {
	if len(args) != 2 {
		return reply.NewErrReply("ERR wrong number of arguments for 'rpoplpush' command")
	}
	sourceKey := string(args[0])
	destKey := string(args[1])

	// lock

	// get source entity
	sourceList, errReply := db.getAsList(sourceKey)
	if errReply != nil {
		return errReply
	}
	if sourceList == nil {
		return &reply.NullBulkReply{}
	}

	// get dest entity
	destList, _, errReply := db.getOrInitList(destKey)
	if errReply != nil {
		return errReply
	}

	// pop and push
	val, _ := sourceList.RemoveLast().([]byte)
	destList.Insert(0, val)

	if sourceList.Len() == 0 {
		db.RemoveEntity(sourceKey)
	}

	db.addAof(utils.ToCmdLine3("rpoplpush", args...))
	return reply.NewBulkReply(val)
}

// RPush inserts element at last of list
func RPush(db *DB, args [][]byte) resp.Reply {
	// parse args
	if len(args) < 2 {
		return reply.NewErrReply("ERR wrong number of arguments for 'rpush' command")
	}
	key := string(args[0])
	values := args[1:]

	// lock

	// get or init entity
	list, _, errReply := db.getOrInitList(key)
	if errReply != nil {
		return errReply
	}

	// put list
	for _, value := range values {
		list.Add(value)
	}
	db.addAof(utils.ToCmdLine3("rpush", args...))
	return reply.NewIntReply(int64(list.Len()))
}

// RPushX inserts element at last of list only if list exists
func RPushX(db *DB, args [][]byte) resp.Reply {
	if len(args) < 2 {
		return reply.NewErrReply("ERR wrong number of arguments for 'rpush' command")
	}
	key := string(args[0])
	values := args[1:]

	// lock

	// get or init entity
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.NewIntReply(0)
	}

	// put list
	for _, value := range values {
		list.Add(value)
	}
	db.addAof(utils.ToCmdLine3("rpushx", args...))

	return reply.NewIntReply(int64(list.Len()))
}
