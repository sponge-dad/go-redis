package database

import (
	"go-redis/aof"
	"go-redis/config"
	databaseface "go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/logger"
	"go-redis/resp/reply"
	"strconv"
	"strings"
)

type StandaloneDatabase struct {
	dbSet []*DB
	aofHandler *aof.AofHandler
}

func NewStandaloneDatabase() *StandaloneDatabase {
	database := &StandaloneDatabase{}
	if config.Properties.Databases <= 0 {
		config.Properties.Databases = 16
	}
	database.dbSet = make([]*DB, config.Properties.Databases)
	for i := range database.dbSet {
		db := newDB()
		db.index = i
		database.dbSet[i] = db
	}

	if config.Properties.AppendOnly {
		aofHandler ,err := aof.NewAofHandler(database)
		if err != nil {
			panic(err)
		}
		database.aofHandler = aofHandler
		for _, db := range database.dbSet {
			idb := db
			idb.addAof = func(line CmdLine) {
				database.aofHandler.AddAof(idb.index, line)
			}
		}
	}

	return database
}

func (d *StandaloneDatabase) Exec(client resp.Connection, args databaseface.CmdLine) resp.Reply {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()
	cmd := strings.ToLower(string(args[0]))
	if cmd == "select" {
		if len(args) != 2 {
			return reply.NewArgNumErrReply("select")
		}
		return execSelect(client, d, args[1:])
	}
	dbIndex := client.GetDBIndex()
	db := d.dbSet[dbIndex]

	return db.Exec(client, args)
}

func (d *StandaloneDatabase) Close() error {
	return nil

}

func (d *StandaloneDatabase) AfterClientClose(c resp.Connection) error {
	return nil
}

func execSelect(c resp.Connection, database *StandaloneDatabase, args databaseface.CmdLine) resp.Reply{
	id, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.NewErrReply("ERR invalid DB index")
	}
	if id >= len(database.dbSet) {
		return reply.NewErrReply("ERR DB index is out of range")
	}
	c.SelectDB(id)
	return reply.NewOkReply()
}

