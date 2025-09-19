package database

import (
	"go-redis/datastruct/dict"
)

func makeTestDB() *DB {
	return &DB{
		data:   dict.NewSyncDict(),
		addAof: func(line CmdLine) {

		},
	}
}
