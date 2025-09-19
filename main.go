package main

import (
	"fmt"
	"go-redis/config"
	"go-redis/logger"
	"go-redis/resp/handler"
	"go-redis/tcp"
	"os"
)

const configFile string = "redis.conf"

var defaultProperties = &config.ServerProperties{
	Bind: "0.0.0.0",
	Port: 19222,
}


func fileExist(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	logger.Setup(&logger.Settings{
		Path: "logs",
		Name:"godis",
		Ext:"log",
		TimeFormat: "2006-01-02",
	})

	if fileExist(configFile) {
		config.SetupConfig(configFile)
	} else {
		config.Properties = defaultProperties
	}

	err := tcp.ListenAndServeWithSignal(
		tcp.Config{
			Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
		},
		handler.NewRespHandler(),
	)
	if err != nil {
		return 
	}
}

// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
// *3\r\n$6\r\nGETSET\r\n$3\r\nkey\r\n$6\r\nsponge\r\n
// *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
// *2\r\n$6\r\nselect\r\n$1\r\n1\r\n