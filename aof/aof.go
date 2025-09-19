package aof

import (
	"go-redis/config"
	databaseface "go-redis/interface/database"
	"go-redis/lib/utils"
	"go-redis/logger"
	"go-redis/resp/connection"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"io"
	"os"
	"strconv"
)

type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 16
)

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

// AofHandler 作用是：
// 1. 从管道中接收数据
// 2. 写入Aof文件
type AofHandler struct {
	db databaseface.Database
	aofChan chan *payload
	aofFile *os.File
	aofFilename string
	currentDB int
}

// NewAofHandler 构造函数
func NewAofHandler(db databaseface.Database) (*AofHandler, error) {
	handler := &AofHandler{
		db:db,
		aofFilename: config.Properties.AppendFilename,
	}
	// LoadAof
	err := handler.LoadAof()
	if err != nil {
		return nil, err
	}

	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile
	// channel
	handler.aofChan = make(chan *payload, aofQueueSize)
	go func() {
		handler.handleAof()
	}()

	return handler, nil
}


// AddAof：用户的指令包装成payload放入管道
func (handler *AofHandler) AddAof(dbIndex int, cmd CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payload{
			cmdLine: cmd,
			dbIndex: dbIndex,
		}
	}
}


// handleAof 将管道中的payload写入磁盘
func (handler *AofHandler) handleAof() {
	handler.currentDB = 0
	for p := range handler.aofChan {
		if p.dbIndex != handler.currentDB {
			data := reply.NewMultiBulkReply(utils.ToCmdLine("select", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Error(err)
				continue
			}
			handler.currentDB = p.dbIndex
		}
		data := reply.NewMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Error(err)
			continue
		}
	}
}

// LoadAof 重启Redis后加载aof文件
func (handler *AofHandler) LoadAof() error {
	file, err := os.Open(handler.aofFilename)
	if err != nil {
		return err
	}
	defer file.Close()
	ch := parser.ParseStream(file)
	fakeConn := &connection.Connection{}
	for p := range ch {
		if p.Err != nil {
			if p.Err  == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}

		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		ret := handler.db.Exec(fakeConn, r.Args)
		if reply.IsErrReply(ret) {
			logger.Error("exec error", ret.ToBytes())
		}
	}
	return nil
}


