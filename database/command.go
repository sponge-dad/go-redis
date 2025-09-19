package database

import (
	"strings"
)
// 策略模式
var cmdTable = make(map[string]*command)
type command struct {
	exector ExecFunc
	arity int // number of args
}

func RegisterCommand(name string, exector ExecFunc, arity int) {
	cmdTable[strings.ToLower(name)] = &command{
		exector: exector,
		arity: arity,
	}
}
