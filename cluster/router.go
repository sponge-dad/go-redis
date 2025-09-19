package cluster

import (
	"go-redis/interface/resp"
)

type CmdFunc func(cluster *ClusterDatabase, c resp.Connection, cmdArg [][]byte) resp.Reply

func makeRouter() map[string]CmdFunc {
	m := make(map[string]CmdFunc)
	m["exist"] = defaultFunc
	m["del"] = del
	m["exists"] = defaultFunc
	m["flushdb"] = flushdb
	m["type"] = defaultFunc
	m["rename"] = rename
	m["renamenx"] = rename
	m["keys"] = defaultFunc
	m["get"] = defaultFunc
	m["set"] = defaultFunc
	m["setnx"] = defaultFunc
	m["getset"] = defaultFunc
	m["getstrlen"] = defaultFunc
	m["select"] = execSelect
	return m
}

func defaultFunc(cluster *ClusterDatabase, c resp.Connection, cmdArg [][]byte) resp.Reply {
	key := string(cmdArg[1])
	node := cluster.peerPicker.PickNode(key)
	return cluster.relay(node, c, cmdArg)
}


