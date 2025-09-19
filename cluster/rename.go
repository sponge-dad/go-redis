package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

// rename key1 key2
func rename(cluster *ClusterDatabase, c resp.Connection, cmdArg [][]byte) resp.Reply {
	if len(cmdArg) != 3 {
		return reply.NewErrReply("ERR Wrong number args")
	}
	key1, key2 := string(cmdArg[1]), string(cmdArg[2])
	node1 := cluster.peerPicker.PickNode(key1)
	node2 := cluster.peerPicker.PickNode(key2)
	if node1 != node2 {
		return reply.NewErrReply("ERR rename must within on peer")
	}
	return cluster.db.Exec(c, cmdArg)
}

