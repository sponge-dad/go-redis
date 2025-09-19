package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

func flushdb (cluster *ClusterDatabase, c resp.Connection, cmdArg [][]byte) resp.Reply {
	replies := cluster.broadcast(c, cmdArg)
	var errReply reply.ErrorReply
	for _, rep := range replies {
		if reply.IsErrReply(rep) {
			errReply = rep.(reply.ErrorReply)
			break
		}
	}
	if errReply == nil {
		return reply.NewOkReply()
	}
	return reply.NewErrReply("error: " + errReply.Error())
}
