package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

func del (cluster *ClusterDatabase, c resp.Connection, cmdArg [][]byte) resp.Reply {
	replies := cluster.broadcast(c, cmdArg)
	var errReply reply.ErrorReply
	var deleted int64
	for _, rep := range replies {
		if reply.IsErrReply(rep) {
			errReply = rep.(reply.ErrorReply)
			break
		}
		intReply, ok := rep.(*reply.IntReply)
		if !ok {
			errReply = reply.NewErrReply("error")
		}
		deleted += intReply.Code
	}
	if errReply == nil {
		return reply.NewIntReply(deleted)
	}
	return reply.NewErrReply("error: " + errReply.Error())
}

