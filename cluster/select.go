package cluster

import (
	"go-redis/interface/resp"
)

func execSelect (cluster *ClusterDatabase, c resp.Connection, cmdArg [][]byte) resp.Reply  {
	return cluster.db.Exec(c, cmdArg)
}
