package cluster

import (
	"context"
	pool "github.com/jolestar/go-commons-pool"
	"go-redis/config"
	"go-redis/database"
	databaseface "go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/lib/consistenthash"
	"go-redis/logger"
	"go-redis/resp/reply"
	"strings"
)

type ClusterDatabase struct {
	self string
	db databaseface.Database

	nodes []string
	peerPicker *consistenthash.NodeMap // 节点选择器
	peerConnection map[string] *pool.ObjectPool
}


func NewClusterDatabase() *ClusterDatabase {
	clusterDatabase := &ClusterDatabase{
		self: config.Properties.Self,
		db: database.NewStandaloneDatabase(),
		peerPicker:consistenthash.NewNodeMap(nil),
		peerConnection: make(map[string]*pool.ObjectPool),
	}
	n := make([]string, 0, len(config.Properties.Peers) + 1)
	for _, node := range config.Properties.Peers {
		n = append(n, node)
	}
	n = append(n, clusterDatabase.self)
	clusterDatabase.nodes = n
	clusterDatabase.peerPicker.AddNodes(n...)
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		 clusterDatabase.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &ConnectionFactory{peer})
	}
	return clusterDatabase
}

func (cluster *ClusterDatabase) Exec(client resp.Connection, args databaseface.CmdLine) resp.Reply {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()
	router := makeRouter()
	cmdName := strings.ToLower(string(args[0]))
	cmdFunc, ok := router[cmdName]
	if !ok {
		return reply.NewErrReply("not supported cmd" + cmdName)
	}
	return cmdFunc(cluster, client, args)
}

func (cluster *ClusterDatabase) Close() error {
	return cluster.db.Close()
}


func (cluster *ClusterDatabase) AfterClientClose(c resp.Connection) error {
	return cluster.db.AfterClientClose(c)
}


