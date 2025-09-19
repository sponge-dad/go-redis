package cluster

import (
	"context"
	"errors"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/client"
	"go-redis/resp/reply"
	"strconv"
)

func (cluster *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	pool, ok := cluster.peerConnection[peer]
	if !ok {
		return nil, errors.New("connection not found")
	}
	object, err := pool.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}
	c, ok := object.(*client.Client)
	if !ok {
		return nil, errors.New("wrong type")
	}
	return c, nil
}

func (cluster *ClusterDatabase) returnPeerClient(peer string, c *client.Client) error {
	pool, ok := cluster.peerConnection[peer]
	if !ok {
		return errors.New("connection not found")
	}
	return pool.ReturnObject(context.Background(), c)
}

func (cluster *ClusterDatabase) relay(peer string, c resp.Connection, args [][]byte) resp.Reply {
	if peer == cluster.self {
		return cluster.db.Exec(c, args)
	}
	target := cluster.peerPicker.PickNode(peer)
	cc, err := cluster.getPeerClient(target)
	if err != nil {
		return reply.NewErrReply(err.Error())
	}
	defer func() {
		_ = cluster.returnPeerClient(target, cc)
	}()
	cc.Send(utils.ToCmdLine("select", strconv.Itoa(c.GetDBIndex())))
	return cc.Send(args)
}

func (cluster *ClusterDatabase) broadcast(c resp.Connection, args [][]byte) map[string] resp.Reply {
	replies := make(map[string]resp.Reply)
	for _, node := range cluster.nodes {
		res := cluster.relay(node, c, args)
		replies[node] = res
	}
	return replies
}