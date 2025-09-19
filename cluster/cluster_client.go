package cluster

import (
	"context"
	"errors"
	pool "github.com/jolestar/go-commons-pool"
	"go-redis/resp/client"
)

type ConnectionFactory struct {
	Peer string
}

func (f *ConnectionFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	c, err := client.MakeClient(f.Peer)
	if err != nil {
		return nil, err
	}
	c.Start()
	obj := pool.NewPooledObject(c)
	return obj, nil
}

func (f *ConnectionFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	c, ok := object.Object.(*client.Client)
	if !ok {
		return errors.New("type mismatch")
	}
	c.Close()
	return nil
}

func (f *ConnectionFactory) ValidateObject(ctx context.Context, object *pool.PooledObject) bool {
	c := object.Object.(*client.Client)
	reply := c.Send([][]byte{[]byte("PING")})
	return reply != nil && reply.ToBytes() != nil && string(reply.ToBytes()) == "+PONG\r\n"
}

func (f *ConnectionFactory) ActivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}

func (f *ConnectionFactory) PassivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}




