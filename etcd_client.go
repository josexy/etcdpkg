package etcd

import (
	"context"
	"iter"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

type EtcdClient struct {
	client        *clientv3.Client
	leaseID       clientv3.LeaseID
	cancelFunc    func()
	keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
}

func NewClient(endpoints []string, leaseTTL int64) (*EtcdClient, error) {
	c, err := clientv3.New(clientv3.Config{
		Endpoints:       endpoints,
		DialTimeout:     5 * time.Second,
		MaxUnaryRetries: 5,
		DialOptions: []grpc.DialOption{
			grpc.WithBlock(),
		},
	})
	if err != nil {
		return nil, err
	}

	_, err = c.Status(context.Background(), endpoints[0])
	if err != nil {
		return nil, err
	}

	client := &EtcdClient{
		client: c,
	}
	if leaseTTL > 0 {
		lease := clientv3.NewLease(c)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		leaseResp, err := lease.Grant(ctx, leaseTTL)
		if err != nil {
			return nil, err
		}

		ctx, cancelFunc := context.WithCancel(context.Background())
		leaseRespChan, err := lease.KeepAlive(ctx, leaseResp.ID)
		if err != nil || leaseRespChan == nil {
			cancelFunc()
			return nil, err
		}
		client.leaseID = leaseResp.ID
		client.cancelFunc = cancelFunc
		client.keepAliveChan = leaseRespChan
		go client.keepalive()
	}
	return client, nil
}

func (c *EtcdClient) Client() *clientv3.Client { return c.client }

func (c *EtcdClient) LeaseID() clientv3.LeaseID { return c.leaseID }

func (c *EtcdClient) keepalive() {
	for {
		select {
		case <-c.client.Ctx().Done():
			return
		case rsp, ok := <-c.keepAliveChan:
			if !ok || rsp == nil {
				return
			}
		}
	}
}

func (c *EtcdClient) revoke() (err error) {
	c.cancelFunc()
	time.Sleep(time.Millisecond * 50)
	_, err = c.client.Revoke(context.Background(), c.leaseID)
	return
}

func (c *EtcdClient) Close() (err error) {
	if c.leaseID > 0 {
		c.revoke()
	}
	return c.client.Close()
}

func (c *EtcdClient) get(ctx context.Context, key string, opts ...clientv3.OpOption) (kvs []*KeyValue, err error) {
	resp, err := c.client.Get(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return
	}
	kvs = make([]*KeyValue, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		kvs = append(kvs, convertKeyValue(kv))
	}
	return
}

func (c *EtcdClient) Get(ctx context.Context, key string) (*KeyValue, error) {
	kvs, err := c.get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 {
		return nil, nil
	}
	return kvs[0], nil
}

func (c *EtcdClient) GetWithSort(ctx context.Context, prefix string, limit int64, ascend bool) (kvs []*KeyValue, err error) {
	var opts []clientv3.OpOption
	opts = append(opts, clientv3.WithPrefix(), clientv3.WithLimit(limit))
	var order clientv3.SortOrder
	if ascend {
		order = clientv3.SortAscend
	} else {
		order = clientv3.SortDescend
	}
	opts = append(opts, clientv3.WithSort(clientv3.SortByKey, order))
	return c.get(ctx, prefix, opts...)
}

func (c *EtcdClient) GetWithPrefix(ctx context.Context, prefix string) (kvs []*KeyValue, err error) {
	return c.get(ctx, prefix, clientv3.WithPrefix())
}

type RangeResult struct {
	Kvs []*KeyValue
	Err error
}

func (c *EtcdClient) GetWithRange(ctx context.Context, prefix string, limit int64) iter.Seq[*RangeResult] {
	opts := []clientv3.OpOption{
		clientv3.WithLimit(limit), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(prefix)),
	}
	key := prefix
	return func(yield func(*RangeResult) bool) {
		for {
			resp, err := c.client.Get(ctx, key, opts...)
			if err != nil {
				yield(&RangeResult{Err: err})
				break
			}
			n := len(resp.Kvs)
			if n == 0 {
				break
			}
			kvs := make([]*KeyValue, 0, n)
			for _, kv := range resp.Kvs {
				kvs = append(kvs, convertKeyValue(kv))
			}
			if !yield(&RangeResult{Kvs: kvs}) {
				break
			}
			key = string(append(resp.Kvs[n-1].Key, 0))
			if !resp.More {
				break
			}
		}
	}
}

func (c *EtcdClient) Delete(ctx context.Context, key string) (err error) {
	_, err = c.client.Delete(ctx, key)
	return
}

func (c *EtcdClient) DeleteWithPrefix(ctx context.Context, prefix string) (err error) {
	_, err = c.client.Delete(ctx, prefix, clientv3.WithPrefix())
	return
}

func (c *EtcdClient) Put(ctx context.Context, key string, value string) (err error) {
	var opts []clientv3.OpOption
	if c.leaseID > 0 {
		opts = append(opts, clientv3.WithLease(c.leaseID))
	}
	_, err = c.client.Put(ctx, key, value, opts...)
	return
}

func convertKeyValue(kv *mvccpb.KeyValue) *KeyValue {
	return &KeyValue{
		Key:            kv.Key,
		Value:          kv.Value,
		Lease:          kv.Lease,
		Version:        kv.Version,
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
	}
}
