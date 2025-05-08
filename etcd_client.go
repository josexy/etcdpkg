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
	opt                *options
	client             *clientv3.Client
	cancelFunc         func()
	leaseID            clientv3.LeaseID
	leaseKeepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
}

func NewClient(endpoints []string, opts ...Option) (*EtcdClient, error) {
	opt := newOption(opts...)
	config := clientv3.Config{
		Endpoints:            endpoints,
		DialTimeout:          opt.dialTimeout,
		DialKeepAliveTimeout: opt.dialTimeout,
		MaxUnaryRetries:      5,
		DialOptions:          []grpc.DialOption{grpc.WithBlock()},
	}
	if opt.tlsInfoS != nil {
		var err error
		if config.TLS, err = opt.tlsInfoS.ClientConfig(); err != nil {
			return nil, err
		}
	}
	c, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}

	for _, endpoint := range endpoints {
		ctx, cancel := context.WithTimeout(context.Background(), opt.contextTimeout)
		_, err = c.Status(ctx, endpoint)
		cancel()
		if err != nil {
			return nil, err
		}
	}

	client := &EtcdClient{
		client: c,
		opt:    opt,
	}

	if opt.leaseTTL > 0 {
		lease := clientv3.NewLease(c)
		ctx, cancel := context.WithTimeout(context.Background(), opt.contextTimeout)
		defer cancel()

		leaseRsp, err := lease.Grant(ctx, opt.leaseTTL)
		if err != nil {
			return nil, err
		}

		ctx, cancelFunc := context.WithCancel(context.Background())
		leaseRespChan, err := lease.KeepAlive(ctx, leaseRsp.ID)
		if err != nil || leaseRespChan == nil {
			cancelFunc()
			return nil, err
		}
		client.cancelFunc = cancelFunc
		client.leaseID = leaseRsp.ID
		client.leaseKeepAliveChan = leaseRespChan
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
		case rsp, ok := <-c.leaseKeepAliveChan:
			if !ok || rsp == nil {
				return
			}
		}
	}
}

func (c *EtcdClient) revoke() (err error) {
	// cancel/revoke the lease keepalive firstly
	c.cancelFunc()
	time.Sleep(time.Millisecond * 50)
	ctx, cancel := context.WithTimeout(context.Background(), c.opt.contextTimeout)
	defer cancel()
	_, err = c.client.Revoke(ctx, c.leaseID)
	return
}

func (c *EtcdClient) Close() error {
	if c.leaseID > 0 {
		c.revoke()
	}
	return c.client.Close()
}

func (c *EtcdClient) GetWithOpts(ctx context.Context, key string, opts ...clientv3.OpOption) (kvs []*KeyValue, err error) {
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
	kvs, err := c.GetWithOpts(ctx, key)
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
	return c.GetWithOpts(ctx, prefix, opts...)
}

func (c *EtcdClient) GetWithPrefix(ctx context.Context, prefix string) (kvs []*KeyValue, err error) {
	return c.GetWithOpts(ctx, prefix, clientv3.WithPrefix())
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
			// append char '\0' to string
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
