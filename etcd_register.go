package etcd

import "context"

type Register[T any] struct {
	*EtcdClient
	marshaler Marshaler[T]
}

func NewTypedRegister[T any](endpoints []string, leaseTTL int64, marshaler Marshaler[T]) (*Register[T], error) {
	client, err := NewClient(endpoints, leaseTTL)
	if err != nil {
		return nil, err
	}
	return &Register[T]{
		EtcdClient: client,
		marshaler:  marshaler,
	}, nil
}

func (r *Register[T]) Register(ctx context.Context, key string, value T) error {
	data, err := r.marshaler.MarshalValue(value)
	if err != nil {
		return err
	}
	return r.Put(ctx, key, string(data))
}

func (r *Register[T]) Deregister(ctx context.Context, key string) error {
	return r.Delete(ctx, key)
}
