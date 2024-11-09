package etcd

import (
	"context"
	"log/slog"
	"sync"

	"github.com/josexy/etcdpkg/storeindexer"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type WatchType byte

const (
	DeltaAdd WatchType = iota
	DeltaUpdate
	DeltaDelete
)

func (op WatchType) String() string {
	switch op {
	case DeltaAdd:
		return "DeltaAdd"
	case DeltaUpdate:
		return "DeltaUpdate"
	case DeltaDelete:
		return "DeltaDelete"
	default:
		return "Unknown"
	}
}

type (
	Store[T any]           storeindexer.Store[T]
	Marshaler[T any]       interface{ MarshalValue(T) ([]byte, error) }
	Unmarshaler[T any]     interface{ UnmarshalValue([]byte) (T, error) }
	MarshalerFunc[T any]   func(T) ([]byte, error)
	UnmarshalerFunc[T any] func([]byte) (T, error)
)

func (m MarshalerFunc[T]) MarshalValue(v T) ([]byte, error)     { return m(v) }
func (m UnmarshalerFunc[T]) UnmarshalValue(v []byte) (T, error) { return m(v) }

type Delta struct {
	Type WatchType
	Kv   *KeyValue
}

type Discovery[T any] struct {
	*EtcdClient
	lws         sync.Map
	unmarshaler Unmarshaler[T]
}

func NewTypedDiscovery[T any](endpoints []string, unmarshal Unmarshaler[T]) (*Discovery[T], error) {
	client, err := NewClient(endpoints, 0)
	if err != nil {
		return nil, err
	}
	return NewTypedDiscoveryFromClient[T](client, unmarshal), nil
}

func NewTypedDiscoveryFromClient[T any](client *EtcdClient, unmarshaler Unmarshaler[T]) *Discovery[T] {
	return &Discovery[T]{EtcdClient: client, unmarshaler: unmarshaler}
}

func (discovery *Discovery[T]) ListWatch(ctx context.Context, prefix string) (lw ListWatcher[T], err error) {
	if value, ok := discovery.lws.Load(prefix); ok {
		return value.(ListWatcher[T]), nil
	}
	store := storeindexer.New[T]()
	kvs, err := discovery.GetWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	// List
	for _, kv := range kvs {
		obj, err := discovery.unmarshaler.UnmarshalValue(kv.Value)
		if err != nil {
			return nil, err
		}
		store.AddIfNotPresent(kv.KeyString(), obj)
	}
	lw = newListerWatcher(ctx, store)
	discovery.lws.Store(prefix, lw)
	// Watch
	discovery.watchRes(prefix, lw)
	return
}

func (discovery *Discovery[T]) watchRes(prefix string, lw ListWatcher[T]) {
	eventsCh := make(chan []Delta, 2048)

	go processWatcher(lw.Watcher(), discovery.EtcdClient, prefix, eventsCh)
	go processDeltas(lw.Watcher(), discovery.unmarshaler, eventsCh)
}

func (discovery *Discovery[T]) StopWatch(prefix string) {
	if value, ok := discovery.lws.Load(prefix); ok {
		lw := value.(ListWatcher[T])
		lw.Watcher().Stop()
		lw.Watcher().GetStore().Clear()
	}
	discovery.lws.Delete(prefix)
}

func (discovery *Discovery[T]) Close() error {
	discovery.lws.Range(func(key, value any) bool {
		lw := value.(ListWatcher[T])
		lw.Watcher().Stop()
		lw.Watcher().GetStore().Clear()
		return true
	})
	discovery.lws.Clear()
	return discovery.EtcdClient.Close()
}

func processWatcher[T any](watcher Watcher[T], client *EtcdClient, prefix string, eventsCh chan<- []Delta) {
	defer func() {
		if e := recover(); e != nil {
			slog.Error("processWatcher panic", "error", e)
		}
	}()

	watchChan := client.Client().Watch(watcher.Context(), prefix, clientv3.WithPrefix())
	for {
		select {
		case <-watcher.Context().Done():
			return
		case watchResp := <-watchChan:
			if err := watchResp.Err(); err != nil {
				return
			}
			if len(watchResp.Events) == 0 {
				continue
			}
			deltas := make([]Delta, 0, len(watchResp.Events))
			for _, event := range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					var watchType WatchType
					if event.IsCreate() {
						watchType = DeltaAdd
					} else {
						watchType = DeltaUpdate
					}
					deltas = append(deltas, Delta{Type: watchType, Kv: convertKeyValue(event.Kv)})
				case mvccpb.DELETE:
					deltas = append(deltas, Delta{Type: DeltaDelete, Kv: convertKeyValue(event.Kv)})
				}
			}
			select {
			case eventsCh <- deltas:
			default:
			}
		}
	}
}

func processDeltas[T any](watcher Watcher[T], unmarshaler Unmarshaler[T], eventsCh <-chan []Delta) {
	defer func() { recover() }()
	for {
		select {
		case <-watcher.Context().Done():
			return
		case events, ok := <-eventsCh:
			if !ok {
				return
			}
			for _, event := range events {
				switch event.Type {
				case DeltaAdd, DeltaUpdate:
					newObj, err := unmarshaler.UnmarshalValue(event.Kv.Value)
					if err != nil {
						continue
					}
					if oldObj, _, ok := watcher.GetStore().Get(event.Kv.KeyString()); ok {
						watcher.GetStore().Update(event.Kv.KeyString(), newObj)
						if handler := watcher.GetEventHandler(); handler != nil {
							handler.OnUpdate(
								&TypedKeyObject[T]{Key: event.Kv.KeyString(), Object: oldObj},
								&TypedKeyObject[T]{Key: event.Kv.KeyString(), Object: newObj},
							)
						}
					} else {
						watcher.GetStore().AddIfNotPresent(event.Kv.KeyString(), newObj)
						if handler := watcher.GetEventHandler(); handler != nil {
							handler.OnAdd(&TypedKeyObject[T]{Key: event.Kv.KeyString(), Object: newObj})
						}
					}
				case DeltaDelete:
					oldObj, _, ok := watcher.GetStore().Get(event.Kv.KeyString())
					if !ok {
						continue
					}
					if watcher.GetStore().Remove(event.Kv.KeyString()); !ok {
						continue
					}
					if handler := watcher.GetEventHandler(); handler != nil {
						handler.OnDelete(&TypedKeyObject[T]{Key: event.Kv.KeyString(), Object: oldObj})
					}
				}
			}
		}
	}
}
