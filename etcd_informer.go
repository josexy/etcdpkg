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
	EventAdded WatchType = iota
	EventUpdated
	EventDeleted
)

func (op WatchType) String() string {
	switch op {
	case EventAdded:
		return "Added"
	case EventUpdated:
		return "Updated"
	case EventDeleted:
		return "Deleted"
	default:
		return "Unknown"
	}
}

type (
	Store[T any] storeindexer.Store[T]

	Encoder[T any] interface{ Encode(T) ([]byte, error) }
	Decoder[T any] interface{ Decode([]byte) (T, error) }

	EncoderFunc[T any] func(T) ([]byte, error)
	DecoderFunc[T any] func([]byte) (T, error)
)

func (f EncoderFunc[T]) Encode(v T) ([]byte, error) { return f(v) }
func (f DecoderFunc[T]) Decode(v []byte) (T, error) { return f(v) }

type Delta struct {
	Type WatchType
	Kv   *KeyValue
}

type Informer[T any] struct {
	*EtcdClient
	lws     sync.Map
	encoder Encoder[T]
	decoder Decoder[T]
}

type InformerOption[T any] struct {
	Encoder Encoder[T]
	Decoder Decoder[T]
}

func NewInformer[T any](endpoints []string, informerOpt InformerOption[T], etcdOpt ...Option) (*Informer[T], error) {
	client, err := NewClient(endpoints, etcdOpt...)
	if err != nil {
		return nil, err
	}
	return NewInformerFromClient[T](client, informerOpt), nil
}

func NewInformerFromClient[T any](client *EtcdClient, informerOpt InformerOption[T]) *Informer[T] {
	return &Informer[T]{EtcdClient: client, encoder: informerOpt.Encoder, decoder: informerOpt.Decoder}
}

func (inf *Informer[T]) Set(ctx context.Context, key string, value T) error {
	data, err := inf.encoder.Encode(value)
	if err != nil {
		return err
	}
	return inf.Put(ctx, key, string(data))
}

func (inf *Informer[T]) ListWatch(ctx context.Context, prefix string) (lw ListWatcher[T], err error) {
	if value, ok := inf.lws.Load(prefix); ok {
		return value.(ListWatcher[T]), nil
	}
	store := storeindexer.New[T]()
	kvs, err := inf.GetWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	// List
	for _, kv := range kvs {
		obj, err := inf.decoder.Decode(kv.Value)
		if err != nil {
			return nil, err
		}
		store.AddIfNotPresent(kv.KeyString(), obj)
	}
	lw = newListerWatcher(ctx, store)
	inf.lws.Store(prefix, lw)
	// Watch
	inf.watchRes(prefix, lw)
	return
}

func (inf *Informer[T]) watchRes(prefix string, lw ListWatcher[T]) {
	eventsCh := make(chan []Delta, 2048)

	go processWatcher(lw.Watcher(), inf.EtcdClient, prefix, eventsCh)
	go processDeltas(lw.Watcher(), inf.decoder, eventsCh)
}

func (inf *Informer[T]) StopWatch(prefix string) {
	if value, ok := inf.lws.Load(prefix); ok {
		lw := value.(ListWatcher[T])
		lw.Watcher().Stop()
		lw.Watcher().GetStore().Clear()
	}
	inf.lws.Delete(prefix)
}

func (inf *Informer[T]) Close() error {
	inf.lws.Range(func(key, value any) bool {
		lw := value.(ListWatcher[T])
		lw.Watcher().Stop()
		lw.Watcher().GetStore().Clear()
		return true
	})
	inf.lws.Clear()
	return inf.EtcdClient.Close()
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
						watchType = EventAdded
					} else {
						watchType = EventUpdated
					}
					deltas = append(deltas, Delta{Type: watchType, Kv: convertKeyValue(event.Kv)})
				case mvccpb.DELETE:
					deltas = append(deltas, Delta{Type: EventDeleted, Kv: convertKeyValue(event.Kv)})
				}
			}
			select {
			case eventsCh <- deltas:
			default:
			}
		}
	}
}

func processDeltas[T any](watcher Watcher[T], decoder Decoder[T], eventsCh <-chan []Delta) {
	defer func() {
		if e := recover(); e != nil {
			slog.Error("processDeltas panic", "error", e)
		}
	}()
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
				case EventAdded, EventUpdated:
					newObj, err := decoder.Decode(event.Kv.Value)
					if err != nil {
						continue
					}
					if oldObj, _, ok := watcher.GetStore().Get(event.Kv.KeyString()); ok {
						watcher.GetStore().Update(event.Kv.KeyString(), newObj)
						if handler := watcher.GetEventHandler(); handler != nil {
							handler.OnUpdate(
								&KeyObject[T]{Key: event.Kv.KeyString(), Object: oldObj},
								&KeyObject[T]{Key: event.Kv.KeyString(), Object: newObj},
							)
						}
					} else {
						watcher.GetStore().AddIfNotPresent(event.Kv.KeyString(), newObj)
						if handler := watcher.GetEventHandler(); handler != nil {
							handler.OnAdd(&KeyObject[T]{Key: event.Kv.KeyString(), Object: newObj})
						}
					}
				case EventDeleted:
					oldObj, _, ok := watcher.GetStore().Get(event.Kv.KeyString())
					if !ok {
						continue
					}
					if watcher.GetStore().Remove(event.Kv.KeyString()); !ok {
						continue
					}
					if handler := watcher.GetEventHandler(); handler != nil {
						handler.OnDelete(&KeyObject[T]{Key: event.Kv.KeyString(), Object: oldObj})
					}
				}
			}
		}
	}
}
