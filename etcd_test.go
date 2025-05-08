package etcd_test

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	etcdpkg "github.com/josexy/etcdpkg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var endpoints = []string{
	"127.0.0.1:22379",
}

func printKvs(t *testing.T, kvs []*etcdpkg.KeyValue) {
	t.Log()
	for _, kv := range kvs {
		t.Log("->", kv.KeyString(), kv.ValueString(), kv.CreateRevision, kv.ModRevision, kv.Version, kv.Lease)
	}
}

func TestEtcdClient(t *testing.T) {
	client, err := etcdpkg.NewClient(endpoints, etcdpkg.WithLeaseTTL(3))
	require.NoError(t, err)
	defer client.Close()
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		client.Put(ctx, "key"+strconv.Itoa(i), "value"+strconv.Itoa(i))
	}

	kvs, _ := client.GetWithSort(ctx, "key", 0, true)
	printKvs(t, kvs)

	client.Put(ctx, "key2", "test")
	client.Put(ctx, "key3", "test")
	client.Put(ctx, "key3", "test2")

	kvs, _ = client.GetWithSort(ctx, "key", 0, true)
	printKvs(t, kvs)

	client.DeleteWithPrefix(ctx, "key")
}

func TestEtcdInformer(t *testing.T) {
	const prefix = "/prefix/"
	marshaler := etcdpkg.EncoderFunc[string](func(v string) ([]byte, error) {
		return []byte(v), nil
	})
	unmarshaler := etcdpkg.DecoderFunc[string](func(d []byte) (string, error) {
		return string(d), nil
	})

	informer, err := etcdpkg.NewInformer(endpoints, etcdpkg.InformerOption[string]{
		Encoder: marshaler,
		Decoder: unmarshaler,
	})
	require.NoError(t, err)

	t.Logf("watch prefix: %v", prefix)
	lw, err := informer.ListWatch(context.Background(), prefix)
	require.NoError(t, err)

	lw.Watcher().AddEventHandler(etcdpkg.EventHandlerFunc[string]{
		AddFunc: func(kv *etcdpkg.KeyObject[string]) {
			t.Logf("Add %s:%s\n", kv.Key, kv.Object)
		},
		UpdateFunc: func(oldkv, newKv *etcdpkg.KeyObject[string]) {
			t.Logf("Update => %s:%s --> %s:%s\n", oldkv.Key, oldkv.Object, newKv.Key, newKv.Object)
		},
		DeleteFunc: func(kv *etcdpkg.KeyObject[string]) {
			t.Logf("Delete %s:%s\n", kv.Key, kv.Object)
		},
	})

	go func() {
		informer, err := etcdpkg.NewInformer(endpoints, etcdpkg.InformerOption[string]{
			Encoder: marshaler,
			Decoder: unmarshaler,
		}, etcdpkg.WithLeaseTTL(5))
		if err != nil {
			panic(err)
		}

		hostPort1 := net.JoinHostPort("127.0.0.1", "2003")
		hostPort2 := net.JoinHostPort("127.0.0.1", "2004")

		key1 := prefix + hostPort1
		key2 := prefix + hostPort2

		t.Log("add key", key1)
		t.Log("add key", key2)
		_ = informer.Put(context.Background(), key1, hostPort1)
		_ = informer.Put(context.Background(), key2, hostPort2)

		time.Sleep(time.Second)
		t.Log("update key", key2)
		_ = informer.Put(context.Background(), key2, net.JoinHostPort("127.0.0.1", "2111"))

		time.Sleep(time.Second)
		t.Log("del key", key1)
		t.Log("del key", key2)
		informer.Delete(context.Background(), key1)
		informer.Delete(context.Background(), key2)

		informer.Close()
		t.Log("service register close")
	}()

	time.Sleep(time.Second * 5)
	informer.Close()
	time.Sleep(time.Millisecond * 100)
}

func TestEtcdPagination(t *testing.T) {
	client, err := etcdpkg.NewClient(endpoints, etcdpkg.WithLeaseTTL(10))
	assert.Nil(t, err)
	defer client.Close()

	kvs, err := client.GetWithPrefix(context.Background(), "test")
	assert.Nil(t, err)
	t.Log("list kvs", len(kvs))
	for _, kv := range kvs {
		t.Log(kv.String())
	}
	for i := 0; i < 10; i++ {
		client.Put(context.Background(), "test/key"+strconv.Itoa(i), time.Now().String())
	}
	kvs, err = client.GetWithSort(context.Background(), "test", 0, true)
	assert.Nil(t, err)
	t.Log("list kvs", len(kvs))
	for _, kv := range kvs {
		t.Log(kv.String())
	}
	for rr := range client.GetWithRange(context.Background(), "test", 3) {
		t.Log("-->", len(rr.Kvs), rr.Err)
	}
}
