package etcd_test

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	etcd "github.com/josexy/etcdpkg"
)

var endpoints = []string{
	"127.0.0.1:22379",
}

func printKvs(t *testing.T, kvs []*etcd.KeyValue) {
	t.Log()
	for _, kv := range kvs {
		t.Log("->", kv.KeyString(), kv.ValueString(), kv.CreateRevision, kv.ModRevision, kv.Version, kv.Lease)
	}
}

func TestEtcdClient(t *testing.T) {
	client, err := etcd.NewClient(endpoints, 3)
	if err != nil {
		t.Fatal(err)
	}
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

func TestEtcdRegisterAndDiscovery(t *testing.T) {
	const prefix = "/prefix/"
	discovery, err := etcd.NewTypedDiscovery(endpoints, etcd.UnmarshalerFunc[string](func(d []byte) (string, error) {
		return string(d), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("watch prefix: %v", prefix)
	lw, err := discovery.ListWatch(context.Background(), prefix)
	if err != nil {
		t.Fatal(err)
	}
	lw.Watcher().AddEventHandler(etcd.EventHandlerFunc[string]{
		AddFunc: func(kv *etcd.TypedKeyObject[string]) {
			t.Logf("Add %s:%s\n", kv.Key, kv.Object)
		},
		UpdateFunc: func(oldkv, newKv *etcd.TypedKeyObject[string]) {
			t.Logf("Update => %s:%s --> %s:%s\n", oldkv.Key, oldkv.Object, newKv.Key, newKv.Object)
		},
		DeleteFunc: func(kv *etcd.TypedKeyObject[string]) {
			t.Logf("Delete %s:%s\n", kv.Key, kv.Object)
		},
	})

	go func() {
		service, err := etcd.NewTypedRegister(endpoints, 5, etcd.MarshalerFunc[string](func(v string) ([]byte, error) {
			return []byte(v), nil
		}))
		if err != nil {
			panic(err)
		}

		hostPort1 := net.JoinHostPort("127.0.0.1", "2003")
		hostPort2 := net.JoinHostPort("127.0.0.1", "2004")

		key1 := prefix + hostPort1
		key2 := prefix + hostPort2

		t.Log("add key", key1)
		t.Log("add key", key2)
		_ = service.Put(context.Background(), key1, hostPort1)
		_ = service.Put(context.Background(), key2, hostPort2)

		time.Sleep(time.Second)
		t.Log("update key", key2)
		_ = service.Put(context.Background(), key2, net.JoinHostPort("127.0.0.1", "2111"))

		time.Sleep(time.Second)
		t.Log("del key", key1)
		t.Log("del key", key2)
		service.Delete(context.Background(), key1)
		service.Delete(context.Background(), key2)

		service.Close()
		t.Log("service register close")
	}()

	time.Sleep(time.Second * 5)
	discovery.Close()
	time.Sleep(time.Millisecond * 100)
}

func TestDiscoveryStopWatch(t *testing.T) {
	discovery, err := etcd.NewTypedDiscovery(endpoints, etcd.UnmarshalerFunc[string](func(d []byte) (string, error) {
		return string(d), nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	lw, err := discovery.ListWatch(context.Background(), "/test")
	if err != nil {
		t.Fatal(err)
	}
	lw.Watcher().AddEventHandler(etcd.EventHandlerFunc[string]{
		AddFunc: func(kv *etcd.TypedKeyObject[string]) {
			t.Logf("Add %s:%s\n", kv.Key, kv.Object)
		},
		UpdateFunc: func(oldkv, newKv *etcd.TypedKeyObject[string]) {
			t.Logf("Update => %s:%s --> %s:%s\n", oldkv.Key, oldkv.Object, newKv.Key, newKv.Object)
		},
		DeleteFunc: func(kv *etcd.TypedKeyObject[string]) {
			t.Logf("Delete %s:%s\n", kv.Key, kv.Object)
		},
	})

	register, err := etcd.NewTypedRegister(endpoints, 7, etcd.MarshalerFunc[string](func(v string) ([]byte, error) { return []byte(v), nil }))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(time.Millisecond * 500)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				t.Log(ctx.Err())
				return
			case <-ticker.C:
				key := strconv.Itoa(time.Now().Second())
				register.Put(context.Background(), "/test2/"+key, key)
			}
		}
	}()
	go func() {
		time.Sleep(time.Second * 4)
		t.Log("stop watch")
		discovery.StopWatch("/test2")
	}()

	time.Sleep(time.Second * 5)
	t.Log("close register")
	cancel()
	register.Close()

	time.Sleep(time.Second)
	t.Log("close discover")
	discovery.Close()
	time.Sleep(time.Second)
	t.Log("done")
}
