# etcdpkg
etcd client package wrapper with register/discovery

# install

```shell
go get github.com/josexy/etcdpkg
```

# usage

- Register
- Discovery

```go
import etcd "github.com/josexy/etcdpkg"

func main() {
	r, err := etcd.NewTypedRegister([]string{"127.0.0.1:2379"}, 10, etcd.MarshalerFunc[string](func(v string) ([]byte, error) {
		return []byte(v), nil
	}))
	if err != nil {
		log.Fatalln(err)
	}
	r.Register(context.Background(), "/prefix/svc1", "localhost")
	r.Close()
}
```

```go
func main() {
	d, err := etcd.NewTypedDiscovery([]string{"127.0.0.1:2379"}, etcd.UnmarshalerFunc[string](func(d []byte) (string, error) {
		return string(d), nil
	}))
	if err != nil {
		log.Fatalln(err)
	}
	lw, err := d.ListWatch(context.Background(), "/prefix")
	if err != nil {
		log.Fatalln(err)
	}
	lw.Watcher().AddEventHandler(etcd.EventHandlerFunc[string]{
		AddFunc: func(kv *etcd.TypedKeyObject[string]) {
			log.Printf("Add %s:%s\n", kv.Key, kv.Object)
		},
		UpdateFunc: func(oldkv, newKv *etcd.TypedKeyObject[string]) {
			log.Printf("Update %s:%s --> %s:%s\n", oldkv.Key, oldkv.Object, newKv.Key, newKv.Object)
		},
		DeleteFunc: func(kv *etcd.TypedKeyObject[string]) {
			log.Printf("Delete %s:%s\n", kv.Key, kv.Object)
		},
	})
	time.Sleep(time.Hour)
	d.Close()
}
```