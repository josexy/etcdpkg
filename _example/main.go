package main

import (
	"context"
	"log"
	"math/rand/v2"
	"strconv"

	etcdpkg "github.com/josexy/etcdpkg"
	"github.com/josexy/etcdpkg/registry"
	"github.com/josexy/etcdpkg/service"
)

func main() {
	informer, err := etcdpkg.NewInformer([]string{"127.0.0.1:22379"},
		etcdpkg.InformerOption[*service.ServiceInstance]{
			Encoder: service.DefaultJSONEncoder(),
			Decoder: service.DefaultJSONDecoder(),
		}, etcdpkg.WithLeaseTTL(5))
	if err != nil {
		panic(err)
	}
	defer informer.Close()
	registry := registry.NewRegistrar(informer)
	registry.Register(context.Background(), &service.ServiceInstance{
		ID:        strconv.FormatInt(rand.Int64(), 10),
		Name:      "test-svc",
		Version:   "v1.0.0",
		Metadata:  map[string]string{},
		Endpoints: []string{"tcp://127.0.0.1:8080"},
	})
	watcher, _ := registry.Watch(context.Background(), "test-svc")
	defer watcher.Stop()
	svcInstances, _ := watcher.Next()
	for _, svcInstance := range svcInstances {
		log.Println("watch service", svcInstance)
	}
}
