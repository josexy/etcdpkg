package registry_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	etcdpkg "github.com/josexy/etcdpkg"
	"github.com/josexy/etcdpkg/registry"
	"github.com/josexy/etcdpkg/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var endpoints = []string{
	"127.0.0.1:22379",
}

func TestEtcdRegistry(t *testing.T) {
	informer, err := etcdpkg.NewInformer(endpoints, etcdpkg.InformerOption[*service.ServiceInstance]{
		Encoder: service.DefaultJSONEncoder(),
		Decoder: service.DefaultJSONDecoder(),
	}, etcdpkg.WithLeaseTTL(5))
	require.Nil(t, err)
	registry := registry.NewRegistrar(informer)
	testRegistryOperations(t, registry)
}

func testRegistryOperations(t *testing.T, registry registry.ServiceRegistry) {
	ctx := context.Background()

	svcName := "test-faked-svc"
	svcAddr := "127.0.0.1"
	svcPort := 8080
	if hostAddr := getHostAddr(t); hostAddr != "" {
		svcAddr = hostAddr
	}

	var svcInstances []*service.ServiceInstance
	registerSvcInstances := func() {
		for i := 0; i < 2; i++ {
			svcInstance := &service.ServiceInstance{
				ID:      strconv.FormatInt(rand.Int64(), 10),
				Name:    svcName,
				Version: "v1.0.0",
				Metadata: map[string]string{
					"dev": "true",
				},
				Endpoints: []string{fmt.Sprintf("tcp://%s:%d", svcAddr, svcPort)},
			}
			assert.Nil(t, registry.Register(ctx, svcInstance))
			t.Log("register new service", svcInstance)
			svcInstances = append(svcInstances, svcInstance)
			time.Sleep(time.Second * 2)
		}
	}
	deregisterSvcInstances := func() {
		for _, svcInstance := range svcInstances {
			registry.Deregister(ctx, svcInstance)
			t.Log("deregister service", svcInstance)
			time.Sleep(time.Second * 2)
		}
	}

	registerSvcInstances()
	time.Sleep(time.Second * 5)

	listSvcInstances, err := registry.GetService(ctx, svcName)
	assert.Nil(t, err)
	for _, instance := range listSvcInstances {
		t.Log("->", instance)
	}

	watcher, err := registry.Watch(ctx, svcName)
	assert.Nil(t, err)
	go func() {
		for {
			svcInstances, err := watcher.Next()
			assert.Nil(t, err)
			t.Log("watch service", len(svcInstances))
			for _, svcInstance := range svcInstances {
				t.Log("watch service", svcInstance)
			}
			t.Log("watch service-----------")
		}
	}()
	time.Sleep(time.Second * 2)
	registerSvcInstances()
	time.Sleep(time.Second * 5)
	deregisterSvcInstances()
	time.Sleep(time.Second)
}

func getHostAddr(t *testing.T) string {
	var hostAddr netip.Addr
	addrs, err := net.InterfaceAddrs()
	assert.Nil(t, err)
	for _, addr := range addrs {
		prefix, err := netip.ParsePrefix(addr.String())
		assert.Nil(t, err)
		if prefix.Addr().Is4() && !prefix.Addr().IsLoopback() {
			hostAddr = prefix.Addr()
			break
		}
	}
	if hostAddr.IsValid() {
		return hostAddr.String()
	}
	return ""
}
