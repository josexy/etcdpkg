package registry

import (
	"context"
	"sync"

	etcdpkg "github.com/josexy/etcdpkg"
	"github.com/josexy/etcdpkg/service"
	"github.com/josexy/etcdpkg/watcher"
)

const defaultNamespace = "/default-services"

// Registrar is service registrar.
type Registrar interface {
	// Register the registration.
	Register(ctx context.Context, service *service.ServiceInstance) error
	// Deregister the registration.
	Deregister(ctx context.Context, service *service.ServiceInstance) error
}

// Discovery is service discovery.
type Discovery interface {
	// GetService return the service instances in memory according to the service name.
	GetService(ctx context.Context, serviceName string) ([]*service.ServiceInstance, error)
	// Watch creates a watcher according to the service name.
	Watch(ctx context.Context, serviceName string) (watcher.Watcher, error)
}

type ServiceRegistry interface {
	Registrar
	Discovery
}

type etcdRegistrar struct {
	informer  *etcdpkg.Informer[*service.ServiceInstance]
	namespace string // prefix to the key

	mu  sync.RWMutex
	lws map[string]etcdpkg.ListWatcher[*service.ServiceInstance]
}

func NewRegistrar(informer *etcdpkg.Informer[*service.ServiceInstance]) ServiceRegistry {
	return &etcdRegistrar{
		namespace: defaultNamespace,
		informer:  informer,
		lws:       make(map[string]etcdpkg.ListWatcher[*service.ServiceInstance]),
	}
}
