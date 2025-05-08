package registry

import (
	"context"

	"github.com/josexy/etcdpkg/service"
)

func (r *etcdRegistrar) Register(ctx context.Context, service *service.ServiceInstance) error {
	if service == nil {
		return nil
	}
	key := r.namespace + "/" + service.Name + "/" + service.ID
	return r.informer.Set(ctx, key, service)
}

func (r *etcdRegistrar) Deregister(ctx context.Context, service *service.ServiceInstance) error {
	if service == nil {
		return nil
	}
	key := r.namespace + "/" + service.Name + "/" + service.ID
	return r.informer.Delete(ctx, key)
}
