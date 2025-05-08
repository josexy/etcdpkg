package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	etcdpkg "github.com/josexy/etcdpkg"
	"github.com/josexy/etcdpkg/service"
	"github.com/josexy/etcdpkg/watcher"
)

type listWatcher struct {
	etcdpkg.ListWatcher[*service.ServiceInstance]
	stopFn       func()
	serviceName  string
	notification chan struct{}
}

func newListerWatcher(lw etcdpkg.ListWatcher[*service.ServiceInstance], serviceName string, stopFn func()) *listWatcher {
	l := &listWatcher{
		ListWatcher:  lw,
		serviceName:  serviceName,
		notification: make(chan struct{}, 1),
		stopFn:       stopFn,
	}
	if l.Lister().Len() > 0 {
		// initial List
		l.notification <- struct{}{}
	}
	lw.Watcher().AddEventHandler(etcdpkg.EventHandlerFunc[*service.ServiceInstance]{
		AddFunc: func(newKeyObj *etcdpkg.KeyObject[*service.ServiceInstance]) {
			l.notification <- struct{}{}
		},
		UpdateFunc: func(oldKeyObj, newKeyObj *etcdpkg.KeyObject[*service.ServiceInstance]) {
			l.notification <- struct{}{}
		},
		DeleteFunc: func(oldKeyObj *etcdpkg.KeyObject[*service.ServiceInstance]) {
			l.notification <- struct{}{}
		},
	})
	return l
}

func (lw *listWatcher) Next() ([]*service.ServiceInstance, error) {
	// block here
	<-lw.notification
	return lw.Watcher().GetStore().ListValues(), nil
}

func (lw *listWatcher) Stop() error {
	lw.Watcher().Stop()
	lw.stopFn()
	return nil
}

func (r *etcdRegistrar) GetService(ctx context.Context, serviceName string) ([]*service.ServiceInstance, error) {
	if serviceName == "" {
		return nil, nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if lw, ok := r.lws[serviceName]; !ok {
		kvs, err := r.informer.GetWithPrefix(ctx, r.namespace+"/"+serviceName)
		if err != nil {
			return nil, err
		}
		return convertKvs2SvcInstancesForServiceName(kvs, serviceName)
	} else {
		return lw.Lister().List(ctx)
	}
}

func (r *etcdRegistrar) Watch(ctx context.Context, serviceName string) (watcher.Watcher, error) {
	if serviceName == "" {
		return nil, errors.New("service name cannot be empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.lws[serviceName]; ok {
		return nil, fmt.Errorf("%s watcher already exists", serviceName)
	}

	prefix := r.namespace + "/" + serviceName
	lw, err := r.informer.ListWatch(ctx, prefix)
	if err != nil {
		return nil, err
	}
	r.lws[serviceName] = lw
	return newListerWatcher(lw, serviceName, func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.lws, serviceName)
	}), nil
}

func convertKvs2SvcInstancesForServiceName(kvs []*etcdpkg.KeyValue, serviceName string) ([]*service.ServiceInstance, error) {
	instances := make([]*service.ServiceInstance, 0, len(kvs))
	for _, kv := range kvs {
		instance := new(service.ServiceInstance)
		if err := json.Unmarshal([]byte(kv.Value), instance); err != nil {
			return nil, err
		}
		if instance.Name != serviceName {
			continue
		}
		instances = append(instances, instance)
	}
	return instances, nil
}
