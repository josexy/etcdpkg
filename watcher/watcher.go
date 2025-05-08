package watcher

import "github.com/josexy/etcdpkg/service"

// Watcher is service watcher.
type Watcher interface {
	// Next returns services in the following two cases:
	// 1.the first time to watch and the service instance list is not empty.
	// 2.any service instance changes found.
	// if the above two conditions are not met, it will block until context deadline exceeded or canceled
	Next() ([]*service.ServiceInstance, error)
	// Stop close the watcher.
	Stop() error
}
