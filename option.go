package etcd

import (
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
)

type Option interface {
	apply(*options)
}

type options struct {
	dialTimeout    time.Duration
	contextTimeout time.Duration
	tlsInfoS       *transport.TLSInfo
	leaseTTL       int64
}

type OptionFunc func(*options)

func (f OptionFunc) apply(opts *options) { f(opts) }

func WithDialTimeout(dialTimeout time.Duration) Option {
	return OptionFunc(func(o *options) { o.dialTimeout = dialTimeout })
}

func WithContextTimeout(contextTimeout time.Duration) Option {
	return OptionFunc(func(o *options) { o.contextTimeout = contextTimeout })
}

func WithTLS(tlsInfo *transport.TLSInfo) Option {
	return OptionFunc(func(o *options) { o.tlsInfoS = tlsInfo })
}

func WithLeaseTTL(leaseTTL int64) Option {
	return OptionFunc(func(o *options) { o.leaseTTL = leaseTTL })
}

func newOption(opts ...Option) *options {
	opt := &options{
		dialTimeout:    15 * time.Second,
		contextTimeout: 10 * time.Second,
	}
	for _, o := range opts {
		o.apply(opt)
	}
	return opt
}
