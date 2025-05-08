package etcd

import "context"

type KeyObject[T any] struct {
	Key    string
	Object T
}

type EventHandler[T any] interface {
	OnAdd(newKeyObj *KeyObject[T])
	OnUpdate(oldKeyObj, newKeyObj *KeyObject[T])
	OnDelete(oldKeyObj *KeyObject[T])
}

type EventHandlerFunc[T any] struct {
	AddFunc    func(newKeyObj *KeyObject[T])
	UpdateFunc func(oldKeyObj, newKeyObj *KeyObject[T])
	DeleteFunc func(oldKeyObj *KeyObject[T])
}

func (h EventHandlerFunc[T]) OnAdd(newKeyObj *KeyObject[T]) {
	if h.AddFunc != nil {
		h.AddFunc(newKeyObj)
	}
}

func (h EventHandlerFunc[T]) OnUpdate(oldKeyObj, newKeyObj *KeyObject[T]) {
	if h.UpdateFunc != nil {
		h.UpdateFunc(oldKeyObj, newKeyObj)
	}
}
func (h EventHandlerFunc[T]) OnDelete(oldKeyObj *KeyObject[T]) {
	if h.DeleteFunc != nil {
		h.DeleteFunc(oldKeyObj)
	}
}

type Lister[T any] interface {
	Len() int
	List(context.Context) ([]T, error)
}

type Watcher[T any] interface {
	// the context for the stop watch cancel
	Context() context.Context
	// stop the watcher
	Stop()
	GetStore() Store[T]
	AddEventHandler(EventHandler[T])
	GetEventHandler() EventHandler[T]
}

type ListWatcher[T any] interface {
	Lister() Lister[T]
	Watcher() Watcher[T]
}

type listerWatcher[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc
	store  Store[T]
	*lister[T]
	*watcher[T]
}

func newListerWatcher[T any](ctx context.Context, store Store[T]) ListWatcher[T] {
	cctx, ccancel := context.WithCancel(ctx)
	lw := &listerWatcher[T]{
		ctx:    cctx,
		cancel: ccancel,
		store:  store,
	}
	lw.lister = &lister[T]{lw: lw}
	lw.watcher = &watcher[T]{lw: lw}
	return lw
}

func (lw *listerWatcher[T]) Lister() Lister[T] { return lw.lister }

func (lw *listerWatcher[T]) Watcher() Watcher[T] { return lw.watcher }

type lister[T any] struct{ lw *listerWatcher[T] }

func (l *lister[T]) List(context.Context) ([]T, error) { return l.lw.store.ListValues(), nil }

func (l *lister[T]) Len() int { return l.lw.store.Len() }

type watcher[T any] struct {
	lw      *listerWatcher[T]
	handler EventHandler[T]
}

func (w *watcher[T]) Context() context.Context { return w.lw.ctx }

func (w *watcher[T]) GetStore() Store[T] { return w.lw.store }

func (w *watcher[T]) AddEventHandler(handler EventHandler[T]) { w.handler = handler }

func (w *watcher[T]) GetEventHandler() EventHandler[T] { return w.handler }

func (w *watcher[T]) Stop() { w.lw.cancel() }
