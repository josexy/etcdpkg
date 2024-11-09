package storeindexer

import (
	"slices"
	"sync"
)

type Store[T any] interface {
	AddIfNotPresent(key string, value T) bool
	Update(key string, value T)
	Get(key string) (value T, index int, ok bool)
	GetByIndex(index int) (value T, ok bool)
	GetKeyByIndex(index int) (key string, ok bool)
	Remove(key string) bool
	RemoveByIndex(index int) bool
	ListKeys() []string
	ListValues() []T
	Len() int
	Clear()
	Range(fn func(key string, value T) bool)
}

var _ Store[any] = (*StoreIndexer[any])(nil)

type StoreIndexer[T any] struct {
	lock    sync.RWMutex
	indexes map[string]int
	items   []T
	keys    []string
}

func New[T any]() *StoreIndexer[T] {
	return &StoreIndexer[T]{
		indexes: make(map[string]int, 16),
		items:   make([]T, 0, 16),
		keys:    make([]string, 0, 16),
	}
}

func (si *StoreIndexer[T]) AddIfNotPresent(key string, value T) bool {
	si.lock.Lock()
	defer si.lock.Unlock()
	return si.addOneIfNotPresent(key, value)
}

func (si *StoreIndexer[T]) Update(key string, value T) {
	si.lock.Lock()
	defer si.lock.Unlock()
	si.addOne(key, value)
}

func (si *StoreIndexer[T]) addOneIfNotPresent(key string, value T) bool {
	if _, ok := si.indexes[key]; !ok {
		n := len(si.indexes)
		si.indexes[key] = n
		si.keys = append(si.keys, key)
		si.items = append(si.items, value)
		return true
	}
	return false
}

func (si *StoreIndexer[T]) addOne(key string, value T) {
	if index, ok := si.indexes[key]; ok {
		si.items[index] = value
	} else {
		n := len(si.indexes)
		si.indexes[key] = n
		si.keys = append(si.keys, key)
		si.items = append(si.items, value)
	}
}

func (si *StoreIndexer[T]) deleteOne(key string) bool {
	if index, ok := si.indexes[key]; ok {
		delete(si.indexes, key)
		// rebuild the remaining indexes after index
		for i := index + 1; i < len(si.items); i++ {
			si.indexes[si.keys[i]] = i - 1
		}
		si.keys = slices.Delete(si.keys, index, index+1)
		si.items = slices.Delete(si.items, index, index+1)
		return true
	}
	return false
}

func (si *StoreIndexer[T]) GetByIndex(index int) (value T, ok bool) {
	si.lock.RLock()
	defer si.lock.RUnlock()
	if index < 0 || index >= len(si.items) {
		return
	}
	return si.items[index], true
}

func (si *StoreIndexer[T]) GetKeyByIndex(index int) (key string, ok bool) {
	si.lock.RLock()
	defer si.lock.RUnlock()
	if index < 0 || index >= len(si.keys) {
		return
	}
	return si.keys[index], true
}

func (si *StoreIndexer[T]) Get(key string) (value T, index int, ok bool) {
	si.lock.RLock()
	defer si.lock.RUnlock()
	index, ok = si.indexes[key]
	if !ok {
		return
	}
	return si.items[index], index, true
}

func (si *StoreIndexer[T]) Remove(key string) bool {
	si.lock.Lock()
	defer si.lock.Unlock()
	return si.deleteOne(key)
}

func (si *StoreIndexer[T]) RemoveByIndex(index int) bool {
	si.lock.Lock()
	defer si.lock.Unlock()
	if index < 0 || index >= len(si.keys) {
		return false
	}
	return si.deleteOne(si.keys[index])
}

func (si *StoreIndexer[T]) ListKeys() []string {
	si.lock.RLock()
	defer si.lock.RUnlock()
	return si.keys
}

func (si *StoreIndexer[T]) ListValues() []T {
	si.lock.RLock()
	defer si.lock.RUnlock()
	return si.items
}

func (si *StoreIndexer[T]) Len() int {
	si.lock.RLock()
	defer si.lock.RUnlock()
	return len(si.items)
}

func (si *StoreIndexer[T]) Clear() {
	si.lock.Lock()
	defer si.lock.Unlock()
	clear(si.indexes)
	clear(si.keys)
	clear(si.items)
	si.keys = si.keys[:0]
	si.items = si.items[:0]
}

func (si *StoreIndexer[T]) Range(fn func(key string, value T) bool) {
	if fn == nil {
		return
	}
	si.lock.RLock()
	defer si.lock.RUnlock()
	for i, value := range si.items {
		if !fn(si.keys[i], value) {
			return
		}
	}
}
