package storeindexer_test

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/josexy/etcdpkg/storeindexer"
)

func TestStoreIndexer(t *testing.T) {
	store := storeindexer.New[string]()
	for i := 0; i < 10; i++ {
		store.AddIfNotPresent("test_"+strconv.Itoa(i), strconv.Itoa(rand.Intn(100000)))
	}
	t.Log(store.ListKeys())
	t.Log(store.ListValues())
	t.Log(store.Get("test_2"))
	t.Log(store.GetByIndex(4))
	t.Log(store.GetKeyByIndex(4))
	store.Update("test_2", "2222222")
	store.Remove("test_1")
	store.RemoveByIndex(5)
	t.Log(store.Len())

	t.Log(store.ListKeys())
	t.Log(store.ListValues())
	t.Log(store.Len())
	store.Clear()
	t.Log(store.Len())
}
