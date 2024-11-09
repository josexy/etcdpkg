package etcd

import (
	"bytes"
	"encoding/json"
	"unsafe"
)

type KeyValue struct {
	Key            []byte `json:"key"`
	Value          []byte `json:"value"`
	CreateRevision int64  `json:"createRevision"`
	ModRevision    int64  `json:"modRevision"`
	Version        int64  `json:"version"`
	Lease          int64  `json:"lease"`
}

func (kv *KeyValue) KeyString() string {
	return unsafe.String(unsafe.SliceData(kv.Key), len(kv.Key))
}

func (kv *KeyValue) ValueString() string {
	return unsafe.String(unsafe.SliceData(kv.Value), len(kv.Value))
}

func (kv *KeyValue) String() string {
	str, _ := json.Marshal(kv)
	return string(str)
}

func (kv *KeyValue) Clone() *KeyValue {
	nkv := new(KeyValue)
	*nkv = *kv
	nkv.Key = bytes.Clone(kv.Key)
	nkv.Value = bytes.Clone(kv.Value)
	return nkv
}
