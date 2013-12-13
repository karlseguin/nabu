package nabu

import (
	"github.com/karlseguin/nabu/key"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

const IDMAP_BUCKET_COUNT = uint32(32)

type IdMap struct {
	counter uint64
	lookup  map[uint32]*IdMapBucket
}

type IdMapBucket struct {
	sync.RWMutex
	lookup map[string]key.Type
}

func newIdMap() *IdMap {
	m := &IdMap{
		lookup: make(map[uint32]*IdMapBucket, IDMAP_BUCKET_COUNT),
	}
	for i := uint32(0); i < IDMAP_BUCKET_COUNT; i++ {
		m.lookup[i] = &IdMapBucket{
			lookup: make(map[string]key.Type),
		}
	}
	return m
}

func (m *IdMap) get(s string, create bool) key.Type {
	bucket := m.getBucket(s)

	bucket.RLock()
	id, exists := bucket.lookup[s]
	bucket.RUnlock()
	if exists {
		return id
	}
	if create == false {
		return key.NULL
	}

	bucket.Lock()
	defer bucket.Unlock()

	id, exists = bucket.lookup[s]
	if exists {
		return id
	}

	id = key.Type(atomic.AddUint64(&m.counter, 1))
	bucket.lookup[s] = id
	return id
}

func (m *IdMap) remove(s string) {
	bucket := m.getBucket(s)
	bucket.Lock()
	delete(bucket.lookup, s)
	bucket.Unlock()
}

func (m *IdMap) getBucket(s string) *IdMapBucket {
	h := fnv.New32a()
	h.Write([]byte(s))
	index := h.Sum32() % IDMAP_BUCKET_COUNT
	return m.lookup[index]
}
