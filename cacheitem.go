package nabu

import (
  "sort"
  "sync"
  "time"
)

type CacheItem struct {
  key string
  sync.RWMutex
  index Indexes
  version uint64
  sources Indexes
  accessed time.Time
}

func newCacheItem(db *Database, key string, indexNames []string) *CacheItem {
  sources := make(Indexes, len(indexNames))
  if db.lookupIndexes(indexNames, sources) == false {
    return nil
  }
  return &CacheItem {
    key: key,
    sources: sources,
    index: make(Indexes, 1),
  }
}

func (ci *CacheItem) touchIfReady() bool {
  ci.Lock()
  defer ci.Unlock()
  if ci.accessed.IsZero() {
    return false
  }
  ci.accessed = time.Now()
  return true
}

func (ci *CacheItem) build() {
  sort.Sort(ci.sources)
  indexes := ci.sources
  first := indexes[0]
  cached := newIndex(ci.key)
  indexCount := len(ci.sources)

  for id, _ := range first.ids {
    for j := 1; j < indexCount; j++ {
      if _, exists := indexes[j].ids[id]; exists == false {
        goto nomatch
      }
    }
    cached.ids[id] = struct{}{}
    nomatch:
  }
  ci.index[0] = cached
  ci.Lock()
  ci.accessed = time.Now()
  ci.Unlock()
}
