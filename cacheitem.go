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
  ci.sources.rlock()
  defer ci.sources.runlock()
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

func (ci *CacheItem) change(change *Change) {
  if change.added {
    ci.added(change)
  } else {
    ci.removed(change)
  }
}

func (ci *CacheItem) added(change *Change) {
  id := change.id
  indexes := ci.sources
  indexes.rlock()
  defer indexes.runlock()
  indexCount := len(indexes)
  for i := 0; i < indexCount; i++ {
    if _, exists := indexes[i].ids[id]; exists == false {
      return
    }
  }
  ci.index[0].Add(id)
}

func (ci *CacheItem) removed(change *Change) {
  ci.index[0].Remove(change.id)
}
