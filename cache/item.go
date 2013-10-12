package cache

import (
  "sort"
  "sync"
  "time"
  "nabu/indexes"
)

type Item struct {
  key string
  sync.RWMutex
  index indexes.Indexes
  version uint64
  sources indexes.Indexes
  accessed time.Time
}

func newItem(fetcher IndexFetcher, key string, indexNames []string) *Item {
  sources := make(indexes.Indexes, len(indexNames))
  if fetcher.LookupIndexes(indexNames, sources) == false {
    return nil
  }
  return &Item {
    key: key,
    sources: sources,
    index: make(indexes.Indexes, 1),
  }
}

func (ci *Item) touchIfReady() bool {
  ci.Lock()
  defer ci.Unlock()
  if ci.accessed.IsZero() {
    return false
  }
  ci.accessed = time.Now()
  return true
}

func (ci *Item) build() {
  ci.sources.RLock()
  defer ci.sources.RUnlock()
  sort.Sort(ci.sources)
  idx := ci.sources
  first := idx[0]
  cached := indexes.New(ci.key)
  indexCount := len(ci.sources)

  for id, _ := range first.Ids {
    for j := 1; j < indexCount; j++ {
      if _, exists := idx[j].Ids[id]; exists == false {
        goto nomatch
      }
    }
    cached.Ids[id] = struct{}{}
    nomatch:
  }
  ci.index[0] = cached
  ci.Lock()
  ci.accessed = time.Now()
  ci.Unlock()
}

func (ci *Item) change(change *Change) {
  if change.added {
    ci.added(change)
  } else {
    ci.removed(change)
  }
}

func (ci *Item) added(change *Change) {
  id := change.id
  indexes := ci.sources
  indexes.RLock()
  defer indexes.RUnlock()
  indexCount := len(indexes)
  for i := 0; i < indexCount; i++ {
    if _, exists := indexes[i].Ids[id]; exists == false {
      return
    }
  }
  ci.index[0].Add(id)
}

func (ci *Item) removed(change *Change) {
  ci.index[0].Remove(change.id)
}
