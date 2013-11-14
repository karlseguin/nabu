package cache

import (
  "sort"
  "sync"
  "time"
  "github.com/karlseguin/nabu/indexes"
  "container/list"
)

type Item struct {
  key string
  sync.RWMutex
  promoted time.Time
  element *list.Element
  index indexes.Indexes
  sources indexes.Indexes
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

func (item *Item) readyAndPromotable() (bool, bool) {
  item.RLock()
  promoted := item.promoted
  item.RUnlock()
  if promoted.IsZero() { return false, false }

  now := time.Now()
  stale := now.Add(-time.Minute)
  if promoted.After(stale) {
    return true, false
  }
  item.Lock()
  item.promoted = now
  item.Unlock()

  return true, true
}

func (item *Item) build() {
  item.sources.RLock()
  defer item.sources.RUnlock()
  sort.Sort(item.sources)
  idx := item.sources
  first := idx[0]
  cached := indexes.New(item.key)
  indexCount := len(item.sources)

  for id, _ := range first.Ids {
    for j := 1; j < indexCount; j++ {
      if _, exists := idx[j].Ids[id]; exists == false {
        goto nomatch
      }
    }
    cached.Ids[id] = struct{}{}
    nomatch:
  }
  item.index[0] = cached
  item.Lock()
  item.promoted = time.Now().Add(time.Minute * -60)
  item.Unlock()
}

func (item *Item) change(change *Change) {
  if change.added {
    item.added(change)
  } else {
    item.removed(change)
  }
}

func (item *Item) added(change *Change) {
  id := change.id
  indexes := item.sources
  indexes.RLock()
  defer indexes.RUnlock()
  indexCount := len(indexes)
  for i := 0; i < indexCount; i++ {
    if _, exists := indexes[i].Ids[id]; exists == false {
      return
    }
  }
  item.index[0].Add(id)
}

func (item *Item) removed(change *Change) {
  item.index[0].Remove(change.id)
}
