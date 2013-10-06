package nabu

import (
  "fmt"
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

func newCacheItem(db *Database, key string, indexes Indexes) *CacheItem {
  ci := &CacheItem{
    key: key,
    index: make(Indexes, 1),
    sources: make(Indexes, db.maxIndexesPerQuery),
  }
  for i, index := range indexes {
    ci.sources[i] = index
  }
  return ci
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

func (ci *CacheItem) rebuild() {

}
// func (t *CacheTask) Run(db *Database) *Item {
//   defer t.Close()
//   first := indexes[0]
//   cached := newIndex(name)
//   indexCount := len(indexes)
//   for id, _ := range first.ids {
//     for j := 1; j < indexCount; j++ {
//       if _, exists := indexes[j].ids[id]; exists == false {
//         goto nomatch
//       }
//     }
//     cached.ids[id] = struct{}{}
//     nomatch:
//   }
//   return Indexes{cached}
// }
