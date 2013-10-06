package nabu

import (
  "sync"
  "time"
)

type CacheItem struct {
  version int
  sync.RWMutex
  index Indexes
  accessed time.Time
}

func newCacheItem() *CacheItem {
  return &CacheItem{
    accessed: time.Now(),
    index: make(Indexes, 1),
  }
}

func (ci *CacheItem) Touch() {
  ci.Lock()
  ci.accessed = time.Now()
  ci.Unlock()
}
