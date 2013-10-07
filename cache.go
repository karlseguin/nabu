package nabu

import (
  "sort"
  "sync"
  "strings"
)

type Cache struct {
  sync.RWMutex
  db *Database
  newQueue chan *CacheItem
  lookup map[string]*CacheItem
}

func newCache(db *Database) *Cache {
  c := &Cache {
    db: db,
    lookup: make(map[string]*CacheItem),
    newQueue: make(chan *CacheItem, 1024),
  }
  for i := 0; i < db.cacheWorkers; i++ { go c.workers() }
  return c
}

func (c *Cache) Get(indexNames []string) (Indexes, bool) {
  if cached, exists := c.get(indexNames); exists {
    return cached, true
  }
  return nil, false
}

func (c *Cache) get(indexNames []string) (Indexes, bool) {
  sort.Strings(indexNames)
  key := strings.Join(indexNames, "&")
  c.RLock()
  item, exists := c.lookup[key]
  c.RUnlock()
  if exists && item.touchIfReady() {
    return item.index, true
  }

  item = newCacheItem(c.db, key, indexNames)
  if item == nil { //happens when we have an invalid index
    return nil, false
  }

  c.Lock()
  if _, exists := c.lookup[key]; exists {
    c.Unlock()
    return nil, false
  }
  c.lookup[key] = item
  c.Unlock()
  select {
    case c.newQueue <- item:
    default:
  }
  return nil, false
}

func (c *Cache) workers() {
  for {
    select {
    case item := <- c.newQueue:
      item.build()
    }
  }
}

// func (c *Cache) rebuild(item *CacheItem) {
//   now := time.Now()
//   item.RLock()
//   accessed := item.accessed
//   item.RUnlock()

//   if accessed.Before(now.Add(time.Minute * -5)) {
//     c.remove(item)
//   } else {
//     item.rebuild()
//   }
// }

// func (c *Cache) remove(item *CacheItem) {
//   c.Lock()
//   defer c.Unlock()
//   delete(c.lookup, item.key)
// }
