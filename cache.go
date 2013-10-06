package nabu

import (
  "time"
  "sync"
)

type Cache struct {
  sync.RWMutex
  db *Database
  queue chan *CacheItem
  lookup map[string]*CacheItem
}

func newCache(db *Database) *Cache {
  c := &Cache {
    db: db,
    queue: make(chan *CacheItem, 8196),
    lookup: make(map[string]*CacheItem),
  }
  for i := 0; i < db.cacheWorkers; i++ { go c.workers() }
  return c
}

func (c *Cache) Get(indexes Indexes) (Indexes, bool) {
  if cached, exists := c.get(indexes); exists {
    return cached, true
  }
  return indexes, false
}

func (c *Cache) get(indexes Indexes) (Indexes, bool) {
  key := "_#"
  for _, index := range indexes { key += index.name + "#" }

  c.RLock()
  item, exists := c.lookup[key]
  c.RUnlock()
  if exists && item.touchIfReady() {
    println("ssss")
    return item.index, true
  }

  item = newCacheItem(c.db, key, indexes)
  c.Lock()
  if _, exists := c.lookup[key]; exists {
    c.Unlock()
    return nil, false
  }
  c.lookup[key] = item
  c.Unlock()
  select {
    case c.queue <- item:
    default:
  }
  return nil, false
}

func (c *Cache) workers() {
  for {
    item := <- c.queue
    if item.accessed.IsZero() {
      c.build(item)
    } else {
      c.rebuild(item)
    }
  }
}

func (c *Cache) build(item *CacheItem) {
  item.rebuild()
  //item.accessed = time.Now()
}

func (c *Cache) rebuild(item *CacheItem) {
  now := time.Now()
  item.RLock()
  accessed := item.accessed
  item.RUnlock()

  if accessed.Before(now.Add(time.Minute * -5)) {
    c.remove(item)
  } else {
    item.rebuild()
  }
}

func (c *Cache) remove(item *CacheItem) {
  c.Lock()
  defer c.Unlock()
  delete(c.lookup, item.key)
}
