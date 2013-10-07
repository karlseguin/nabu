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
  changeQueue chan *Change
  lookup map[string]*CacheItem
  bucketLock sync.RWMutex
  buckets map[string]*ChangeBucket
}

func newCache(db *Database) *Cache {
  c := &Cache {
    db: db,
    lookup: make(map[string]*CacheItem),
    newQueue: make(chan *CacheItem, 1024),
    changeQueue: make(chan *Change, 4096),
    buckets: make(map[string]*ChangeBucket),
  }
  for i := 0; i < db.cacheWorkers; i++ { go c.workers() }
  return c
}

func (c *Cache) get(indexNames []string) (Indexes, bool) {
  if cached, exists := c.doget(indexNames); exists {
    return cached, true
  }
  return nil, false
}

func (c *Cache) changed(indexName string, id string, added bool) {
  c.changeQueue <- &Change{id: id, indexName: indexName, added: added,}
}

func (c *Cache) doget(indexNames []string) (Indexes, bool) {
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
      c.reverseIndex(item)
      item.build()
    case change := <- c.changeQueue:
      c.applyChange(change)
    }
  }
}

func (c *Cache) reverseIndex(item *CacheItem) {
  c.bucketLock.Lock()
  defer c.bucketLock.Unlock()
  for _, index := range item.sources {
    name := index.name
    bucket, exists := c.buckets[name]
    if exists == false {
      bucket = newChangeBucket(name)
      c.buckets[name] = bucket
    }
    bucket.add(item)
  }
}

func (c *Cache) applyChange(change *Change) {
  c.bucketLock.RLock()
  bucket, exists := c.buckets[change.indexName]
  c.bucketLock.RUnlock()
  if exists == false { return }
  bucket.process(change)
}
