package cache

import (
  "sort"
  "sync"
  "strings"
  "nabu/indexes"
)

type IndexFetcher interface {
  LookupIndexes(indexNames []string, target indexes.Indexes) bool
}

type Cache struct {
  sync.RWMutex
  fetcher IndexFetcher
  newQueue chan *Item
  changeQueue chan *Change
  lookup map[string]*Item
  bucketLock sync.RWMutex
  buckets map[string]*ChangeBucket
}

func New(fetcher IndexFetcher, workerCount int) *Cache {
  c := &Cache {
    fetcher: fetcher,
    lookup: make(map[string]*Item),
    newQueue: make(chan *Item, 1024),
    changeQueue: make(chan *Change, 4096),
    buckets: make(map[string]*ChangeBucket),
  }
  for i := 0; i < workerCount; i++ { go c.workers() }
  return c
}

func (c *Cache) Changed(indexName string, id string, added bool) {
  c.changeQueue <- &Change{
    id: id,
    indexName: indexName,
    added: added,
  }
}

func (c *Cache) Get(indexNames []string) (indexes.Indexes, bool) {
  if cached, exists := c.get(indexNames); exists {
    return cached, true
  }
  return nil, false
}

func (c *Cache) get(indexNames []string) (indexes.Indexes, bool) {
  sort.Strings(indexNames)
  key := strings.Join(indexNames, "&")
  c.RLock()
  item, exists := c.lookup[key]
  c.RUnlock()
  if exists && item.touchIfReady() {
    return item.index, true
  }

  item = newItem(c.fetcher, key, indexNames)
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

func (c *Cache) reverseIndex(item *Item) {
  c.bucketLock.Lock()
  defer c.bucketLock.Unlock()
  for _, index := range item.sources {
    name := index.Name
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
