// Nabu's query cache. Updates to documents incrementally
// updates cached results. The cache's growth is unbound. However,
// cached indexes which have not been used for maxCacheStaleness will
// be cleaned up
package cache

import (
  "sort"
  "sync"
  "time"
  "strings"
  "container/list"
  "github.com/karlseguin/nabu/key"
  "github.com/karlseguin/nabu/indexes"
)

type IndexFetcher interface {
  LookupIndexes(indexNames []string, target indexes.Indexes) bool
}

// The cache manager
type Cache struct {
  sync.RWMutex
  lru *list.List
  fetcher IndexFetcher
  newQueue chan *Item
  promotables chan *Item
  changeQueue chan *Change
  lookup map[string]*Item
  bucketLock sync.RWMutex
  buckets map[string]*ChangeBucket
}

// Creates a new cache
func New(fetcher IndexFetcher, workerCount int, maxStaleness time.Duration) *Cache {
  c := &Cache {
    lru: list.New(),
    fetcher: fetcher,
    lookup: make(map[string]*Item),
    newQueue: make(chan *Item, 1024),
    promotables: make(chan *Item, 1024),
    changeQueue: make(chan *Change, 4096),
    buckets: make(map[string]*ChangeBucket),
  }
  for i := 0; i < workerCount; i++ { go c.workers() }
  if workerCount > 0  { go c.maintenance(maxStaleness) }
  return c
}

// Notify the cache that the specific index was updated (id was added or removed)
func (c *Cache) Changed(indexName string, id key.Type, added bool) {
  c.changeQueue <- &Change{
    id: id,
    indexName: indexName,
    added: added,
  }
}

// Given a set of indexes, get the cached representation. When no
// cached representation exists, one will be built in the background
// for subsequent calls
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
  if exists {
    if ready, promotable := item.readyAndPromotable(); ready {
      if promotable { c.promote(item) }
      return item.index, true
    }
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

// Whenever a cache value is used, it's promoted
func (c *Cache) promote(item *Item) {
  select {
    case c.promotables <- item:
    default:
  }
}

// background workers which build new cached indexes
// or incrementally update existing ones
func (c *Cache) workers() {
  for {
    select {
    case item := <- c.newQueue:
      item.build()
      c.reverseIndex(item)
      c.promotables <- item
    case change := <- c.changeQueue:
      c.applyChange(change)
    }
  }
}

// We keep a reverse map so that when an actual index is updated
// we know which cached versions need to be incrementally updated
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

// Update a cached index
func (c *Cache) applyChange(change *Change) {
  c.bucketLock.RLock()
  bucket, exists := c.buckets[change.indexName]
  c.bucketLock.RUnlock()
  if exists == false { return }
  bucket.process(change)
}

// Promote recently used cache items. At every 50 promotions,
// see if any stale cached index should be removed
func (c *Cache) maintenance(maxStaleness time.Duration) {
  maxStaleness *= -1
  i := 0
  for {
    item := <- c.promotables
    if item.element != nil { //not a new item
      c.lru.MoveToFront(item.element)
    } else {
      item.element = c.lru.PushFront(item)
    }
    if i == 50 { //arbitrary
      c.gc(maxStaleness)
      i = 0
    }
  }
}

// Remove stale cached indexes. Since this is ordered by time,
// we can immediately break out once we've found a non-stale index.
// Also, to prevent this from blocking, we limit how many stale indexes
// we can clean up in a single pass
func (c *Cache) gc(maxStaleness time.Duration) {
  stale := time.Now().Add(maxStaleness)
  for i := 0; i < 100; i++ {
    element := c.lru.Back()
    if element == nil { return }
    item := element.Value.(*Item)
    item.RLock()
    if item.promoted.After(stale) {
      item.RUnlock()
      return
    }
    item.RUnlock()
    c.lru.Remove(element)
    c.Lock()
    delete(c.lookup, item.key)
    c.Unlock()
    for _, index := range item.sources {
      name := index.Name
      if bucket, exists := c.buckets[name]; exists {
        bucket.remove(item)
      }
    }
  }
}
