package nabu

import (
  "sync"
)

type Cache struct {
  sync.RWMutex
  taskPool CacheTaskPool
  taskQueue chan *CacheTask
  lookup map[string]*CacheItem
}

func newCache(config *Configuration) *Cache {
  c := &Cache {
    lookup: make(map[string]*CacheItem),
    taskPool: newCacheTaskPool(512),
    taskQueue: make(chan *CacheTask, 2048),
  }
  for i := 0; i < config.cacheWorkers; i++ {
    go c.workers()
  }
  return c
}

func (c *Cache) Get(indexes Indexes) (Indexes, bool) {
  if cached, exists := c.get(indexes); exists {
    return cached, true
  }
  return indexes, false
}
func (c *Cache) get(indexes Indexes) (Indexes, bool) {
  key := c.buildKey(indexes)
  c.RLock()
  item, exists := c.lookup[key]
  c.RUnlock()
  if exists {
    item.Touch()
    return item.index, true
  }

  task := c.taskPool.Get(key)
  for _, index := range indexes { task.IndexName(index.name) }
  select {
    case c.taskQueue <- task:
    default:
  }
  return nil, false
}

func (c *Cache) buildKey(indexes Indexes) string {
  key := "_#"
  for _, index := range indexes { key += index.name + "#" }
  return key
}

func (c *Cache) workers() {
  for {
    select {
      case task := <- c.taskQueue:
        c.run(task)
    }
  }
}

func (c *Cache) run(task *CacheTask){
  //task.Run()
}
