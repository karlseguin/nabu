package nabu

type CacheTaskPool chan *CacheTask

func newCacheTaskPool(size int) CacheTaskPool {
  pool := make(CacheTaskPool, size)
  for i := 0; i < size; i++ {
    pool <- newCacheTask(pool, "")
  }
  return pool
}

func (pool CacheTaskPool) Get(key string) *CacheTask {
  select {
    case task := <- pool:
      task.key = key
      return task
    default:
      return newCacheTask(nil, key)
  }
}

type CacheTask struct {
  key string
  indexCount int
  pool CacheTaskPool
  indexNames []string
}

func newCacheTask(pool CacheTaskPool, key string) *CacheTask {
  return &CacheTask{
    key: key,
    pool: pool,
    indexNames: make([]string, 10),
  }
}

func (t *CacheTask) IndexName(name string) {
  t.indexNames[t.indexCount] = name
  t.indexCount++
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

func (t *CacheTask) Close() {
  if t.pool == nil { return }
  t.indexCount = 0
  t.pool <- t
}
