package nabu

import (
  "sync"
  "github.com/karlseguin/nabu/key"
)

// Documents are sharded across multiple buckets to increase
// concurrency
type Bucket struct {
  sync.RWMutex
  lookup map[key.Type]Document
}
