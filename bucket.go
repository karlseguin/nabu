package nabu

import (
  "sync"
  "nabu/key"
)

type Bucket struct {
  sync.RWMutex
  lookup map[key.Type]Document
}
