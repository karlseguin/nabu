package nabu

import (
  "sync"
  "github.com/karlseguin/nabu/key"
)

type Bucket struct {
  sync.RWMutex
  lookup map[key.Type]Document
}
