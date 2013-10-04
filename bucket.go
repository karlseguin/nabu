package nabu

import (
  "sync"
)

type Bucket struct {
  sync.RWMutex
  lookup map[string]Document
}
