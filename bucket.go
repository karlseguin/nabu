package nabu

import (
	"github.com/karlseguin/nabu/key"
	"sync"
)

// Documents are sharded across multiple buckets to increase
// concurrency
type Bucket struct {
	sync.RWMutex
	Lookup map[key.Type]Document
}
