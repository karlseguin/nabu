package cache

import (
	"github.com/karlseguin/nabu/indexes"
)

type CachedIndex struct {
	indexes.Index
}

func newCachedIndex(index indexes.Index) *CachedIndex {
	return &CachedIndex{
		Index: index,
	}
}
