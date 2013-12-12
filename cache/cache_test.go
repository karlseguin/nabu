package cache

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/indexes"
	"testing"
	"time"
)

func TestCacheHandelsMiss(t *testing.T) {
	spec := gspec.New(t)
	cache := New(newFetcher(), 0, 10)
	indexes, exists := cache.Get([]string{"a", "b"})
	spec.Expect(exists).ToEqual(false)
	spec.Expect(indexes).ToBeNil()
}

func TestCacheQueuesANewItem(t *testing.T) {
	spec := gspec.New(t)
	cache := New(newFetcher(), 0, 10)
	cache.Get([]string{"a", "b"})
	item := <-cache.newQueue
	spec.Expect(len(item.sources)).ToEqual(2)
	spec.Expect(item.sources[0].Name).ToEqual("A")
	spec.Expect(item.sources[1].Name).ToEqual("B")
}

func TestCacheReturnsACachedItem(t *testing.T) {
	spec := gspec.New(t)
	cache := New(newFetcher(), 0, 10)
	cache.Get([]string{"a", "b"})
	item := <-cache.newQueue
	item.promoted = time.Now()
	index, exists := cache.Get([]string{"a", "b"})
	spec.Expect(exists).ToEqual(true)
	spec.Expect(len(index)).ToEqual(1)
}

type FakeFetcher struct {
	indexA *indexes.Index
	indexB *indexes.Index
}

func newFetcher() *FakeFetcher {
	f := new(FakeFetcher)
	f.indexA = indexes.New("A")
	f.indexA.Add(1)
	f.indexA.Add(2)
	f.indexA.Add(3)
	f.indexA.Add(4)
	f.indexB = indexes.New("B")
	f.indexB.Add(10)
	f.indexB.Add(2)
	f.indexB.Add(4)
	return f
}

func (f *FakeFetcher) LookupIndexes(indexNames []string, target indexes.Indexes) bool {
	for i, name := range indexNames {
		if name == "a" {
			target[i] = f.indexA
		}
		if name == "b" {
			target[i] = f.indexB
		}
	}
	return true
}
