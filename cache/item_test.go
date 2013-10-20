package cache

import (
  "testing"
  "github.com/karlseguin/gspec"
)

func TestBuildsTheCachedItem(t *testing.T) {
  spec := gspec.New(t)
  item := newItem(newFetcher(), "testItem", []string{"a", "b"})
  spec.Expect(item.accessed.IsZero()).ToEqual(true)
  item.build()
  spec.Expect(item.accessed.IsZero()).ToEqual(false)
  spec.Expect(len(item.index[0].Ids)).ToEqual(2)
  spec.Expect(item.index[0].Contains("b")).ToEqual(true)
  spec.Expect(item.index[0].Contains("d")).ToEqual(true)
}

func TestANewItemIsNotReadyUntilBuilt(t *testing.T) {
  spec := gspec.New(t)
  item := newItem(newFetcher(), "testItem", []string{"a", "b"})
  spec.Expect(item.touchIfReady()).ToEqual(false)
  spec.Expect(item.accessed.IsZero()).ToEqual(true)
  item.build()
  spec.Expect(item.touchIfReady()).ToEqual(true)
  spec.Expect(item.accessed.IsZero()).ToEqual(false)
}

func TestAddedItemWhichIsNotAMatch(t *testing.T) {
  spec := gspec.New(t)
  item := newItem(newFetcher(), "testItem", []string{"a", "b"})
  item.build()
  item.change(&Change{id: "z", added: true, indexName: "B"})
  spec.Expect(len(item.index[0].Ids)).ToEqual(2)
}

func TestAddedItemWhichIsAMatch(t *testing.T) {
  spec := gspec.New(t)
  item := newItem(newFetcher(), "testItem", []string{"a", "b"})
  item.build()
  item.sources[0].Add("a")
  item.change(&Change{id: "a", added: true, indexName: "B"})
  spec.Expect(len(item.index[0].Ids)).ToEqual(3)
}

func TestRemoveItemWhichDoesNotExist(t *testing.T) {
  spec := gspec.New(t)
  item := newItem(newFetcher(), "testItem", []string{"a", "b"})
  item.build()
  item.change(&Change{id: "y", added: false, indexName: "B"})
  spec.Expect(len(item.index[0].Ids)).ToEqual(2)
}

func TestRemoveItemWhichExists(t *testing.T) {
  spec := gspec.New(t)
  item := newItem(newFetcher(), "testItem", []string{"a", "b"})
  item.build()
  item.change(&Change{id: "b", added: false, indexName: "B"})
  spec.Expect(len(item.index[0].Ids)).ToEqual(1)
}
