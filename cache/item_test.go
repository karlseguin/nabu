package cache

import (
  "testing"
  "nabu/indexes"
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


type FakeFetcher struct {
  indexA *indexes.Index
  indexB *indexes.Index
}

func newFetcher() *FakeFetcher {
  f := new(FakeFetcher)
  f.indexA = indexes.New("A")
  f.indexA.Add("a")
  f.indexA.Add("b")
  f.indexA.Add("c")
  f.indexA.Add("d")
  f.indexB = indexes.New("B")
  f.indexB.Add("z")
  f.indexB.Add("b")
  f.indexB.Add("d")
  return f
}

func (f *FakeFetcher) LookupIndexes(indexNames []string, target indexes.Indexes) bool {
  for i, name := range indexNames {
    if name == "a" { target[i] = f.indexA }
    if name == "b" { target[i] = f.indexB }
  }
  return true
}
