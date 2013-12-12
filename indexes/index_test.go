package indexes

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"sort"
	"testing"
)

func TestIndexAddAnItem(t *testing.T) {
	spec := gspec.New(t)
	index := NewIndex("_")
	index.Add(44)
	spec.Expect(index.Len()).ToEqual(1)
	spec.Expect(index.Contains(44)).ToEqual(true)
}

func TestIndexCanRemoveNonExistingItem(t *testing.T) {
	spec := gspec.New(t)
	index := NewIndex("_")
	index.Add(1)
	index.Remove(2)
	spec.Expect(index.Len()).ToEqual(1)
	spec.Expect(index.Contains(1)).ToEqual(true)
}

func TestIndexCanRemoveAnItem(t *testing.T) {
	spec := gspec.New(t)
	index := NewIndex("_")
	index.Add(1)
	index.Remove(1)
	spec.Expect(index.Len()).ToEqual(0)
}

func TestIndexesAreSortedFromSmallestToLargest(t *testing.T) {
	spec := gspec.New(t)
	index1 := NewIndex("_")
	for i := 0; i < 4; i++ {
		index1.Add(key.Type(i))
	}
	index2 := NewIndex("_")
	for i := 0; i < 7; i++ {
		index2.Add(key.Type(i))
	}
	index3 := NewIndex("_")
	for i := 0; i < 13; i++ {
		index3.Add(key.Type(i))
	}

	indexes := Indexes{index2, index1, index3}
	sort.Sort(indexes)
	spec.Expect(indexes[0].Len()).ToEqual(4)
	spec.Expect(indexes[1].Len()).ToEqual(7)
	spec.Expect(indexes[2].Len()).ToEqual(13)
}
