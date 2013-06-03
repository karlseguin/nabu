package nabu

import (
  "testing"
)

func TestAddsAnIdToTheSortedIndex(t *testing.T) {
  spec := Spec(t)
  index := NewSortedIndex()
  index.Set(9000, "it's over")
  assertOrder(spec, index, "it's over")
}

func TestAddsMultipleIdsWithTheSameRank(t *testing.T) {
  spec := Spec(t)
  index := NewSortedIndex()
  index.Set(9000, "it's over")
  index.Set(9000, "9000")
  index.Set(9000, "teehee")
  assertOrder(spec, index, "9000", "it's over", "teehee")
}

func TestAddsMultipleIndexesWithDifferentRanks(t *testing.T) {
  spec := Spec(t)
  index := NewSortedIndex()
  index.Set(2, "b")
  index.Set(1, "a")
  index.Set(3, "c")
  index.Set(2, "bb")
  assertOrder(spec, index, "a", "b", "bb", "c")
}

func TestOverridesExistingRank(t *testing.T) {
  spec := Spec(t)
  index := NewSortedIndex()
  index.Set(1, "a")
  index.Set(2, "b")
  index.Set(3, "a")
  assertOrder(spec, index, "b", "a")
}

func TestNoopRemoveNonExistingItem(t *testing.T) {
  spec := Spec(t)
  index := NewSortedIndex()
  index.Set(1, "a")
  index.Remove("b")
  assertOrder(spec, index, "a")
}

func TestRemoveItem(t *testing.T) {
  spec := Spec(t)
  index := NewSortedIndex()
  index.Set(2, "b")
  index.Set(1, "a")
  index.Set(3, "c")
  index.Set(2, "bb")
  index.Remove("b")
  assertOrder(spec, index, "a", "bb", "c")
}

func assertOrder(spec *S, index SortedIndex, ids ...string) {
  iterator := index.Forward()
  for _, id := range ids {
    _, actual := iterator.Current()
    spec.Expect(actual).ToEqual(id)
    iterator.Next()
  }
  iterator.Close()

  iterator = index.Backward()
  for i := len(ids) - 1; i >= 0; i-- {
    _, actual := iterator.Current()
    spec.Expect(actual).ToEqual(ids[i])
    iterator.Next()
  }
  iterator.Close()
}
// assertOrder(spec, idx, "it's over")