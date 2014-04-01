package indexes

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"testing"
)

func TestSortedSetOrderOnInsert(t *testing.T) {
	spec := gspec.New(t)
	s := newSortedSet("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	spec.Expect(s.list[1].id).ToEqual(key.Type(2))
	spec.Expect(s.list[2].id).ToEqual(key.Type(1))
	spec.Expect(s.list[3].id).ToEqual(key.Type(3))
	spec.Expect(s.Len()).ToEqual(3)
}

func TestSortedSetOrderOnNoopUpdate(t *testing.T) {
	spec := gspec.New(t)
	s := newSortedSet("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	s.SetString(key.Type(1), "banana")
	s.SetString(key.Type(2), "apples")
	spec.Expect(s.list[1].id).ToEqual(key.Type(2))
	spec.Expect(s.list[2].id).ToEqual(key.Type(1))
	spec.Expect(s.list[3].id).ToEqual(key.Type(3))
	spec.Expect(s.Len()).ToEqual(3)
}

func TestSortedSetOrderOnUpdate(t *testing.T) {
	spec := gspec.New(t)
	s := newSortedSet("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	s.SetString(key.Type(1), "prune")
	spec.Expect(s.list[1].id).ToEqual(key.Type(2))
	spec.Expect(s.list[2].id).ToEqual(key.Type(3))
	spec.Expect(s.list[3].id).ToEqual(key.Type(1))
	spec.Expect(s.Len()).ToEqual(3)
}

func TestSortedSetOrderOnRemove(t *testing.T) {
	spec := gspec.New(t)
	s := newSortedSet("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	s.Remove(key.Type(3))
	spec.Expect(s.list[1].id).ToEqual(key.Type(2))
	spec.Expect(s.list[2].id).ToEqual(key.Type(1))
	spec.Expect(s.Len()).ToEqual(2)
}

func TestSortedSetOrderOnRemoveX(t *testing.T) {
	spec := gspec.New(t)
	s := newSortedSet("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges", 4, "zebra", 5, "zz")
	s.Remove(key.Type(1))
	s.Remove(key.Type(4))
	s.Remove(key.Type(5))
	spec.Expect(s.list[1].id).ToEqual(key.Type(2))
	spec.Expect(s.list[2].id).ToEqual(key.Type(3))
	spec.Expect(s.Len()).ToEqual(2)
}

func TestSortedSetContains(t *testing.T) {
	spec := gspec.New(t)
	s := newSortedSet("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	s.SetString(key.Type(1), "prune")
	s.SetString(key.Type(4), "apes")
	s.Remove(1)
	spec.Expect(s.Contains(key.Type(1))).ToEqual(0)
	spec.Expect(s.Contains(key.Type(3))).ToEqual(3)
}

func TestSortedSetForwardsIterator(t *testing.T) {
	s := newSortedSet("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	assertSortedSetIterator(t, s.Forwards(), 2, 1, 3)
}

func TestSortedSetForwardsIteratorWithOffset(t *testing.T) {
	s := newSortedSet("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	assertSortedSetIterator(t, s.Forwards().Offset(1), 1, 3)
}

func TestSortedSetBackwardsIterator(t *testing.T) {
	s := newSortedSet("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	assertSortedSetIterator(t, s.Backwards(), 3, 1, 2)
}

func TestSortedSetBackwardsIteratorWithOffset(t *testing.T) {
	s := newSortedSet("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	assertSortedSetIterator(t, s.Backwards().Offset(1), 1, 2)
}

func sortedSetLoad(set *SortedSet, values ...interface{}) {
	for i := 0; i < len(values); i += 2 {
		set.SetString(key.Type(values[i].(int)), values[i+1].(string))
	}
}

func assertSortedSetIterator(t *testing.T, iterator Iterator, expectedInts ...int) {
	spec := gspec.New(t)
	expected := make([]key.Type, len(expectedInts))
	for index, id := range expectedInts {
		expected[index] = key.Type(id)
	}
	for id := iterator.Current(); id != key.NULL; id = iterator.Next() {
		spec.Expect(id).ToEqual(expected[0])
		copy(expected, expected[1:])
	}
}
