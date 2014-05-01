package indexes

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"testing"
)

func TestSortedStringsBulkLoad(t *testing.T) {
	spec := gspec.New(t)
	s := NewSortedStrings("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	s.BulkLoad([]key.Type{key.Type(5), key.Type(10), key.Type(2)})
	spec.Expect(s.list[0].id).ToEqual(key.NULL)
	spec.Expect(s.list[1].id).ToEqual(key.Type(5))
	spec.Expect(s.list[2].id).ToEqual(key.Type(10))
	spec.Expect(s.list[3].id).ToEqual(key.Type(2))
	spec.Expect(s.list[4].id).ToEqual(key.NULL)
	spec.Expect(s.Len()).ToEqual(3)
}

func TestSortedStringsOrderOnInsert(t *testing.T) {
	spec := gspec.New(t)
	s := NewSortedStrings("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	spec.Expect(s.list[0].id).ToEqual(key.NULL)
	spec.Expect(s.list[1].id).ToEqual(key.Type(2))
	spec.Expect(s.list[2].id).ToEqual(key.Type(1))
	spec.Expect(s.list[3].id).ToEqual(key.Type(3))
	spec.Expect(s.list[4].id).ToEqual(key.NULL)
	spec.Expect(s.Len()).ToEqual(3)
}

func TestSortedStringsOrderOnNoopUpdate(t *testing.T) {
	spec := gspec.New(t)
	s := NewSortedStrings("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	s.SetString(key.Type(1), "banana")
	s.SetString(key.Type(2), "apples")
	spec.Expect(s.list[1].id).ToEqual(key.Type(2))
	spec.Expect(s.list[2].id).ToEqual(key.Type(1))
	spec.Expect(s.list[3].id).ToEqual(key.Type(3))
	spec.Expect(s.Len()).ToEqual(3)
}

func TestSortedStringsOrderOnUpdate(t *testing.T) {
	spec := gspec.New(t)
	s := NewSortedStrings("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	s.SetString(key.Type(1), "prune")
	spec.Expect(s.list[1].id).ToEqual(key.Type(2))
	spec.Expect(s.list[2].id).ToEqual(key.Type(3))
	spec.Expect(s.list[3].id).ToEqual(key.Type(1))
	spec.Expect(s.Len()).ToEqual(3)
}

func TestSortedStringsOrderOnRemove(t *testing.T) {
	spec := gspec.New(t)
	s := NewSortedStrings("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	s.Remove(key.Type(3))
	spec.Expect(s.list[1].id).ToEqual(key.Type(2))
	spec.Expect(s.list[2].id).ToEqual(key.Type(1))
	spec.Expect(s.Len()).ToEqual(2)
}

func TestSortedStringsOrderOnRemoveX(t *testing.T) {
	spec := gspec.New(t)
	s := NewSortedStrings("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges", 4, "zebra", 5, "zz")
	s.Remove(key.Type(1))
	s.Remove(key.Type(4))
	s.Remove(key.Type(5))
	spec.Expect(s.list[1].id).ToEqual(key.Type(2))
	spec.Expect(s.list[2].id).ToEqual(key.Type(3))
	spec.Expect(s.Len()).ToEqual(2)
}

func TestSortedStringsContains(t *testing.T) {
	spec := gspec.New(t)
	s := NewSortedStrings("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	s.SetString(key.Type(1), "prune")
	s.SetString(key.Type(4), "apes")
	s.Remove(1)
	spec.Expect(s.Contains(key.Type(1))).ToEqual(false)
	spec.Expect(s.Contains(key.Type(3))).ToEqual(true)
}

func TestSortedStringsForwardsIterator(t *testing.T) {
	s := NewSortedStrings("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	assertSortedStringsIterator(t, s.Forwards(), 2, 1, 3)
}

func TestSortedStringsForwardsIteratorWithOffset(t *testing.T) {
	s := NewSortedStrings("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	assertSortedStringsIterator(t, s.Forwards().Offset(1), 1, 3)
}

func TestSortedStringsBackwardsIterator(t *testing.T) {
	s := NewSortedStrings("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	assertSortedStringsIterator(t, s.Backwards(), 3, 1, 2)
}

func TestSortedStringsBackwardsIteratorWithOffset(t *testing.T) {
	s := NewSortedStrings("test")
	sortedSetLoad(s, 1, "banana", 2, "apples", 3, "oranges")
	assertSortedStringsIterator(t, s.Backwards().Offset(1), 1, 2)
}

func sortedSetLoad(set *SortedStrings, values ...interface{}) {
	for i := 0; i < len(values); i += 2 {
		set.SetString(key.Type(values[i].(int)), values[i+1].(string))
	}
}

func assertSortedStringsIterator(t *testing.T, iterator Iterator, expectedInts ...int) {
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
