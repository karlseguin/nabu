package indexes

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"testing"
)

func TestSkiplistLength(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	spec.Expect(s.Len()).ToEqual(3)
}

func TestSkiplistForwardIteration(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Forwards(0), "a", "b", "c")
}

func TestSkiplistBackwardIteration(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Backwards(0), "c", "b", "a")
}

func TestSkiplistForwardIterationWithOffset(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Forwards(1), "b", "c")
}

func TestSkiplistBackwardIterationWithOffset(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Backwards(1), "b", "a")
}

func TestSkiplistForwardIterationWithOffsetAtRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Forwards(3), "")
}

func TestSkiplistBackwardIterationWithOffsetAtRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Backwards(3), "")
}

func TestSkiplistForwardIterationWithOffsetOutsideOfRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Forwards(4), "")
}

func TestSkiplistBackwardIterationWithOffsetOutsideOfRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Backwards(4), "")
}

func TestSkiplistRankingIfMemberDoesNotExist(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	_, exists := s.Rank("z")
	gspec.New(t).Expect(exists).ToEqual(false)
}

func TestSkiplistRankingIfMemberExist(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	rank, exists := s.Rank("c")
	spec.Expect(exists).ToEqual(true)
	spec.Expect(rank).ToEqual(2)
}

func TestSkipListAppendOnEmpty(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Append("x")
	assertIterator(t, s.Forwards(0), "x")
	spec.Expect(s.Rank("x")).ToEqual(1)
}

func TestSkipListPrependOnEmpty(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Prepend("x")
	assertIterator(t, s.Forwards(0), "x")
	spec.Expect(s.Rank("x")).ToEqual(-1)
}

func TestSkipListAppendToList(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Load([]key.Type{"a", "b"})
	s.Set("c", 33)
	s.Append("x")
	assertIterator(t, s.Forwards(0), "a", "b", "c", "x")
	spec.Expect(s.Rank("x")).ToEqual(34)
}

func TestSkipListPrependToList(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Load([]key.Type{"a", "b"})
	s.Set("c", -333)
	s.Prepend("x")
	assertIterator(t, s.Forwards(0), "x", "c", "a", "b")
	spec.Expect(s.Rank("x")).ToEqual(-334)
}

func TestSkipListReplace(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Set("a", 1)
	s.Set("b", 2)
	s.Set("a", 1)
	assertIterator(t, s.Forwards(0), "a", "b")
	spec.Expect(s.Rank("a")).ToEqual(1)
}

//todo expand this
func TestSkipListSetAndRemoveItems(t *testing.T) {
	s := newSkiplist()
	s.Set("a", 1)
	s.Set("b", 2)
	s.Set("c", 3)
	assertIterator(t, s.Forwards(0), "a", "b", "c")

	s.Remove("d")
	assertIterator(t, s.Forwards(0), "a", "b", "c")

	s.Remove("b")
	assertIterator(t, s.Forwards(0), "a", "c")

	s.Remove("c")
	assertIterator(t, s.Forwards(0), "a")

	s.Set("b", 0)
	assertIterator(t, s.Forwards(0), "b", "a")

	s.Remove("a")
	assertIterator(t, s.Forwards(0), "b")
}
