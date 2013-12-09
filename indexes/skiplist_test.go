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
	assertIterator(t, s.Forwards(), "a", "b", "c")
}

func TestSkiplistBackwardIteration(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Backwards(), "c", "b", "a")
}

func TestSkiplistForwardIterationWithOffset(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Forwards().Offset(1), "b", "c")
}

func TestSkiplistBackwardIterationWithOffset(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Backwards().Offset(1), "b", "a")
}

func TestSkiplistForwardIterationWithOffsetAtRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Forwards().Offset(3), "")
}

func TestSkiplistBackwardIterationWithOffsetAtRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Backwards().Offset(3), "")
}

func TestSkiplistForwardIterationWithOffsetOutsideOfRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Forwards().Offset(4), "")
}

func TestSkiplistBackwardIterationWithOffsetOutsideOfRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Backwards().Offset(4), "")
}

func TestSkiplistForwardIterationWithRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c", "d", "e", "f"})
	assertIterator(t, s.Forwards().Range(1, 3), "b", "c", "d")
}

func TestSkiplistForwardIterationWithOffsetAndRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c", "d", "e", "f"})
	assertIterator(t, s.Forwards().Range(1, 3).Offset(1), "c", "d")
}

func TestSkiplistForwardIterationWithRangeOutsideBounds(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Forwards().Range(-10, 10).Offset(1), "b", "c")
}

func TestSkiplistBackwardIterationWithRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c", "d", "e", "f"})
	assertIterator(t, s.Backwards().Range(1, 3), "d", "c", "b")
}

func TestSkiplistBackwardIterationWithOffsetAndRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c", "d", "e", "f"})
	assertIterator(t, s.Backwards().Range(1, 3).Offset(1), "c", "b")
}

func TestSkiplistBackwardIterationWithRangeOutsideBounds(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Backwards().Range(-10, 10).Offset(1), "b", "a")
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
	assertIterator(t, s.Forwards(), "x")
	spec.Expect(s.Rank("x")).ToEqual(1)
}

func TestSkipListPrependOnEmpty(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Prepend("x")
	assertIterator(t, s.Forwards(), "x")
	spec.Expect(s.Rank("x")).ToEqual(-1)
}

func TestSkipListAppendToList(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Load([]key.Type{"a", "b"})
	s.Set("c", 33)
	s.Append("x")
	assertIterator(t, s.Forwards(), "a", "b", "c", "x")
	spec.Expect(s.Rank("x")).ToEqual(34)
}

func TestSkipListPrependToList(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Load([]key.Type{"a", "b"})
	s.Set("c", -333)
	s.Prepend("x")
	assertIterator(t, s.Forwards(), "x", "c", "a", "b")
	spec.Expect(s.Rank("x")).ToEqual(-334)
}

func TestSkipListReplace(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Set("a", 1)
	s.Set("b", 2)
	s.Set("a", 1)
	assertIterator(t, s.Forwards(), "a", "b")
	spec.Expect(s.Rank("a")).ToEqual(1)
}

//todo expand this
func TestSkipListSetAndRemoveItems(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Set("a", 1)
	s.Set("b", 2)
	s.Set("c", 3)
	assertIterator(t, s.Forwards(), "a", "b", "c")
	spec.Expect(s.offset(0, s.head).id).ToEqual(key.Type("a"))
	spec.Expect(s.offset(1, s.head).id).ToEqual(key.Type("b"))
	spec.Expect(s.offset(2, s.head).id).ToEqual(key.Type("c"))
	spec.Expect(s.offset(3, s.head)).ToBeNil()

	s.Remove("d")
	assertIterator(t, s.Forwards(), "a", "b", "c")

	s.Remove("b")
	assertIterator(t, s.Forwards(), "a", "c")
	spec.Expect(s.offset(0, s.head).id).ToEqual(key.Type("a"))
	spec.Expect(s.offset(1, s.head).id).ToEqual(key.Type("c"))
	spec.Expect(s.offset(2, s.head)).ToBeNil()

	s.Remove("c")
	assertIterator(t, s.Forwards(), "a")
	spec.Expect(s.offset(0, s.head).id).ToEqual(key.Type("a"))
	spec.Expect(s.offset(1, s.head)).ToBeNil()

	s.Set("b", 0)
	assertIterator(t, s.Forwards(), "b", "a")
	spec.Expect(s.offset(0, s.head).id).ToEqual(key.Type("b"))
	spec.Expect(s.offset(1, s.head).id).ToEqual(key.Type("a"))
	spec.Expect(s.offset(2, s.head)).ToBeNil()

	s.Remove("a")
	assertIterator(t, s.Forwards(), "b")
	spec.Expect(s.offset(0, s.head).id).ToEqual(key.Type("b"))
	spec.Expect(s.offset(1, s.head)).ToBeNil()
}
