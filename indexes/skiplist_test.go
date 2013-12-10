package indexes

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"math/rand"
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
	assertIterator(t, s.Forwards().Offset(0), "a", "b", "c")
}

func TestSkiplistBackwardIteration(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Backwards().Offset(0), "c", "b", "a")
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
	assertIterator(t, s.Forwards().Range(1, 3).Offset(0), "b", "c", "d")
}

func TestSkiplistForwardIterationWithOffsetAndRange(t *testing.T) {
	for i := 0; i < 500; i++ {
		rand.Seed(int64(i))
		s := newSkiplist()
		s.Load([]key.Type{"a", "b", "c", "d", "e", "f"})
		assertIterator(t, s.Forwards().Range(1, 3).Offset(1), "c", "d")
	}
}

func TestSkiplistForwardIterationWithRangeOutsideBounds(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Forwards().Range(-10, 10).Offset(1), "b", "c")
}

func TestSkiplistBackwardIterationWithRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c", "d", "e", "f"})
	assertIterator(t, s.Backwards().Range(1, 3).Offset(0), "d", "c", "b")
}

func TestSkiplistBackwardIterationWithOffsetAndRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c", "d", "e", "f"})
	assertIterator(t, s.Backwards().Range(1, 3).Offset(1), "c", "b")
}

func TestSkiplistBackwardIterationWithRangeOutsideBounds(t *testing.T) {
	for i := 0; i < 500; i++ {
		rand.Seed(int64(i))
		s := newSkiplist()
		s.Load([]key.Type{"a", "b", "c"})
		assertIterator(t, s.Backwards().Range(-10, 10).Offset(1), "b", "a")
	}
}

func TestSkiplistRankingIfMemberDoesNotExist(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	_, exists := s.GetScore("z")
	gspec.New(t).Expect(exists).ToEqual(false)
}

func TestSkiplistRankingIfMemberExist(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Load([]key.Type{"a", "b", "c"})
	rank, exists := s.GetScore("c")
	spec.Expect(exists).ToEqual(true)
	spec.Expect(rank).ToEqual(2)
}

func TestSkiplistAppendOnEmpty(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Append("x")
	assertIterator(t, s.Forwards().Offset(0), "x")
	spec.Expect(s.GetScore("x")).ToEqual(1)
}

func TestSkiplistPrependOnEmpty(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Prepend("x")
	assertIterator(t, s.Forwards().Offset(0), "x")
	spec.Expect(s.GetScore("x")).ToEqual(-1)
}

func TestSkiplistAppendToList(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Load([]key.Type{"a", "b"})
	s.Set("c", 33)
	s.Append("x")
	assertIterator(t, s.Forwards().Offset(0), "a", "b", "c", "x")
	spec.Expect(s.GetScore("x")).ToEqual(34)
}

func TestSkiplistPrependToList(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Load([]key.Type{"a", "b"})
	s.Set("c", -333)
	s.Prepend("x")
	assertIterator(t, s.Forwards().Offset(0), "x", "c", "a", "b")
	spec.Expect(s.GetScore("x")).ToEqual(-334)
}

func TestSkiplistReplace(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Set("a", 1)
	s.Set("b", 2)
	s.Set("a", 1)
	assertIterator(t, s.Forwards().Offset(0), "a", "b")
	spec.Expect(s.GetScore("a")).ToEqual(1)
}

func TestSkiplistSetAndRemoveItems(t *testing.T) {
	spec := gspec.New(t)
	for i := 0; i < 500; i++ {
		rand.Seed(int64(i))
		s := newSkiplist()
		s.Set("a", 1)
		s.Set("b", 2)
		s.Set("c", 3)
		assertIterator(t, s.Forwards().Offset(0), "a", "b", "c")
		spec.Expect(s.offset(0).id).ToEqual(key.Type("a"))
		spec.Expect(s.offset(1).id).ToEqual(key.Type("b"))
		spec.Expect(s.offset(2).id).ToEqual(key.Type("c"))
		spec.Expect(s.offset(3).id).ToEqual(key.NULL)

		s.Remove("d")
		assertIterator(t, s.Forwards().Offset(0), "a", "b", "c")

		s.Remove("b")
		assertIterator(t, s.Forwards().Offset(0), "a", "c")
		spec.Expect(s.offset(0).id).ToEqual(key.Type("a"))
		spec.Expect(s.offset(1).id).ToEqual(key.Type("c"))
		spec.Expect(s.offset(2).id).ToEqual(key.NULL)

		s.Remove("c")
		assertIterator(t, s.Forwards().Offset(0), "a")
		spec.Expect(s.offset(0).id).ToEqual(key.Type("a"))
		spec.Expect(s.offset(1).id).ToEqual(key.NULL)

		s.Set("b", 0)
		assertIterator(t, s.Forwards().Offset(0), "b", "a")
		spec.Expect(s.offset(0).id).ToEqual(key.Type("b"))
		spec.Expect(s.offset(1).id).ToEqual(key.Type("a"))
		spec.Expect(s.offset(2).id).ToEqual(key.NULL)

		s.Remove("a")
		assertIterator(t, s.Forwards().Offset(0), "b")
		spec.Expect(s.offset(0).id).ToEqual(key.Type("b"))
		spec.Expect(s.offset(1).id).ToEqual(key.NULL)
	}
}
