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
	s.Load([]key.Type{1, 2, 3})
	spec.Expect(s.Len()).ToEqual(3)
}

func TestSkiplistForwardIteration(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Forwards().Offset(0), 1, 2, 3)
}

func TestSkiplistBackwardIteration(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Backwards().Offset(0), 3, 2, 1)
}

func TestSkiplistForwardIterationWithOffset(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Forwards().Offset(1), 2, 3)
}

func TestSkiplistBackwardIterationWithOffset(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Backwards().Offset(1), 2, 1)
}

func TestSkiplistForwardIterationWithOffsetAtRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Forwards().Offset(3), key.NULL)
}

func TestSkiplistBackwardIterationWithOffsetAtRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Backwards().Offset(3), key.NULL)
}

func TestSkiplistForwardIterationWithOffsetOutsideOfRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Forwards().Offset(4), key.NULL)
}

func TestSkiplistBackwardIterationWithOffsetOutsideOfRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Backwards().Offset(4), key.NULL)
}

func TestSkiplistForwardIterationWithRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{1, 2, 3, 4, 5, 6})
	assertIterator(t, s.Forwards().Range(1, 3).Offset(0), 2, 3, 4)
}

func TestSkiplistForwardIterationWithOffsetAndRange(t *testing.T) {
	for i := 0; i < 500; i++ {
		rand.Seed(int64(i))
		s := newSkiplist()
		s.Load([]key.Type{1, 2, 3, 4, 5, 6})
		assertIterator(t, s.Forwards().Range(1, 3).Offset(1), 3, 4)
	}
}

func TestSkiplistForwardIterationWithRangeOutsideBounds(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Forwards().Range(-10, 10).Offset(1), 2, 3)
}

func TestSkiplistBackwardIterationWithRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{1, 2, 3, 4, 5, 6})
	assertIterator(t, s.Backwards().Range(1, 3).Offset(0), 4, 3, 2)
}

func TestSkiplistBackwardIterationWithOffsetAndRange(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{1, 2, 3, 4, 5, 6})
	assertIterator(t, s.Backwards().Range(1, 3).Offset(1), 3, 2)
}

func TestSkiplistBackwardIterationWithRangeOutsideBounds(t *testing.T) {
	for i := 0; i < 500; i++ {
		rand.Seed(int64(i))
		s := newSkiplist()
		s.Load([]key.Type{1, 2, 3})
		assertIterator(t, s.Backwards().Range(-10, 10).Offset(1), 2, 1)
	}
}

func TestSkiplistRankingIfMemberDoesNotExist(t *testing.T) {
	s := newSkiplist()
	s.Load([]key.Type{1, 2, 3})
	_, exists := s.GetScore(100)
	gspec.New(t).Expect(exists).ToEqual(false)
}

func TestSkiplistRankingIfMemberExist(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Load([]key.Type{1, 2, 3})
	rank, exists := s.GetScore(3)
	spec.Expect(exists).ToEqual(true)
	spec.Expect(rank).ToEqual(2)
}

func TestSkiplistAppendOnEmpty(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Append(33)
	assertIterator(t, s.Forwards().Offset(0), 33)
	spec.Expect(s.GetScore(33)).ToEqual(1)
}

func TestSkiplistPrependOnEmpty(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Prepend(33)
	assertIterator(t, s.Forwards().Offset(0), 33)
	spec.Expect(s.GetScore(33)).ToEqual(-1)
}

func TestSkiplistAppendToList(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Load([]key.Type{1, 2})
	s.Set(3, 33)
	s.Append(4)
	assertIterator(t, s.Forwards().Offset(0), 1, 2, 3, 4)
	spec.Expect(s.GetScore(4)).ToEqual(34)
}

func TestSkiplistPrependToList(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Load([]key.Type{1, 2})
	s.Set(3, -333)
	s.Prepend(4)
	assertIterator(t, s.Forwards().Offset(0), 4, 3, 1, 2)
	spec.Expect(s.GetScore(4)).ToEqual(-334)
}

func TestSkiplistReplace(t *testing.T) {
	spec := gspec.New(t)
	s := newSkiplist()
	s.Set(1, 1)
	s.Set(2, 2)
	s.Set(1, 1)
	assertIterator(t, s.Forwards().Offset(0), 1, 2)
	spec.Expect(s.GetScore(1)).ToEqual(1)
}

func TestSkiplistSetAndRemoveItems(t *testing.T) {
	spec := gspec.New(t)
	for i := 0; i < 500; i++ {
		rand.Seed(int64(i))
		s := newSkiplist()
		s.Set(1, 1)
		s.Set(2, 2)
		s.Set(3, 3)
		assertIterator(t, s.Forwards().Offset(0), 1, 2, 3)
		spec.Expect(s.offset(0).id).ToEqual(key.Type(1))
		spec.Expect(s.offset(1).id).ToEqual(key.Type(2))
		spec.Expect(s.offset(2).id).ToEqual(key.Type(3))
		spec.Expect(s.offset(3).id).ToEqual(key.NULL)

		s.Remove(4)
		assertIterator(t, s.Forwards().Offset(0), 1, 2, 3)

		s.Remove(2)
		assertIterator(t, s.Forwards().Offset(0), 1, 3)
		spec.Expect(s.offset(0).id).ToEqual(key.Type(1))
		spec.Expect(s.offset(1).id).ToEqual(key.Type(3))
		spec.Expect(s.offset(2).id).ToEqual(key.NULL)

		s.Remove(3)
		assertIterator(t, s.Forwards().Offset(0), 1)
		spec.Expect(s.offset(0).id).ToEqual(key.Type(1))
		spec.Expect(s.offset(1).id).ToEqual(key.NULL)

		s.Set(2, 0)
		assertIterator(t, s.Forwards().Offset(0), 2, 1)
		spec.Expect(s.offset(0).id).ToEqual(key.Type(2))
		spec.Expect(s.offset(1).id).ToEqual(key.Type(1))
		spec.Expect(s.offset(2).id).ToEqual(key.NULL)

		s.Remove(1)
		assertIterator(t, s.Forwards().Offset(0), 2)
		spec.Expect(s.offset(0).id).ToEqual(key.Type(2))
		spec.Expect(s.offset(1).id).ToEqual(key.NULL)
	}
}
