package indexes

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"math/rand"
	"testing"
)

func TestSortedIntsLength(t *testing.T) {
	spec := gspec.New(t)
	s := NewSortedInts("test")
	s.Load([]key.Type{1, 2, 3})
	spec.Expect(s.Len()).ToEqual(3)
}

func TestSortedIntsForwardIteration(t *testing.T) {
	s := NewSortedInts("test")
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Forwards().Offset(0), 1, 2, 3)
}

func TestSortedIntsBackwardIteration(t *testing.T) {
	s := NewSortedInts("test")
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Backwards().Offset(0), 3, 2, 1)
}

func TestSortedIntsForwardIterationWithOffset(t *testing.T) {
	s := NewSortedInts("test")
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Forwards().Offset(1), 2, 3)
}

func TestSortedIntsBackwardIterationWithOffset(t *testing.T) {
	s := NewSortedInts("test")
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Backwards().Offset(1), 2, 1)
}

func TestSortedIntsForwardIterationWithOffsetAtRange(t *testing.T) {
	s := NewSortedInts("test")
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Forwards().Offset(3), key.NULL)
}

func TestSortedIntsBackwardIterationWithOffsetAtRange(t *testing.T) {
	s := NewSortedInts("test")
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Backwards().Offset(3), key.NULL)
}

func TestSortedIntsForwardIterationWithOffsetOutsideOfRange(t *testing.T) {
	s := NewSortedInts("test")
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Forwards().Offset(4), key.NULL)
}

func TestSortedIntsBackwardIterationWithOffsetOutsideOfRange(t *testing.T) {
	s := NewSortedInts("test")
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Backwards().Offset(4), key.NULL)
}

func TestSortedIntsForwardIterationWithRange(t *testing.T) {
	for i := 0; i < 1000; i++ {
		rand.Seed(int64(i))
		s := NewSortedInts("test")
		s.Load([]key.Type{1, 2, 3, 4, 5, 6})
		assertIterator(t, s.Forwards().Range(1, 3).Offset(0), 2, 3, 4)
	}
}

func TestSortedIntsForwardIterationMovesUpToNextClosest(t *testing.T) {
	for i := 0; i < 1000; i++ {
		rand.Seed(int64(i))
		s := NewSortedInts("test")
		s.SetInt(key.Type(1), 1)
		s.SetInt(key.Type(3), 3)
		s.SetInt(key.Type(4), 4)
		s.SetInt(key.Type(5), 5)
		assertIterator(t, s.Forwards().Range(2, 4).Offset(0), 3, 4)
	}
}

func TestSortedIntsForwardIterationWithOffsetAndRange(t *testing.T) {
	for i := 0; i < 1000; i++ {
		rand.Seed(int64(i))
		s := NewSortedInts("test")
		s.Load([]key.Type{1, 2, 3, 4, 5, 6})
		assertIterator(t, s.Forwards().Range(1, 3).Offset(1), 3, 4)
	}
}

func TestSortedIntsForwardIterationWithRangeOutsideBounds(t *testing.T) {
	for i := 0; i < 1; i++ {
		rand.Seed(int64(i))
		s := NewSortedInts("test")
		s.Load([]key.Type{1, 2, 3})
		assertIterator(t, s.Forwards().Range(-10, 10).Offset(1), 2, 3)
	}
}

func TestSortedIntsBackwardIterationWithRange(t *testing.T) {
	for i := 0; i < 100; i++ {
		rand.Seed(int64(i))
		s := NewSortedInts("test")
		s.Load([]key.Type{1, 2, 3, 4, 5, 6})
		assertIterator(t, s.Backwards().Range(1, 3).Offset(0), 4, 3, 2)
	}
}

func TestSortedIntsBackwardIterationWithOffsetAndRange(t *testing.T) {
	for i := 0; i < 100; i++ {
		rand.Seed(int64(i))
		s := NewSortedInts("test")
		s.Load([]key.Type{1, 2, 3, 4, 5, 6})
		assertIterator(t, s.Backwards().Range(1, 3).Offset(1), 3, 2)
	}
}

func TestSortedIntsBackwardIterationWithRangeOutsideBounds(t *testing.T) {
	for i := 0; i < 500; i++ {
		rand.Seed(int64(i))
		s := NewSortedInts("test")
		s.Load([]key.Type{1, 2, 3})
		assertIterator(t, s.Backwards().Range(-10, 10).Offset(1), 2, 1)
	}
}

func TestSortedIntsBackwardIterationMovesUpToPreviousClosest(t *testing.T) {
	for i := 0; i < 1000; i++ {
		rand.Seed(int64(i))
		s := NewSortedInts("test")
		s.SetInt(key.Type(1), 1)
		s.SetInt(key.Type(3), 3)
		s.SetInt(key.Type(4), 4)
		s.SetInt(key.Type(5), 5)
		assertIterator(t, s.Backwards().Range(2, 4).Offset(0), 4, 3)
	}
}

func TestSortedIntsRankingIfMemberDoesNotExist(t *testing.T) {
	spec := gspec.New(t)
	s := NewSortedInts("test")
	s.Load([]key.Type{1, 2, 3})
	_, exists := s.Score(100)
	spec.Expect(exists).ToEqual(false)
	spec.Expect(s.Contains(100)).ToEqual(false)
}

func TestSortedIntsRankingIfMemberExist(t *testing.T) {
	spec := gspec.New(t)
	s := NewSortedInts("test")
	s.Load([]key.Type{1, 2, 3})
	rank, exists := s.Score(3)
	spec.Expect(exists).ToEqual(true)
	spec.Expect(rank).ToEqual(2)
	spec.Expect(s.Contains(3)).ToEqual(true)
}

func TestSortedIntsReplace(t *testing.T) {
	spec := gspec.New(t)
	s := NewSortedInts("test")
	s.SetInt(1, 1)
	s.SetInt(2, 2)
	s.SetInt(1, 1)
	assertIterator(t, s.Forwards().Offset(0), 1, 2)
	score, _ := s.Score(1)
	spec.Expect(score).ToEqual(1)
}

func TestSortedIntsSetAndRemoveItems(t *testing.T) {
	spec := gspec.New(t)
	for i := 0; i < 500; i++ {
		rand.Seed(int64(i))
		s := NewSortedInts("test")
		s.SetInt(1, 1)
		s.SetInt(2, 2)
		s.SetInt(3, 3)
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

		s.SetInt(2, 0)
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

func assertIterator(t *testing.T, iterator Iterator, ids ...key.Type) {
	defer iterator.Close()
	spec := gspec.New(t)
	i := 0
	for id := iterator.Current(); id != key.NULL; id = iterator.Next() {
		spec.Expect(id).ToEqual(key.Type(ids[i]))
		i++
	}
}
