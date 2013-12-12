package indexes

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"testing"
)

//static indexes are padded, so this isn't as meaningless as it seems
//(but it's still pretty meaningless)
func TestStaticScoreSortLength(t *testing.T) {
	spec := gspec.New(t)
	s := &StaticScoreSort{}
	s.Load([]key.Type{1, 2, 3})
	spec.Expect(s.Len()).ToEqual(3)
}

func TestStaticScoreSortForwardIteration(t *testing.T) {
	s := &StaticScoreSort{}
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Forwards(), 1, 2, 3)
}

func TestStaticScoreSortBackwardIteration(t *testing.T) {
	s := &StaticScoreSort{}
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Backwards(), 3, 2, 1)
}

func TestStaticScoreSortForwardIterationWithOffset(t *testing.T) {
	s := &StaticScoreSort{}
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Forwards().Offset(1), 2, 3)
}

func TestStaticScoreSortBackwardIterationWithOffset(t *testing.T) {
	s := &StaticScoreSort{}
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Backwards().Offset(1), 2, 1)
}

func TestStaticScoreSortForwardIterationWithOffsetAtRange(t *testing.T) {
	s := &StaticScoreSort{}
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Forwards().Offset(3), key.NULL)
}

func TestStaticScoreSortBackwardIterationWithOffsetAtRange(t *testing.T) {
	s := &StaticScoreSort{}
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Backwards().Offset(3), key.NULL)
}

func TestStaticScoreSortForwardIterationWithOffsetOutsideOfRange(t *testing.T) {
	s := &StaticScoreSort{}
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Forwards().Offset(4), key.NULL)
}

func TestStaticScoreSortBackwardIterationWithOffsetOutsideOfRange(t *testing.T) {
	s := &StaticScoreSort{}
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Backwards().Offset(4), key.NULL)
}

func TestStaticScoreSortScoringIfMemberDoesNotExist(t *testing.T) {
	s := &StaticScoreSort{}
	s.Load([]key.Type{1, 2, 3})
	_, exists := s.GetScore(99)
	gspec.New(t).Expect(exists).ToEqual(false)
}

func TestStaticScoreSortScoringIfMemberExist(t *testing.T) {
	spec := gspec.New(t)
	s := &StaticScoreSort{}
	s.Load([]key.Type{1, 2, 3})
	score, exists := s.GetScore(3)
	spec.Expect(exists).ToEqual(true)
	spec.Expect(score).ToEqual(2)
}

func TestStaticScoreSortCanAppendAValue(t *testing.T) {
	spec := gspec.New(t)
	s := &StaticScoreSort{}
	s.Load([]key.Type{1, 2, 3})
	s.Append(4)
	assertIterator(t, s.Forwards(), 1, 2, 3, 4)
	spec.Expect(s.Len()).ToEqual(4)
	spec.Expect(s.GetScore(4)).ToEqual(3)
}

func TestStaticScoreSortCanPrependAValue(t *testing.T) {
	spec := gspec.New(t)
	s := &StaticScoreSort{}
	s.Load([]key.Type{1, 2, 3})
	s.Prepend(99)
	assertIterator(t, s.Forwards(), 99, 1, 2, 3)
	spec.Expect(s.Len()).ToEqual(4)
	spec.Expect(s.GetScore(99)).ToEqual(-1)
}
