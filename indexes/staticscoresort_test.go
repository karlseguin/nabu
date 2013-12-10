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
	s.Load([]key.Type{"a", "b", "c"})
	spec.Expect(s.Len()).ToEqual(3)
}

func TestStaticScoreSortForwardIteration(t *testing.T) {
	s := &StaticScoreSort{}
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Forwards(), "a", "b", "c")
}

func TestStaticScoreSortBackwardIteration(t *testing.T) {
	s := &StaticScoreSort{}
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Backwards(), "c", "b", "a")
}

func TestStaticScoreSortForwardIterationWithOffset(t *testing.T) {
	s := &StaticScoreSort{}
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Forwards().Offset(1), "b", "c")
}

func TestStaticScoreSortBackwardIterationWithOffset(t *testing.T) {
	s := &StaticScoreSort{}
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Backwards().Offset(1), "b", "a")
}

func TestStaticScoreSortForwardIterationWithOffsetAtRange(t *testing.T) {
	s := &StaticScoreSort{}
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Forwards().Offset(3), "")
}

func TestStaticScoreSortBackwardIterationWithOffsetAtRange(t *testing.T) {
	s := &StaticScoreSort{}
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Backwards().Offset(3), "")
}

func TestStaticScoreSortForwardIterationWithOffsetOutsideOfRange(t *testing.T) {
	s := &StaticScoreSort{}
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Forwards().Offset(4), "")
}

func TestStaticScoreSortBackwardIterationWithOffsetOutsideOfRange(t *testing.T) {
	s := &StaticScoreSort{}
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Backwards().Offset(4), "")
}

func TestStaticScoreSortScoringIfMemberDoesNotExist(t *testing.T) {
	s := &StaticScoreSort{}
	s.Load([]key.Type{"a", "b", "c"})
	_, exists := s.GetScore("z")
	gspec.New(t).Expect(exists).ToEqual(false)
}

func TestStaticScoreSortScoringIfMemberExist(t *testing.T) {
	spec := gspec.New(t)
	s := &StaticScoreSort{}
	s.Load([]key.Type{"a", "b", "c"})
	score, exists := s.GetScore("c")
	spec.Expect(exists).ToEqual(true)
	spec.Expect(score).ToEqual(2)
}

func TestStaticScoreSortCanAppendAValue(t *testing.T) {
	spec := gspec.New(t)
	s := &StaticScoreSort{}
	s.Load([]key.Type{"a", "b", "c"})
	s.Append("d")
	assertIterator(t, s.Forwards(), "a", "b", "c", "d")
	spec.Expect(s.Len()).ToEqual(4)
	spec.Expect(s.GetScore("d")).ToEqual(3)
}

func TestStaticScoreSortCanPrependAValue(t *testing.T) {
	spec := gspec.New(t)
	s := &StaticScoreSort{}
	s.Load([]key.Type{"a", "b", "c"})
	s.Prepend("z")
	assertIterator(t, s.Forwards(), "z", "a", "b", "c")
	spec.Expect(s.Len()).ToEqual(4)
	spec.Expect(s.GetScore("z")).ToEqual(-1)
}
