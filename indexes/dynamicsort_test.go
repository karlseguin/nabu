package indexes

import (
  "testing"
  "nabu/key"
  "github.com/karlseguin/gspec"
)

func TestDynamicRankSortLength(t *testing.T) {
  spec := gspec.New(t)
  s := &DynamicRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  spec.Expect(s.Len()).ToEqual(3)
}

func TestDynamicRankSortForwardIteration(t *testing.T) {
  s := &DynamicRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  assertIterator(t, s.Forwards(0), "a", "b", "c")
}

func TestDynamicRankSortBackwardIteration(t *testing.T) {
  s := &DynamicRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  assertIterator(t, s.Backwards(0), "c", "b", "a")
}

func TestDynamicRankSortForwardIterationWithOffset(t *testing.T) {
  s := &DynamicRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  assertIterator(t, s.Forwards(1), "b", "c")
}

func TestDynamicRankSortBackwardIterationWithOffset(t *testing.T) {
  s := &DynamicRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  assertIterator(t, s.Backwards(1), "b", "a")
}

func TestDynamicRankSortForwardIterationWithOffsetAtRange(t *testing.T) {
  s := &DynamicRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  assertIterator(t, s.Forwards(3), "")
}

func TestDynamicRankSortBackwardIterationWithOffsetAtRange(t *testing.T) {
  s := &DynamicRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  assertIterator(t, s.Backwards(3), "")
}

func TestDynamicRankSortForwardIterationWithOffsetOutsideOfRange(t *testing.T) {
  s := &DynamicRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  assertIterator(t, s.Forwards(4), "")
}

func TestDynamicRankSortBackwardIterationWithOffsetOutsideOfRange(t *testing.T) {
  s := &DynamicRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  assertIterator(t, s.Backwards(4), "")
}

func TestDynamicRankSortRankingIfMemberDoesNotExist(t *testing.T) {
  s := &DynamicRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  _, exists := s.Rank("z")
  gspec.New(t).Expect(exists).ToEqual(false)
}

func TestDynamicRankSortRankingIfMemberExist(t *testing.T) {
  spec := gspec.New(t)
  s := &DynamicRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  rank, exists := s.Rank("c")
  spec.Expect(exists).ToEqual(true)
  spec.Expect(rank).ToEqual(2)
}
