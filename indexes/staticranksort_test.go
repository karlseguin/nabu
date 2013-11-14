package indexes

import (
  "testing"
  "github.com/karlseguin/gspec"
  "github.com/karlseguin/nabu/key"
)

//static indexes are padded, so this isn't as meaningless as it seems
//(but it's still pretty meaningless)
func TestStaticRankSortLength(t *testing.T) {
  spec := gspec.New(t)
  s := &StaticRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  spec.Expect(s.Len()).ToEqual(3)
}

func TestStaticRankSortForwardIteration(t *testing.T) {
  s := &StaticRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  assertIterator(t, s.Forwards(0), "a", "b", "c")
}

func TestStaticRankSortBackwardIteration(t *testing.T) {
  s := &StaticRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  assertIterator(t, s.Backwards(0), "c", "b", "a")
}

func TestStaticRankSortForwardIterationWithOffset(t *testing.T) {
  s := &StaticRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  assertIterator(t, s.Forwards(1), "b", "c")
}

func TestStaticRankSortBackwardIterationWithOffset(t *testing.T) {
  s := &StaticRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  assertIterator(t, s.Backwards(1), "b", "a")
}

func TestStaticRankSortForwardIterationWithOffsetAtRange(t *testing.T) {
  s := &StaticRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  assertIterator(t, s.Forwards(3), "")
}

func TestStaticRankSortBackwardIterationWithOffsetAtRange(t *testing.T) {
  s := &StaticRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  assertIterator(t, s.Backwards(3), "")
}

func TestStaticRankSortForwardIterationWithOffsetOutsideOfRange(t *testing.T) {
  s := &StaticRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  assertIterator(t, s.Forwards(4), "")
}

func TestStaticRankSortBackwardIterationWithOffsetOutsideOfRange(t *testing.T) {
  s := &StaticRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  assertIterator(t, s.Backwards(4), "")
}

func TestStaticRankSortRankingIfMemberDoesNotExist(t *testing.T) {
  s := &StaticRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  _, exists := s.Rank("z")
  gspec.New(t).Expect(exists).ToEqual(false)
}

func TestStaticRankSortRankingIfMemberExist(t *testing.T) {
  spec := gspec.New(t)
  s := &StaticRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  rank, exists := s.Rank("c")
  spec.Expect(exists).ToEqual(true)
  spec.Expect(rank).ToEqual(2)
}

func TestStaticRankSortCanAppendAValue(t *testing.T) {
  spec := gspec.New(t)
  s := &StaticRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  s.Append("d")
  assertIterator(t, s.Forwards(0), "a", "b", "c", "d")
  spec.Expect(s.Len()).ToEqual(4)
  spec.Expect(s.Rank("d")).ToEqual(3)
}

func TestStaticRankSortCanPrependAValue(t *testing.T) {
  spec := gspec.New(t)
  s := &StaticRankSort{}
  s.Load([]key.Type{"a", "b", "c"})
  s.Prepend("z")
  assertIterator(t, s.Forwards(0), "z", "a", "b", "c")
  spec.Expect(s.Len()).ToEqual(4)
  spec.Expect(s.Rank("z")).ToEqual(-1)
}
