package indexes

import (
  "testing"
  "nabu/key"
  "github.com/karlseguin/gspec"
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
