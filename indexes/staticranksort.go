package indexes

import (
  "sync"
  "nabu/key"
)

type StaticRankSort struct {
  length int
  sync.RWMutex
  ids []key.Type
  lookup map[key.Type]int
}

func (s *StaticRankSort) Len() int {
  s.RLock()
  defer s.RUnlock()
  return s.length
}

func (s *StaticRankSort) CanRank() bool {
  return true
}

func (s *StaticRankSort) Load(ids []key.Type) {
  length := len(ids)+2
  padded := make([]key.Type, length)
  lookup := make(map[key.Type]int, length)
  padded[0] = key.NULL
  for index, id := range ids {
    padded[index+1] = id
    lookup[id] = index
  }
  padded[length-1] = key.NULL

  s.Lock()
  s.length = length - 2
  s.ids = padded
  s.lookup = lookup
  s.Unlock()
}

func (s *StaticRankSort) Rank(id key.Type) (int, bool) {
  s.RLock()
  defer s.RUnlock()
  rank, exists := s.lookup[id]
  return rank, exists
}

func (s *StaticRankSort) Forwards(offset int) Iterator {
  if offset > s.Len() { return emptyIterator }
  s.RLock()
  return &StaticRankSortForwardIterator{
    s: s,
    position: offset+1,
  }
}

func (s *StaticRankSort) Backwards(offset int) Iterator {
  if offset > s.Len() { return emptyIterator }

  s.RLock()
  return &StaticRankSortBackwardsIterator{
    s: s,
    position: s.Len() - offset,
  }
}

type StaticRankSortForwardIterator struct {
  position int
  s *StaticRankSort
}

func (i *StaticRankSortForwardIterator) Next() key.Type {
  i.position++
  return i.Current()
}

func (i *StaticRankSortForwardIterator) Current() key.Type {
  return i.s.ids[i.position]
}

func (i *StaticRankSortForwardIterator) Close() {
  i.s.RUnlock()
}

type StaticRankSortBackwardsIterator struct {
  position int
  s *StaticRankSort
}

func (i *StaticRankSortBackwardsIterator) Next() key.Type {
  i.position--
  return i.Current()
}

func (i *StaticRankSortBackwardsIterator) Current() key.Type {
  return i.s.ids[i.position]
}

func (i *StaticRankSortBackwardsIterator) Close() {
  i.s.RUnlock()
}
