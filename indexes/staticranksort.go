package indexes

import (
  "sync"
  "github.com/karlseguin/nabu/key"
)

type StaticRankSort struct {
  length int
  ids []key.Type
  paddedLength int
  lock sync.RWMutex
  lookup map[key.Type]int
}

func (s *StaticRankSort) Len() int {
  s.lock.RLock()
  defer s.lock.RUnlock()
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

  s.lock.Lock()
  s.paddedLength = length
  s.length = length - 2
  s.ids = padded
  s.lookup = lookup
  s.lock.Unlock()
}

func (s *StaticRankSort) Rank(id key.Type) (int, bool) {
  s.lock.RLock()
  defer s.lock.RUnlock()
  rank, exists := s.lookup[id]
  return rank, exists
}

func (s *StaticRankSort) Append(id key.Type) {
  s.lock.Lock()
  defer s.lock.Unlock()
  l := s.paddedLength
  s.modify(id, 0, l, l-1, s.lookup[s.ids[l-2]]+1)
}

func (s *StaticRankSort) Prepend(id key.Type) {
  s.lock.Lock()
  defer s.lock.Unlock()
  s.modify(id, 1, 0, 1, s.lookup[s.ids[1]]-1)
}

func (s *StaticRankSort) modify(id key.Type, offset, newNull, newIndex, newRank int) {
  l := s.paddedLength
  padded := make([]key.Type, l+1)
  copy(padded[offset:], s.ids)
  padded[newNull] = key.NULL
  padded[newIndex] = id
  s.paddedLength++
  s.length++
  s.ids = padded
  s.lookup[id] = newRank
}

func (s *StaticRankSort) Forwards(offset int) Iterator {
  if offset > s.Len() { return emptyIterator }
  s.lock.RLock()
  return &StaticSortForwardIterator{
    lock: &s.lock,
    position: offset+1,
    ids: s.ids[0:s.paddedLength],
  }
}

func (s *StaticRankSort) Backwards(offset int) Iterator {
  if offset > s.Len() { return emptyIterator }

  s.lock.RLock()
  return &StaticSortBackwardsIterator{
    lock: &s.lock,
    ids: s.ids[0:s.paddedLength],
    position: s.length - offset,
  }
}
