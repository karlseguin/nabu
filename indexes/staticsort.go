package indexes

import (
  "sync"
  "nabu/key"
)

type StaticSort struct {
  length int
  sync.RWMutex
  ids []key.Type
}

func (s *StaticSort) Len() int {
  s.RLock()
  defer s.RUnlock()
  return s.length
}

func (s *StaticSort) CanRank() bool {
  return false
}

func (s *StaticSort) Load(ids []key.Type) {
  length := len(ids)+2
  padded := make([]key.Type, length)
  padded[0] = key.NULL
  copy(padded[1:length-1], ids)
  padded[length-1] = key.NULL

  s.Lock()
  s.length = length - 2
  s.ids = padded
  s.Unlock()
}

func (s *StaticSort) Rank(id key.Type) (int, bool) {
  return 0, false
}

func (s *StaticSort) Forwards(offset int) Iterator {
  if offset > s.Len() { return emptyIterator }
  s.RLock()
  return &StaticSortForwardIterator{
    s: s,
    position: offset+1,
  }
}

func (s *StaticSort) Backwards(offset int) Iterator {
  if offset > s.Len() { return emptyIterator }

  s.RLock()
  return &StaticSortBackwardsIterator{
    s: s,
    position: s.Len() - offset,
  }
}

type StaticSortForwardIterator struct {
  position int
  s *StaticSort
}

func (i *StaticSortForwardIterator) Next() key.Type {
  i.position++
  return i.Current()
}

func (i *StaticSortForwardIterator) Current() key.Type {
  return i.s.ids[i.position]
}

func (i *StaticSortForwardIterator) Close() {
  i.s.RUnlock()
}

type StaticSortBackwardsIterator struct {
  position int
  s *StaticSort
}

func (i *StaticSortBackwardsIterator) Next() key.Type {
  i.position--
  return i.Current()
}

func (i *StaticSortBackwardsIterator) Current() key.Type {
  return i.s.ids[i.position]
}

func (i *StaticSortBackwardsIterator) Close() {
  i.s.RUnlock()
}
