package indexes

import (
  "sync"
)

type StaticSort struct {
  length int
  sync.RWMutex
  ids []string
}

func (s *StaticSort) Len() int {
  s.RLock()
  defer s.RUnlock()
  return s.length
}

func (s *StaticSort) CanRank() bool {
  return false
}

func (s *StaticSort) Load(ids []string) {
  length := len(ids)+2
  padded := make([]string, length)
  padded[0] = ""
  copy(padded[1:length-1], ids)
  padded[length-1] = ""

  s.Lock()
  s.length = length - 2
  s.ids = padded
  s.Unlock()
}

func (s *StaticSort) Rank(id string) (int, bool) {
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

func (i *StaticSortForwardIterator) Next() string {
  i.position++
  return i.Current()
}

func (i *StaticSortForwardIterator) Current() string {
  return i.s.ids[i.position]
}

func (i *StaticSortForwardIterator) Close() {
  i.s.RUnlock()
}

type StaticSortBackwardsIterator struct {
  position int
  s *StaticSort
}

func (i *StaticSortBackwardsIterator) Next() string {
  i.position--
  return i.Current()
}

func (i *StaticSortBackwardsIterator) Current() string {
  return i.s.ids[i.position]
}

func (i *StaticSortBackwardsIterator) Close() {
  i.s.RUnlock()
}
