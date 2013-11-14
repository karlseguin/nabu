package indexes

import (
  "sync"
  "github.com/karlseguin/nabu/key"
)

type StaticSort struct {
  length int
  ids []key.Type
  paddedLength int
  lock sync.RWMutex
}

func (s *StaticSort) Len() int {
  s.lock.RLock()
  defer s.lock.RUnlock()
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

  s.lock.Lock()
  s.paddedLength = length
  s.length = length - 2
  s.ids = padded
  s.lock.Unlock()
}

func (s *StaticSort) Rank(id key.Type) (int, bool) {
  return 0, false
}

func (s *StaticSort) Append(id key.Type) {
  s.lock.Lock()
  defer s.lock.Unlock()
  l := s.paddedLength
  s.modify(id, 0, l, l-1)
}

func (s *StaticSort) Prepend(id key.Type) {
  s.lock.Lock()
  defer s.lock.Unlock()
  s.modify(id, 1, 0, 1)
}

func (s *StaticSort) modify(id key.Type, offset, newNull, newIndex int) {
  l := s.paddedLength
  padded := make([]key.Type, l+1)
  copy(padded[offset:], s.ids)
  padded[newNull] = key.NULL
  padded[newIndex] = id
  s.paddedLength++
  s.length++
  s.ids = padded
}

func (s *StaticSort) Forwards(offset int) Iterator {
  if offset > s.Len() { return emptyIterator }
  s.lock.RLock()
  return &StaticSortForwardIterator{
    lock: &s.lock,
    position: offset+1,
    ids: s.ids[0:s.paddedLength],
  }
}

func (s *StaticSort) Backwards(offset int) Iterator {
  if offset > s.Len() { return emptyIterator }

  s.lock.RLock()
  return &StaticSortBackwardsIterator{
    lock: &s.lock,
    ids: s.ids[0:s.paddedLength],
    position: s.length - offset,
  }
}

type StaticSortForwardIterator struct {
  position int
  ids []key.Type
  lock *sync.RWMutex
}

func (i *StaticSortForwardIterator) Next() key.Type {
  i.position++
  return i.Current()
}

func (i *StaticSortForwardIterator) Current() key.Type {
  return i.ids[i.position]
}

func (i *StaticSortForwardIterator) Close() {
  i.lock.RUnlock()
}

type StaticSortBackwardsIterator struct {
  position int
  ids []key.Type
  lock *sync.RWMutex
}

func (i *StaticSortBackwardsIterator) Next() key.Type {
  i.position--
  return i.Current()
}

func (i *StaticSortBackwardsIterator) Current() key.Type {
  return i.ids[i.position]
}

func (i *StaticSortBackwardsIterator) Close() {
  i.lock.RUnlock()
}
