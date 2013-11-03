package indexes

import (
  "sync"
  "nabu/key"
  "container/list"
)

type DynamicRankSort struct {
  length int
  ids *list.List
  lock sync.RWMutex
  lookup map[key.Type]*DynamicRankSortValue
}

type DynamicRankSortValue  struct {
  rank int
  id key.Type
  element *list.Element
}

func (s *DynamicRankSort) Len() int {
  s.lock.RLock()
  defer s.lock.RUnlock()
  return len(s.lookup)
}

func (s *DynamicRankSort) CanRank() bool {
  return true
}

func (s *DynamicRankSort) Load(ids []key.Type) {
  newIds := list.New()
  lookup := make(map[key.Type]*DynamicRankSortValue, len(ids))
  for index, id := range ids {
    element := newIds.PushBack(id)
    lookup[id] = &DynamicRankSortValue{
      id: id,
      rank: index,
      element: element,
    }
  }
  newIds.PushFront(key.NULL)
  newIds.PushBack(key.NULL)

  s.lock.Lock()
  s.ids = newIds
  s.lookup = lookup
  s.lock.Unlock()
}

func (s *DynamicRankSort) Rank(id key.Type) (int, bool) {
  s.lock.RLock()
  wrapper, exists := s.lookup[id]
  s.lock.RUnlock()
  if exists == false { return 0, false }
  return wrapper.rank, true
}

func (s *DynamicRankSort) Forwards(offset int) Iterator {
  if offset > s.Len() { return emptyIterator }
  s.lock.RLock()

  element := s.ids.Front().Next() //padding
  for i := 0; i < offset; i++ { element = element.Next() }
  return &DynamicSortForwardIterator{
    lock: &s.lock,
    element: element,
  }
}

func (s *DynamicRankSort) Backwards(offset int) Iterator {
  if offset > s.Len() { return emptyIterator }
  s.lock.RLock()

  element := s.ids.Back().Prev() //padding
  for i := 0; i < offset; i++ { element = element.Prev() }
  return &DynamicSortBackwardsIterator{
    lock: &s.lock,
    element: element,
  }
}

type DynamicSortForwardIterator struct {
  lock *sync.RWMutex
  element *list.Element
}

func (i *DynamicSortForwardIterator) Next() key.Type {
  i.element = i.element.Next()
  return i.Current()
}

func (i *DynamicSortForwardIterator) Current() key.Type {
  return i.element.Value.(key.Type)
}

func (i *DynamicSortForwardIterator) Close() {
  i.lock.RUnlock()
}

type DynamicSortBackwardsIterator struct {
  lock *sync.RWMutex
  element *list.Element
}

func (i *DynamicSortBackwardsIterator) Next() key.Type {
  i.element = i.element.Prev()
  return i.Current()
}

func (i *DynamicSortBackwardsIterator) Current() key.Type {
  return i.element.Value.(key.Type)
}

func (i *DynamicSortBackwardsIterator) Close() {
  i.lock.RUnlock()
}
