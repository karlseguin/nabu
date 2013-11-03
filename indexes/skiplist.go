package indexes

import (
  "sync"
  "math"
  "nabu/key"
  "math/rand"
)

const (
  maxLevel = 32
)

var  (
  maxLevelIndex = maxLevel - 1
  slices = make([]uint32, maxLevel)
)

func init() {
  total := uint32(0)
  for i := maxLevelIndex; i > -1; i-- {
    total += uint32(math.Pow(2, float64(i)))
    slices[maxLevelIndex-i] = total
  }
}

type Skiplist struct {
  levels int
  lock sync.RWMutex
  head *SkiplistNode
  tail *SkiplistNode
  lookup map[key.Type]int
}

type SkiplistNode struct {
  rank int
  id key.Type
  next []*SkiplistNode
  prev *SkiplistNode
}

func newSkiplist() *Skiplist {
  head := &SkiplistNode {
    id: key.NULL,
    next: make([]*SkiplistNode, maxLevel),
  }
  tail := &SkiplistNode {
    id: key.NULL,
  }

  return &Skiplist {
    levels: 0,
    head: head,
    tail: tail,
    lookup: make(map[key.Type]int),
  }
}

func (s *Skiplist) Set(id key.Type, rank int) {
  s.lock.Lock()
  defer s.lock.Unlock()
  s.delete(id)

  level := s.getLevel()
  node := &SkiplistNode {
    id: id,
    rank: rank,
    next: make([]*SkiplistNode, level+1),
  }

  current := s.head
  for i := level; i >= 0; i-- {
    for ; current.next[i] != nil; current = current.next[i] {
      next := current.next[i]
      if next.rank > rank || (next.rank == rank && next.id > id) || next == s.tail { break }
    }
    node.next[i] = current.next[i]
    current.next[i] = node
    node.prev = current
  }
  if node.next[0] == nil {
    s.tail.prev = node
    node.next[0] = s.tail
  } else {
    node.next[0].prev = node
  }
  s.lookup[id] = rank
}

func (s *Skiplist) Delete(id key.Type) {
  s.lock.Lock()
  defer s.lock.Unlock()
  s.delete(id)
}

func (s *Skiplist) delete(id key.Type) {
  rank, exists := s.lookup[id]
  if exists == false { return }

  current := s.head
  for i := s.levels; i >= 0; i-- {
    for ; current.next[i] != nil; current = current.next[i] {
      next := current.next[i]
      if next.rank > rank || next.id == id { break }
    }
    if current.next[i] != nil && current.next[i].id == id {
      current.next[i] = current.next[i].next[i]
    }
  }
  if current.next[0] == nil {
    s.tail.prev = current
  }
  delete(s.lookup, id)
}

func (s *Skiplist) getLevel() int {
  roll := rand.Uint32()
  for i := 0; i <= s.levels; i++ {
    if roll < slices[i] {
      return i
    }
  }
  if s.levels < maxLevelIndex {
    s.levels++
  }
  return s.levels
}

func (s *Skiplist) Len() int {
  s.lock.RLock()
  defer s.lock.RUnlock()
  return len(s.lookup)
}

func (s *Skiplist) CanRank() bool {
  return true
}

func (s *Skiplist) Load(ids []key.Type) {
  s.lock.Lock()
  s.head.next = make([]*SkiplistNode, maxLevel)
  s.lookup = make(map[key.Type]int)
  s.tail.prev = nil
  s.levels = 0
  s.lock.Unlock()
  for index, id := range ids {
    s.Set(id, index)
  }
}

func (s *Skiplist) Rank(id key.Type) (int, bool) {
  s.lock.RLock()
  rank, exists := s.lookup[id]
  s.lock.RUnlock()
  return rank, exists
}

func (s *Skiplist) Forwards(offset int) Iterator {
  if offset > s.Len() { return emptyIterator }
  s.lock.RLock()

  node := s.head.next[0]
  for i := 0; i < offset; i++ { node = node.next[0] }
  return &SkipListForwardIterator{
    node: node,
    lock: &s.lock,
  }
}

func (s *Skiplist) Backwards(offset int) Iterator {
  if offset > s.Len() { return emptyIterator }
  s.lock.RLock()

  node := s.tail.prev
  for i := 0; i < offset; i++ { node = node.prev }
  return &SkipListBackwardsIterator{
    node: node,
    lock: &s.lock,
  }
}

type SkipListForwardIterator struct {
  lock *sync.RWMutex
  node *SkiplistNode
}

func (i *SkipListForwardIterator) Next() key.Type {
  i.node = i.node.next[0]
  return i.Current()
}

func (i *SkipListForwardIterator) Current() key.Type {
  return i.node.id
}

func (i *SkipListForwardIterator) Close() {
  i.lock.RUnlock()
}

type SkipListBackwardsIterator struct {
  lock *sync.RWMutex
  node *SkiplistNode
}

func (i *SkipListBackwardsIterator) Next() key.Type {
  i.node = i.node.prev
  return i.Current()
}

func (i *SkipListBackwardsIterator) Current() key.Type {
  return i.node.id
}

func (i *SkipListBackwardsIterator) Close() {
  i.lock.RUnlock()
}
