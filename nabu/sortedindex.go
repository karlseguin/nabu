package nabu

import (
  "time"
  "sync"
  "math/rand"
)

const (
  maxLevel = 32
  p = 0.5
)

type SortedIndexIterator interface {
  Next()
  HasNext() bool
  Current() (int, string)
  Close()
}

type SortedIndex interface {
  Set(rank int, id string)
  Remove(id string) (int, bool)
  Forward() SortedIndexIterator
  Backward() SortedIndexIterator
}

func NewSortedIndex() SortedIndex {
  head := &SkiplistNode {
    id: "HEAD",
    rank: 0,
    next: make([]*SkiplistNode, maxLevel + 1),
  }
  return &Skiplist {
    levels: 1,
    rand: rand.New(rand.NewSource(time.Now().Unix())),
    head: head,
    tail: head,
    lookup: make(map[string]int, 16392),
  }
}

type Skiplist struct {
  levels int
  rand *rand.Rand
  head *SkiplistNode
  tail *SkiplistNode
  lookup map[string]int
  sync.RWMutex
}

type SkiplistNode struct {
  id string
  rank int
  next []*SkiplistNode
  prev *SkiplistNode
}

func (s *Skiplist) Set(rank int, id string) {
  s.RLock()
  old, exists := s.lookup[id]
  s.RUnlock()
  if exists {
    if old == rank { return }
    s.Remove(id)
  }

  var level int
  for ; level < maxLevel && s.rand.Float64() < p; level++ {
    if (level == s.levels) {
      s.levels++
      break
    }
  }

  node := &SkiplistNode {
    id: id,
    rank: rank,
    next: make([]*SkiplistNode, level+1),
  }

  s.Lock()
  defer s.Unlock()
  current := s.head
  for i := s.levels - 1; i >= 0; i-- {
    for ; current.next[i] != nil; current = current.next[i] {
      next := current.next[i]
      if next.rank > rank || (next.rank == rank && next.id > id) { break }
    }
    if i <= level {
      node.next[i] = current.next[i]
      current.next[i] = node
      node.prev = current
    }
  }
  if node.next[0] == nil {
    s.tail = node
  } else {
    node.next[0].prev = node
  }
  s.lookup[id] = rank
}

func (s *Skiplist) Remove(id string) (int, bool) {
  s.RLock()
  rank, exists := s.lookup[id]
  s.RUnlock()
  if exists == false { return 0, false }

  s.Lock()
  defer s.Unlock()
  current := s.head
  for i := s.levels - 1; i >= 0; i-- {
    for ; current.next[i] != nil; current = current.next[i] {
      if current.next[i].rank == rank && current.next[i].id == id {
        current.next[i] = current.next[i].next[i]
        break
      }
      next := current.next[i]
      if next.rank > rank || (next.rank == rank && next.id > id) {
        break
      }
    }
  }
  if current.next[0] == nil {
    s.tail = current
  } else {
    current.next[0].prev = current
  }
  return rank, true
}

func (n *SkiplistNode) String() string {
  return n.id
}

func (s *Skiplist) Forward() SortedIndexIterator {
  s.RLock()
  return &SkiplistForwardIterator{
    current: s.head.next[0],
    unlock: func() { s.RUnlock() },
  }
}

func (s *Skiplist) Backward() SortedIndexIterator {
  s.RLock()
  return &SkiplistBackwardIterator{
    current: s.tail,
    unlock: func() { s.RUnlock() },
  }
}

type SkiplistForwardIterator struct {
  current *SkiplistNode
  unlock func()
}

func (i *SkiplistForwardIterator) HasNext() bool {
  return i.current != nil
}

func (i *SkiplistForwardIterator) Next() {
  i.current = i.current.next[0]
}

func (i *SkiplistForwardIterator) Current() (int, string) {
  return i.current.rank, i.current.id
}

func (i *SkiplistForwardIterator) Close() {
  i.unlock()
}

type SkiplistBackwardIterator struct {
  current *SkiplistNode
  unlock func()
}

func (i *SkiplistBackwardIterator) HasNext() bool {
  return i.current.id != "HEAD"
}

func (i *SkiplistBackwardIterator) Next() {
  i.current = i.current.prev
}

func (i *SkiplistBackwardIterator) Current() (int, string) {
  return i.current.rank, i.current.id
}

func (i *SkiplistBackwardIterator) Close() {
  i.unlock()
}