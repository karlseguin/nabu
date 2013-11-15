package indexes

import (
  "sync"
  "math"
  "math/rand"
  "github.com/karlseguin/nabu/key"
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

// A dynamic sorted index. Ideal for sorted indexes which are frequently
// modified
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
    prev: head,
  }

  return &Skiplist {
    levels: 0,
    head: head,
    tail: tail,
    lookup: make(map[key.Type]int),
  }
}

// Stores a id within the index with the specified rank
func (s *Skiplist) Set(id key.Type, rank int) {
  s.lock.Lock()
  defer s.lock.Unlock()
  if r, exists := s.lookup[id]; exists {
    if r == rank { return }
    s.remove(id)
  }

  level := s.getLevel()
  node := &SkiplistNode {
    id: id,
    rank: rank,
    next: make([]*SkiplistNode, level+1),
  }

  current := s.head
  for i := s.levels; i >= 0; i-- {
    for ; current.next[i] != nil; current = current.next[i] {
      next := current.next[i]
      if next.rank > rank || (next.rank == rank && next.id > id) || next == s.tail { break }
    }
    if i > level { continue }
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

// Removes the id from the index
func (s *Skiplist) Remove(id key.Type) {
  s.lock.Lock()
  defer s.lock.Unlock()
  s.remove(id)
}

// Appends the id to the end of the index giving it a rank of
// the current max rank + 1.
func (s *Skiplist) Append(id key.Type) {
  s.lock.RLock()
  highRank := s.tail.prev.rank
  s.lock.RUnlock()
  s.Set(id, highRank + 1)
}

// Prepends the id to the end of the index giving it a rank of
// the current min rank - 1 (this could negative)
func (s *Skiplist) Prepend(id key.Type) {
  s.lock.RLock()
  var lowRank int
  if s.head.next[0] != nil {
    lowRank = s.head.next[0].rank
  }
  s.lock.RUnlock()
  s.Set(id, lowRank - 1)
}

func (s *Skiplist) remove(id key.Type) {
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

// Determins the level to place a new item (0-31)
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

// Number of items in the index
func (s *Skiplist) Len() int {
  s.lock.RLock()
  defer s.lock.RUnlock()
  return len(s.lookup)
}

// This index is able to rank documents, and thus can be
// used for the post-sorting used by index-first queries
func (s *Skiplist) CanRank() bool {
  return true
}

// Bulk loads ids into the index. This replaces any existing values.
// The rank is implied by the array order.
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

// Ranks a document
func (s *Skiplist) Rank(id key.Type) (int, bool) {
  s.lock.RLock()
  rank, exists := s.lookup[id]
  s.lock.RUnlock()
  return rank, exists
}

// Generates a forward iterator (from low rank to high)
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

// Generates a backward iterator (from high rank to low)
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

// Forward skip list iterator
type SkipListForwardIterator struct {
  lock *sync.RWMutex
  node *SkiplistNode
}

// Move to the next (higher ranked) item
func (i *SkipListForwardIterator) Next() key.Type {
  i.node = i.node.next[0]
  return i.Current()
}

// Key for the current item
func (i *SkipListForwardIterator) Current() key.Type {
  return i.node.id
}

// Release the iterator
func (i *SkipListForwardIterator) Close() {
  i.lock.RUnlock()
}

// Backward skip list iterator
type SkipListBackwardsIterator struct {
  lock *sync.RWMutex
  node *SkiplistNode
}

// Move to the next (lower ranked) item
func (i *SkipListBackwardsIterator) Next() key.Type {
  i.node = i.node.prev
  return i.Current()
}

// Key for the current item
func (i *SkipListBackwardsIterator) Current() key.Type {
  return i.node.id
}

// Release the iterator
func (i *SkipListBackwardsIterator) Close() {
  i.lock.RUnlock()
}
