package indexes

import (
	"github.com/karlseguin/nabu/key"
	"math"
	"math/rand"
	"sync"
)

const (
	maxLevel = 32
)

var (
	maxLevelIndex = maxLevel - 1
	slices        = make([]uint32, maxLevel)
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
	lock   sync.RWMutex
	head   *SkiplistNode
	tail   *SkiplistNode
	lookup map[key.Type]int
}

type SkiplistNode struct {
	rank  int
	id    key.Type
	next  []*SkiplistNode
	width []int
	prev  *SkiplistNode
}

func newSkiplist() *Skiplist {
	head := &SkiplistNode{
		id:   key.NULL,
		next: make([]*SkiplistNode, maxLevel),
	}
	tail := &SkiplistNode{
		id:    key.NULL,
		prev:  head,
		width: make([]int, maxLevel),
	}

	return &Skiplist{
		levels: 0,
		head:   head,
		tail:   tail,
		lookup: make(map[key.Type]int),
	}
}

// Stores a id within the index with the specified rank
func (s *Skiplist) Set(id key.Type, rank int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if r, exists := s.lookup[id]; exists {
		if r == rank {
			return
		}
		s.remove(id)
	}

	level := s.getLevel()
	node := &SkiplistNode{
		id:    id,
		rank:  rank,
		width: make([]int, level+1),
		next:  make([]*SkiplistNode, level+1),
	}

	current := s.head
	for i := s.levels; i >= 0; i-- {
		for ; current.next[i] != nil; current = current.next[i] {
			next := current.next[i]
			if next.rank > rank || (next.rank == rank && next.id > id) || next == s.tail {
				break
			}
		}
		if i > level {
			next := current.next[i]
			if next != nil {
				next.width[i]++
			}
			continue
		}
		if i == 0 {
			node.width[0] = 1
		} else {
			width := s.getWidth(current.next[i-1], i-1, rank)
			for j := i; j <= level; j++ {
				node.width[j] += width
			}
			node.width[i] += 1
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
	for i := 1; i <= level; i++ {
		next := node.next[i]
		if next != nil {
			next.width[i] -= node.width[i] - 1
		}
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
	s.Set(id, highRank+1)
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
	s.Set(id, lowRank-1)
}

func (s *Skiplist) remove(id key.Type) {
	rank, exists := s.lookup[id]
	if exists == false {
		return
	}

	current := s.head
	for i := s.levels; i >= 0; i-- {
		for ; current.next[i] != nil; current = current.next[i] {
			next := current.next[i]
			if next.id == id {
				current.next[i] = next.next[i]
				nn := next.next[i]
				if nn != nil {
					nn.width[i] += next.width[i] - 1
				}
				break
			} else if next.rank > rank {
				break
			}
		}
	}
	if current.next[0] == nil {
		s.tail.prev = current
	}
	delete(s.lookup, id)
}

// Get a node's width
func (s *Skiplist) getWidth(node *SkiplistNode, level int, rank int) int {
	width := 0
	for ; node != nil && node != s.tail; node = node.next[level] {
		if node.rank > rank {
			break
		}
		width += node.width[level]
	}
	return width
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

// Go to the node at the specified offset
// assumes the list is already read-locked
func (s *Skiplist) offset(offset int, current *SkiplistNode) *SkiplistNode {
	skipped := -1
	if current != s.head {
		current = current.prev
	}
	for i := len(current.next) - 1; i >= 0; i-- {
		next := current.next[i]

		if next == nil {
			continue
		}

		width := next.width[i]
		if skipped+width > offset {
			continue
		}
		current = next
		for ; current != s.tail; current = current.next[i] {
			skipped += current.width[i]
			if skipped == offset {
				return current
			}

			next := current.next[i]
			if next == nil || next.width[i]+skipped > offset {
				break
			}
		}
	}
	return nil
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
func (s *Skiplist) Forwards() Iterator {
	s.lock.RLock()
	return &SkipListForwardIterator{
		node: s.head.next[0],
		list: s,
		to:   s.tail.prev.rank,
	}
}

// Generates a backward iterator (from high rank to low)
func (s *Skiplist) Backwards() Iterator {
	s.lock.RLock()
	return &SkipListBackwardsIterator{
		node: s.tail.prev,
		list: s,
		from: s.head.next[0].rank,
	}
}

// Forward skip list iterator
type SkipListForwardIterator struct {
	list *Skiplist
	node *SkiplistNode
	to   int
}

// Move to the next (higher ranked) item
func (i *SkipListForwardIterator) Next() key.Type {
	i.node = i.node.next[0]
	if i.node.rank > i.to {
		i.node = i.list.tail
	}
	return i.Current()
}

// Key for the current item
func (i *SkipListForwardIterator) Current() key.Type {
	return i.node.id
}

// Sets the iterator's offset
func (i *SkipListForwardIterator) Offset(offset int) Iterator {
	i.node = i.list.offset(offset, i.node)
	if i.node == nil {
		i.node = i.list.tail
	}
	return i
}

// Specified the range of values to interate over
func (i *SkipListForwardIterator) Range(from, to int) Iterator {
	// from is already lower than our lowest rank
	if from <= i.node.rank {
		return i
	}
	s := i.list
	node := s.head
	for i := s.levels; i >= 0; i-- {
		for ; node.next[i] != nil; node = node.next[i] {
			next := node.next[i]
			if next.rank > from {
				break
			}
		}
	}
	i.node = node
	i.to = to
	return i
}

// Release the iterator
func (i *SkipListForwardIterator) Close() {
	i.list.lock.RUnlock()
}

// Backward skip list iterator
type SkipListBackwardsIterator struct {
	list *Skiplist
	node *SkiplistNode
	from int
}

// Move to the next (lower ranked) item
func (i *SkipListBackwardsIterator) Next() key.Type {
	i.node = i.node.prev
	if i.node.rank < i.from {
		i.node = i.list.head
	}
	return i.Current()
}

// Key for the current item
func (i *SkipListBackwardsIterator) Current() key.Type {
	return i.node.id
}

// Release the iterator
func (i *SkipListBackwardsIterator) Offset(offset int) Iterator {
	for j := 0; j < offset; j++ {
		i.node = i.node.prev
		if i.node.id == key.NULL {
			return i
		}
	}
	return i
}

// Specified the range of values to interate over
func (i *SkipListBackwardsIterator) Range(from, to int) Iterator {
	// to is already greater than our lowest rank
	if to >= i.node.rank {
		return i
	}

	s := i.list
	node := s.head
	for i := s.levels; i >= 0; i-- {
		for ; node.next[i] != nil; node = node.next[i] {
			next := node.next[i]
			if next.rank > to {
				break
			}
		}
	}
	i.node = node
	i.from = from
	return i
}

// Release the iterator
func (i *SkipListBackwardsIterator) Close() {
	i.list.lock.RUnlock()
}
