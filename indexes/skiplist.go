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
	score int
	id    key.Type
	next  []*SkiplistNode
	width []int
	prev  *SkiplistNode
}

func newSkiplist() *Skiplist {
	head := &SkiplistNode{
		id:    key.NULL,
		width: make([]int, maxLevel),
		next:  make([]*SkiplistNode, maxLevel),
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

// Stores a id within the index with the specified score
func (s *Skiplist) Set(id key.Type, score int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if r, exists := s.lookup[id]; exists {
		if r == score {
			return
		}
		s.remove(id)
	}

	level := s.getLevel()
	node := &SkiplistNode{
		id:    id,
		score: score,
		width: make([]int, level+1),
		next:  make([]*SkiplistNode, level+1),
	}

	current := s.head
	for i := s.levels; i >= 0; i-- {
		for ; current.next[i] != nil; current = current.next[i] {
			next := current.next[i]
			if next.score > score || (next.score == score && next.id > id) || next == s.tail {
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
			width := s.getWidth(current.next[i-1], i-1, score)
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
	s.lookup[id] = score
}

// Removes the id from the index
func (s *Skiplist) Remove(id key.Type) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.remove(id)
}

// Appends the id to the end of the index giving it a score of
// the current max score + 1.
func (s *Skiplist) Append(id key.Type) {
	s.lock.RLock()
	highScore := s.tail.prev.score
	s.lock.RUnlock()
	s.Set(id, highScore+1)
}

// Prepends the id to the end of the index giving it a score of
// the current min score - 1 (this could negative)
func (s *Skiplist) Prepend(id key.Type) {
	s.lock.RLock()
	var lowScore int
	if s.head.next[0] != nil {
		lowScore = s.head.next[0].score
	}
	s.lock.RUnlock()
	s.Set(id, lowScore-1)
}

func (s *Skiplist) remove(id key.Type) {
	score, exists := s.lookup[id]
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
			} else if next.score > score {
				next.width[i] -= 1
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
func (s *Skiplist) getWidth(node *SkiplistNode, level int, score int) int {
	width := 0
	for ; node != nil && node != s.tail; node = node.next[level] {
		if node.score > score {
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
func (s *Skiplist) offset(offset int) *SkiplistNode {
	skipped := -1
	current := s.head
	for i := s.levels; i >= 0; i-- {
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
	return s.tail
}

func (s *Skiplist) getRank(score int, first bool) int {
	width := 0
	current := s.head
	for i := s.levels; i >= 0; i-- {
		for ; current != s.tail; current = current.next[i] {
			if current.score == score {
				width += current.width[i] - 1
				if first {
					for current := current.prev; current.prev != s.head && current.score == score; current = current.prev {
						width -= 1
					}
				} else {
					for current := current.next[0]; current.next[0] != s.tail && current.score == score; current = current.next[0] {
						width += 1
					}
				}
				return width
			}
			width += current.width[i]
			next := current.next[i]
			if next == nil || next.score > score {
				if current != s.head {
					width -= 1
				}
				break
			}
			// we're looking for the last of the maching records
			// which means moving forward, which means not counting
			// direct childrens
			if first == false {
				width--
			}
		}
	}

	if current != s.head && current != s.tail {
		width++
	}
	return width
}

// Number of items in the index
func (s *Skiplist) Len() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.lookup)
}

// This index is able to score documents, and thus can be
// used for the post-sorting used by index-first queries
func (s *Skiplist) CanScore() bool {
	return true
}

// Bulk loads ids into the index. This replaces any existing values.
// The score is implied by the array order.
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
func (s *Skiplist) GetScore(id key.Type) (int, bool) {
	s.lock.RLock()
	score, exists := s.lookup[id]
	s.lock.RUnlock()
	return score, exists
}

// Generates a forward iterator (from low score to high)
func (s *Skiplist) Forwards() Iterator {
	s.lock.RLock()
	return &SkipListForwardIterator{
		list: s,
		node: s.head.next[0],
		to:   s.tail.prev.score,
	}
}

// Generates a backward iterator (from high score to low)
func (s *Skiplist) Backwards() Iterator {
	s.lock.RLock()
	return &SkipListBackwardsIterator{
		node: s.tail.prev,
		list: s,
		from: s.head.next[0].score,
	}
}

// Forward skip list iterator
type SkipListForwardIterator struct {
	list   *Skiplist
	node   *SkiplistNode
	offset int
	to     int
}

// Move to the next (higher score) item
func (i *SkipListForwardIterator) Next() key.Type {
	i.node = i.node.next[0]
	if i.node.score > i.to {
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
	offset += i.offset
	if offset > 0 {
		i.node = i.list.offset(offset)
	}
	return i
}

// Specified the range of values to interate over
func (i *SkipListForwardIterator) Range(from, to int) Iterator {
	i.offset = i.list.getRank(from, true)
	i.to = to
	return i
}

// Release the iterator
func (i *SkipListForwardIterator) Close() {
	i.list.lock.RUnlock()
}

// Backward skip list iterator
type SkipListBackwardsIterator struct {
	list   *Skiplist
	node   *SkiplistNode
	offset int
	from   int
}

// Move to the next (lower score) item
func (i *SkipListBackwardsIterator) Next() key.Type {
	i.node = i.node.prev
	if i.node.score < i.from {
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
	offset += i.offset
	if offset > 0 {
		i.node = i.list.offset(i.offset + offset)
	}
	return i
}

// Specified the range of values to interate over
func (i *SkipListBackwardsIterator) Range(from, to int) Iterator {
	i.offset = len(i.list.lookup) - i.list.getRank(to, false)
	i.from = from
	return i
}

// Release the iterator
func (i *SkipListBackwardsIterator) Close() {
	i.list.lock.RUnlock()
}
