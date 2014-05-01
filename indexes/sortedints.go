package indexes

import (
	"fmt"
	"github.com/karlseguin/nabu/key"
	"math"
	"math/rand"
	"strings"
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

// A SortedInts sorted index. Ideal for sorted indexes which are frequently
// modified
type SortedInts struct {
	name   string
	levels int
	lock   sync.RWMutex
	head   *SortedIntsNode
	tail   *SortedIntsNode
	lookup map[key.Type]int
}

type SortedIntsNode struct {
	score int
	id    key.Type
	next  []*SortedIntsNode
	width []int
	prev  *SortedIntsNode
}

func NewSortedInts(name string) *SortedInts {
	head := &SortedIntsNode{
		id:    key.NULL,
		width: make([]int, maxLevel),
		next:  make([]*SortedIntsNode, maxLevel),
	}

	tail := &SortedIntsNode{
		id:    key.NULL,
		prev:  head,
		width: make([]int, maxLevel),
	}

	for i := 0; i < maxLevel; i++ {
		head.next[i] = tail
	}

	return &SortedInts{
		levels: 0,
		name:   name,
		head:   head,
		tail:   tail,
		lookup: make(map[key.Type]int),
	}
}

func (s *SortedInts) draw() {
	println("\n")

	for level := s.levels; level >= 0; level-- {
		if s.head.next[level] == nil {
			continue
		}
		print(level, ": ")
		for node := s.head.next[level]; node != s.tail; node = node.next[level] {
			width := node.width[level]
			print("--", strings.Repeat("------", width-1), node.score, "(", width, ")")
		}
		println("")
	}
	fmt.Println("")
}

func (s *SortedInts) Load(values []key.Type) {
	for index, value := range values {
		s.setInt(value, index)
	}
}

func (s *SortedInts) Set(id key.Type) {
	s.SetInt(id, 0)
}

// Stores a id within the index with the specified score
func (s *SortedInts) SetInt(id key.Type, score int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if r, exists := s.lookup[id]; exists {
		if r == score {
			return
		}
		s.remove(id)
	}
	s.setInt(id, score)
}

func (s *SortedInts) SetString(id key.Type, score string) {
	s.SetInt(id, 0)
}

// Removes the id from the index
func (s *SortedInts) Remove(id key.Type) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.remove(id)
}

// Number of items in the index
// Assumes the index is already read-locked
func (s *SortedInts) Len() int {
	return len(s.lookup)
}

// Number of items in the index
// Assumes the index is already read-locked
func (s *SortedInts) Name() string {
	return s.name
}

// Tests membership of a document
// Assumes the index is already read-locked
func (s *SortedInts) Contains(id key.Type) bool {
	_, exists := s.lookup[id]
	return exists
}

// Ranks a document
// Assumes the index is already read-locked
func (s *SortedInts) Score(id key.Type) (int, bool) {
	score, exists := s.lookup[id]
	return score, exists
}

// Read locks the index
func (s *SortedInts) RLock() {
	s.lock.RLock()
}

// Releases the lock
func (s *SortedInts) RUnlock() {
	s.lock.RUnlock()
}

func (s *SortedInts) setInt(id key.Type, score int) {
	level := s.getLevel()
	node := &SortedIntsNode{
		id:    id,
		score: score,
		width: make([]int, level+1),
		next:  make([]*SortedIntsNode, level+1),
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

func (s *SortedInts) remove(id key.Type) {
	score, exists := s.lookup[id]
	if exists == false {
		return
	}

	current := s.head
	for i := s.levels; i >= 0; i-- {
		for ; current.next[i] != s.tail; current = current.next[i] {
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
	if current.next[0] == s.tail {
		s.tail.prev = current
	}
	delete(s.lookup, id)
}

// Get a node's width
func (s *SortedInts) getWidth(node *SortedIntsNode, level int, score int) int {
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
func (s *SortedInts) getLevel() int {
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
func (s *SortedInts) offset(offset int) *SortedIntsNode {
	skipped := -1
	prev := s.head
	for i := s.levels; i >= 0; i-- {
		current := prev.next[i]
		for {
			t := current.width[i] + skipped
			if current == s.tail || t > offset {
				break
			}
			if t == offset {
				return current
			}
			prev = current
			current = current.next[i]
			skipped = t
		}
	}
	return s.tail
}

func (s *SortedInts) GetRank(score int, first bool) int {
	width := -1
	prev := s.head
	for i := s.levels; i >= 0; i-- {
		current := prev.next[i]
		for {
			if current == s.tail || current.score > score {
				break
			}
			width += current.width[i]
			if current.score == score {
				if first {
					for current := current.prev; current.score == score && current != s.head; current = current.prev {
						width--
					}
				} else {
					for current := current.next[0]; current.score == score && current != s.tail; current = current.next[0] {
						width++
					}
				}
				return width
			}
			prev = current
			current = current.next[i]
		}
	}

	if width == -1 {
		return 0
	}
	if first {
		return width + 1 //next closest
	}
	return width
}

// Generates a forward iterator (from low score to high)
func (s *SortedInts) Forwards() Iterator {
	s.lock.RLock()
	return &SortedIntsForwardIterator{
		list: s,
		node: s.head.next[0],
		to:   s.tail.prev.score,
	}
}

// Generates a backward iterator (from high score to low)
func (s *SortedInts) Backwards() Iterator {
	s.lock.RLock()
	return &SortedIntsBackwardsIterator{
		list: s,
		node: s.tail.prev,
		from: s.head.next[0].score,
	}
}

// Forward skip list iterator
type SortedIntsForwardIterator struct {
	list   *SortedInts
	node   *SortedIntsNode
	offset int
	to     int
}

// Move to the next (higher score) item
func (i *SortedIntsForwardIterator) Next() key.Type {
	i.node = i.node.next[0]
	if i.node.score > i.to {
		i.node = i.list.tail
	}
	return i.Current()
}

// Key for the current item
func (i *SortedIntsForwardIterator) Current() key.Type {
	return i.node.id
}

// Sets the iterator's offset
func (i *SortedIntsForwardIterator) Offset(offset int) Iterator {
	offset += i.offset
	if offset > 0 {
		i.node = i.list.offset(offset)
	}
	return i
}

// Specified the range of values to interate over
func (i *SortedIntsForwardIterator) Range(from, to int) Iterator {
	i.offset = i.list.GetRank(from, true)
	i.to = to
	return i
}

// Release the iterator
func (i *SortedIntsForwardIterator) Close() {
	i.list.lock.RUnlock()
}

// Backward skip list iterator
type SortedIntsBackwardsIterator struct {
	list   *SortedInts
	node   *SortedIntsNode
	offset int
	from   int
}

// Move to the next (lower score) item
func (i *SortedIntsBackwardsIterator) Next() key.Type {
	i.node = i.node.prev
	if i.node.score < i.from {
		i.node = i.list.head
	}
	return i.Current()
}

// Key for the current item
func (i *SortedIntsBackwardsIterator) Current() key.Type {
	return i.node.id
}

// Release the iterator
func (i *SortedIntsBackwardsIterator) Offset(offset int) Iterator {
	if i.offset > 0 {
		offset = i.offset - offset
		i.node = i.list.offset(offset)
	} else if offset > 0 {
		i.node = i.list.offset(i.list.Len() - offset - 1)
	}
	return i
}

// Specified the range of values to interate over
func (i *SortedIntsBackwardsIterator) Range(from, to int) Iterator {
	i.offset = i.list.GetRank(to, false)
	i.from = from
	return i
}

// Release the iterator
func (i *SortedIntsBackwardsIterator) Close() {
	i.list.lock.RUnlock()
}
