package indexes

import (
	"github.com/karlseguin/nabu/key"
	"sort"
	"sync"
	"sync/atomic"
)

var NullSortedItem = &SortedItem{0, key.NULL, ""}

type SortedSet struct {
	name      string
	list      []*SortedItem
	lock      sync.RWMutex
	writeLock sync.Mutex
	lookup    map[key.Type]*SortedItem
}

type SortedItem struct {
	rank  int64
	id    key.Type
	score string
}

func newSortedSet(name string) *SortedSet {
	return &SortedSet{
		name:   name,
		list:   []*SortedItem{NullSortedItem, NullSortedItem},
		lookup: make(map[key.Type]*SortedItem),
	}
}

func (s *SortedSet) Name() string {
	return s.name
}

func (s *SortedSet) Len() int {
	return len(s.lookup)
}

func (s *SortedSet) SetInt(id key.Type, score int) {
	panic("Cannot call SetInt on sortedset")
}

func (s *SortedSet) SetString(id key.Type, score string) {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()
	current, exists := s.lookup[id]

	if exists {
		if current.score == score {
			return
		}
		s.remove(current)
	}

	target := int64(sort.Search(len(s.list)-2, func(i int) bool { return s.list[i+1].score >= score }) + 1)
	list := make([]*SortedItem, len(s.list)+1)
	copy(list, s.list[:target])
	for i, l := int(target), len(s.list); i < l; i++ {
		item := s.list[i]
		atomic.AddInt64(&item.rank, 1)
		list[i+1] = item
	}

	item := &SortedItem{id: id, score: score, rank: target}
	list[target] = item

	s.lock.Lock()
	s.list = list
	s.lookup[id] = item
	s.lock.Unlock()
}

func (s *SortedSet) Remove(id key.Type) {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()
	current, exists := s.lookup[id]
	if exists {
		s.remove(current)
	}
}

func (s *SortedSet) remove(item *SortedItem) {
	length := int64(len(s.list))-1
	list := make([]*SortedItem, length)
	copy(list, s.list[:item.rank])
	for i := item.rank; i < length; i++ {
		item := s.list[i+1]
		item.rank--
		list[i] = item
	}

	s.lock.Lock()
	s.list = list
	delete(s.lookup, item.id)
	s.lock.Unlock()
}

func (s *SortedSet) Contains(id key.Type) (int, bool) {
	if item, exists := s.lookup[id]; exists {
		return int(item.rank), exists
	}
	return 0, false
}

func (s *SortedSet) GetRank(score int, first bool) int {
	return 0
}

func (s *SortedSet) RLock() {
	s.lock.RLock()
}

func (s *SortedSet) RUnlock() {
	s.lock.RUnlock()
}

// Returns a forward iterator
func (s *SortedSet) Forwards() Iterator {
	s.lock.RLock()
	return &SortedSetForwardIterator{
		position: 1,
		set:      s,
	}
}

// Returns a backward iterator
func (s *SortedSet) Backwards() Iterator {
	s.lock.RLock()
	return &SortedSetBackwardIterator{
		position: s.Len(),
		set:      s,
	}
}

// Forward iterator through a static sort index
type SortedSetForwardIterator struct {
	position int
	set      *SortedSet
}

// Moves forward and gets the value
func (i *SortedSetForwardIterator) Next() key.Type {
	i.position++
	return i.Current()
}

// Gets the value
func (i *SortedSetForwardIterator) Current() key.Type {
	return i.set.list[i.position].id
}

// Sets the iterators offset
func (i *SortedSetForwardIterator) Offset(offset int) Iterator {
	// consider the 2 padding values
	if offset > len(i.set.list)-2 {
		i.position = 0 //the padded head will break the loop
	} else {
		i.position += offset
	}
	return i
}

// Panics. Ranged queries aren't supported on static sort indexes
func (i *SortedSetForwardIterator) Range(from, to int) Iterator {
	panic("Cannot have a ranged query on a sortedset")
}

// Releases the iterator
func (i *SortedSetForwardIterator) Close() {
	i.set.lock.RUnlock()
}

// Backward iterator through a static sort index
type SortedSetBackwardIterator struct {
	position int
	set      *SortedSet
}

// Moves backward and gets the value
func (i *SortedSetBackwardIterator) Next() key.Type {
	i.position--
	return i.Current()
}

// Gets the value
func (i *SortedSetBackwardIterator) Current() key.Type {
	return i.set.list[i.position].id
}

// Sets the iterators offset
func (i *SortedSetBackwardIterator) Offset(offset int) Iterator {
	// consider the 2 padding values
	if offset >= len(i.set.list)-2 {
		i.position = 0 //the padded head will break the loop
	} else {
		i.position -= offset
	}
	return i
}

// Panics. Ranged queries aren't supported on static sort indexes
func (i *SortedSetBackwardIterator) Range(from, to int) Iterator {
	panic("Cannot have a ranged query on a sortedset")
}

// Releases the iterator
func (i *SortedSetBackwardIterator) Close() {
	i.set.lock.RUnlock()
}
