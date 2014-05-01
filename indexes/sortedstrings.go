package indexes

import (
	"github.com/karlseguin/nabu/key"
	"sort"
	"sync"
	"sync/atomic"
)

var NullSortedItem = &SortedItem{0, key.NULL, ""}

type SortedStrings struct {
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

func NewSortedStrings(name string) *SortedStrings {
	return &SortedStrings{
		name:   name,
		list:   []*SortedItem{NullSortedItem, NullSortedItem},
		lookup: make(map[key.Type]*SortedItem),
	}
}

func (s *SortedStrings) Name() string {
	return s.name
}

func (s *SortedStrings) Len() int {
	return len(s.lookup)
}

func (s *SortedStrings) BulkLoad(ids []key.Type) {
	list := make([]*SortedItem, len(ids)+2)
	lookup := make(map[key.Type]*SortedItem, len(ids))
	list[0] = NullSortedItem
	list[len(list)-1] = NullSortedItem
	for index, id := range ids {
		item := &SortedItem{id: id, score: "", rank: int64(index)}
		list[index+1] = item
		lookup[id] = item
	}
	s.lock.Lock()
	s.list = list
	s.lookup = lookup
	s.lock.Unlock()
}

func (s *SortedStrings) Set(id key.Type) {
	s.SetString(id, "")
}

func (s *SortedStrings) SetInt(id key.Type, score int) {
	s.SetString(id, "")
}

func (s *SortedStrings) SetString(id key.Type, score string) {
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

func (s *SortedStrings) Remove(id key.Type) {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()
	current, exists := s.lookup[id]
	if exists {
		s.remove(current)
	}
}

func (s *SortedStrings) remove(item *SortedItem) {
	length := int64(len(s.list)) - 1
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

func (s *SortedStrings) Contains(id key.Type) bool {
		_, exists := s.lookup[id]
		return exists
}

func (s *SortedStrings) Score(id key.Type) (int, bool) {
	if item, exists := s.lookup[id]; exists {
		return int(item.rank), exists
	}
	return 0, false
}

func (s *SortedStrings) GetRank(score int, first bool) int {
	return 0
}

func (s *SortedStrings) RLock() {
	s.lock.RLock()
}

func (s *SortedStrings) RUnlock() {
	s.lock.RUnlock()
}

// Returns a forward iterator
func (s *SortedStrings) Forwards() Iterator {
	s.lock.RLock()
	return &SortedStringsForwardIterator{
		position: 1,
		set:      s,
	}
}

// Returns a backward iterator
func (s *SortedStrings) Backwards() Iterator {
	s.lock.RLock()
	return &SortedStringsBackwardIterator{
		position: s.Len(),
		set:      s,
	}
}

// Forward iterator through a static sort index
type SortedStringsForwardIterator struct {
	position int
	set      *SortedStrings
}

// Moves forward and gets the value
func (i *SortedStringsForwardIterator) Next() key.Type {
	i.position++
	return i.Current()
}

// Gets the value
func (i *SortedStringsForwardIterator) Current() key.Type {
	return i.set.list[i.position].id
}

// Sets the iterators offset
func (i *SortedStringsForwardIterator) Offset(offset int) Iterator {
	// consider the 2 padding values
	if offset > len(i.set.list)-2 {
		i.position = 0 //the padded head will break the loop
	} else {
		i.position += offset
	}
	return i
}

// Panics. Ranged queries aren't supported on static sort indexes
func (i *SortedStringsForwardIterator) Range(from, to int) Iterator {
	panic("Cannot have a ranged query on a sortedset")
}

// Releases the iterator
func (i *SortedStringsForwardIterator) Close() {
	i.set.lock.RUnlock()
}

// Backward iterator through a static sort index
type SortedStringsBackwardIterator struct {
	position int
	set      *SortedStrings
}

// Moves backward and gets the value
func (i *SortedStringsBackwardIterator) Next() key.Type {
	i.position--
	return i.Current()
}

// Gets the value
func (i *SortedStringsBackwardIterator) Current() key.Type {
	return i.set.list[i.position].id
}

// Sets the iterators offset
func (i *SortedStringsBackwardIterator) Offset(offset int) Iterator {
	// consider the 2 padding values
	if offset >= len(i.set.list)-2 {
		i.position = 0 //the padded head will break the loop
	} else {
		i.position -= offset
	}
	return i
}

// Panics. Ranged queries aren't supported on static sort indexes
func (i *SortedStringsBackwardIterator) Range(from, to int) Iterator {
	panic("Cannot have a ranged query on a sortedset")
}

// Releases the iterator
func (i *SortedStringsBackwardIterator) Close() {
	i.set.lock.RUnlock()
}
