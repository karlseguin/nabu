package indexes

import (
	"github.com/karlseguin/nabu/key"
	"sync"
)

// A set meant for low-cardinality values
type Set struct {
	counter    int
	name       string
	ids        []key.Type
	lookup     map[key.Type]int
	lock       sync.RWMutex
	modifyLock sync.Mutex
}

func newSet(name string) *Set {
	return &Set{
		name:   name,
		lookup: make(map[key.Type]int),
	}
}

// Assumes the set is already read-locked
func (s *Set) Name() string {
	return s.name
}

func (s *Set) SetInt(id key.Type, score int) {
	s.lock.RLock()
	_, exists := s.lookup[id]
	s.lock.RUnlock()
	if exists {
		return
	}

	s.lock.Lock()
	s.counter++
	s.lookup[id] = s.counter
	s.lock.Unlock()
	s.addId(id)
}

func (s *Set) Remove(id key.Type) {
	s.lock.RLock()
	_, exists := s.lookup[id]
	s.lock.RUnlock()
	if exists == false {
		return
	}

	s.lock.Lock()
	delete(s.lookup, id)
	s.lock.Unlock()
	s.removeId(id)
}

// Get the number of documents indexed
// Assumes the set is already read-locked
func (s *Set) Len() int {
	return len(s.lookup)
}

// Assumes the set is already read-locked
func (s *Set) Contains(id key.Type) (int, bool) {
	position, exists := s.lookup[id]
	return position, exists
}

// Get the score for an individual id
func (s *Set) GetRank(id int, first bool) int {
	return 0
}

func (s *Set) RLock() {
	s.lock.RLock()
}

func (s *Set) RUnlock() {
	s.lock.RUnlock()
}

func (s *Set) addId(id key.Type) {
	s.modifyLock.Lock()
	defer s.modifyLock.Unlock()

	s.lock.RLock()
	l := len(s.ids)
	if l == 0 {
		l = 2
	}
	ids := make([]key.Type, l+1)
	for i := 1; i < l-1; i++ {
		ids[i] = s.ids[i]
	}
	s.lock.RUnlock()

	ids[0] = key.NULL
	ids[l-1] = id
	ids[l] = key.NULL

	s.lock.Lock()
	s.ids = ids
	s.lock.Unlock()
}

func (s *Set) removeId(target key.Type) {
	s.modifyLock.Lock()
	defer s.modifyLock.Unlock()

	s.lock.RLock()
	l := len(s.ids) - 1
	ids := make([]key.Type, l)

	index := 1
	for i := 1; i < l; i++ {
		id := s.ids[i]
		if id != target {
			ids[index] = id
			index++
		}
	}
	s.lock.RUnlock()

	ids[0] = key.NULL
	ids[l-1] = key.NULL

	s.lock.Lock()
	s.ids = ids
	s.lock.Unlock()
}

// Returns a forward iterator
func (s *Set) Forwards() Iterator {
	s.lock.RLock()
	return &SetForwardIterator{
		position: 1,
		set:      s,
	}
}

// Returns a backward iterator
func (s *Set) Backwards() Iterator {
	s.lock.RLock()
	return &SetBackwardIterator{
		position: s.Len(),
		set:      s,
	}
}

// Forward iterator through a static sort index
type SetForwardIterator struct {
	position int
	set      *Set
}

// Moves forward and gets the value
func (i *SetForwardIterator) Next() key.Type {
	i.position++
	return i.Current()
}

// Gets the value
func (i *SetForwardIterator) Current() key.Type {
	return i.set.ids[i.position]
}

// Sets the iterators offset
func (i *SetForwardIterator) Offset(offset int) Iterator {
	// consider the 2 padding values
	if offset > len(i.set.ids)-2 {
		i.position = 0 //the padded head will break the loop
	} else {
		i.position += offset
	}
	return i
}

// Panics. Ranged queries aren't supported on static sort indexes
func (i *SetForwardIterator) Range(from, to int) Iterator {
	panic("Cannot have a ranged query on a set")
}

// Releases the iterator
func (i *SetForwardIterator) Close() {
	i.set.lock.RUnlock()
}

// Backward iterator through a static sort index
type SetBackwardIterator struct {
	position int
	set      *Set
}

// Moves backward and gets the value
func (i *SetBackwardIterator) Next() key.Type {
	i.position--
	return i.Current()
}

// Gets the value
func (i *SetBackwardIterator) Current() key.Type {
	return i.set.ids[i.position]
}

// Sets the iterators offset
func (i *SetBackwardIterator) Offset(offset int) Iterator {
	// consider the 2 padding values
	if offset >= len(i.set.ids)-2 {
		i.position = 0 //the padded head will break the loop
	} else {
		i.position -= offset
	}
	return i
}

// Panics. Ranged queries aren't supported on static sort indexes
func (i *SetBackwardIterator) Range(from, to int) Iterator {
	panic("Cannot have a ranged query on a set")
}

// Releases the iterator
func (i *SetBackwardIterator) Close() {
	i.set.lock.RUnlock()
}
