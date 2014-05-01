package indexes

import (
	"github.com/karlseguin/nabu/key"
	"sync"
)

// A set meant for low-cardinality values
type SetString struct {
	name       string
	ids        []key.Type
	lookup     map[key.Type]struct{}
	lock       sync.RWMutex
	modifyLock sync.Mutex
}

func NewSetString(name string) *SetString {
	return &SetString{
		ids: []key.Type{key.NULL, key.NULL},
		name:   name,
		lookup: make(map[key.Type]struct{}),
	}
}

// Assumes the set is already read-locked
func (s *SetString) Name() string {
	return s.name
}

func (s *SetString) Set(id key.Type) {
	s.lock.RLock()
	_, exists := s.lookup[id]
	s.lock.RUnlock()
	if exists {
		return
	}

	s.lock.Lock()
	s.lookup[id] = struct{}{}
	s.lock.Unlock()
	s.addId(id)
}

func (s *SetString) Remove(id key.Type) {
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
func (s *SetString) Len() int {
	return len(s.lookup)
}

func (s *SetString) Contains(id key.Type) bool {
	_, exists := s.lookup[id]
	return exists
}

func (s *SetString) RLock() {
	s.lock.RLock()
}

func (s *SetString) RUnlock() {
	s.lock.RUnlock()
}

func (s *SetString) addId(id key.Type) {
	s.modifyLock.Lock()
	defer s.modifyLock.Unlock()

	s.lock.RLock()
	l := len(s.ids)
	ids := make([]key.Type, l + 1)
	copy(ids, s.ids)
	s.lock.RUnlock()

	ids[l-1] = id
	ids[l] = key.NULL

	s.lock.Lock()
	s.ids = ids
	s.lock.Unlock()
}

func (s *SetString) removeId(target key.Type) {
	s.modifyLock.Lock()
	defer s.modifyLock.Unlock()

	s.lock.RLock()
	l := len(s.ids)
	ids := make([]key.Type, l-1)
	index := 0
	for i := 0; i < l; i++ {
		id := s.ids[i]
		if id != target {
			ids[index] = id
			index++
		}
	}
	s.lock.RUnlock()

	s.lock.Lock()
	s.ids = ids
	s.lock.Unlock()
}

// Returns a forward iterator
func (s *SetString) Forwards() Iterator {
	s.lock.RLock()
	return &SetStringForwardIterator{
		position: 1,
		set:      s,
	}
}

// Returns a backward iterator
func (s *SetString) Backwards() Iterator {
	s.lock.RLock()
	return &SetStringBackwardIterator{
		position: s.Len(),
		set:      s,
	}
}

// Forward iterator through a static sort index
type SetStringForwardIterator struct {
	position int
	set      *SetString
}

// Moves forward and gets the value
func (i *SetStringForwardIterator) Next() key.Type {
	i.position++
	return i.Current()
}

// Gets the value
func (i *SetStringForwardIterator) Current() key.Type {
	return i.set.ids[i.position]
}

// SetStrings the iterators offset
func (i *SetStringForwardIterator) Offset(offset int) Iterator {
	// consider the 2 padding values
	if offset > len(i.set.ids)-2 {
		i.position = 0 //the padded head will break the loop
	} else {
		i.position += offset
	}
	return i
}

// Panics. Ranged queries aren't supported on static sort indexes
func (i *SetStringForwardIterator) Range(from, to int) Iterator {
	panic("Cannot have a ranged query on a set")
}

// Releases the iterator
func (i *SetStringForwardIterator) Close() {
	i.set.lock.RUnlock()
}

// Backward iterator through a static sort index
type SetStringBackwardIterator struct {
	position int
	set      *SetString
}

// Moves backward and gets the value
func (i *SetStringBackwardIterator) Next() key.Type {
	i.position--
	return i.Current()
}

// Gets the value
func (i *SetStringBackwardIterator) Current() key.Type {
	return i.set.ids[i.position]
}

// SetStrings the iterators offset
func (i *SetStringBackwardIterator) Offset(offset int) Iterator {
	// consider the 2 padding values
	if offset >= len(i.set.ids)-2 {
		i.position = 0 //the padded head will break the loop
	} else {
		i.position -= offset
	}
	return i
}

// Panics. Ranged queries aren't supported on static sort indexes
func (i *SetStringBackwardIterator) Range(from, to int) Iterator {
	panic("Cannot have a ranged query on a set")
}

// Releases the iterator
func (i *SetStringBackwardIterator) Close() {
	i.set.lock.RUnlock()
}
