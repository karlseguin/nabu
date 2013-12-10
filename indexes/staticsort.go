package indexes

import (
	"github.com/karlseguin/nabu/key"
	"sync"
)

// A static sort index not capable of scoring documents. This index
// is ideal when the sort index doesn't update frequently (possibly
// only updated asynchronously on a schedule) and are small
type StaticSort struct {
	length       int
	ids          []key.Type
	paddedLength int
	lock         sync.RWMutex
	modifyLock   sync.Mutex
}

// Get the number of documents indexed
func (s *StaticSort) Len() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.length
}

// Whether this type of index has the index score
func (s *StaticSort) CanScore() bool {
	return false
}

// Bulk load ids into the index. This replaces any existing
// values. The order is implied from the array order
func (s *StaticSort) Load(ids []key.Type) {
	s.modifyLock.Lock()
	defer s.modifyLock.Unlock()

	length := len(ids) + 2
	padded := make([]key.Type, length)
	padded[0] = key.NULL
	copy(padded[1:length-1], ids)
	padded[length-1] = key.NULL

	s.lock.Lock()
	s.paddedLength = length
	s.length = length - 2
	s.ids = padded
	s.lock.Unlock()
}

// Always returns 0
func (s *StaticSort) GetScore(id key.Type) (int, bool) {
	return 0, false
}

// Append an id
func (s *StaticSort) Append(id key.Type) {
	s.modify(id, 0, -1, -1)
}

// Prepend an id
func (s *StaticSort) Prepend(id key.Type) {
	s.modify(id, 1, 0, 1)
}

func (s *StaticSort) modify(id key.Type, offset, newNull, newIndex int) {
	s.modifyLock.Lock()
	defer s.modifyLock.Unlock()

	l := s.paddedLength
	padded := make([]key.Type, l+1)
	copy(padded[offset:], s.ids)

	if newNull == -1 {
		newNull = l
		newIndex = l - 1
	}
	padded[newNull] = key.NULL
	padded[newIndex] = id

	s.lock.Lock()
	s.paddedLength++
	s.length++
	s.ids = padded
	s.lock.Unlock()
}

// Returns a forward iterator
func (s *StaticSort) Forwards() Iterator {
	s.lock.RLock()
	return &StaticSortForwardIterator{
		lock:     &s.lock,
		position: 1,
		ids:      s.ids[0:s.paddedLength],
	}
}

// Returns a backward iterator
func (s *StaticSort) Backwards() Iterator {
	s.lock.RLock()
	return &StaticSortBackwardsIterator{
		lock:     &s.lock,
		ids:      s.ids[0:s.paddedLength],
		position: s.length,
	}
}

// Forward iterator through a static sort index
type StaticSortForwardIterator struct {
	position int
	ids      []key.Type
	lock     *sync.RWMutex
}

// Moves forward and gets the value
func (i *StaticSortForwardIterator) Next() key.Type {
	i.position++
	return i.Current()
}

// Gets the value
func (i *StaticSortForwardIterator) Current() key.Type {
	return i.ids[i.position]
}

// Sets the iterators offset
func (i *StaticSortForwardIterator) Offset(offset int) Iterator {
	// consider the 2 padding values
	if offset > len(i.ids)-2 {
		i.position = 0 //the padded head will break the loop
	} else {
		i.position += offset
	}
	return i
}

// Panics. Ranged queries aren't supported on static sort indexes
func (i *StaticSortForwardIterator) Range(from, to int) Iterator {
	panic("Cannot have a ranged query on a static index")
}

// Releases the iterator
func (i *StaticSortForwardIterator) Close() {
	i.lock.RUnlock()
}

// Backward iterator through a static sort index
type StaticSortBackwardsIterator struct {
	position int
	ids      []key.Type
	lock     *sync.RWMutex
}

// Moves backward and gets the value
func (i *StaticSortBackwardsIterator) Next() key.Type {
	i.position--
	return i.Current()
}

// Gets the value
func (i *StaticSortBackwardsIterator) Current() key.Type {
	return i.ids[i.position]
}

// Sets the iterators offset
func (i *StaticSortBackwardsIterator) Offset(offset int) Iterator {
	// consider the 2 padding values
	if offset >= len(i.ids)-2 {
		i.position = 0 //the padded head will break the loop
	} else {
		i.position -= offset
	}
	return i
}

// Panics. Ranged queries aren't supported on static sort indexes
func (i *StaticSortBackwardsIterator) Range(from, to int) Iterator {
	panic("Cannot have a ranged query on a static index")
}

// Releases the iterator
func (i *StaticSortBackwardsIterator) Close() {
	i.lock.RUnlock()
}
