package indexes

import (
	"github.com/karlseguin/nabu/key"
	"sync"
)

// A static sort index capable of scoring documents. This index
// is ideal when the sort index doesn't update frequently (possibly
// only updated asynchronously on a schedule) but is rather large
// (say, > 5000 items)
type StaticScoreSort struct {
	length       int
	ids          []key.Type
	paddedLength int
	lock         sync.RWMutex
	modifyLock   sync.Mutex
	lookup       map[key.Type]int
}

// Get the number of documents indexed
func (s *StaticScoreSort) Len() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.length
}

// Whether this type of index can get an index's score
func (s *StaticScoreSort) CanScore() bool {
	return true
}

// Bulk load ids into the index. This replaces any existing
// values. The order is implied from the array order
func (s *StaticScoreSort) Load(ids []key.Type) {
	s.modifyLock.Lock()
	defer s.modifyLock.Unlock()
	length := len(ids) + 2
	padded := make([]key.Type, length)
	lookup := make(map[key.Type]int, length)
	padded[0] = key.NULL
	for index, id := range ids {
		padded[index+1] = id
		lookup[id] = index
	}
	padded[length-1] = key.NULL

	s.lock.Lock()
	s.paddedLength = length
	s.length = length - 2
	s.ids = padded
	s.lookup = lookup
	s.lock.Unlock()
}

// Get the score for an individual id
func (s *StaticScoreSort) GetScore(id key.Type) (int, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	score, exists := s.lookup[id]
	return score, exists
}

// Append an id
func (s *StaticScoreSort) Append(id key.Type) {
	s.modify(id, 0, -1, -1)
}

// Prepend an id
func (s *StaticScoreSort) Prepend(id key.Type) {
	s.modify(id, 1, 0, 1)
}

func (s *StaticScoreSort) modify(id key.Type, offset, newNull, newIndex int) {
	s.modifyLock.Lock()
	defer s.modifyLock.Unlock()

	l := s.paddedLength
	padded := make([]key.Type, l+1)
	copy(padded[offset:], s.ids)
	var newScore int
	if newNull == -1 {
		newScore = s.lookup[s.ids[l-2]] + 1
	} else {
		newScore = s.lookup[s.ids[1]] - 1
	}

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
	s.lookup[id] = newScore
	s.lock.Unlock()
}

// Returns a forward iterator
func (s *StaticScoreSort) Forwards() Iterator {
	s.lock.RLock()
	return &StaticSortForwardIterator{
		lock:     &s.lock,
		position: 1,
		ids:      s.ids[0:s.paddedLength],
	}
}

// Returns a backward iterator
func (s *StaticScoreSort) Backwards() Iterator {
	s.lock.RLock()
	return &StaticSortBackwardsIterator{
		lock:     &s.lock,
		ids:      s.ids[0:s.paddedLength],
		position: s.length,
	}
}
