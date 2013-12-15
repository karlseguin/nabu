package indexes

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"testing"
)

func TestSetLength(t *testing.T) {
	spec := gspec.New(t)
	s := newSet("test")
	setLoad(s, 1, 2, 3)
	spec.Expect(s.Len()).ToEqual(3)
}

func TestSetForwardIteration(t *testing.T) {
	s := newSet("test")
	setLoad(s, 1, 2, 3)
	assertIterator(t, s.Forwards(), 1, 2, 3)
}

func TestSetBackwardIteration(t *testing.T) {
	s := newSet("test")
	setLoad(s, 1, 2, 3)
	assertIterator(t, s.Backwards(), 3, 2, 1)
}

func TestSetForwardIterationWithOffset(t *testing.T) {
	s := newSet("test")
	setLoad(s, 1, 2, 3)
	assertIterator(t, s.Forwards().Offset(1), 2, 3)
}

func TestSetBackwardIterationWithOffset(t *testing.T) {
	s := newSet("test")
	setLoad(s, 1, 2, 3)
	assertIterator(t, s.Backwards().Offset(1), 2, 1)
}

func TestSetForwardIterationWithOffsetAtRange(t *testing.T) {
	s := newSet("test")
	setLoad(s, 1, 2, 3)
	assertIterator(t, s.Forwards().Offset(3), key.NULL)
}

func TestSetBackwardIterationWithOffsetAtRange(t *testing.T) {
	s := newSet("test")
	setLoad(s, 1, 2, 3)
	assertIterator(t, s.Backwards().Offset(3), key.NULL)
}

func TestSetForwardIterationWithOffsetOutsideOfRange(t *testing.T) {
	s := newSet("test")
	setLoad(s, 1, 2, 3)
	assertIterator(t, s.Forwards().Offset(4), key.NULL)
}

func TestSetBackwardIterationWithOffsetOutsideOfRange(t *testing.T) {
	s := newSet("test")
	setLoad(s, 1, 2, 3)
	assertIterator(t, s.Backwards().Offset(4), key.NULL)
}

func TestSetCanDeleteItem(t *testing.T) {
	s := newSet("test")
	setLoad(s, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	s.Remove(3)
	s.Remove(6)
	s.SetInt(key.Type(3), 0)
	assertIterator(t, s.Backwards(), 3, 9, 8, 7, 5, 4, 2, 1)
}

func setLoad(set *Set, ids ...int) {
	for _, id := range ids {
		set.SetInt(key.Type(id), 0)
	}
}
