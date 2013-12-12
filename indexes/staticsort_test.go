package indexes

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"testing"
)

//static indexes are padded, so this isn't as meaningless as it seems
//(but it's still pretty meaningless)
func TestStaticSortLength(t *testing.T) {
	spec := gspec.New(t)
	s := &StaticSort{}
	s.Load([]key.Type{1, 2, 3})
	spec.Expect(s.Len()).ToEqual(3)
}

func TestStaticSortForwardIteration(t *testing.T) {
	s := &StaticSort{}
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Forwards(), 1, 2, 3)
}

func TestStaticSortBackwardIteration(t *testing.T) {
	s := &StaticSort{}
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Backwards(), 3, 2, 1)
}

func TestStaticSortForwardIterationWithOffset(t *testing.T) {
	s := &StaticSort{}
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Forwards().Offset(1), 2, 3)
}

func TestStaticSortBackwardIterationWithOffset(t *testing.T) {
	s := &StaticSort{}
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Backwards().Offset(1), 2, 1)
}

func TestStaticSortForwardIterationWithOffsetAtRange(t *testing.T) {
	s := &StaticSort{}
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Forwards().Offset(3), key.NULL)
}

func TestStaticSortBackwardIterationWithOffsetAtRange(t *testing.T) {
	s := &StaticSort{}
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Backwards().Offset(3), key.NULL)
}

func TestStaticSortForwardIterationWithOffsetOutsideOfRange(t *testing.T) {
	s := &StaticSort{}
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Forwards().Offset(4), key.NULL)
}

func TestStaticSortBackwardIterationWithOffsetOutsideOfRange(t *testing.T) {
	s := &StaticSort{}
	s.Load([]key.Type{1, 2, 3})
	assertIterator(t, s.Backwards().Offset(4), key.NULL)
}

func TestStaticSortCanAppendAValue(t *testing.T) {
	spec := gspec.New(t)
	s := &StaticSort{}
	s.Load([]key.Type{1, 2, 3})
	s.Append(4)
	assertIterator(t, s.Forwards(), 1, 2, 3, 4)
	spec.Expect(s.Len()).ToEqual(4)
}

func TestStaticSortCanPrependAValue(t *testing.T) {
	spec := gspec.New(t)
	s := &StaticSort{}
	s.Load([]key.Type{1, 2, 3})
	s.Prepend(99)
	assertIterator(t, s.Forwards(), 99, 1, 2, 3)
	spec.Expect(s.Len()).ToEqual(4)
}

func assertIterator(t *testing.T, iterator Iterator, ids ...key.Type) {
	defer iterator.Close()
	spec := gspec.New(t)
	i := 0
	for id := iterator.Current(); id != key.NULL; id = iterator.Next() {
		spec.Expect(id).ToEqual(key.Type(ids[i]))
		i++
	}
}
