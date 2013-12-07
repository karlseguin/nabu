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
	s.Load([]key.Type{"a", "b", "c"})
	spec.Expect(s.Len()).ToEqual(3)
}

func TestStaticSortForwardIteration(t *testing.T) {
	s := &StaticSort{}
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Forwards(0), "a", "b", "c")
}

func TestStaticSortBackwardIteration(t *testing.T) {
	s := &StaticSort{}
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Backwards(0), "c", "b", "a")
}

func TestStaticSortForwardIterationWithOffset(t *testing.T) {
	s := &StaticSort{}
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Forwards(1), "b", "c")
}

func TestStaticSortBackwardIterationWithOffset(t *testing.T) {
	s := &StaticSort{}
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Backwards(1), "b", "a")
}

func TestStaticSortForwardIterationWithOffsetAtRange(t *testing.T) {
	s := &StaticSort{}
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Forwards(3), "")
}

func TestStaticSortBackwardIterationWithOffsetAtRange(t *testing.T) {
	s := &StaticSort{}
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Backwards(3), "")
}

func TestStaticSortForwardIterationWithOffsetOutsideOfRange(t *testing.T) {
	s := &StaticSort{}
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Forwards(4), "")
}

func TestStaticSortBackwardIterationWithOffsetOutsideOfRange(t *testing.T) {
	s := &StaticSort{}
	s.Load([]key.Type{"a", "b", "c"})
	assertIterator(t, s.Backwards(4), "")
}

func TestStaticSortCanAppendAValue(t *testing.T) {
	spec := gspec.New(t)
	s := &StaticSort{}
	s.Load([]key.Type{"a", "b", "c"})
	s.Append("d")
	assertIterator(t, s.Forwards(0), "a", "b", "c", "d")
	spec.Expect(s.Len()).ToEqual(4)
}

func TestStaticSortCanPrependAValue(t *testing.T) {
	spec := gspec.New(t)
	s := &StaticSort{}
	s.Load([]key.Type{"a", "b", "c"})
	s.Prepend("z")
	assertIterator(t, s.Forwards(0), "z", "a", "b", "c")
	spec.Expect(s.Len()).ToEqual(4)
}

func assertIterator(t *testing.T, iterator Iterator, ids ...string) {
	defer iterator.Close()
	spec := gspec.New(t)
	i := 0
	for id := iterator.Current(); len(id) != 0; id = iterator.Next() {
		spec.Expect(id).ToEqual(key.Type(ids[i]))
		i++
	}
}
