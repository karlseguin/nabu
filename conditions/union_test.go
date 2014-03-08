package conditions

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"testing"
)

func TestUnionDoesNotContainANonExistantId(t *testing.T) {
	spec := gspec.New(t)
	union := NewUnion([]string{"apple", "orange"})
	union.On(makeSetIndex(20, 23, 24, 25, 26))
	union.On(makeSetIndex(20, 25, 28, 29))
	_, exists := union.Contains(key.Type(22))
	spec.Expect(exists).ToEqual(false)
}

func TestSetContainsAnExistingIdIfJustOneIndexContainsIt(t *testing.T) {
	spec := gspec.New(t)
	union := NewUnion([]string{"apple", "orange"})
	union.On(makeSetIndex(20, 23, 24, 25, 26))
	union.On(makeSetIndex(20, 25, 28, 29))
	_, exists := union.Contains(key.Type(24))
	spec.Expect(exists).ToEqual(true)
}

func TestSetContainsAnExistingIdIfMultipleIndexesContainsIt(t *testing.T) {
	spec := gspec.New(t)
	union := NewUnion([]string{"apple", "orange"})
	union.On(makeSetIndex(20, 23, 24, 25, 26))
	union.On(makeSetIndex(20, 25, 28, 29))
	_, exists := union.Contains(key.Type(20))
	spec.Expect(exists).ToEqual(true)
}
