package conditions

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"testing"
)

func TestSetReturnsTheLength(t *testing.T) {
	spec := gspec.New(t)
	set := NewSet("x", "10")
	set.On(makeSetIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(set.Len()).ToEqual(8)
}

func TestSetReturnsTheLengthWhenNone(t *testing.T) {
	spec := gspec.New(t)
	set := NewSet("x", "x")
	set.On(makeSetIndex())
	spec.Expect(set.Len()).ToEqual(0)
}

func TestSetDoesNotContainANonExistantId(t *testing.T) {
	spec := gspec.New(t)
	set := NewSet("x", "10")
	set.On(makeSetIndex(1, 7, 8, 10, 11, 12, 13, 20))
	_, exists := set.Contains(key.Type(22))
	spec.Expect(exists).ToEqual(false)
}

func TestSetContainsAnExistingId(t *testing.T) {
	spec := gspec.New(t)
	set := NewSet("x", "10")
	set.On(makeSetIndex(1, 7, 8, 10, 11, 12, 13, 20))
	_, exists := set.Contains(key.Type(13))
	spec.Expect(exists).ToEqual(true)
}

func makeSetIndex(ids ...int) indexes.Index {
	set := indexes.NewIndex("test", true, false)
	for _, id := range ids {
		set.SetInt(key.Type(id), 0)
	}
	return set
}
