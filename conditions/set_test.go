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
	spec.Expect(set.Contains(key.Type(22))).ToEqual(false)
}

func TestSetContainsAnExistingId(t *testing.T) {
	spec := gspec.New(t)
	set := NewSet("x", "10")
	set.On(makeSetIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(set.Contains(key.Type(13))).ToEqual(true)
}

func makeSetIndex(ids ...int) indexes.Index {
	set := indexes.NewSetString("test")
	for _, id := range ids {
		set.Set(key.Type(id))
	}
	return set
}
