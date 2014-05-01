package indexes

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"testing"
)

func TestEmptyContains(t *testing.T) {
	spec := gspec.New(t)
	empty := NewEmpty("x")
	empty.Set(key.Type(4))
	spec.Expect(empty.Contains(key.Type(4))).ToEqual(false)
}

func TestEmptyLength(t *testing.T) {
	spec := gspec.New(t)
	empty := NewEmpty("x")
	empty.Set(key.Type(4))
	empty.Set(key.Type(6))
	spec.Expect(empty.Len()).ToEqual(0)
}

func TestEmptyForwards(t *testing.T) {
	spec := gspec.New(t)
	empty := NewEmpty("x")
	empty.Set(key.Type(4))
	iter := empty.Forwards()
	spec.Expect(iter.Current()).ToEqual(key.NULL)
}

func TestEmptyBackwards(t *testing.T) {
	spec := gspec.New(t)
	empty := NewEmpty("x")
	empty.Set(key.Type(4))
	iter := empty.Backwards()
	spec.Expect(iter.Current()).ToEqual(key.NULL)
}
