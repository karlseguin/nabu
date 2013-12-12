package nabu

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"testing"
)

func TestIdMapReturnsANewId(t *testing.T) {
	spec := gspec.New(t)
	m := newIdMap()
	spec.Expect(m.get("over", true)).ToEqual(key.Type(1))
	spec.Expect(m.get("9000", true)).ToEqual(key.Type(2))
}

func TestIdMapReturnsAnExistingId(t *testing.T) {
	spec := gspec.New(t)
	m := newIdMap()
	m.get("over", true)
	m.get("9000", true)
	spec.Expect(m.get("over", false)).ToEqual(key.Type(1))
}

func TestIdMapDoesNotCreateANewId(t *testing.T) {
	spec := gspec.New(t)
	m := newIdMap()
	spec.Expect(m.get("over", false)).ToEqual(key.NULL)
}
