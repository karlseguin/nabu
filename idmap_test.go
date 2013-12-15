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

func TestIdMapRemovesAnId(t *testing.T) {
	spec := gspec.New(t)
	m := newIdMap()
	m.get("over", true)
	m.remove("over")
	spec.Expect(m.get("over", false)).ToEqual(key.NULL)
}

func TestLoadsTheMap(t *testing.T) {
	spec := gspec.New(t)
	m := newIdMap()
	data := map[uint]string{1: "a1", 2: "b2", 10: "c3"}
	m.load(data)
	spec.Expect(m.get("c3", false)).ToEqual(key.Type(10))
	spec.Expect(m.get("c4", true)).ToEqual(key.Type(11))
}
