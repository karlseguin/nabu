package nabu

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"testing"
)

func TestMetaCanBeSet(t *testing.T) {
	spec := gspec.New(t)
	meta := newMeta()
	meta.IntId(33)
	meta.IndexInt("age", 22)
	meta.IndexInt("power", 9001)
	spec.Expect(meta.getId(nil)).ToEqual(key.Type(33))
	spec.Expect(len(meta.iIndexes)).ToEqual(2)
}

func TestMetaCanBeSetWithStringId(t *testing.T) {
	spec := gspec.New(t)
	meta := newMeta()
	meta.StringId("123aa")
	spec.Expect(meta.getId(newIdMap())).ToEqual(key.Type(1))
}
