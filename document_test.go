package nabu

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"testing"
)

func TestMetaCanBeSet(t *testing.T) {
	spec := gspec.New(t)
	meta := newMeta(nil, true)
	spec.Expect(meta.IsUpdate).ToEqual(true)
	meta.IntId(33)

	meta.SortedInt("age", 22)
	meta.SortedInt("power", 9001)
	spec.Expect(meta.getId()).ToEqual(key.Type(33))
	spec.Expect(len(meta.sortedInts)).ToEqual(2)
}

func TestMetaCanBeSetWithStringId(t *testing.T) {
	spec := gspec.New(t)
	meta := newMeta(SmallDB(), false)
	spec.Expect(meta.IsUpdate).ToEqual(false)
	spec.Expect(meta.StringId("123aa")).ToEqual(uint(1))
	id, stringId := meta.getId()
	spec.Expect(id).ToEqual(key.Type(1))
	spec.Expect(stringId).ToEqual("123aa")
}
