package nabu

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"testing"
)

func TestMetaCanBeSet(t *testing.T) {
	spec := gspec.New(t)
	meta := newMeta()
	meta.Id(33)
	meta.Index("children", "ghanima")
	meta.Index("children", "leto")
	spec.Expect(meta.getId(nil)).ToEqual(key.Type(33))
	spec.Expect(len(meta.indexes)).ToEqual(1)
	values, _ := meta.indexes["children"]
	spec.Expect(values[0]).ToEqual("ghanima")
	spec.Expect(values[1]).ToEqual("leto")
}


func TestMetaCanBeSetWithStringId(t *testing.T) {
	spec := gspec.New(t)
	meta := newMeta()
	meta.StringId("123aa")
	spec.Expect(meta.getId(newIdMap())).ToEqual(key.Type(1))
}
