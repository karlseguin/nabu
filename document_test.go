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
	spec.Expect(meta.id).ToEqual(key.Type(33))
	spec.Expect(len(meta.indexes)).ToEqual(1)
	values, _ := meta.indexes["children"]
	spec.Expect(values[0]).ToEqual("ghanima")
	spec.Expect(values[1]).ToEqual("leto")
}
