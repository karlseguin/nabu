package nabu

import (
	"github.com/karlseguin/gspec"
	"testing"
)

func TestMetaCanBeSet(t *testing.T) {
	spec := gspec.New(t)
	meta := newMeta()
	meta.Id("paul")
	meta.Index("children", "ghanima")
	meta.Index("children", "leto")
	spec.Expect(string(meta.id)).ToEqual("paul")
	spec.Expect(len(meta.indexes)).ToEqual(1)
	values, _ := meta.indexes["children"]
	spec.Expect(values[0]).ToEqual("ghanima")
	spec.Expect(values[1]).ToEqual("leto")
}
