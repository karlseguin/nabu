package nabu

import (
  "testing"
  "github.com/karlseguin/gspec"
)

func TestMetaCanBeSet(t *testing.T) {
  spec := gspec.New(t)
  meta := newMeta()
  meta.Id("paul")
  meta.Index("children", "ghanima")
  meta.Index("children", "leto")
  spec.Expect(meta.id).ToEqual("paul")
  spec.Expect(len(meta.indexes)).ToEqual(2)
  _, exists := meta.indexes["children$ghanima"]
  spec.Expect(exists).ToEqual(true)
  _, exists = meta.indexes["children$leto"]
  spec.Expect(exists).ToEqual(true)
}
