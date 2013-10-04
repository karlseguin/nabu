package nabu

import (
  "testing"
  "github.com/karlseguin/gspec"
)

func TestMetaCanBeSet(t *testing.T) {
  spec := gspec.New(t)
  meta := newMeta()
  meta.Id("paul")
  meta.Index("ghanima")
  meta.Index("leto")
  meta.Indexes("spice", "worm")
  spec.Expect(meta.id).ToEqual("paul")
  spec.Expect(len(meta.indexes)).ToEqual(4)
  _, exists := meta.indexes["ghanima"]
  spec.Expect(exists).ToEqual(true)
  _, exists = meta.indexes["leto"]
  spec.Expect(exists).ToEqual(true)
  _, exists = meta.indexes["spice"]
  spec.Expect(exists).ToEqual(true)
  _, exists = meta.indexes["worm"]
  spec.Expect(exists).ToEqual(true)
}
