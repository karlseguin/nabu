package nabu

import (
  "github.com/karlseguin/nabu/key"
)

/*
Any document stored in nabu must implement this interface:

    func (t *Tree) ReadMeta(m *nabu.Meta) {
      m.Id(key.Type(t.Id))
      m.Index("tree:borough", t.Borough)
      m.Index("tree:species", t.Species)
      m.Sort("tree:age", t.Age)
    }
*/
type Document interface {
  ReadMeta(meta *Meta)
}

// Meta describes a document
type Meta struct {
  id key.Type
  sorts map[string]int
  indexes map[string][]string
}

func newMeta() *Meta {
  return &Meta{
    sorts: make(map[string]int),
    indexes: make(map[string][]string),
  }
}

// The document's Id
func (m *Meta) Id(id key.Type) *Meta {
  m.id = id
  return m
}

// A document's index and value. Can be called multiple times
func (m *Meta) Index(indexName, value string) *Meta {
  index, exists := m.indexes[indexName]
  if exists == false {
    index = make([]string, 0, 1)
  }
  m.indexes[indexName] = append(index, value)
  return m
}

// A document's sort and rank. Can be called Multiple times
func (m *Meta) Sort(name string, rank int) *Meta {
  m.sorts[name] = rank
  return m
}
