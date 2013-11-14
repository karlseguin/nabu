package nabu

import (
  "github.com/karlseguin/nabu/key"
)

type Document interface {
  ReadMeta(meta *Meta)
}

type Meta struct {
  id key.Type
  sorts map[string]int
  indexes map[string]struct{}
}

func newMeta() *Meta {
  return &Meta{
    sorts: make(map[string]int),
    indexes: make(map[string]struct{}),
  }
}

func (m *Meta) Id(id key.Type) *Meta {
  m.id = id
  return m
}

func (m *Meta) Index(index, value string) *Meta {
  m.indexes[index + "$" + value] = struct{}{}
  return m
}

func (m *Meta) Sort(name string, rank int) *Meta {
  m.sorts[name] = rank
  return m
}
