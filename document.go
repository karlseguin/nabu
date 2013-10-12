package nabu

type Document interface {
  ReadMeta(meta *Meta)
}

type Meta struct {
  id string
  indexes map[string]struct{}
}

func newMeta() *Meta {
  return &Meta{
    indexes: make(map[string]struct{}),
  }
}

func (m *Meta) Id(id string) *Meta {
  m.id = id
  return m
}

func (m *Meta) Index(index, value string) *Meta {
  m.indexes[index + "$" + value] = struct{}{}
  return m
}
