package nabu

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

func (m *Meta) Index(index string) *Meta {
  m.indexes[index] = struct{}{}
  return m
}

func (m *Meta) Indexes(indexes ...string) *Meta {
  for _, index := range indexes {
    m.indexes[index] = struct{}{}
  }
  return m
}
