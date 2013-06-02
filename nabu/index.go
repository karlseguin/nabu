package nabu

type Index interface {
  Remove(id string)
  Add(id string)
  Exists(id string) bool
  Count() int
}

func NewIndex() Index {
  return &SetIndex {
    values: make(map[string]bool, 1048576),
  }
}