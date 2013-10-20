package indexes

import (
  "nabu/key"
)

type Sort interface {
  Len() int
  CanRank() bool
  Load(ids []key.Type)
  Rank(id key.Type) (int, bool)
  Forwards(offset int) Iterator
  Backwards(offset int) Iterator
}

type Iterator interface {
  Next() key.Type
  Current() key.Type
  Close()
}

func NewSort() Sort {
  return &StaticSort{}
}

type EmptyIterator struct {}

func (i *EmptyIterator) Next() key.Type { return key.NULL }
func (i *EmptyIterator) Current() key.Type { return key.NULL }
func (i *EmptyIterator) Close() {}

var emptyIterator = &EmptyIterator{}

// type Sort struct {
//   List []string
//   Lookup map[string]int
// }

// func (s *Sort) Add(id string, rank int) {
//   s.List[rank] = id
//   s.Lookup[id] = rank
// }

// func (s *Sort) Len() int {
//   return len(s.List)
// }

// func NewSort(size int) *Sort {
//   s := &Sort{List: make([]string, size), Lookup: make(map[string]int),}
//   for i := 0; i < size; i++ {
//     id := strconv.Itoa(i)
//     s.List[i] = id
//     s.Lookup[id] = i
//   }
//   return s
// }
