package indexes

import (
  "sync"
)

type Index struct {
  sync.RWMutex
  Name string
  Ids map[string]struct{}
}

func New(name string) *Index {
  return &Index{
    Name: name,
    Ids: make(map[string]struct{}),
  }
}

func (i *Index) Add(id string) {
  i.Lock()
  defer i.Unlock()
  i.Ids[id] = struct{}{}
}

func (i *Index) Remove(id string) {
  i.Lock()
  defer i.Unlock()
  delete(i.Ids, id)
}

func (i *Index) Contains(id string) bool {
  i.RLock()
  defer i.RUnlock()
  _, exists := i.Ids[id]
  return exists
}

type Indexes []*Index

func (indexes Indexes) Len() int {
  return len(indexes)
}

func (indexes Indexes) Less(i, j int) bool {
  return len(indexes[i].Ids) < len(indexes[j].Ids)
}

func (indexes Indexes) Swap(i, j int) {
  x := indexes[i]
  indexes[i] = indexes[j]
  indexes[j] = x
}

func (indexes Indexes) RLock() {
  for _, index := range indexes { index.RLock() }
}

func (indexes Indexes) RUnlock() {
  for _, index := range indexes { index.RUnlock() }
}
