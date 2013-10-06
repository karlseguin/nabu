package nabu

import (
  "sync"
)

type Index struct {
  name string
  sync.RWMutex
  ids map[string]struct{}
}

func newIndex(name string) *Index {
  return &Index{
    name: name,
    ids: make(map[string]struct{}),
  }
}

func (i *Index) Add(id string) {
  i.Lock()
  defer i.Unlock()
  i.ids[id] = struct{}{}
}

func (i *Index) Remove(id string) {
  i.Lock()
  defer i.Unlock()
  delete(i.ids, id)
}

func (i *Index) Contains(id string) bool {
  i.RLock()
  defer i.RUnlock()
  _, exists := i.ids[id]
  return exists
}

type Indexes []*Index

func (indexes Indexes) Len() int {
  return len(indexes)
}

func (indexes Indexes) Less(i, j int) bool {
  return len(indexes[i].ids) < len(indexes[j].ids)
}

func (indexes Indexes) Swap(i, j int) {
  x := indexes[i]
  indexes[i] = indexes[j]
  indexes[j] = x
}

func (indexes Indexes) rlock() {
  for _, index := range indexes { index.RLock() }
}

func (indexes Indexes) runlock() {
  for _, index := range indexes { index.RUnlock() }
}
