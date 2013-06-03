package nabu

import (
  "sync"
)

type Index interface {
  Remove(id string)
  Add(id string)
  Exists(id string) bool
  Count() int
}

type Set struct {
  values map[string]bool
  sync.RWMutex
}

func NewIndex() Index {
  return &Set {
    values: make(map[string]bool, 16392),
  }
}

func (i *Set) Remove(id string) {
  i.Lock()
  defer i.Unlock()
  delete(i.values, id)
}

func (i *Set) Add(id string) {
  i.Lock()
  defer i.Unlock()
  i.values[id] = true
}

func (i *Set) Exists(id string) bool {
  i.RLock()
  defer i.RUnlock()
  return i.values[id]
}

func (i *Set) Count() int {
  i.RLock()
  defer i.RUnlock()
  return len(i.values)
}