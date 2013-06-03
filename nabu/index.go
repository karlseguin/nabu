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
  lock *sync.RWMutex
}

func NewIndex() Index {
  return &Set {
    values: make(map[string]bool, 16392),
    lock: new(sync.RWMutex),
  }
}

func (i *Set) Remove(id string) {
  i.lock.Lock()
  defer i.lock.Unlock()
  delete(i.values, id)
}

func (i *Set) Add(id string) {
  i.lock.Lock()
  defer i.lock.Unlock()
  i.values[id] = true
}

func (i *Set) Exists(id string) bool {
  i.lock.RLock()
  defer i.lock.RUnlock()
  return i.values[id]
}

func (i *Set) Count() int {
  i.lock.RLock()
  defer i.lock.RUnlock()
  return len(i.values)
}