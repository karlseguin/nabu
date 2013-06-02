package nabu

import (
  "sync"
)

type SetIndex struct {
  values map[string]bool
  sync.RWMutex
}

func (i *SetIndex) Remove(id string) {
  i.Lock()
  defer i.Unlock()
  delete(i.values, id)
}

func (i *SetIndex) Add(id string) {
  i.Lock()
  defer i.Unlock()
  i.values[id] = true
}

func (i *SetIndex) Exists(id string) bool {
  i.RLock()
  defer i.RUnlock()
  return i.values[id]
}

func (i *SetIndex) Count() int {
  i.RLock()
  defer i.RUnlock()
  return len(i.values)
}

