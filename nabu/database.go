package nabu

import (
  "sort"
  "sync"
)

type Database struct {
  indexes map[string]Index
  resources map[string]Resource
  iLock *sync.RWMutex
  rLock *sync.RWMutex
}

func DB() *Database {
  return &Database {
    indexes: make(map[string]Index, 256),
    resources: make(map[string]Resource, 1048576),
    iLock: new(sync.RWMutex),
    rLock: new(sync.RWMutex),
  }
}

func (db *Database) Find(indexNames ...string) *Query {
  indexes := make([]Index, len(indexNames)) //todo mnenomize
  db.iLock.RLock()
  for i, name := range indexNames {
    indexes[i] = db.indexes[name]
  }
  db.iLock.RUnlock()
  return NewQuery(db, indexes)
}

func (db *Database) Update(resource Resource) {
  id := resource.GetId()
  db.rLock.Lock()
  p, exists := db.resources[id]
  db.resources[id] = resource
  db.rLock.Unlock()

  indexes := resource.GetIndexes()
  if exists {
    // hate everything about this
    existings := p.GetIndexes()
    sort.Strings(existings)
    sort.Strings(indexes)
    length := len(indexes)
    existingLength := len(existings)
    j := 0
    for i := 0; i < length; {
      index := indexes[i]
      var existing string
      if j < existingLength { existing = existings[j] }
      if index > existing && existing != "" {
        db.unindex(id, existing)
        j++
      } else if index < existing || existing == ""  {
        db.index(id, index)
        i++
      } else {
        i++
        j++
      }
    }
  } else {
    db.index(id, indexes...)
  }
}

func (db *Database) Remove(id string) {
  db.rLock.Lock()
  resource, exists := db.resources[id]
  delete(db.resources, id)
  db.rLock.Unlock()
  if !exists { return }

  db.iLock.RLock()
  for _, index := range resource.GetIndexes() {
    db.indexes[index].Remove(id)
  }
  db.iLock.RUnlock()
}

func (db *Database) index(id string, indexNames ...string) {
  db.iLock.RLock()
  for _, name := range indexNames {
    index, exists := db.indexes[name]
    if !exists {
      db.iLock.RUnlock()
      index = db.createIndex(name)
      db.iLock.RLock()
    }
    index.Add(id)
  }
  db.iLock.RUnlock()
}

func (db *Database) unindex(id string, indexNames ...string) {
  db.iLock.RLock()
  for _, name := range indexNames {
    if index, exists := db.indexes[name]; exists {
      index.Remove(id)
    }
  }
  db.iLock.RUnlock()
}

func (db *Database) createIndex(name string) Index {
  db.iLock.Lock()
  defer db.iLock.Unlock()
  index, exists := db.indexes[name];
  if !exists {
    index = NewIndex()
    db.indexes[name] = index
  }
  return index
}