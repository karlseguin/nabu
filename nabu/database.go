package nabu

import (
  "sort"
  "sync"
)

type Database struct {
  indexes map[string]Index
  resources map[string]Resource
  rLock *sync.RWMutex
}

func DB(indexNames []string) *Database {
  indexes := make(map[string]Index, len(indexNames))
  for _, name := range indexNames {
    indexes[name] = NewIndex()
  }
  return &Database {
    indexes: indexes,
    resources: make(map[string]Resource, 1048576),
    rLock: new(sync.RWMutex),
  }
}

func (db *Database) Find(indexNames ...string) *Query {
  indexes := make([]Index, len(indexNames)) //todo mnenomize
  for i, name := range indexNames {
    indexes[i] = db.indexes[name]
  }
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
  if exists {
    db.unindex(id, resource.GetIndexes()...)
  }
}

func (db *Database) index(id string, indexNames ...string) {
  for _, name := range indexNames {
    if index, exists := db.indexes[name]; exists {
      index.Add(id)
    }
  }
}

func (db *Database) unindex(id string, indexNames ...string) {
  for _, name := range indexNames {
    if index, exists := db.indexes[name]; exists {
      index.Remove(id)
    }
  }
}