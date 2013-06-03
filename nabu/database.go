package nabu

import (
  "sync"
)

type Database struct {
  indexes map[string]Index
  sorts map[string]SortedIndex
  resources map[string]Resource
  rLock *sync.RWMutex
}

func DB(indexNames []string, sortNames []string) *Database {
  indexes := make(map[string]Index, len(indexNames))
  for _, name := range indexNames {
    indexes[name] = NewIndex()
  }
  sorts := make(map[string]SortedIndex, len(sortNames))
  for _, name := range sortNames {
    sorts[name] = NewSortedIndex()
  }
  return &Database {
    indexes: indexes,
    sorts: sorts,
    resources: make(map[string]Resource, 16392),
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

  if exists {
    db.removeFromSorts(id, p.GetSorts())
    db.unindex(id, p.GetIndexes())
  }
  db.addToSorts(id, resource.GetSorts())
  db.index(id, resource.GetIndexes())
}

func (db *Database) Remove(id string) {
  db.rLock.Lock()
  resource, exists := db.resources[id]
  delete(db.resources, id)
  db.rLock.Unlock()
  if exists {
    db.unindex(id, resource.GetIndexes())
  }
}

func (db *Database) index(id string, indexNames []string) {
  for _, name := range indexNames {
    if index, exists := db.indexes[name]; exists {
      index.Add(id)
    }
  }
}

func (db *Database) unindex(id string, indexNames []string) {
  for _, name := range indexNames {
    if index, exists := db.indexes[name]; exists {
      index.Remove(id)
    }
  }
}

func (db *Database) addToSorts(id string, sorts map[string]int) {
  for name, rank := range sorts {
    if index, exists := db.sorts[name]; exists {
      index.Set(rank, id)
    }
  }
}

func (db *Database) removeFromSorts(id string, sorts map[string]int) {
  for name, _ := range sorts {
    if index, exists := db.sorts[name]; exists {
      index.Remove(id)
    }
  }
}