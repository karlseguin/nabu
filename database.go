package nabu

import (
  "fmt"
)

type Database struct {
  *Configuration
  queryPool chan *Query
  sorts map[string]*Sort
  indexes map[string]Index
  sortedResults chan *SortedResult
  unsortedResults chan *UnsortedResult
}

func New(c *Configuration) *Database {
  db := &Database{
    Configuration: c,
    sorts: make(map[string]*Sort),
    indexes: make(map[string]Index),
    queryPool: make(chan *Query, c.queryPoolSize),
    sortedResults: make(chan *SortedResult, c.sortedResultPoolSize),
    unsortedResults: make(chan *UnsortedResult, c.unsortedResultPoolSize),
  }
  for i := 0; i < c.queryPoolSize; i++ {
    newQuery(db) //it automatically enqueues itself
  }
  for i := 0; i < c.sortedResultPoolSize; i++ {
    db.sortedResults <- newSortedResult(db)
  }
  for i := 0; i < c.unsortedResultPoolSize; i++ {
    db.unsortedResults <- newUnsortedResult(db)
  }
  return db
}

func (db *Database) Query(name string) *Query {
  sort, exists := db.sorts[name]
  if exists == false {
    panic(fmt.Sprintf("unknown sort index %q", name))
  }
  q := <-db.queryPool
  q.sort = sort
  q.sortLength = sort.Len()
  return q
}

func (db *Database) AddSort(name string, list []string) {
  s := &Sort {
    list: list,
    lookup: make(map[string]int, len(list)),
  }
  for i := 0; i < len(list); i++ {
    s.lookup[list[i]] = i
  }
  db.sorts[name] = s
}

func (db *Database) AddIndex(name string, index Index) {
  db.indexes[name] = index
}
