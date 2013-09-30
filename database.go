package nabu

import (
  "fmt"
  "sort"
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

func (db *Database) Find(query *Query) Result {
  indexCount := query.indexCount
  if indexCount == 0 {
    return db.findWithNoIndexes(query)
  }

  indexes := query.GetIndexes()
  sort.Sort(indexes)
  firstLength := len(indexes[0])
  if firstLength == 0 { return EmptyResult }

  if firstLength <= db.maxUnsortedSize && query.sortLength / firstLength > 100 {
    return db.findByIndex(query, indexes)
  }
  return db.findBySort(query, indexes)
}

func (db *Database) findWithNoIndexes(query *Query) Result {
  s := *query.sort
  limit := query.limit
  sortLength := query.sortLength
  result := <- db.sortedResults
  if query.desc {
    for i := sortLength-1; i >= 0; i-- {
      if result.add(s.list[i]) == limit { break }
    }
  } else {
    for i := 0; i < sortLength; i++ {
      if result.add(s.list[i]) == limit { break }
    }
  }

  return result
}

func (db *Database) findByIndex(query *Query, indexes Indexes) Result {
  first := indexes[0]
  indexCount := len(indexes)
  ranking := query.sort.lookup
  result := <- db.unsortedResults
  for value, _ := range first {
    for j := 1; j < indexCount; j++ {
      if _, exists := indexes[j][value]; exists == false {
        goto nomatch
      }
    }
    if rank, exists := ranking[value]; exists {
      result.add(value, rank)
    }
    nomatch:
  }
  return result.finalize(query)
}

func (db *Database) findBySort(query *Query, indexes Indexes) Result {
  s := *query.sort
  limit := query.limit
  sortLength := query.sortLength
  indexCount := query.indexCount
  result := <- db.sortedResults
  if query.desc {
    for i := sortLength-1; i >= 0; i-- {
      value := s.list[i]
      for j := 0; j < indexCount; j++ {
        if _, exists := indexes[j][value]; exists == false {
          goto nomatchdesc
        }
      }
      if result.add(value) == limit { break }
      nomatchdesc:
    }
  } else {
    for i := 0; i < sortLength; i++ {
      value := s.list[i]
      for j := 0; j < indexCount; j++ {
        if _, exists := indexes[j][value]; exists == false {
          goto nomatchasc
        }
      }
      if result.add(value) == limit { break }
      nomatchasc:
    }
  }
  return result
}
