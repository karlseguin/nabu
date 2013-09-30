package nabu

import (
  "fmt"
  "sort"
)

type Query struct {
  limit int
  desc bool
  sort *Sort
  offset int
  db *Database
  sortLength int
  indexCount int
  indexes Indexes
}

func newQuery(db *Database) *Query {
  q := &Query{
    db: db,
    indexes: make([]Index, 10),
  }
  q.reset()
  return q
}

func (q *Query) Index(name string) *Query {
  index, exists := q.db.indexes[name]
  if exists == false { panic(fmt.Sprintf("unknown index %q", name)) }
  q.indexes[q.indexCount] = index
  q.indexCount++
  return q
}

func (q *Query) Indexes(names ...string) *Query {
  l := len(names)
  for i := 0; i < l; i++ {
    if index, exists := q.db.indexes[names[i]]; exists == false {
      panic(fmt.Sprintf("unknown index %q", names[i]))
    } else {
      q.indexes[q.indexCount + i] = index
    }
  }
  q.indexCount += l
  return q
}

func (q *Query) Limit(limit int) *Query {
  q.limit = limit
  if q.limit >  q.db.maxLimit {
    q.limit =  q.db.maxLimit
  }
  return q
}

func (q *Query) Desc() *Query {
  q.desc = true
  return q
}

func (q *Query) Offset(offset int) *Query {
  q.offset = offset
  return q
}

func (q *Query) Execute() Result {
  defer q.reset()
  return q.execute()
}

func (q *Query) reset() {
  q.offset = 0
  q.desc = false
  q.indexCount = 0
  q.limit = q.db.defaultLimit
  q.db.queryPool <- q
}


func (q *Query) execute() Result {
  indexCount := q.indexCount
  if indexCount == 0 {
    return q.findWithNoIndexes()
  }
  indexes := q.indexes[0:q.indexCount]
  sort.Sort(indexes)
  firstLength := len(indexes[0])

  if firstLength == 0 {
    return EmptyResult
  }

  if firstLength <= q.db.maxUnsortedSize && q.sortLength / firstLength > 100 {
    return q.findByIndex(indexes)
  }
  return q.findBySort(indexes)
}

func (q *Query) findWithNoIndexes() Result {
  s := *q.sort
  limit := q.limit
  sortLength := q.sortLength
  result := <- q.db.sortedResults
  if q.desc {
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

func (q *Query) findByIndex(indexes Indexes) Result {
  first := indexes[0]
  indexCount := len(indexes)
  ranking := q.sort.lookup
  result := <- q.db.unsortedResults
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
  return result.finalize(q)
}

func (q *Query) findBySort(indexes Indexes) Result {
  s := *q.sort
  limit := q.limit
  sortLength := q.sortLength
  indexCount := q.indexCount
  result := <- q.db.sortedResults
  if q.desc {
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
