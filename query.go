package nabu

import (
  "sort"
)

type Query struct {
  upto int
  limit int
  desc bool
  sort *Sort
  offset int
  db *Database
  sortLength int
  indexCount int
  indexes Indexes
  indexNames []string
  includeTotal bool
}

func newQuery(db *Database) *Query {
  q := &Query{
    db: db,
    indexes: make(Indexes, db.maxIndexesPerQuery),
    indexNames: make([]string, db.maxIndexesPerQuery),
  }
  q.reset()
  return q
}

func (q *Query) Where(params ...string) *Query {
  l := len(params)
  for i := 0; i < l; i+=2 {
    q.indexNames[q.indexCount + (i/2)] = params[i] + "$" + params[i+1]
  }
  q.indexCount += l / 2
  return q
}

func (q *Query) Limit(limit int) *Query {
  q.limit = limit
  if q.limit >  q.db.maxLimit {
    q.limit =  q.db.maxLimit
  }
  if q.includeTotal == false {
    q.upto = q.limit + 1
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

func (q *Query) IncludeTotal() *Query {
  q.includeTotal = true
  q.upto = q.db.maxTotal
  return q
}

func (q *Query) Execute() Result {
  defer q.reset()
  indexCount := q.indexCount
  if indexCount == 0 {
    return q.findWithNoIndexes()
  }
  if indexCount == 1 {
    indexes := q.loadIndexes()
    if indexes == nil { return EmptyResult }
    indexes.rlock()
    defer indexes.runlock()
    return q.execute(indexes)
  }

  cached, ok := q.db.cache.Get(q.indexNames[0:indexCount])
  if ok { return q.execute(cached) }

  indexes := q.loadIndexes()
  if indexes == nil { return EmptyResult }
  indexes.rlock()
  defer indexes.runlock()
  sort.Sort(indexes)
  return q.execute(indexes)
}

func (q *Query) loadIndexes() Indexes {
  if q.db.lookupIndexes(q.indexNames[0:q.indexCount], q.indexes) == false {
    return nil
  }
  return q.indexes[0:q.indexCount]
}

func (q *Query) reset() {
  q.offset = 0
  q.desc = false
  q.indexCount = 0
  q.includeTotal = false
  q.limit = q.db.defaultLimit
  q.upto = q.db.defaultLimit + 1
  q.db.queryPool <- q
}

func (q *Query) execute(indexes Indexes) Result {
  firstLength := len(indexes[0].ids)
  if firstLength == 0 {
    return EmptyResult
  }
  if q.sortLength > firstLength*20 && firstLength <= q.db.maxUnsortedSize {
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
    for i := sortLength-1-q.offset; i >= 0; i-- {
      if result.add(s.list[i]) == limit { break }
    }
  } else {
    for i := q.offset; i < sortLength; i++ {
      if result.add(s.list[i]) == limit { break }
    }
  }
  result.hasMore = sortLength > (q.offset + q.limit)
  result.total = sortLength
  if q.includeTotal == false {
    result.total = -1
  } else if result.total > q.upto {
    result.total = q.upto
  }
  return result
}

func (q *Query) findByIndex(indexes Indexes) Result {
  first := indexes[0]
  indexCount := len(indexes)
  ranking := q.sort.lookup
  result := <- q.db.unsortedResults
  for id, _ := range first.ids {
    for j := 1; j < indexCount; j++ {
      if _, exists := indexes[j].ids[id]; exists == false {
        goto nomatch
      }
    }
    if rank, exists := ranking[id]; exists {
      result.add(id, rank)
    }
    nomatch:
  }
  return result.finalize(q)
}

func (q *Query) findBySort(indexes Indexes) Result {
  s := *q.sort
  found := 0
  limit := q.limit
  sortLength := q.sortLength
  indexCount := q.indexCount
  result := <- q.db.sortedResults
  if q.desc {
    for i := sortLength-1; i >= 0; i-- {
      id := s.list[i]
      for j := 0; j < indexCount; j++ {
        if _, exists := indexes[j].ids[id]; exists == false {
          goto nomatchdesc
        }
      }
      result.total++
      if result.total > q.offset {
        if found < limit {
          result.add(id)
          found++
        } else if result.total >= q.upto {
          break
        }
      }
      nomatchdesc:
    }
  } else {
    for i := 0; i < sortLength; i++ {
      id := s.list[i]
      for j := 0; j < indexCount; j++ {
        if _, exists := indexes[j].ids[id]; exists == false {
          goto nomatchasc
        }
      }
      result.total++
      if result.total > q.offset {
        if found < limit {
          result.add(id)
          found++
        } else if result.total >= q.upto {
          break
        }
      }
      nomatchasc:
    }
  }
  result.hasMore = result.total > (q.offset + q.limit)
  if q.includeTotal == false {
    result.total = -1
  }
  return result
}
