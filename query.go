package nabu

import (
  "sort"
)

type Query struct {
  upto int
  limit int
  desc bool
  empty bool
  sort *Sort
  offset int
  db *Database
  sortLength int
  indexCount int
  indexes Indexes
  includeTotal bool
}

func newQuery(db *Database) *Query {
  q := &Query{
    db: db,
    indexes: make([]*Index, 10),
  }
  q.reset()
  return q
}

func (q *Query) Where(params ...string) *Query {
  l := len(params)
  for i := 0; i < l; i+=2 {
    indexName := params[i] + "$" + params[i+1]
    if index, exists := q.db.indexes[indexName]; exists == false {
      q.empty = true
    } else {
      q.indexes[q.indexCount + (i/2)] = index
    }
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
  if q.empty { return EmptyResult }
  return q.execute()
}

func (q *Query) reset() {
  q.offset = 0
  q.desc = false
  q.empty = false
  q.indexCount = 0
  q.includeTotal = false
  q.limit = q.db.defaultLimit
  q.upto = q.db.defaultLimit + 1
  q.db.queryPool <- q
}

func (q *Query) execute() Result {
  indexCount := q.indexCount
  if indexCount == 0 {
    return q.findWithNoIndexes()
  }
  q.indexes.rlock(q.indexCount)
  defer q.indexes.runlock(q.indexCount)

  indexes := q.indexes[0:q.indexCount]
  sort.Sort(indexes)
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