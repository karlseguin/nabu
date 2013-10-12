package nabu

import (
  "sort"
  "nabu/indexes"
)

type Query struct {
  upto int
  limit int
  desc bool
  offset int
  cache bool
  db *Database
  sortLength int
  indexCount int
  includeTotal bool
  sort indexes.Sort
  indexNames []string
  indexes indexes.Indexes
}

func newQuery(db *Database) *Query {
  q := &Query{
    db: db,
    cache: true,
    indexes: make(indexes.Indexes, db.maxIndexesPerQuery),
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

func (q *Query) NoCache() *Query {
  q.cache = false
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
  q.sortLength = q.sort.Len()
  if indexCount == 0 {
    return q.findWithNoIndexes()
  }
  if indexCount == 1 {
    indexes := q.loadIndexes()
    if indexes == nil { return EmptyResult }
    indexes.RLock()
    defer indexes.RUnlock()
    return q.execute(indexes)
  }

  if q.cache == true {
    cached, ok := q.db.cache.Get(q.indexNames[0:indexCount])
    if ok { return q.execute(cached) }
  }

  indexes := q.loadIndexes()
  if indexes == nil { return EmptyResult }
  indexes.RLock()
  defer indexes.RUnlock()
  sort.Sort(indexes)
  return q.execute(indexes)
}

func (q *Query) loadIndexes() indexes.Indexes {
  if q.db.LookupIndexes(q.indexNames[0:q.indexCount], q.indexes) == false {
    return nil
  }
  return q.indexes[0:q.indexCount]
}

func (q *Query) execute(indexes indexes.Indexes) Result {
  firstLength := len(indexes[0].Ids)
  if firstLength == 0 {
    return EmptyResult
  }
  if q.sort.CanRank() && q.sortLength > firstLength*20 && firstLength <= q.db.maxUnsortedSize {
    return q.findByIndex(indexes)
  }
  return q.findBySort(indexes)
}

func (q *Query) findWithNoIndexes() Result {
  limit := q.limit
  sortLength := q.sortLength
  result := <- q.db.sortedResults
  var iterator indexes.Iterator
  if q.desc {
    iterator = q.sort.Backwards(q.offset)
  } else {
    iterator = q.sort.Forwards(q.offset)
  }

  for id := iterator.Current(); id != ""; id = iterator.Next() {
    if result.add(id) == limit { break }
  }
  iterator.Close()

  result.hasMore = sortLength > (q.offset + q.limit)
  result.total = sortLength
  if q.includeTotal == false {
    result.total = -1
  } else if result.total > q.upto {
    result.total = q.upto
  }
  return result
}

func (q *Query) findByIndex(indexes indexes.Indexes) Result {
  first := indexes[0]
  indexCount := len(indexes)
  result := <- q.db.unsortedResults
  for id, _ := range first.Ids {
    for j := 1; j < indexCount; j++ {
      if _, exists := indexes[j].Ids[id]; exists == false {
        goto nomatch
      }
    }
    if rank, exists := q.sort.Rank(id); exists {
      result.add(id, rank)
    }
    nomatch:
  }
  return result.finalize(q)
}

func (q *Query) findBySort(idx indexes.Indexes) Result {
  found := 0
  limit := q.limit
  indexCount := q.indexCount
  var iterator indexes.Iterator

  result := <- q.db.sortedResults
  if q.desc {
    iterator = q.sort.Backwards(0)
  } else {
    iterator = q.sort.Forwards(0)
  }

  for id := iterator.Current(); id != ""; id = iterator.Next() {
    for j := 0; j < indexCount; j++ {
      if _, exists := idx[j].Ids[id]; exists == false {
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
  iterator.Close()
  result.hasMore = result.total > (q.offset + q.limit)
  if q.includeTotal == false {
    result.total = -1
  }
  return result
}

func (q *Query) reset() {
  q.offset = 0
  q.cache = true
  q.desc = false
  q.indexCount = 0
  q.includeTotal = false
  q.limit = q.db.defaultLimit
  q.upto = q.db.defaultLimit + 1
  q.db.queryPool <- q
}
