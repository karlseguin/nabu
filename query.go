package nabu

import (
  "fmt"
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

func (q *Query) GetIndexes() Indexes {
  return q.indexes[0:q.indexCount]
}

func (q *Query) Execute() Result {
  defer q.reset()
  return q.db.Find(q)
}

func (q *Query) reset() {
  q.offset = 0
  q.desc = false
  q.indexCount = 0
  q.limit = q.db.defaultLimit
  q.db.queryPool <- q
}
