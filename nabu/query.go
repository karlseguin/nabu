package nabu

import (
  "sort"
)

type Query struct {
  db *Database
  filters *Filters
  sort SortedIndex
  limit int
  desc bool
}

type Filters struct {
  indexes []*Set
}

func (f *Filters) Len() int {
  return len(f.indexes)
}

func (f *Filters) Less(a int, b int) bool {
  return f.indexes[a].Count() < f.indexes[b].Count()
}
func (f *Filters) Swap(a int, b int) {
  t := f.indexes[a]
  f.indexes[a] = f.indexes[b]
  f.indexes[b] = t
}

func NewQuery(db *Database, sort SortedIndex, indexes []*Set) *Query {
  return &Query {
    db: db,
    sort: sort,
    filters: &Filters {
      indexes: indexes,
    },
  }
}

func (q *Query) Result() []string {
  return q.filter()
}

func (q *Query) Limit(limit int) *Query {
  q.limit = limit
  return q
}

func (q *Query) Desc() *Query {
  q.desc = true
  return q
}

//todo: when one of the filters is small (especially in relation to the length of q.sort),
// it's more efficient first filter then sort
func (q *Query) filter() []string {
  sort.Sort(q.filters)

  var iterator SortedIndexIterator
  if q.desc {
    iterator = q.sort.Backward()
  } else {
    iterator = q.sort.Forward()
  }
  defer iterator.Close()

  for _, index := range q.filters.indexes {
    index.lock.RLock()
    defer index.lock.RUnlock()
  }

  found := 0
  var matches []string
  for ; iterator.HasNext(); iterator.Next() {
    _, id := iterator.Current()
    for _, index := range q.filters.indexes {
      if index.values[id] == false { goto nomatch }
    }
    matches = append(matches, id)
    found++
    if found == q.limit { break }
nomatch:
  }
  return matches
}