package nabu

type Query struct {
  db *Database
  indexes []Index
  limit int
}

func NewQuery(db *Database, indexes []Index) *Query {
  return &Query {
    db: db,
    indexes: indexes,
  }
}

func (q *Query) Result() []string {
  return q.filter()
}

func (q *Query) Limit(limit int) *Query {
  q.limit = limit
  return q
}

func (q *Query) filter() []string {
  smallest := q.indexes[0]
  smallestLength := smallest.Count()
  var others []map[string]bool

  for _, index := range q.indexes[1:] {
    if index == nil { continue }
    length := index.Count()
    if length < smallestLength {
      others = append(others, smallest.(*Set).values)
      smallest = index
      smallestLength = length
    } else {
      others = append(others, index.(*Set).values)
    }
  }
  found := 0
  var matches []string
  for key, _ := range smallest.(*Set).values {
    for _, index := range others {
      if index[key] == false { goto nomatch }
    }
    matches = append(matches, key)
    found++
    if found == q.limit { break }
nomatch:
  }
  return matches
}