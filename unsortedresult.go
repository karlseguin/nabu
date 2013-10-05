// A result which expects unsorted results and sorts them

package nabu

import (
  "sort"
)

type UnsortedResult struct {
  found int
  db *Database
  ids []string
  original []string
  rank map[string]int
}

func newUnsortedResult(db *Database) *UnsortedResult{
  r := &UnsortedResult{
    db: db,
    found: 0,
    original: make([]string, db.maxUnsortedSize),
    rank: make(map[string]int, db.maxUnsortedSize),
  }
  return r
}

func (r *UnsortedResult) Ids() []string {
  return r.ids[0:r.found]
}

func (r *UnsortedResult) Len() int {
  return r.found
}

func (r *UnsortedResult) add(value string, rank int) {
  r.original[r.found] = value
  r.rank[value] = rank
  r.found++
}

func (r *UnsortedResult) finalize(q *Query) *UnsortedResult {
  r.ids = r.original[0:r.found]
  sort.Sort(r)

  if q.desc {
    to := r.found - q.offset
    if to < 0 {
      r.found = 0
      return r
    }
    from := to - q.limit
    if from < 0 { from  = 0}
    r.ids = r.original[from:to]
    r.found = to - from
    for i := 0; i < r.found/2; i++ {
      j := r.found - i - 1
      x := r.ids[i]
      r.ids[i] = r.ids[j]
      r.ids[j] = x
    }
  } else {
    from := q.offset
    to := r.found
    if r.found > q.limit { to = q.limit }
    if from > to {
      r.found = 0
      return r
    }
    r.ids = r.original[from:to]
    r.found = to - from
  }
  return r
}

func (r *UnsortedResult) Close() {
  r.found = 0
  r.db.unsortedResults <- r
}

func (r *UnsortedResult) Less(i, j int) bool {
  return r.rank[r.ids[i]] < r.rank[r.ids[j]]
}

func (r *UnsortedResult) Swap(i, j int) {
  x := r.ids[i]
  r.ids[i] = r.ids[j]
  r.ids[j] = x
}
