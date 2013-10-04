// A result which expects unsorted results and sorts them

package nabu

import (
  "sort"
)

type UnsortedResult struct {
  found int
  db *Database
  ids []string
  rank map[string]int
}

func newUnsortedResult(db *Database) *UnsortedResult{
  return &UnsortedResult{
    db: db,
    found: 0,
    ids: make([]string, db.maxUnsortedSize),
    rank: make(map[string]int, db.maxUnsortedSize),
  }
}

func (r *UnsortedResult) Ids() []string {
  return r.ids[0:r.found]
}

func (r *UnsortedResult) Len() int {
  return r.found
}

func (r *UnsortedResult) add(value string, rank int) {
  r.ids[r.found] = value
  r.rank[value] = rank
  r.found++
}

func (r *UnsortedResult) finalize(q *Query) *UnsortedResult {
  original := r.ids
  r.ids = r.ids[0:r.found]
  sort.Sort(r)
  r.ids = original
  if r.found > q.limit { r.found = q.limit }
  if q.desc {
    for i := 0; i < r.found/2; i++ {
      j := r.found - i - 1
      x := r.ids[i]
      r.ids[i] = r.ids[j]
      r.ids[j] = x
    }
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
