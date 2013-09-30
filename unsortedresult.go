// A result which expects unsorted results and sorts them

package nabu

import (
  "sort"
)

type UnsortedResult struct {
  found int
  db *Database
  data []string
  rank map[string]int
}

func newUnsortedResult(db *Database) *UnsortedResult{
  return &UnsortedResult{
    db: db,
    found: 0,
    data: make([]string, db.maxUnsortedSize),
    rank: make(map[string]int, db.maxUnsortedSize),
  }
}

func (r *UnsortedResult) Data() []string {
  return r.data[0:r.found]
}

func (r *UnsortedResult) Len() int {
  return r.found
}

func (r *UnsortedResult) add(value string, rank int) {
  r.data[r.found] = value
  r.rank[value] = rank
  r.found++
}

func (r *UnsortedResult) finalize(q *Query) *UnsortedResult {
  original := r.data
  r.data = r.data[0:r.found]
  sort.Sort(r)
  r.data = original
  if r.found > q.limit { r.found = q.limit }
  if q.desc {
    for i := 0; i < r.found/2; i++ {
      j := r.found - i - 1
      x := r.data[i]
      r.data[i] = r.data[j]
      r.data[j] = x
    }
  }
  return r
}

func (r *UnsortedResult) Close() {
  r.found = 0
  r.db.unsortedResults <- r
}

func (r *UnsortedResult) Less(i, j int) bool {
  return r.rank[r.data[i]] < r.rank[r.data[j]]
}

func (r *UnsortedResult) Swap(i, j int) {
  x := r.data[i]
  r.data[i] = r.data[j]
  r.data[j] = x
}

