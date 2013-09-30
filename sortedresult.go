// A result which expects already sorted values to be added

package nabu

type SortedResult struct {
  found int
  db *Database
  data []string
}

func newSortedResult(db *Database) *SortedResult{
  return &SortedResult{
    db: db,
    found: 0,
    data: make([]string, db.maxLimit),
  }
}

func (r *SortedResult) Data() []string {
  return r.data[0:r.found]
}

func (r *SortedResult) Len() int {
  return r.found
}

func (r *SortedResult) Close() {
  r.found = 0
  r.db.sortedResults <- r
}

func (r *SortedResult) add(value string) int {
  r.data[r.found] = value
  r.found++
  return r.found
}
