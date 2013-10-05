// A result which expects already sorted values to be added

package nabu

type SortedResult struct {
  found int
  total int
  db *Database
  ids []string
}

func newSortedResult(db *Database) *SortedResult{
  return &SortedResult{
    db: db,
    found: 0,
    ids: make([]string, db.maxLimit),
  }
}

func (r *SortedResult) Ids() []string {
  return r.ids[0:r.found]
}

func (r *SortedResult) Len() int {
  return r.found
}

func (r *SortedResult) Close() {
  r.found = 0
  r.total = 0
  r.db.sortedResults <- r
}

func (r *SortedResult) add(value string) int {
  r.ids[r.found] = value
  r.found++
  return r.found
}
