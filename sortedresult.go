// A result which expects already sorted values to be added

package nabu

type SortedResult struct {
  found int
  total int
  db *Database
  hasMore bool
  ids []string
  documents []Document
}

func newSortedResult(db *Database) *SortedResult{
  return &SortedResult{
    db: db,
    found: 0,
    ids: make([]string, db.maxLimit),
    documents: make([]Document, db.maxLimit),
  }
}

func (r *SortedResult) Len() int {
  return r.found
}

func (r *SortedResult) Total() int {
  return r.total
}

func (r *SortedResult) HasMore() bool {
  return r.hasMore
}

func (r *SortedResult) Ids() []string {
  return r.ids[0:r.found]
}

func (r *SortedResult) Docs() []Document {
  for i := 0; i < r.found; i++ {
    r.documents[i] = r.db.Get(r.ids[i])
  }
  return r.documents[0:r.found]
}

func (r *SortedResult) Close() {
  r.found = 0
  r.total = 0
  r.hasMore = false
  r.db.sortedResults <- r
}

func (r *SortedResult) add(value string) int {
  r.ids[r.found] = value
  r.found++
  return r.found
}