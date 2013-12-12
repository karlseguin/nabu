package nabu

import (
	"github.com/karlseguin/nabu/key"
)

// A result container which expects to be populated with pre-ordered documents
type SortedResult struct {
	found     int
	total     int
	db        *Database
	hasMore   bool
	documents []Document
	ids       []uint
}

func newSortedResult(db *Database) *SortedResult {
	return &SortedResult{
		db:        db,
		found:     0,
		ids:       make([]uint, db.maxLimit),
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

func (r *SortedResult) Ids() []uint {
	return r.ids[0:r.found]
}

func (r *SortedResult) Docs() []Document {
	for i := 0; i < r.found; i++ {
		r.documents[i] = r.db.Get(key.Type(r.ids[i]))
	}
	return r.documents[0:r.found]
}

func (r *SortedResult) Close() {
	r.found = 0
	r.total = 0
	r.hasMore = false
	r.db.sortedResults <- r
}

func (r *SortedResult) add(value key.Type) int {
	r.ids[r.found] = uint(value)
	r.found++
	return r.found
}
