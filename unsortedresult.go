package nabu

import (
	"github.com/karlseguin/nabu/key"
	"sort"
)

// A result container which expects to be populated with unordered documents
type UnsortedResult struct {
	found     int
	total     int
	hasMore   bool
	db        *Database
	documents []Document
	ids       []uint
	original  []uint
	score     map[uint]int
}

func newUnsortedResult(db *Database) *UnsortedResult {
	r := &UnsortedResult{
		db:       db,
		found:    0,
		original: make([]uint, db.maxUnsortedSize),
		score:    make(map[uint]int, db.maxUnsortedSize),
	}
	min := db.maxUnsortedSize
	if db.maxLimit < min {
		min = db.maxLimit
	}
	r.documents = make([]Document, min)
	return r
}

func (r *UnsortedResult) Len() int {
	return r.found
}

func (r *UnsortedResult) Total() int {
	return r.total
}

func (r *UnsortedResult) HasMore() bool {
	return r.hasMore
}

func (r *UnsortedResult) Ids() []uint {
	return r.ids[0:r.found]
}

func (r *UnsortedResult) Documents() []Document {
	for i := 0; i < r.found; i++ {
		r.documents[i] = r.db.Get(r.ids[i])
	}
	return r.documents[0:r.found]
}

func (r *UnsortedResult) add(value key.Type, score int) {
	v := uint(value)
	r.original[r.found] = v
	r.score[v] = score
	r.found++
}

func (r *UnsortedResult) finalize(q *NormalQuery) *UnsortedResult {
	r.total = r.found
	r.ids = r.original[0:r.found]
	sort.Sort(r)

	if q.desc {
		to := r.found - q.offset
		if to < 0 {
			r.found = 0
		} else {
			from := to - q.limit
			if from < 0 {
				from = 0
			}
			r.ids = r.original[from:to]
			r.found = to - from
			for i := 0; i < r.found/2; i++ {
				j := r.found - i - 1
				x := r.ids[i]
				r.ids[i] = r.ids[j]
				r.ids[j] = x
			}
		}
	} else {
		from := q.offset
		to := r.found
		if to > q.limit+from {
			to = from+q.limit
		}
		if from > to {
			r.found = 0
		} else {
			r.ids = r.original[from:to]
			r.found = to - from
		}
	}

	r.hasMore = r.found != 0 && r.total > (q.offset+r.found)
	if q.includeTotal == false {
		r.total = -1
	} else if q.upto < r.total {
		r.total = q.upto
	}
	return r
}

func (r *UnsortedResult) Close() {
	r.found = 0
	r.total = 0
	r.hasMore = false
	r.db.unsortedResults <- r
}

func (r *UnsortedResult) Less(i, j int) bool {
	return r.score[r.ids[i]] < r.score[r.ids[j]]
}

func (r *UnsortedResult) Swap(i, j int) {
	x := r.ids[i]
	r.ids[i] = r.ids[j]
	r.ids[j] = x
}
