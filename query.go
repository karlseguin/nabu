package nabu

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"sort"
)

// Build and executes a query against the database
type Query struct {
	upto         int
	limit        int
	desc         bool
	offset       int
	cache        bool
	db           *Database
	sortLength   int
	indexCount   int
	includeTotal bool
	sort         indexes.Sort
	indexNames   []string
	indexes      indexes.Indexes
}

// Queries are statically created upfront and reused
func newQuery(db *Database) *Query {
	q := &Query{
		db:         db,
		cache:      true,
		indexes:    make(indexes.Indexes, db.maxIndexesPerQuery),
		indexNames: make([]string, db.maxIndexesPerQuery),
	}
	q.reset()
	return q
}

// Filter results for the query and value. Where can be called multiple
// times. Each must have an even number of parameters (indexName, value):
//
//    Where("type", "dog", "size", "small").Where("color", "white")
//
func (q *Query) Where(params ...string) *Query {
	l := len(params)
	for i := 0; i < l; i += 2 {
		q.indexNames[q.indexCount+(i/2)] = params[i] + "$" + params[i+1]
	}
	q.indexCount += l / 2
	return q
}

// Don't cache the result or use the cache to generate the result
// Caches are incrementally updated as changes come in, so this should
// only be used for one-off queries
func (q *Query) NoCache() *Query {
	q.cache = false
	return q
}

// Limit the number of documents returned
func (q *Query) Limit(limit int) *Query {
	q.limit = limit
	if q.limit > q.db.maxLimit {
		q.limit = q.db.maxLimit
	}
	if q.includeTotal == false {
		q.upto = q.limit + 1
	}
	return q
}

// Paging offset to start at
func (q *Query) Offset(offset int) *Query {
	q.offset = offset
	return q
}

// Sort the documents by descending order
func (q *Query) Desc() *Query {
	q.desc = true
	return q
}

// By default, a total count of matching document won't be returned.
// Instead, only the results HasMore will be set. Including the total
// count is less efficient (and unecessary for infinite scrolling or
// for requests beyond the first page). The total will be capped at the
// configured MaxTotal
func (q *Query) IncludeTotal() *Query {
	q.includeTotal = true
	q.upto = q.db.maxTotal
	return q
}

// Executes the query, returning a result. The result must be closed
// once you are done with it
func (q *Query) Execute() Result {
	defer q.reset()
	indexCount := q.indexCount
	q.sortLength = q.sort.Len()
	if indexCount == 0 {
		return q.findWithNoIndexes()
	}
	if indexCount > 1 && q.cache == true {
		if cached, ok := q.db.cache.Get(q.indexNames[0:indexCount]); ok {
			q.indexCount = 1
			cached.RLock()
			defer cached.RUnlock()
			return q.execute(cached)
		}
	}

	indexes := q.loadIndexes()
	if indexes == nil {
		return EmptyResult
	}
	indexes.RLock()
	defer indexes.RUnlock()
	return q.execute(indexes)
}

// Loads the indexes used by the query
func (q *Query) loadIndexes() indexes.Indexes {
	if q.db.LookupIndexes(q.indexNames[0:q.indexCount], q.indexes) == false {
		return nil
	}
	indexes := q.indexes[0:q.indexCount]
	if q.indexCount > 1 {
		sort.Sort(indexes)
	}
	return indexes
}

// Determines wither an index-based query will be used or a sort-based query.
// The choice is based on the type of sort index (whether it can rank documents),
// whether the smallest index fits within the configured maximum unsorted size and,
// whether the smallest index is sufficiently small compared to the sort index.
func (q *Query) execute(indexes indexes.Indexes) Result {
	firstLength := len(indexes[0].Ids)
	if firstLength == 0 {
		return EmptyResult
	}
	if q.sort.CanRank() && q.sortLength > firstLength*10 && firstLength <= q.db.maxUnsortedSize {
		return q.findByIndex(indexes)
	}
	return q.findBySort(indexes)
}

// An optimized code path for when no index is provided (just walking through
// a sort index)
func (q *Query) findWithNoIndexes() Result {
	limit := q.limit
	sortLength := q.sortLength
	result := <-q.db.sortedResults
	var iterator indexes.Iterator
	if q.desc {
		iterator = q.sort.Backwards(q.offset)
	} else {
		iterator = q.sort.Forwards(q.offset)
	}

	for id := iterator.Current(); id != key.NULL; id = iterator.Next() {
		if result.add(id) == limit {
			break
		}
	}
	iterator.Close()

	result.hasMore = sortLength > (q.offset + q.limit)
	result.total = sortLength
	if q.includeTotal == false {
		result.total = -1
	} else if result.total > q.upto {
		result.total = q.upto
	}
	return result
}

// Filter by indexes, then sort. Ideal when the smallest index is quite a bit
// smaller than the sort index
func (q *Query) findByIndex(indexes indexes.Indexes) Result {
	first := indexes[0]
	indexCount := len(indexes)
	result := <-q.db.unsortedResults
	for id, _ := range first.Ids {
		for j := 1; j < indexCount; j++ {
			if _, exists := indexes[j].Ids[id]; exists == false {
				goto nomatch
			}
		}
		if rank, exists := q.sort.Rank(id); exists {
			result.add(id, rank)
		}
	nomatch:
	}
	return result.finalize(q)
}

// Walk the sort index and filter out results
func (q *Query) findBySort(idx indexes.Indexes) Result {
	found := 0
	limit := q.limit
	indexCount := q.indexCount
	var iterator indexes.Iterator

	result := <-q.db.sortedResults
	if q.desc {
		iterator = q.sort.Backwards(0)
	} else {
		iterator = q.sort.Forwards(0)
	}

	for id := iterator.Current(); id != key.NULL; id = iterator.Next() {
		for j := 0; j < indexCount; j++ {
			if _, exists := idx[j].Ids[id]; exists == false {
				goto nomatchdesc
			}
		}
		result.total++
		if result.total > q.offset {
			if found < limit {
				result.add(id)
				found++
			} else if result.total >= q.upto {
				break
			}
		}
	nomatchdesc:
	}
	iterator.Close()
	result.hasMore = result.total > (q.offset + q.limit)
	if q.includeTotal == false {
		result.total = -1
	}
	return result
}

// Reset the query and release it back into the pool
func (q *Query) reset() {
	q.sort = nil
	q.offset = 0
	q.cache = true
	q.desc = false
	q.indexCount = 0
	q.includeTotal = false
	q.limit = q.db.defaultLimit
	q.upto = q.db.defaultLimit + 1
	q.db.queryPool <- q
}
