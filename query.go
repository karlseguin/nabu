package nabu

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"sort"
)

// Build and executes a query against the database
type Query struct {
	upto          int
	limit         int
	desc          bool
	offset        int
	cache         bool
	db            *Database
	sortLength    int
	indexCount    int
	includeTotal  bool
	sortCondition Condition
	sort          indexes.Index
	ranged        bool
	indexNames    []string
	conditions    Conditions
	indexes       indexes.Indexes
}

// Queries are statically created upfront and reused
func newQuery(db *Database) *Query {
	q := &Query{
		db:         db,
		cache:      true,
		indexes:    make(indexes.Indexes, db.maxIndexesPerQuery),
		indexNames: make([]string, db.maxIndexesPerQuery),
		conditions: make(Conditions, db.maxIndexesPerQuery),
	}
	q.reset()
	return q
}

// Filter results for the query and value. Where can be called multiple
// times. Each must have an even number of parameters (indexName, value):
//
//    Where("age", nabu.GT(10))
//
func (q *Query) Where(indexName string, condition Condition) *Query {
	if indexName == q.sort.Name() {
		q.sortCondition = condition
	} else {
		q.indexNames[q.indexCount] = indexName
		q.conditions[q.indexCount] = condition
		q.indexCount++
	}
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

	// if indexCount == 0 {
	// 	return q.findWithNoIndexes()
	// }
	// if indexCount > 1 && q.cache == true {
	// 	if cached, ok := q.db.cache.Get(q.indexNames[0:indexCount]); ok {
	// 		q.indexCount = 1
	// 		cached.RLock()
	// 		defer cached.RUnlock()
	// 		return q.execute(cached)
	// 	}
	// }

	if q.prepareConditions() == false {
		return EmptyResult
	}
	return q.execute()
}

// Loads the indexes used by the query
func (q *Query) prepareConditions() bool {
	indexCount := q.indexCount
	if q.db.LookupIndexes(q.indexNames[0:indexCount], q.indexes) == false {
		return false
	}
	for i := 0; i < indexCount; i++ {
		q.conditions[i].On(q.indexes[i])
	}
	if indexCount > 1 {
		sort.Sort(q.conditions[0:indexCount])
	}
	return true
}

// Determines wither an index-based query will be used or a sort-based query.
// The choice is based on the type of sort index (whether it can rank documents),
// whether the smallest index fits within the configured maximum unsorted size and,
// whether the smallest index is sufficiently small compared to the sort index.
func (q *Query) execute() Result {
	if q.conditions[0].Len() == 0 {
		return EmptyResult
	}
	return q.findBySort()
	// if q.sortLength > firstLength*10 && firstLength <= q.db.maxUnsortedSize {
	// 	return q.findByIndex(indexes)
	// }
}

// An optimized code path for when no index is provided (just walking through
// a sort index)
func (q *Query) findWithNoIndexes() Result {
	limit := q.limit
	sortLength := q.sortLength
	result := <-q.db.sortedResults
	var iterator indexes.Iterator
	if q.desc {
		iterator = q.sort.Backwards()
	} else {
		iterator = q.sort.Forwards()
	}
	if q.sortCondition != nil {
		iterator.Range(q.sortCondition.Range())
	}
	iterator.Offset(q.offset)

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

// Walk the sort index and filter out results
func (q *Query) findBySort() Result {
	found := 0
	limit := q.limit
	indexCount := q.indexCount
	var iterator indexes.Iterator

	result := <-q.db.sortedResults
	if q.desc {
		iterator = q.sort.Backwards()
	} else {
		iterator = q.sort.Forwards()
	}
	if q.sortCondition != nil {
		iterator.Range(q.sortCondition.Range()).Offset(0)
	}

	indexes := q.indexes[0:indexCount]
	indexes.RLock()
	for id := iterator.Current(); id != key.NULL; id = iterator.Next() {
		for j := 0; j < indexCount; j++ {
			if q.conditions[j].Contains(id) == false {
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
	indexes.RUnlock()
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
	q.ranged = false
	q.includeTotal = false
	q.sortCondition = nil
	q.limit = q.db.defaultLimit
	q.upto = q.db.defaultLimit + 1
	q.db.queryPool <- q
}
