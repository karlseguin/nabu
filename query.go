package nabu

import (
	"github.com/karlseguin/nabu/conditions"
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"sort"
)

type Query interface {
	NoCache() Query
	Union(name string, values ...string) Query
	Set(name, value string) Query
	Where(condition Condition) Query
	Desc() Query
	Limit(limit int) Query
	Offset(offset int) Query
	IncludeTotal() Query
	Execute() Result
}

// Build and executes a query against the database
type NormalQuery struct {
	upto           int
	limit          int
	desc           bool
	offset         int
	cache          bool
	db             *Database
	sortLength     int
	conditionCount int
	includeTotal   bool
	sortCondition  RankedCondition
	sort           indexes.Ranked
	dynamicSort    []uint
	ranged         bool
	conditions     Conditions
}

// Queries are statically created upfront and reused
func newQuery(db *Database) Query {
	q := &NormalQuery{
		db:         db,
		cache:      true,
		conditions: make(Conditions, db.maxConditionsPerQuery),
	}
	q.reset()
	return q
}

// Filter on a set.
func (q *NormalQuery) Set(indexName, value string) Query {
	q.addCondition(conditions.NewSet(indexName, value))
	return q
}

// Filter on an a union of set values (tag1 || tag2 || tag3).
func (q *NormalQuery) Union(indexName string, values ...string) Query {
	if len(values) == 1 {
		return q.Set(indexName, values[0])
	}
	q.addCondition(conditions.NewUnion(indexName, values))
	return q
}

// Filter results for the query and value. Where can be called multiple
// times. Each must have an even number of parameters (indexName, value):
//
//    Where(nabu.GT("age", 10))
//
func (q *NormalQuery) Where(condition Condition) Query {
	if q.sort != nil && condition.IndexName() == q.sort.Name() {
		if ranked, ok := condition.(RankedCondition); ok {
			q.sortCondition = ranked
		}
	} else {
		q.addCondition(condition)
	}
	return q
}

func (q *NormalQuery) addCondition(condition Condition) bool {
	if q.conditionCount == q.db.maxConditionsPerQuery {
		return false
	}
	q.conditions[q.conditionCount] = condition
	q.conditionCount++
	return true
}

// Don't cache the result or use the cache to generate the result
// Caches are incrementally updated as changes come in, so this should
// only be used for one-off queries
func (q *NormalQuery) NoCache() Query {
	q.cache = false
	return q
}

// Limit the number of documents returned
func (q *NormalQuery) Limit(limit int) Query {
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
func (q *NormalQuery) Offset(offset int) Query {
	q.offset = offset
	return q
}

// Sort the documents by descending order
func (q *NormalQuery) Desc() Query {
	q.desc = true
	return q
}

// By default, a total count of matching document won't be returned.
// Instead, only the results HasMore will be set. Including the total
// count is less efficient (and unecessary for infinite scrolling or
// for requests beyond the first page). The total will be capped at the
// configured MaxTotal
func (q *NormalQuery) IncludeTotal() Query {
	q.includeTotal = true
	q.upto = q.db.maxTotal
	return q
}

// Executes the query, returning a result. The result must be closed
// once you are done with it
func (q *NormalQuery) Execute() Result {
	defer q.reset()
	conditionCount := q.conditionCount

	if q.dynamicSort != nil {
		q.prepareConditions()
		defer q.conditions[:conditionCount].RUnlock()
		return q.findFromDynamicSort()
	}

	q.sort.RLock()
	if q.sortCondition != nil {
		q.sortCondition.On(q.sort)
		q.sortLength = q.sortCondition.Len()
	} else {
		q.sortLength = q.sort.Len()
	}
	q.sort.RUnlock()

	if conditionCount == 0 {
		return q.findWithNoIndexes()
	}

	q.prepareConditions()
	defer q.conditions[:conditionCount].RUnlock()
	return q.execute()
}

// Loads the indexes used by the query
func (q *NormalQuery) prepareConditions() {
	conditionCount := q.conditionCount
	q.db.LoadIndexes(q.conditions[:conditionCount])
	q.conditions[:conditionCount].RLock()
	if conditionCount > 1 {
		sort.Sort(q.conditions[:conditionCount])
	}
}

// Determines wether an index-based query will be used or a sort-based query.
// The choice is based on the type of sort index (whether it can rank documents),
// whether the smallest index fits within the configured maximum unsorted size and,
// whether the smallest index is sufficiently small compared to the sort index.
func (q *NormalQuery) execute() Result {
	first := q.conditions[0]
	firstLength := first.Len()
	if firstLength == 0 {
		return EmptyResult
	}

	if q.sortLength > firstLength*5 && firstLength <= q.db.maxUnsortedSize && first.CanIterate() {
		return q.findByIndex()
	}
	return q.findBySort()
}

// An optimized code path for when no index is provided (just walking through
// a sort index)
func (q *NormalQuery) findWithNoIndexes() Result {
	limit := q.limit
	result := <-q.db.sortedResults
	result.total = -1
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

	id := iterator.Current()
	for ; id != key.NULL; id = iterator.Next() {
		if result.add(id) == limit {
			break
		}
	}
	result.hasMore = id != key.NULL && iterator.Next() != key.NULL
	iterator.Close()

	if q.includeTotal {
		result.total = q.sortLength
		if result.total > q.upto {
			result.total = q.upto
		}
	}
	return result
}

func (q *NormalQuery) findFromDynamicSort() Result {
	found := 0
	limit := q.limit
	conditionCount := q.conditionCount
	result := <-q.db.sortedResults

	for _, id := range q.dynamicSort {
		keyd := key.Type(id)
		for j := 0; j < conditionCount; j++ {
			if q.conditions[j].Contains(keyd) == false {
				goto nomatchdesc
			}
		}
		result.total++
		if result.total > q.offset {
			if found < limit {
				result.add(keyd)
				found++
			} else if result.total >= q.upto {
				break
			}
		}
	nomatchdesc:
	}
	result.hasMore = result.total > (q.offset + q.limit)
	if q.includeTotal == false {
		result.total = -1
	}
	return result
}

// Walk the sort index and filter out results
func (q *NormalQuery) findBySort() Result {
	found := 0
	limit := q.limit
	conditionCount := q.conditionCount
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

	for id := iterator.Current(); id != key.NULL; id = iterator.Next() {
		for j := 0; j < conditionCount; j++ {
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
	iterator.Close()
	result.hasMore = result.total > (q.offset + q.limit)
	if q.includeTotal == false {
		result.total = -1
	}
	return result
}

// Filter by indexes, then sort. Ideal when the smallest index is quite a bit
// smaller than the sort index
func (q *NormalQuery) findByIndex() Result {
	conditionCount := q.conditionCount
	result := <-q.db.unsortedResults

	var sort RankedContainer
	if q.sortCondition != nil {
		q.sortCondition.On(q.sort)
		sort = q.sortCondition
	} else {
		sort = q.sort
	}

	iterator := q.conditions[0].Iterator()
	defer iterator.Close()

	for id := iterator.Current(); id != key.NULL; id = iterator.Next() {
		for j := 1; j < conditionCount; j++ {
			if q.conditions[j].Contains(id) == false {
				goto nomatch
			}
		}
		if score, exists := sort.Score(id); exists {
			result.add(id, score)
		}
	nomatch:
	}
	return result.finalize(q)
}

// Reset the query and release it back into the pool
func (q *NormalQuery) reset() {
	q.sort = nil
	q.offset = 0
	q.cache = true
	q.desc = false
	q.ranged = false
	q.dynamicSort = nil
	q.conditionCount = 0
	q.sortCondition = nil
	q.includeTotal = false
	q.limit = q.db.defaultLimit
	q.upto = q.db.defaultLimit + 1
	q.db.queryPool <- q
}
