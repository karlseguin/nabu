package nabu

import (
	"bytes"
	"github.com/karlseguin/nabu/conditions"
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"sort"
)

type Query interface {
	NoCache() Query
	Union(name string, values ...string) Query
	Set(name, value string) Query
	Where(index string, condition Condition) Query
	Desc() Query
	Limit(limit int) Query
	Offset(offset int) Query
	IncludeTotal() Query
	Execute() Result
}

// A query index is a wrapper to an index and a condition. Indexes are long-lived
// but conditions are query-specific. QueryIndex wraps the two so that they can
// be manipulated together (you can sort on indexName, for example, without having
// to worry about also tracking the corresponding condition)

// TODO: Conditions should leverage an identity map so that we aren't creating
// the same ones over and over again
type QueryIndex struct {
	indexName string
	condition Condition
}

type QueryIndexes []*QueryIndex

func (qi QueryIndexes) Len() int {
	return len(qi)
}

func (qi QueryIndexes) Less(a, b int) bool {
	return len(qi[a].indexName) < len(qi[b].indexName)
}

func (qi QueryIndexes) Swap(a, b int) {
	qi[a], qi[b] = qi[b], qi[a]
}

// Build and executes a query against the database
type NormalQuery struct {
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
	queryIndexes  QueryIndexes
	indexes       indexes.Indexes
	idBuffer      *bytes.Buffer
}

// Queries are statically created upfront and reused
func newQuery(db *Database) Query {
	q := &NormalQuery{
		db:           db,
		cache:        true,
		indexes:      make(indexes.Indexes, db.maxIndexesPerQuery),
		queryIndexes: make(QueryIndexes, db.maxIndexesPerQuery),
		idBuffer:     new(bytes.Buffer),
	}
	for i := 0; i < db.maxIndexesPerQuery; i++ {
		q.queryIndexes[i] = new(QueryIndex)
	}
	q.Close()
	return q
}

// Filter on a set.
func (q *NormalQuery) Set(indexName, value string) Query {
	qi := q.queryIndexes[q.indexCount]
	qi.indexName = indexName + "=" + value
	qi.condition = conditions.NewSet(value)
	q.indexCount++
	return q
}

// Filter on an a union of set values (tag1 || tag2 || tag3).
func (q *NormalQuery) Union(indexName string, values ...string) Query {
	condition := conditions.NewUnion(values)
	for _, value := range values {
		qi := q.queryIndexes[q.indexCount]
		qi.indexName = indexName + "=" + value
		qi.condition = condition
		q.indexCount++
	}
	return q
}

// Filter results for the query and value. Where can be called multiple
// times. Each must have an even number of parameters (indexName, value):
//
//    Where("age", nabu.GT(10))
//
func (q *NormalQuery) Where(indexName string, condition Condition) Query {
	if indexName == q.sort.Name() {
		q.sortCondition = condition
	} else {
		qi := q.queryIndexes[q.indexCount]
		qi.indexName = indexName
		qi.condition = condition
		q.indexCount++
	}
	return q
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
//
// It's possible for the cache to claim the query, meaning the cache becomes
// responsible for releasing it back into the pool
func (q *NormalQuery) Execute() Result {
	q.sort.RLock()
	if q.sortCondition != nil {
		q.sortCondition.On(q.sort)
		q.sortLength = q.sortCondition.Len()
	} else {
		q.sortLength = q.sort.Len()
	}
	q.sort.RUnlock()

	indexCount := q.indexCount
	if indexCount == 0 {
		defer q.Close()
		return q.findWithOneIndex(q.sort, false)
	}

	if q.cache == true {
		cached, ok, wait := q.db.cache.Get(q)
		if wait == nil {
			defer q.Close()
		} else {
			defer wait.Done()
		}
		if ok {
			return q.findWithOneIndex(cached, true)
		}
	} else {
		defer q.Close()
	}

	if q.prepareConditions() == false {
		return EmptyResult
	}
	defer q.indexes[0:indexCount].RUnlock()
	return q.execute()
}

// Loads the indexes used by the query
func (q *NormalQuery) prepareConditions() bool {
	indexCount := q.indexCount
	if q.db.LookupIndexes(q.queryIndexes[0:indexCount], q.indexes) == false {
		return false
	}
	q.indexes[0:indexCount].RLock()
	for i := 0; i < indexCount; i++ {
		q.queryIndexes[i].condition.On(q.indexes[i])
	}
	if indexCount > 1 {
		sort.Sort(q.indexes[0:indexCount])
	}
	return true
}

// Determines wether an index-based query will be used or a sort-based query.
// The choice is based on the type of sort index (whether it can rank documents),
// whether the smallest index fits within the configured maximum unsorted size and,
// whether the smallest index is sufficiently small compared to the sort index.
func (q *NormalQuery) execute() Result {
	first := q.queryIndexes[0]
	firstLength := first.condition.Len()
	if firstLength == 0 {
		return EmptyResult
	}

	if q.sortLength > firstLength * 5 && firstLength <= q.db.maxUnsortedSize && first.condition.CanIterate() {
		return q.findByIndex()
	}
	return q.findBySort()
}

// An optimized code path for when no filter is provided. This happens on a
// cache hit, or when iterating through a full set
func (q *NormalQuery) findWithOneIndex(index indexes.Index, presorted bool) Result {
	limit := q.limit
	result := <-q.db.sortedResults
	result.total = -1
	var iterator indexes.Iterator
	if q.desc {
		iterator = index.Backwards()
	} else {
		iterator = index.Forwards()
	}
	if presorted == false && q.sortCondition != nil {
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

// Walk the sort index and filter out results
func (q *NormalQuery) findBySort() Result {
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

	for id := iterator.Current(); id != key.NULL; id = iterator.Next() {
		for j := 0; j < indexCount; j++ {
			if _, exists := q.queryIndexes[j].condition.Contains(id); exists == false {
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
	indexCount := q.indexCount
	result := <-q.db.unsortedResults

	var sort Container
	if q.sortCondition != nil {
		q.sortCondition.On(q.sort)
		sort = q.sortCondition
	} else {
		sort = q.sort
	}

	iterator := q.queryIndexes[0].condition.Iterator()
	defer iterator.Close()

	for id := iterator.Current(); id != key.NULL; id = iterator.Next() {
		for j := 1; j < indexCount; j++ {
			if _, exists := q.queryIndexes[j].condition.Contains(id); exists == false {
				goto nomatch
			}
		}
		if score, exists := sort.Contains(id); exists {
			result.add(id, score)
		}
	nomatch:
	}
	return result.finalize(q)
}

// An id which uniquely represents this query, without considering offset,
// limit and direction. This is used for caching.
func (q *NormalQuery) Id() string {
	indexCount := q.indexCount
	sort.Sort(q.queryIndexes[0:indexCount])

	for i := 0; i < indexCount; i++ {
		qi := q.queryIndexes[i]
		q.idBuffer.WriteString(qi.indexName)
		q.idBuffer.WriteString(qi.condition.Key())
		q.idBuffer.WriteByte('&')
	}
	q.idBuffer.WriteString(q.sort.Name())
	if q.sortCondition != nil {
		q.idBuffer.WriteString(q.sortCondition.Key())
	}
	return q.idBuffer.String()
}

// Reset the query and release it back into the pool
func (q *NormalQuery) Close() {
	q.sort = nil
	q.offset = 0
	q.cache = true
	q.desc = false
	q.indexCount = 0
	q.ranged = false
	q.idBuffer.Truncate(0)
	q.includeTotal = false
	q.sortCondition = nil
	q.limit = q.db.defaultLimit
	q.upto = q.db.defaultLimit + 1
	q.db.queryPool <- q
}
