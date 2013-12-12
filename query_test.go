package nabu

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"math/rand"
	"strconv"
	"testing"
)

func TestQueryCapsTheLimit(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	query := <-db.queryPool
	spec.Expect(query.Limit(200).limit).ToEqual(100)
}

func TestQueryPanicsOnUnknownSort(t *testing.T) {
	spec := gspec.New(t)
	defer func() {
		spec.Expect(recover().(string)).ToEqual(`unknown sort index "cats"`)
	}()
	db := SmallDB()
	defer db.Close()
	db.Query("cats")
}

func TestQueryEmptyIndex(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	res := db.Query("created").NoCache().Where("abc", "zzz").Execute()
	defer res.Close()
	spec.Expect(res.Len()).ToEqual(0)
}

func TestQueryExecuteReleasesTheQuery(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	query := db.Query("created").NoCache().Where("age", "29")
	spec.Expect(len(db.queryPool)).ToEqual(0)
	query.Execute()
	spec.Expect(len(db.queryPool)).ToEqual(1)
}

func TestQueryReleaseCanSafelyBeReused(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	query := db.RangeQuery("created", 10, 100).NoCache().Where("age", "29").Desc().IncludeTotal().NoCache().Limit(2)
	query.Execute()
	spec.Expect(query.desc).ToEqual(false)
	spec.Expect(query.limit).ToEqual(10)
	spec.Expect(query.offset).ToEqual(0)
	spec.Expect(query.cache).ToEqual(true)
	spec.Expect(query.indexCount).ToEqual(0)
	spec.Expect(query.includeTotal).ToEqual(false)
	spec.Expect(query.ranged).ToEqual(false)
}

// NO INDEXES
func TestQueryWithNoIndexes(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7}, false)
	result := db.Query("created").Execute()
	defer result.Close()
	assertResult(t, result.Ids(), 1, 2, 3, 4, 5, 6, 7)
}

func TestQueryWithNoIndexesDescending(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7}, false)
	result := db.Query("created").Desc().Execute()
	defer result.Close()
	assertResult(t, result.Ids(), 7, 6, 5, 4, 3, 2, 1)
}

func TestQueryWithNoIndexWithOffset(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7}, false)
	result := db.Query("created").Offset(2).Execute()
	defer result.Close()
	assertResult(t, result.Ids(), 3, 4, 5, 6, 7)
}

func TestQueryWithNoIndexesUsingDescendingWithOffset(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7}, false)
	result := db.Query("created").Desc().Offset(3).Execute()
	defer result.Close()
	assertResult(t, result.Ids(), 4, 3, 2, 1)
}

func TestQueryWithNoIndexesProperlyCalculatesThatItHasNoMore(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7}, false)
	result := db.Query("created").Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryWithNoIndexesProperlyCalculatesThatIsHasMore(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7}, false)
	result := db.Query("created").Limit(2).Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(true)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryWithNoIndexesProperlyCalculatesHasNoMoreDuetoOffset(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7}, false)
	result := db.Query("created").Limit(3).Offset(5).Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryWithNoIndexesProperlyCalculatesThatItHasNoMoreDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7}, false)
	result := db.Query("created").Desc().Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryWithNoIndexesProperlyCalculatesThatIsHasMoreDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7}, false)
	result := db.Query("created").Limit(2).Desc().Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(true)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryWithNoIndexesProperlyCalculatesHasNoMoreDuetoOffsetDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7}, false)
	result := db.Query("created").Limit(3).Offset(5).Desc().Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryWithNoIndexesIncludesTotalCount(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7}, false)
	result := db.Query("created").Limit(3).Offset(5).Desc().IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(7)
}

func TestQueryWithNoIndexesLimitsTheTotalCount(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig().CacheWorkers(0).MaxTotal(4))
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7}, false)
	result := db.Query("created").Limit(2).IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(4)
}

func TestQueryWithNoIndexesLimitsTheTotalCountDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig().CacheWorkers(0).MaxTotal(4))
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7}, false)
	result := db.Query("created").Desc().Limit(2).Desc().IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(4)
}

// SORT-BASED QUERY
func TestQueryWithASingleIndexBySort(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 8, 100}))
	result := db.Query("created").NoCache().Where("a", "1").Execute()
	defer result.Close()
	assertResult(t, result.Ids(), 2, 3, 6, 8)
}

func TestQueryWithTwoIndexesBySort(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 8, 7, 11, 100}))
	addIndex(db, "b$2", makeIndex([]key.Type{1, 3, 5, 8, 11, 10, 100}))
	result := db.Query("created").NoCache().Where("a", "1", "b", "2").Execute()
	defer result.Close()
	assertResult(t, result.Ids(), 3, 8, 11)
}

func TestQueryBySortDescending(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 8, 7, 11, 100}))
	addIndex(db, "b$3", makeIndex([]key.Type{1, 3, 5, 8, 11, 10, 100}))
	result := db.Query("created").NoCache().Where("a", "1", "b", "3").Desc().Execute()
	defer result.Close()
	assertResult(t, result.Ids(), 11, 8, 3)
}

func TestQueryWithTwoIndexesBySortWithOffset(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 8, 7, 11, 100}))
	addIndex(db, "b$2", makeIndex([]key.Type{1, 3, 5, 8, 11, 10, 100}))
	result := db.Query("created").NoCache().Where("a", "1", "b", "2").Offset(1).Execute()
	defer result.Close()
	assertResult(t, result.Ids(), 8, 11)
}

func TestQueryBySortDescendingWithOffset(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 8, 7, 11, 100}))
	addIndex(db, "b$3", makeIndex([]key.Type{1, 3, 5, 8, 11, 10, 100}))
	result := db.Query("created").NoCache().Where("a", "1", "b", "3").Desc().Offset(1).Execute()
	defer result.Close()
	assertResult(t, result.Ids(), 8, 3)
}

func TestQueryBySortProperlyCalculatesThatItHasNoMore(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 8, 7, 11, 100}))
	result := db.Query("created").NoCache().Where("a", "1").Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryBySortProperlyCalculatesThatIsHasMore(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 8, 7, 11, 100}))
	result := db.Query("created").NoCache().Where("a", "1").Limit(2).Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(true)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryBySortProperlyCalculatesHasNoMoreDuetoOffset(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 8, 7, 11, 100}))
	result := db.Query("created").NoCache().Where("a", "1").Limit(2).Offset(5).Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryBySortIncludesTotal(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 8, 7, 11, 100}))
	result := db.Query("created").NoCache().Where("a", "1").Limit(1).Offset(3).IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(6)
}

func TestQueryBySortProperlyCalculatesThatItHasNoMoreDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 8, 7, 11, 100}))
	result := db.Query("created").NoCache().Where("a", "1").Desc().Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryBySortProperlyCalculatesThatIsHasMoreDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 8, 7, 11, 100}))
	result := db.Query("created").NoCache().Where("a", "1").Limit(2).Desc().Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(true)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryBySortProperlyCalculatesHasNoMoreDuetoOffsetDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 8, 7, 11, 100}))
	result := db.Query("created").NoCache().Where("a", "1").Limit(2).Offset(5).Desc().Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryBySortIncludesTotalDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 8, 7, 11, 100}))
	result := db.Query("created").NoCache().Where("a", "1").Limit(1).Offset(3).Desc().IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(6)
}

func TestQueryBySortLimitsTheTotal(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig().CacheWorkers(0).MaxTotal(3))
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 8, 7, 11, 100}))
	result := db.Query("created").NoCache().Where("a", "1").Limit(2).IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(3)
}

func TestQueryBySortLimitsTheTotalDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig().CacheWorkers(0).MaxTotal(3))
	db.Close()
	db.LoadSort("created", []key.Type{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 8, 7, 11, 100}))
	result := db.Query("created").NoCache().Where("a", "1").Limit(2).Desc().IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(3)
}

// INDEX-BASED QUERY
func TestQueryWithASingleIndexByIndex(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", largeSort(1000), false)
	addIndex(db, "a$66", makeIndex([]key.Type{1, 4, 7}))
	result := db.Query("created").NoCache().Where("a", "66").Execute()
	defer result.Close()
	assertResult(t, result.Ids(), 1, 4, 7)
}

func TestQueryWithTwoIndexesByIndex(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", largeSort(1000), false)
	addIndex(db, "a$5", makeIndex([]key.Type{2, 3, 6, 7, 8, 9, 100}))
	addIndex(db, "b$4", makeIndex([]key.Type{1, 3, 5, 7, 9, 10, 101}))
	result := db.Query("created").NoCache().Where("a", "5", "b", "4").Execute()
	defer result.Close()
	assertResult(t, result.Ids(), 3, 7, 9)
}

func TestQueryByIndexDescending(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", largeSort(1000), false)
	addIndex(db, "a$x", makeIndex([]key.Type{2, 3, 6, 7, 8, 9, 101}))
	addIndex(db, "b$y", makeIndex([]key.Type{1, 3, 5, 7, 9, 10, 102}))
	result := db.Query("created").NoCache().Where("a", "x", "b", "y").Desc().Execute()
	defer result.Close()
	assertResult(t, result.Ids(), 9, 7, 3)
}

func TestQueryWithTwoIndexesByIndexWithOffset(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", largeSort(1000), false)
	addIndex(db, "a$5", makeIndex([]key.Type{2, 3, 6, 7, 8, 9, 101}))
	addIndex(db, "b$4", makeIndex([]key.Type{1, 3, 5, 7, 9, 10}))
	result := db.Query("created").NoCache().Where("a", "5", "b", "4").Offset(1).Execute()
	defer result.Close()
	assertResult(t, result.Ids(), 7, 9)
}

func TestQueryByIndexDescendingWithOffset(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", largeSort(1000), false)
	addIndex(db, "a$x", makeIndex([]key.Type{2, 3, 6, 7, 8, 9}))
	addIndex(db, "b$y", makeIndex([]key.Type{1, 3, 5, 7, 9, 10, 101}))
	result := db.Query("created").NoCache().Where("a", "x", "b", "y").Offset(2).Desc().Execute()
	defer result.Close()
	assertResult(t, result.Ids(), 3)
}

func TestQueryByIndexProperlyCalculatesThatItHasNoMore(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", largeSort(1000), false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 7, 8, 9, 101}))
	result := db.Query("created").NoCache().Where("a", "1").Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryByIndexProperlyCalculatesThatIsHasMore(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", largeSort(1000), false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 7, 8, 9, 101}))
	result := db.Query("created").NoCache().Where("a", "1").Limit(2).Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(true)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryByIndexProperlyCalculatesHasNoMoreDuetoOffset(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", largeSort(1000), false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 7, 8, 9, 101}))
	result := db.Query("created").NoCache().Where("a", "1").Limit(2).Offset(5).Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryByIndexIncludesTotal(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", largeSort(1000), false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 7, 8, 9, 1001}))
	result := db.Query("created").NoCache().Where("a", "1").Limit(1).Offset(3).IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(6)
}

func TestQueryByIndexProperlyCalculatesThatItHasNoMoreDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", largeSort(1000), false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 7, 8, 9, 1001}))
	result := db.Query("created").NoCache().Where("a", "1").Desc().Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryByIndexProperlyCalculatesThatIsHasMoreDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", largeSort(1000), false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 7, 8, 9, 1001}))
	result := db.Query("created").NoCache().Where("a", "1").Limit(2).Desc().Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(true)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryByIndexProperlyCalculatesHasNoMoreDuetoOffsetDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", largeSort(1000), false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 7, 8, 9, 1001}))
	result := db.Query("created").NoCache().Where("a", "1").Limit(2).Offset(5).Desc().Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryByIndexIncludesTotalDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	db.LoadSort("created", largeSort(1000), false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 7, 8, 9, 1001}))
	result := db.Query("created").NoCache().Where("a", "1").Limit(1).Offset(3).Desc().IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(6)
}

func TestQueryByIndexLimitsTheTotal(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig().CacheWorkers(0).MaxTotal(3))
	db.Close()
	db.LoadSort("created", largeSort(1000), false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 7, 8, 9, 1001}))
	result := db.Query("created").NoCache().Where("a", "1").Limit(2).IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(3)
}

func TestQueryByIndexLimitsTheTotalDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig().CacheWorkers(0).MaxTotal(3))
	db.Close()
	db.LoadSort("created", largeSort(1000), false)
	addIndex(db, "a$1", makeIndex([]key.Type{2, 3, 6, 7, 8, 9, 1001}))
	result := db.Query("created").NoCache().Where("a", "1").Limit(2).Desc().IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(3)
}

func BenchmarkFindLargeWithNoTotal(b *testing.B) {
	db := setupDb(Configure(nil).SkipLoad(), 100000, 80000, 100000)
	db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Query("created").Where("index_0", "_").Execute().Close()
	}
}

func BenchmarkFindLargeWithTotal(b *testing.B) {
	db := setupDb(Configure(nil).SkipLoad(), 100000, 80000, 100000)
	db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Query("created").Where("index_0", "_").IncludeTotal().Execute().Close()
	}
}

func BenchmarkFindAverageWithNoTotal(b *testing.B) {
	db := setupDb(Configure(nil).SkipLoad(), 100000, 50000, 100000, 1000, 100000)
	db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Query("created").Where("index_0", "_", "index_2", "_").Execute().Close()
	}
}

func BenchmarkFindAverageWithTotalUnderSortThreshold(b *testing.B) {
	db := setupDb(Configure(nil).SkipLoad(), 100000, 50000, 100000, 4000, 100000)
	db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Query("created").Where("index_0", "_", "index_2", "_").IncludeTotal().Execute().Close()
	}
}

func BenchmarkFindAverageWithTotalOverSortThreshold(b *testing.B) {
	db := setupDb(Configure(nil).SkipLoad(), 100000, 50000, 100000, 6000, 100000)
	db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Query("created").Where("index_0", "_", "index_2", "_").IncludeTotal().Execute().Close()
	}
}

func BenchmarkFindSmallWithNoTotal(b *testing.B) {
	db := setupDb(Configure(nil).SkipLoad(), 100000, 75000, 100000, 75000, 100000, 100, 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Query("created").Where("index_0", "_", "index_2", "_", "index_4", "_").Execute().Close()
	}
}

func BenchmarkFindSmallWithTotal(b *testing.B) {
	db := setupDb(Configure(nil).SkipLoad(), 100000, 75000, 100000, 75000, 100000, 100, 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Query("created").Where("index_0", "_", "index_2", "_", "index_4", "_").IncludeTotal().Execute().Close()
	}
}

func makeIndex(values []key.Type) *indexes.Index {
	index := indexes.New("_")
	for _, v := range values {
		index.Ids[v] = struct{}{}
	}
	return index
}

func assertResult(t *testing.T, actual []uint, expected ...uint) {
	if len(actual) != len(expected) {
		t.Errorf("expected %d results, got %d", len(expected), len(actual))
	}
	for i := 0; i < len(actual); i++ {
		if expected[i] != actual[i] {
			t.Errorf("expected value %d to be %v, got %v", i, expected[i], actual[i])
		}
	}
}

func setupDb(config *Configuration, sortLength int, params ...int) *Database {
	db := New(config)
	sort := make([]key.Type, sortLength)
	for i := 0; i < sortLength; i++ {
		sort[i] = key.Type(i)
	}
	db.LoadSort("created", sort, false)

	for i := 0; i < len(params); i += 2 {
		length := params[i]
		maxvalue := int32(params[i+1])
		name := "index_" + strconv.Itoa(i) + "$_"
		index := indexes.New(name)
		for j := 0; j < length; j++ {
			index.Ids[key.Type(rand.Int31n(maxvalue))] = struct{}{}
		}
		addIndex(db, name, index)
	}
	return db
}

func largeSort(size int) []key.Type {
	s := make([]key.Type, size)
	for i := 0; i < size; i++ {
		s[i] = key.Type(i)
	}
	return s
}
