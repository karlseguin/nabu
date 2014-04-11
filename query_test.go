package nabu

import (
	"github.com/karlseguin/gspec"
	"testing"
)

func TestQueryCapsTheLimit(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	query := <-db.queryPool
	spec.Expect(query.Limit(200).(*NormalQuery).limit).ToEqual(100)
}

func TestEmptyQueryOnInvalidIndex(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	_, ok := db.Query("cats").(*EmptyQuery)
	spec.Expect(ok).ToEqual(true)
}

func TestQueryEmptyIndex(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	res := db.Query("created").NoCache().Where(GT("abc", 0)).Execute()
	defer res.Close()
	spec.Expect(res.Len()).ToEqual(0)
}

func TestQueryExecuteReleasesTheQuery(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	query := db.Query("created").NoCache().Where(GT("age", 29))
	spec.Expect(len(db.queryPool)).ToEqual(0)
	query.Execute()
	spec.Expect(len(db.queryPool)).ToEqual(1)
}

// NO INDEXES
func TestQueryWithNoIndexes(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7)
	result := db.Query("created").Execute()
	assertResult(t, result, 1, 2, 3, 4, 5, 6, 7)
}

func TestQueryWithNoIndexesDescending(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7)
	result := db.Query("created").Desc().Execute()
	assertResult(t, result, 7, 6, 5, 4, 3, 2, 1)
}

func TestQueryWithNoIndexesDescendingWithFilter(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7)
	result := db.Query("created").Where(GT("created", 4)).Desc().Execute()
	assertResult(t, result, 7, 6, 5)
}

func TestQueryWithNoIndexWithOffset(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7)
	result := db.Query("created").Offset(2).Execute()
	assertResult(t, result, 3, 4, 5, 6, 7)
}

func TestQueryWithNoIndexesUsingDescendingWithOffset(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7)
	result := db.Query("created").Desc().Offset(3).Execute()
	assertResult(t, result, 4, 3, 2, 1)
}

func TestQueryWithNoIndexesProperlyCalculatesThatItHasNoMore(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7)
	result := db.Query("created").Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryWithNoIndexesProperlyCalculatesThatIsHasMore(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7)
	result := db.Query("created").Limit(2).Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(true)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryWithNoIndexesProperlyCalculatesHasNoMoreDuetoOffset(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7)
	result := db.Query("created").Limit(3).Offset(5).Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryWithNoIndexesProperlyCalculatesThatItHasNoMoreDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7)
	result := db.Query("created").Desc().Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryWithNoIndexesProperlyCalculatesThatIsHasMoreDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7)
	result := db.Query("created").Limit(2).Desc().Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(true)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryWithNoIndexesProperlyCalculatesHasNoMoreDuetoOffsetDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7)
	result := db.Query("created").Limit(3).Offset(5).Desc().Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryWithNoIndexesIncludesTotalCount(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7)
	result := db.Query("created").Limit(3).Offset(5).Desc().IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(7)
}

func TestQueryWithNoIndexesLimitsTheTotalCount(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig().MaxTotal(4))
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7)
	result := db.Query("created").Limit(2).IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(4)
}

func TestQueryWithNoIndexesFiltersTheTotalCount(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7)
	result := db.Query("created").Where(GT("created", 2)).Limit(2).IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(5)
}

func TestQueryWithNoIndexesWithFilter(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7)
	result := db.Query("created").Where(GT("created", 4)).Execute()
	assertResult(t, result, 5, 6, 7)
}

func TestQueryWithNoIndexesLimitsTheTotalCountDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig().MaxTotal(4))
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7)
	result := db.Query("created").Desc().Limit(2).Desc().IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(4)
}

// SORT-BASED QUERY
func TestQueryWithASingleIndexBySort(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	makeIndex(db, "a", 2, 3, 6, 8, 100)
	result := db.Query("created").NoCache().Where(GT("a", 1)).Execute()
	assertResult(t, result, 2, 3, 6, 8)
}

func TestQueryWithASingleIndexByFilteredSort(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	makeIndex(db, "a", 2, 3, 6, 8, 100)
	result := db.Query("created").NoCache().Where(LT("created", 8)).Where(GT("a", 1)).Execute()
	assertResult(t, result, 2, 3, 6)
}

func TestQueryWithTwoIndexesBySort(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	makeIndex(db, "a", 2, 3, 6, 8, 7, 11, 100)
	makeIndex(db, "b", 1, 3, 5, 8, 11, 10, 100)
	result := db.Query("created").NoCache().Where(GT("a", 1)).Where(GT("b", 2)).Execute()
	assertResult(t, result, 3, 8, 11)
}

func TestQueryBySortDescending(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	makeIndex(db, "a", 2, 3, 6, 8, 7, 11, 100)
	makeIndex(db, "b", 1, 3, 5, 8, 11, 10, 100)
	result := db.Query("created").NoCache().Where(GT("a", 1)).Where(GT("b", 3)).Desc().Execute()
	assertResult(t, result, 11, 8)
}

func TestQuerWithFilterHavingFilteredSortDesc(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	makeIndex(db, "a", 2, 3, 6, 8, 100)
	result := db.Query("created").NoCache().Where(GT("created", 2)).Where(GT("a", 1)).Desc().Execute()
	assertResult(t, result, 8, 6, 3)
}

func TestQueryWithTwoIndexesBySortWithOffset(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	makeIndex(db, "a", 2, 3, 6, 8, 7, 11, 100)
	makeIndex(db, "b", 1, 3, 5, 8, 11, 10, 100)
	result := db.Query("created").NoCache().Where(GT("a", 1)).Where(GT("b", 2)).Offset(1).Execute()
	assertResult(t, result, 8, 11)
}

func TestQueryBySortDescendingWithOffset(t *testing.T) {
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	makeIndex(db, "a", 2, 3, 6, 8, 7, 11, 100)
	makeIndex(db, "b", 1, 3, 5, 8, 11, 10, 100)
	result := db.Query("created").NoCache().Where(GT("a", 1)).Where(GT("b", 0)).Desc().Offset(1).Execute()
	assertResult(t, result, 8, 3)
}

func TestQueryBySortProperlyCalculatesThatItHasNoMore(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	makeIndex(db, "a", 2, 3, 6, 8, 7, 11, 100)
	result := db.Query("created").NoCache().Where(GT("a", 1)).Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryBySortProperlyCalculatesThatIsHasMore(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	makeIndex(db, "a", 2, 3, 6, 8, 7, 11, 100)
	result := db.Query("created").NoCache().Where(GT("a", 1)).Limit(2).Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(true)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryBySortProperlyCalculatesHasNoMoreDuetoOffset(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	makeIndex(db, "a", 2, 3, 6, 8, 7, 11, 100)
	result := db.Query("created").NoCache().Where(GT("a", 1)).Limit(2).Offset(5).Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryBySortIncludesTotal(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	makeIndex(db, "a", 2, 3, 6, 8, 7, 11, 100)
	result := db.Query("created").NoCache().Where(GT("a", 1)).Limit(1).Offset(3).IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(6)
}

func TestQueryBySortProperlyCalculatesThatItHasNoMoreDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	makeIndex(db, "a", 2, 3, 6, 8, 7, 11, 100)
	result := db.Query("created").NoCache().Where(GT("a", 1)).Desc().Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryBySortProperlyCalculatesThatIsHasMoreDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	makeIndex(db, "a", 2, 3, 6, 8, 7, 11, 100)
	result := db.Query("created").NoCache().Where(GT("a", 1)).Limit(2).Desc().Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(true)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryBySortProperlyCalculatesHasNoMoreDuetoOffsetDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	makeIndex(db, "a", 2, 3, 6, 8, 7, 11, 100)
	result := db.Query("created").NoCache().Where(GT("a", 1)).Limit(2).Offset(5).Desc().Execute()
	defer result.Close()
	spec.Expect(result.HasMore()).ToEqual(false)
	spec.Expect(result.Total()).ToEqual(-1)
}

func TestQueryBySortIncludesTotalDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig())
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	makeIndex(db, "a", 2, 3, 6, 8, 7, 11, 100)
	result := db.Query("created").NoCache().Where(GT("a", 1)).Limit(1).Offset(3).Desc().IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(6)
}

func TestQueryBySortLimitsTheTotal(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig().MaxTotal(3))
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	makeIndex(db, "a", 2, 3, 6, 8, 7, 11, 100)
	result := db.Query("created").NoCache().Where(GT("a", 1)).Limit(2).IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(3)
}

func TestQueryBySortLimitsTheTotalDesc(t *testing.T) {
	spec := gspec.New(t)
	db := New(SmallConfig().MaxTotal(3))
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	makeIndex(db, "a", 2, 3, 6, 8, 7, 11, 100)
	result := db.Query("created").NoCache().Where(GT("a", 1)).Limit(2).Desc().IncludeTotal().Execute()
	defer result.Close()
	spec.Expect(result.Total()).ToEqual(3)
}

func TestQueryWithUnion(t *testing.T) {
	db := New(SmallConfig().MaxTotal(3))
	db.Close()
	makeIndex(db, "created", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	makeSet(db, "city=caladan", 1, 2, 3)
	makeSet(db, "city=dune", 2, 3, 4)
	result := db.Query("created").NoCache().Union("city", "caladan", "dune").Execute()
	assertResult(t, result, 1, 2, 3, 4)
}

func assertResult(t *testing.T, result Result, expected ...uint) {
	actual := result.Ids()
	if len(actual) != len(expected) {
		t.Errorf("expected %d results, got %d", len(expected), len(actual))
	}
	for i := 0; i < len(actual); i++ {
		if expected[i] != actual[i] {
			t.Errorf("expected value %d to be %v, got %v", i, expected[i], actual[i])
		}
	}
}

func largeSort(size int) []int {
	s := make([]int, size)
	for i := 0; i < size; i++ {
		s[i] = i
	}
	return s
}
