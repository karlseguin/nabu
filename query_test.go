package nabu

import (
  "testing"
  "strconv"
  "math/rand"
  "github.com/karlseguin/gspec"
)

func TestQueryCapsTheLimit(t *testing.T) {
  spec := gspec.New(t)
  db := SmallDB()
  query := <- db.queryPool
  spec.Expect(query.Limit(200).limit).ToEqual(100)
}

func TestQueryPanicsOnUnknownSort(t *testing.T) {
  spec := gspec.New(t)
  defer func() {
    spec.Expect(recover().(string)).ToEqual(`unknown sort index "cats"`)
  }()
  db := SmallDB()
  db.Query("cats")
}

func TestQueryPanicsOnUnknownIndex(t *testing.T) {
  spec := gspec.New(t)
  defer func() {
    spec.Expect(recover().(string)).ToEqual(`unknown index "abc"`)
  }()
  db := SmallDB()
  db.Query("created").Index("abc")
}

func TestQueryPanicsOnUnknownIndexes(t *testing.T) {
  spec := gspec.New(t)
  defer func() {
    spec.Expect(recover().(string)).ToEqual(`unknown index "fail"`)
  }()
  db := SmallDB()
  db.Query("created").Indexes("age", "fail")
}

func TestQueryExecuteReleasesTheQuery(t *testing.T) {
  spec := gspec.New(t)
  db := SmallDB()
  query := db.Query("created").Indexes("age")
  spec.Expect(len(db.queryPool)).ToEqual(0)
  query.Execute()
  spec.Expect(len(db.queryPool)).ToEqual(1)
}

func TestQueryReleaseCanSafelyBeReused(t *testing.T) {
  spec := gspec.New(t)
  db := SmallDB()
  query := db.Query("created").Indexes("age").Desc().Limit(2)
  query.Execute()
  spec.Expect(query.desc).ToEqual(false)
  spec.Expect(query.limit).ToEqual(10)
  spec.Expect(query.offset).ToEqual(0)
  spec.Expect(query.indexCount).ToEqual(0)
}

func TestQueryWithNoIndexes(t *testing.T) {
  db := New(Configure())
  db.AddSort("created", []string{"a", "b", "c", "d", "i", "j", "k"})
  result := db.Query("created").Execute()
  defer result.Close()
  assertResult(t, result.Data(), []string{"a", "b", "c", "d", "i", "j", "k"})
}

func TestQueryWithNoIndexesDescending(t *testing.T) {
  db := New(Configure())
  db.AddSort("created", []string{"a", "b", "c", "d", "i", "j", "k"})
  result := db.Query("created").Desc().Execute()
  defer result.Close()
  assertResult(t, result.Data(), []string{"k", "j", "i", "d", "c", "b", "a"})
}

func TestQueryWithASingleIndexBySort(t *testing.T) {
  db := New(Configure())
  db.AddSort("created", []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"})
  db.AddIndex("a", makeIndex([]string{"b", "c", "f", "h", "z"}))
  result := db.Query("created").Index("a").Execute()
  defer result.Close()
  assertResult(t, result.Data(), []string{"b", "c", "f", "h"})
}

func TestQueryWithTwoIndexesBySort(t *testing.T) {
  db := New(Configure())
  db.AddSort("created", []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"})
  db.AddIndex("a", makeIndex([]string{"b", "c", "f", "h", "g", "k", "z"}))
  db.AddIndex("b", makeIndex([]string{"a", "c", "e", "h","k", "j", "z"}))
  result := db.Query("created").Indexes("a", "b").Execute()
  defer result.Close()
  assertResult(t, result.Data(), []string{"c", "h", "k"})
}

func TestQueryBySortDescending(t *testing.T) {
  db := New(Configure())
  db.AddSort("created", []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"})
  db.AddIndex("a", makeIndex([]string{"b", "c", "f", "h", "g", "k", "z"}))
  db.AddIndex("b", makeIndex([]string{"a", "c", "e", "h","k", "j", "z"}))
  result := db.Query("created").Indexes("a", "b").Desc().Execute()
  defer result.Close()
  assertResult(t, result.Data(), []string{"k", "h", "c"})
}

func TestQueryWithASingleIndexByIndex(t *testing.T) {
  db := New(Configure())
  db.AddSort("created", largeSort(1000))
  db.AddIndex("a", makeIndex([]string{"1", "4", "7", "-1"}))
  result := db.Query("created").Index("a").Execute()
  defer result.Close()
  assertResult(t, result.Data(), []string{"1", "4", "7"})
}

func TestQueryWithTwoIndexesByIndex(t *testing.T) {
   db := New(Configure())
  db.AddSort("created", largeSort(1000))
  db.AddIndex("a", makeIndex([]string{"2", "3", "6", "7", "8", "9", "-1"}))
  db.AddIndex("b", makeIndex([]string{"1", "3", "5", "7","9", "10", "-1"}))
  result := db.Query("created").Indexes("a", "b").Execute()
  defer result.Close()
  assertResult(t, result.Data(), []string{"3", "7", "9"})
}

func TestQueryByIndexDescending(t *testing.T) {
  db := New(Configure())
  db.AddSort("created", largeSort(1000))
  db.AddIndex("a", makeIndex([]string{"2", "3", "6", "7", "8", "9", "-1"}))
  db.AddIndex("b", makeIndex([]string{"1", "3", "5", "7","9", "10", "-1"}))
  result := db.Query("created").Indexes("a", "b").Desc().Execute()
  defer result.Close()
  assertResult(t, result.Data(), []string{"9", "7", "3"})
}

func BenchmarkFindLarge(b *testing.B) {
  db := setupDb(100000, 80000, 100000)
  b.ResetTimer()
  for i := 0; i < b.N; i++ {
    db.Query("created").Index("index_0").Execute().Close()
  }
}

func BenchmarkFindAverage(b *testing.B) {
  db := setupDb(100000, 50000, 100000, 1000, 100000)
  b.ResetTimer()
  for i := 0; i < b.N; i++ {
    db.Query("created").Indexes("index_0", "index_2").Execute().Close()
  }
}

func BenchmarkFindSmall(b *testing.B) {
  db := setupDb(100000, 75000, 100000, 75000, 100000, 100, 100000)
  b.ResetTimer()
  for i := 0; i < b.N; i++ {
    db.Query("created").Indexes("index_0", "index_2", "index_4").Execute().Close()
  }
}

func makeIndex(values []string) map[string]struct{} {
  index := make(Index)
  for _, v := range values {
    index[v] = struct{}{}
  }
  return index
}

func assertResult(t *testing.T, actual []string, expected []string) {
  if len(actual) != len(expected) {
    t.Errorf("expected %d results, got %d", len(expected), len(actual))
  }
  for i := 0; i < len(actual); i++ {
    if expected[i] != actual[i] {
      t.Errorf("expected value %d to be %v, got %v", i, expected[i], actual[i])
    }
  }
}

func setupDb(sortLength int, params ...int) *Database {
  db := New(Configure())
  sort := make([]string, sortLength)
  for i := 0; i < sortLength; i++ {
    sort[i] = strconv.Itoa(i)
  }
  db.AddSort("created", sort)

  for i := 0; i < len(params); i += 2 {
    length := params[i]
    maxvalue := int32(params[i+1])
    index := make(Index, length)
    for j := 0; j < length; j++ {
      value := strconv.Itoa(int(rand.Int31n(maxvalue)))
      index[value] = struct{}{}
    }
    db.AddIndex("index_" + strconv.Itoa(i), index)
  }
  return db
}

func largeSort(size int) []string {
  s := make([]string, size)
  for i := 0; i < size; i++ {
    s[i] = strconv.Itoa(i)
  }
  return s
}

func SmallDB() *Database {
  c := Configure().QueryPoolSize(1).ResultsPoolSize(1, 1)
  db := New(c)
  db.AddSort("created", []string{})
  db.AddIndex("age", make(Index, 0))
  return db
}

