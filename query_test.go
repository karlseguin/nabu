package nabu

import (
  "testing"
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
