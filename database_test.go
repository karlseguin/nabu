package nabu

import (
  "testing"
  "nabu/key"
  "nabu/indexes"
  "github.com/karlseguin/gspec"
)

// this is a very broad test, tsk tsk
func TestDatabaseIsInitializedBasedOnConfiguration(t *testing.T) {
  spec := gspec.New(t)
  conf := Configure().QueryPoolSize(2).DefaultLimit(3).MaxLimit(4).
            MaxUnsortedSize(5).ResultsPoolSize(6, 7).BucketCount(8)
  db := New(conf)
  defer db.Close()
  db.LoadSort("x", []key.Type{})
  spec.Expect(len(db.queryPool)).ToEqual(2)
  spec.Expect(len(db.sortedResults)).ToEqual(6)
  spec.Expect(len(db.unsortedResults)).ToEqual(7)
  spec.Expect(len(db.buckets)).ToEqual(8)
  for i := 0; i < 8; i++ { spec.Expect(db.buckets[i]).ToNotBeNil() }

  query := db.Query("x")
  spec.Expect(query.limit).ToEqual(3)
  query.Limit(23)
  spec.Expect(query.limit).ToEqual(4)
}

func TestInsertANewDocument(t *testing.T) {
  spec := gspec.New(t)
  db := SmallDB()
  defer db.Close()
  db.Update(NewDoc("1134d", []string{"index", "1", "age", "17"}, map[string]int{"trending": 1, "age":3}))
  doc := db.Get("1134d").(*Doc)
  spec.Expect(doc.id).ToEqual("1134d")
  spec.Expect(db.indexes["index$1"].Contains("1134d")).ToEqual(true)
  spec.Expect(db.indexes["age$17"].Contains("1134d")).ToEqual(true)
  spec.Expect(db.sorts["trending"].Rank("1134d")).ToEqual(1)
  spec.Expect(db.sorts["age"].Rank("1134d")).ToEqual(3)
}

func TestUpdatesADocument(t *testing.T) {
  spec := gspec.New(t)
  db := SmallDB()
  defer db.Close()
  db.Update(NewDoc("94", []string{"index", "1", "age", "18"}, map[string]int{"trending": 1, "age":3}))
  db.Update(NewDoc("94", []string{"index", "1", "index", "3"}, map[string]int{"trending": 10, "age":2}))
  doc := db.Get("94").(*Doc)
  spec.Expect(doc.id).ToEqual("94")
  spec.Expect(db.indexes["index$1"].Contains("94")).ToEqual(true)
  spec.Expect(db.indexes["age$18"].Contains("94")).ToEqual(false)
  spec.Expect(db.indexes["index$3"].Contains("94")).ToEqual(true)
  spec.Expect(db.sorts["trending"].Rank("94")).ToEqual(10)
  spec.Expect(db.sorts["age"].Rank("94")).ToEqual(2)
}

func TestRemovesADocument(t *testing.T) {
  spec := gspec.New(t)
  db := SmallDB()
  defer db.Close()
  doc := NewDoc("94", []string{"index", "1", "age", "22"}, map[string]int{"trending": 10, "age":2})
  db.Update(doc)
  db.Remove(doc)
  spec.Expect(db.Get("94")).ToBeNil()
  spec.Expect(db.indexes["index$1"].Contains("94")).ToEqual(false)
  spec.Expect(db.indexes["age$22"].Contains("94")).ToEqual(false)
  spec.Expect(db.sorts["trending"].Rank("94")).ToEqual(0)
  spec.Expect(db.sorts["age"].Rank("94")).ToEqual(0)
}

func TestRemovesADocumentById(t *testing.T) {
  spec := gspec.New(t)
  db := SmallDB()
  defer db.Close()
  doc := NewDoc("87", []string{"index", "1", "age", "9"}, map[string]int{"trending": 10, "age":2})
  db.Update(doc)
  db.RemoveById("87")
  spec.Expect(db.Get("87")).ToBeNil()
  spec.Expect(db.indexes["index$1"].Contains("87")).ToEqual(false)
  spec.Expect(db.indexes["age$9"].Contains("87")).ToEqual(false)
  spec.Expect(db.sorts["trending"].Rank("94")).ToEqual(0)
  spec.Expect(db.sorts["age"].Rank("94")).ToEqual(0)
}

type Doc struct {
  id string
  indexes []string
  sorts map[string]int
}

func NewDoc(id string, indexes []string, sorts map[string]int) *Doc {
  return &Doc {
    id: id,
    sorts: sorts,
    indexes: indexes,
  }
}

func (d *Doc) ReadMeta(meta *Meta) {
  meta.Id(key.Type(d.id))
  for i := 0; i < len(d.indexes); i+=2 {
    meta.Index(d.indexes[i], d.indexes[i+1])
  }
  for sortName, rank := range d.sorts {
    meta.Sort(sortName, rank)
  }
}

func SmallDB() *Database {
  db := New(SmallConfig())
  defer db.Close()
  db.LoadSort("created", []key.Type{})
  addIndex(db, "age$29", indexes.New("age$29"))
  return db
}

func SmallConfig() *Configuration {
  return Configure().QueryPoolSize(1).ResultsPoolSize(1, 1).CacheWorkers(0)
}

func addIndex(db *Database, name string, index *indexes.Index) {
  index.Name = name
  db.indexes[name] = index
}
