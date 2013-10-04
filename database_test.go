package nabu

import (
  "testing"
  "github.com/karlseguin/gspec"
)

// this is a very broad test, tsk tsk
func TestDatabaseIsInitializedBasedOnConfiguration(t *testing.T) {
  spec := gspec.New(t)
  conf := Configure().QueryPoolSize(2).DefaultLimit(3).MaxLimit(4).
            MaxUnsortedSize(5).ResultsPoolSize(6, 7).BucketCount(8)
  db := New(conf)
  db.AddSort("x", []string{})
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
  db.Update(NewDoc("1134d", "index1", "age"))
  doc := db.Get("1134d").(*Doc)
  spec.Expect(doc.id).ToEqual("1134d")
  spec.Expect(db.indexes["index1"].Contains("1134d")).ToEqual(true)
  spec.Expect(db.indexes["age"].Contains("1134d")).ToEqual(true)
}

func TestUpdatesADocument(t *testing.T) {
  spec := gspec.New(t)
  db := SmallDB()
  db.Update(NewDoc("94", "index1", "age"))
  db.Update(NewDoc("94", "index1", "index3"))
  doc := db.Get("94").(*Doc)
  spec.Expect(doc.id).ToEqual("94")
  spec.Expect(db.indexes["index1"].Contains("94")).ToEqual(true)
  spec.Expect(db.indexes["age"].Contains("94")).ToEqual(false)
  spec.Expect(db.indexes["index3"].Contains("94")).ToEqual(true)
}

func TestRemovesADocument(t *testing.T) {
  spec := gspec.New(t)
  db := SmallDB()
  doc := NewDoc("94", "index1", "age")
  db.Update(doc)
  db.Remove(doc)
  spec.Expect(db.Get("94")).ToBeNil()
  spec.Expect(db.indexes["index1"].Contains("94")).ToEqual(false)
  spec.Expect(db.indexes["age"].Contains("94")).ToEqual(false)
}

func TestRemovesADocumentById(t *testing.T) {
  spec := gspec.New(t)
  db := SmallDB()
  doc := NewDoc("87", "index1", "age")
  db.Update(doc)
  db.RemoveById("87")
  spec.Expect(db.Get("87")).ToBeNil()
  spec.Expect(db.indexes["index1"].Contains("87")).ToEqual(false)
  spec.Expect(db.indexes["age"].Contains("87")).ToEqual(false)
}

type Doc struct {
  id string
  indexes []string
}

func NewDoc(id string, indexes ...string) *Doc {
  return &Doc{id, indexes}
}

func (d *Doc) ReadMeta(meta *Meta) {
  meta.Id(d.id).Indexes(d.indexes...)
}

func SmallDB() *Database {
  c := Configure().QueryPoolSize(1).ResultsPoolSize(1, 1)
  db := New(c)
  db.AddSort("created", []string{})
  addIndex(db, "age", newIndex())
  return db
}

func addIndex(db *Database, name string, index *Index) {
  db.indexes[name] = index
}
