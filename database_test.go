package nabu

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"testing"
)

// this is a very broad test, tsk tsk
func TestDatabaseIsInitializedBasedOnConfiguration(t *testing.T) {
	spec := gspec.New(t)
	conf := Configure(nil).QueryPoolSize(2).DefaultLimit(3).MaxLimit(4).
		MaxUnsortedSize(5).ResultsPoolSize(6, 7).BucketCount(8).SkipLoad()
	db := New(conf)
	defer db.Close()
	db.LoadSort("x", []key.Type{}, false)
	spec.Expect(len(db.queryPool)).ToEqual(2)
	spec.Expect(len(db.sortedResults)).ToEqual(6)
	spec.Expect(len(db.unsortedResults)).ToEqual(7)
	spec.Expect(len(db.buckets)).ToEqual(8)
	for i := 0; i < 8; i++ {
		spec.Expect(db.buckets[i]).ToNotBeNil()
	}

	query := db.Query("x")
	spec.Expect(query.limit).ToEqual(3)
	query.Limit(23)
	spec.Expect(query.limit).ToEqual(4)
}

func TestInsertANewDocument(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	db.Update(NewDoc(1123, []string{"index", "1", "age", "17"}, map[string]int{"trending": 1, "age": 3}))
	doc := db.Get(1123).(*Doc)
	spec.Expect(doc.id).ToEqual(uint(1123))
	spec.Expect(db.indexes["index$1"].Contains(1123)).ToEqual(true)
	spec.Expect(db.indexes["age$17"].Contains(1123)).ToEqual(true)
	spec.Expect(db.sorts["trending"].GetScore(1123)).ToEqual(1)
	spec.Expect(db.sorts["age"].GetScore(1123)).ToEqual(3)
}

func TestInsertANewDocumentWithAStringId(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	db.Update(NewStringDoc("XX", []string{"index", "1", "age", "17"}, map[string]int{"trending": 1, "age": 3}))
	doc := db.StringGet("XX").(*StringDoc)
	spec.Expect(doc.id).ToEqual("XX")
	spec.Expect(db.indexes["index$1"].Contains(1)).ToEqual(true)
	spec.Expect(db.indexes["age$17"].Contains(1)).ToEqual(true)
	spec.Expect(db.sorts["trending"].GetScore(1)).ToEqual(1)
	spec.Expect(db.sorts["age"].GetScore(1)).ToEqual(3)
}

func TestUpdatesADocument(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	db.Update(NewDoc(94, []string{"index", "1", "age", "18"}, map[string]int{"trending": 1, "age": 3}))
	db.Update(NewDoc(94, []string{"index", "1", "index", "3"}, map[string]int{"trending": 10, "age": 2}))
	doc := db.Get(94).(*Doc)
	spec.Expect(doc.id).ToEqual(uint(94))
	spec.Expect(db.indexes["index$1"].Contains(94)).ToEqual(true)
	spec.Expect(db.indexes["age$18"].Contains(94)).ToEqual(false)
	spec.Expect(db.indexes["index$3"].Contains(94)).ToEqual(true)
	spec.Expect(db.sorts["trending"].GetScore(94)).ToEqual(10)
	spec.Expect(db.sorts["age"].GetScore(94)).ToEqual(2)
}

func TestRemovesADocument(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	doc := NewDoc(94, []string{"index", "1", "age", "22"}, map[string]int{"trending": 10, "age": 2})
	db.Update(doc)
	db.Remove(doc)
	spec.Expect(db.Get(94)).ToBeNil()
	spec.Expect(db.indexes["index$1"].Contains(94)).ToEqual(false)
	spec.Expect(db.indexes["age$22"].Contains(94)).ToEqual(false)
	spec.Expect(db.sorts["trending"].GetScore(94)).ToEqual(0)
	spec.Expect(db.sorts["age"].GetScore(94)).ToEqual(0)
}

func TestRemovesADocumentById(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	doc := NewDoc(87, []string{"index", "1", "age", "9"}, map[string]int{"trending": 10, "age": 2})
	db.Update(doc)
	db.RemoveById(87)
	spec.Expect(db.Get(87)).ToBeNil()
	spec.Expect(db.indexes["index$1"].Contains(87)).ToEqual(false)
	spec.Expect(db.indexes["age$9"].Contains(87)).ToEqual(false)
	spec.Expect(db.sorts["trending"].GetScore(87)).ToEqual(0)
	spec.Expect(db.sorts["age"].GetScore(87)).ToEqual(0)
}

func TestRemovesADocumentByStringId(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	doc := NewStringDoc("111z", []string{"index", "1", "age", "9"}, map[string]int{"trending": 10, "age": 2})
	db.Update(doc)
	db.RemoveByStringId("111z")
	spec.Expect(db.StringGet("111z")).ToBeNil()
	spec.Expect(db.indexes["index$1"].Contains(1)).ToEqual(false)
	spec.Expect(db.indexes["age$9"].Contains(1)).ToEqual(false)
	spec.Expect(db.sorts["trending"].GetScore(1)).ToEqual(0)
	spec.Expect(db.sorts["age"].GetScore(1)).ToEqual(0)
}

type Doc struct {
	id      uint
	indexes []string
	sorts   map[string]int
}

func NewDoc(id uint, indexes []string, sorts map[string]int) *Doc {
	return &Doc{
		id:      id,
		sorts:   sorts,
		indexes: indexes,
	}
}

func (d *Doc) ReadMeta(meta *Meta) {
	meta.Id(d.id)
	for i := 0; i < len(d.indexes); i += 2 {
		meta.Index(d.indexes[i], d.indexes[i+1])
	}
	for sortName, score := range d.sorts {
		meta.Sort(sortName, score)
	}
}

type StringDoc struct {
	id      string
	indexes []string
	sorts   map[string]int
}

func NewStringDoc(id string, indexes []string, sorts map[string]int) *StringDoc {
	return &StringDoc{
		id:      id,
		sorts:   sorts,
		indexes: indexes,
	}
}

func (d *StringDoc) ReadMeta(meta *Meta) {
	meta.StringId(d.id)
	for i := 0; i < len(d.indexes); i += 2 {
		meta.Index(d.indexes[i], d.indexes[i+1])
	}
	for sortName, score := range d.sorts {
		meta.Sort(sortName, score)
	}
}

func SmallDB() *Database {
	db := New(SmallConfig())
	defer db.Close()
	db.LoadSort("created", []key.Type{}, false)
	addIndex(db, indexes.NewIndex("age$29"))
	return db
}

func SmallConfig() *Configuration {
	return Configure(nil).QueryPoolSize(1).ResultsPoolSize(1, 1).CacheWorkers(0).SkipLoad()
}

func addIndex(db *Database, index indexes.Index) {
	db.indexes[index.Name()] = index
}
