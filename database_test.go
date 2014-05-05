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
	conf := Configure().QueryPoolSize(2).DefaultLimit(3).MaxLimit(4).
		MaxUnsortedSize(5).ResultsPoolSize(6, 7).BucketCount(8).SkipLoad()
	db := New(conf)
	defer db.Close()
	makeIndex(db, "x", 0)
	spec.Expect(len(db.queryPool)).ToEqual(2)
	spec.Expect(len(db.sortedResults)).ToEqual(6)
	spec.Expect(len(db.unsortedResults)).ToEqual(7)
	spec.Expect(len(db.buckets)).ToEqual(8)
	for i := 0; i < 8; i++ {
		spec.Expect(db.buckets[i]).ToNotBeNil()
	}

	query := db.Query("x").(*NormalQuery)
	spec.Expect(query.limit).ToEqual(3)
	query.Limit(23)
	spec.Expect(query.limit).ToEqual(4)
}

func TestInsertANewDocument(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	db.Update(NewDoc(1123, map[string]int{"trending": 1, "age": 3}))
	doc := db.Get(1123).(*Doc)
	spec.Expect(doc.id).ToEqual(uint(1123))
	spec.Expect(db.indexes["trending"].Contains(1123)).ToEqual(true)
	spec.Expect(db.indexes["age"].Contains(1123)).ToEqual(true)
}

func TestInsertANewDocumentWithAStringId(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	db.Update(NewStringDoc("XX", map[string]int{"trending": 1, "age": 3}))
	doc := db.StringGet("XX").(*StringDoc)
	spec.Expect(doc.id).ToEqual("XX")
	spec.Expect(db.indexes["trending"].Contains(1)).ToEqual(true)
	spec.Expect(db.indexes["age"].Contains(1)).ToEqual(true)
}

func TestGetMultipleDocuments(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	db.Update(NewStringDoc("XX", map[string]int{"trending": 1, "age": 3}))
	db.Update(NewStringDoc("XY", map[string]int{"trending": 2, "age": 4}))
	documents := db.StringGets([]string{"XX", "XY", "ZZ"})
	spec.Expect(len(documents)).ToEqual(2)
	spec.Expect(documents[0].(*StringDoc).id).ToEqual("XX")
	spec.Expect(documents[1].(*StringDoc).id).ToEqual("XY")
}

func TestUpdatesADocument(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	db.Update(NewDoc(94, map[string]int{"trending": 1, "age": 3}))
	db.Update(NewDoc(94, map[string]int{"trending": 10, "other": 5}))
	doc := db.Get(94).(*Doc)
	spec.Expect(doc.id).ToEqual(uint(94))
	spec.Expect(db.indexes["trending"].Contains(94)).ToEqual(true)
	spec.Expect(db.indexes["age"].Contains(94)).ToEqual(false)
	spec.Expect(db.indexes["other"].Contains(94)).ToEqual(true)
}

func TestRemovesADocument(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	doc := NewDoc(94, map[string]int{"trending": 10, "age": 2})
	db.Update(doc)
	db.Remove(doc)
	spec.Expect(db.Get(94)).ToBeNil()
	spec.Expect(db.indexes["trending"].Contains(94)).ToEqual(false)
	spec.Expect(db.indexes["age"].Contains(94)).ToEqual(false)
}

func TestRemovesADocumentById(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	doc := NewDoc(87, map[string]int{"trending": 10, "age": 2})
	db.Update(doc)
	db.RemoveById(87)
	spec.Expect(db.Get(87)).ToBeNil()
	spec.Expect(db.indexes["trending"].Contains(87)).ToEqual(false)
	spec.Expect(db.indexes["age"].Contains(87)).ToEqual(false)
}

func TestRemovesADocumentByStringId(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	doc := NewStringDoc("111z", map[string]int{"trending": 10, "age": 2})
	db.Update(doc)
	db.RemoveByStringId("111z")
	spec.Expect(db.StringGet("111z")).ToBeNil()
	spec.Expect(db.indexes["trending"].Contains(1)).ToEqual(false)
	spec.Expect(db.indexes["age"].Contains(1)).ToEqual(false)
}

func TestBulkLoadsANewSortedSet(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	db.BulkLoadSortedString("new", []string{"c", "d", "a"})
	spec.Expect(db.indexes["new"].Contains(db.idMap.get("c", false))).ToEqual(true)
	spec.Expect(db.indexes["new"].Contains(db.idMap.get("d", false))).ToEqual(true)
	spec.Expect(db.indexes["new"].Contains(db.idMap.get("a", false))).ToEqual(true)
	spec.Expect(db.indexes["new"].Contains(db.idMap.get("e", false))).ToEqual(false)
}

func TestBulkLoadsAnExistingSortedSet(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	defer db.Close()
	db.BulkLoadSortedString("new", []string{"c", "d", "a"})
	db.BulkLoadSortedString("new", []string{"e", "a", "f"})
	spec.Expect(db.indexes["new"].Contains(db.idMap.get("e", false))).ToEqual(true)
	spec.Expect(db.indexes["new"].Contains(db.idMap.get("a", false))).ToEqual(true)
	spec.Expect(db.indexes["new"].Contains(db.idMap.get("f", false))).ToEqual(true)
	spec.Expect(db.indexes["new"].Contains(db.idMap.get("c", false))).ToEqual(false)
}

type Doc struct {
	id      uint
	indexes map[string]int
}

func NewDoc(id uint, indexes map[string]int) *Doc {
	return &Doc{
		id:      id,
		indexes: indexes,
	}
}

func (d *Doc) ReadMeta(meta *Meta) {
	meta.IntId(d.id)
	for name, score := range d.indexes {
		meta.SortedInt(name, score)
	}
}

func (d *Doc) GetType() string {
	return "doc"
}

type StringDoc struct {
	id      string
	indexes map[string]int
}

func NewStringDoc(id string, indexes map[string]int) *StringDoc {
	return &StringDoc{
		id:      id,
		indexes: indexes,
	}
}

func (d *StringDoc) ReadMeta(meta *Meta) {
	meta.StringId(d.id)
	for name, score := range d.indexes {
		meta.SortedInt(name, score)
	}
}

func (d *StringDoc) GetType() string {
	return "stringdoc"
}

func SmallDB() *Database {
	db := New(SmallConfig())
	defer db.Close()
	makeIndex(db, "created", 0)
	addIndex(db, indexes.NewSortedInts("age"))
	return db
}

func SmallConfig() *Configuration {
	return Configure().QueryPoolSize(1).ResultsPoolSize(1, 1).NoPersistence().SkipLoad()
}

func addIndex(db *Database, index indexes.Index) {
	db.indexes[index.Name()] = index
}

func makeIndex(db *Database, name string, ids ...int) indexes.Index {
	index := indexes.NewSortedInts(name)
	for _, id := range ids {
		index.SetInt(key.Type(id), id)
	}
	if db != nil {
		addIndex(db, index)
	}
	return index
}

func makeSet(db *Database, name string, ids ...int) indexes.Index {
	index := indexes.NewSetString(name)
	for _, id := range ids {
		index.Set(key.Type(id))
	}
	if db != nil {
		addIndex(db, index)
	}
	return index
}
