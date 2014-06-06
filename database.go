// An in-memory set-based document database
package nabu

import (
	"bytes"
	"encoding/json"
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"github.com/karlseguin/nabu/storage"
	"log"
	"sync"
)

/*
Factory is used to recreate documents from the persisted
representations. Data is a []byte array which was encoded using
encoding/json. If you have a single type of document, a simple
factory will suffice:

   func factory(id key.Type, data []byte) nabu.Document {
     product := new(Product)
     if err := json.Unmarshal(data, &tree); err != nil {
       panic(err)
     }
     return product
   }

If you have multiple documents, you'll either use the id or
information within the data itself to determine the type.
(whether or not you can use the id depends on whether your system
allows you to infer the type based on the id).

   func factory(id uint, data []byte) nabu.Document {
     var m map[string]interface{}
     if err := json.Unmarshal(data, &m); err != nil {
       panic(err)
     }
     type := m["type"].(string)
     if type == "Product" {
       ...
     } else if .... {
       ...
     }
   }
*/
type IntFactory func(id uint, t string, data []byte) Document
type StringFactory func(stringId string, id uint, t string, data []byte) Document

var EmptyIndex = indexes.NewEmpty("<__></__>db_empty")

// Database is the primary point of interaction with Nabu
type Database struct {
	loading bool
	*Configuration
	queryPool       chan *NormalQuery
	dStorage        storage.Storage
	mStorage        storage.Storage
	indexLock       sync.RWMutex
	idMap           *IdMap
	sortedResults   chan *SortedResult
	indexes         map[string]indexes.Index
	unsortedResults chan *UnsortedResult
	Buckets         map[int]*Bucket
}

// Creates a new Database instance. Unless configured to SkipLoad, data from
// the storage path will be restored
func New(c *Configuration) *Database {
	db := &Database{
		Configuration:   c,
		indexes:         make(map[string]indexes.Index),
		queryPool:       make(chan *NormalQuery, c.queryPoolSize),
		Buckets:         make(map[int]*Bucket, c.bucketCount),
		sortedResults:   make(chan *SortedResult, c.sortedResultPoolSize),
		unsortedResults: make(chan *UnsortedResult, c.unsortedResultPoolSize),
		idMap:           newIdMap(),
	}
	if c.persist || c.skipLoad == false {
		db.dStorage = storage.New(c.dbPath + "documents")
		db.mStorage = storage.New(c.dbPath + "idmap")
	} else {
		db.dStorage = storage.NullStorage
		db.mStorage = storage.NullStorage
	}

	for i := 0; i < int(c.bucketCount); i++ {
		db.Buckets[i] = &Bucket{Lookup: make(map[key.Type]Document)}
	}
	for i := 0; i < c.queryPoolSize; i++ {
		newQuery(db) //it automatically enqueues itself
	}
	for i := 0; i < c.sortedResultPoolSize; i++ {
		db.sortedResults <- newSortedResult(db)
	}
	for i := 0; i < c.unsortedResultPoolSize; i++ {
		db.unsortedResults <- newUnsortedResult(db)
	}

	if c.skipLoad == false {
		db.restore()
		if c.persist == false {
			db.dStorage.Close()
			db.mStorage.Close()
			db.dStorage = storage.NullStorage
			db.mStorage = storage.NullStorage
		}
	}
	return db
}

// Generate a Query object against the specified sort index
func (d *Database) Query(indexName string) Query {
	d.indexLock.RLock()
	index, exists := d.indexes[indexName].(indexes.Ranked)
	d.indexLock.RUnlock()
	if exists == false {
		return emptyQuery
	}
	q := <-d.queryPool
	q.sort = index
	return q
}

// Generate a DynamicQuery for the specified ids
func (d *Database) DynamicQuery(ids []uint) Query {
	q := <-d.queryPool
	q.dynamicSort = ids
	return q
}

func (d *Database) StringContains(indexName string, id string) bool {
	typed := d.idMap.get(id, false)
	if typed == key.NULL {
		return false
	}
	return d.KeyContains(indexName, typed)
}

func (d *Database) Contains(indexName string, id uint) bool {
	return d.KeyContains(indexName, key.Type(id))
}
func (d *Database) KeyContains(indexName string, id key.Type) bool {
	d.indexLock.RLock()
	index, exists := d.indexes[indexName].(indexes.Index)
	d.indexLock.RUnlock()
	if exists == false {
		return false
	}
	index.RLock()
	defer index.RUnlock()
	return index.Contains(key.Type(id))
}

// Retrieves a document by id
func (d *Database) Get(id uint) Document {
	return d.get(key.Type(id))
}

// Retrieves a document by id
func (d *Database) StringGet(id string) Document {
	typed := d.idMap.get(id, false)
	if typed == key.NULL {
		return nil
	}
	return d.get(typed)
}

// Retrieves an array of documents
func (d *Database) StringGets(ids []string) []Document {
	documents := make([]Document, len(ids))
	index := 0
	for _, id := range ids {
		typed := d.idMap.get(id, false)
		if typed != key.NULL {
			documents[index] = d.get(typed)
			index += 1
		}
	}
	return documents[:index]
}

// Inserts or updates the document
func (d *Database) Update(doc Document) {
	if doc == nil {
		return
	}
	meta := newMeta(d, true)

	doc.ReadMeta(meta)

	id, stringId := meta.getId()
	bucket := d.getBucket(id)
	bucket.Lock()
	old, isUpdate := bucket.Lookup[id]
	bucket.Lookup[id] = doc
	bucket.Unlock()

	oldMeta := newMeta(d, false)
	if isUpdate {
		old.ReadMeta(oldMeta)
	}
	for name, score := range meta.sortedInts {
		delete(oldMeta.sortedInts, name)
		d.getOrCreateSortedIntIndex(name).SetInt(id, score)
	}
	for name, _ := range oldMeta.sortedInts {
		d.safeDelete(name, id)
	}

	for name, score := range meta.sortedStrings {
		delete(oldMeta.sortedStrings, name)
		d.getOrCreateSortedStringIndex(name).SetString(id, score)
	}
	for name, _ := range oldMeta.sortedStrings {
		d.safeDelete(name, id)
	}

	for name, _ := range meta.setStrings {
		delete(oldMeta.setStrings, name)
		d.getOrCreateSetStringIndex(name).Set(id)
	}
	for name, _ := range oldMeta.setStrings {
		d.safeDelete(name, id)
	}

	for name, _ := range meta.bigSetStrings {
		delete(oldMeta.bigSetStrings, name)
		d.getOrCreateBigSetStringIndex(name).Set(id)
	}
	for name, _ := range oldMeta.bigSetStrings {
		d.safeDelete(name, id)
	}

	if d.loading == false && d.persist {
		idBuffer := id.Serialize()
		defer idBuffer.Close()
		d.dStorage.Put(idBuffer.Bytes(), serializeValue(meta.t, doc))
		if len(stringId) != 0 {
			d.mStorage.Put([]byte(stringId), idBuffer.Bytes())
		}
	}
}

// Removes the document. Safe to call even if the document
// does not exists.
func (d *Database) Remove(doc Document) {
	meta := newMeta(d, false)
	doc.ReadMeta(meta)
	id, stringId := meta.getId()
	for name, _ := range meta.sortedInts {
		d.safeDelete(name, id)
	}
	for name, _ := range meta.sortedStrings {
		d.safeDelete(name, id)
	}
	for name, _ := range meta.setStrings {
		d.safeDelete(name, id)
	}
	for name, _ := range meta.bigSetStrings {
		d.safeDelete(name, id)
	}
	bucket := d.getBucket(id)
	bucket.Lock()
	delete(bucket.Lookup, id)
	bucket.Unlock()

	if d.loading == false {
		idBuffer := id.Serialize()
		defer idBuffer.Close()
		d.dStorage.Remove(idBuffer.Bytes())
		if len(stringId) != 0 {
			d.idMap.remove(stringId)
			d.mStorage.Remove([]byte(stringId))
		}
	}
}

// Removes to the document by id. Safe to call even if the
// id doesn't exist
func (d *Database) RemoveById(id uint) {
	d.removeByTypedId(key.Type(id))
}

// Removes to the document by id. Safe to call even if the
// id doesn't exist
func (d *Database) RemoveByStringId(id string) {
	typed := d.idMap.get(id, false)
	if typed != key.NULL {
		d.removeByTypedId(typed)
	}
}

// Closes the database
func (d *Database) Close() error {
	derr := d.dStorage.Close()
	merr := d.mStorage.Close()
	if derr != nil {
		return derr
	}
	return merr
}

func (d *Database) BulkLoadSortedString(name string, ids []string) {
	index, ok := d.getOrCreateSortedStringIndex(name).(*indexes.SortedStrings)
	if ok == false {
		log.Println(name + " could not be bulk loaded")
	}
	keys := make([]key.Type, len(ids))
	for index, id := range ids {
		keys[index] = d.idMap.get(id, true)
	}
	index.BulkLoad(keys)
}

// Removes to the document by id. Safe to call even if the
// id doesn't exist
func (d *Database) removeByTypedId(id key.Type) {
	doc := d.get(id)
	if doc != nil {
		d.Remove(doc)
	}
}

// Gets a document from the given bucket
func (d *Database) get(id key.Type) Document {
	bucket := d.getBucket(id)
	bucket.RLock()
	defer bucket.RUnlock()
	return bucket.Lookup[id]
}

// Gets the document's bucket
func (d *Database) getBucket(key key.Type) *Bucket {
	return d.Buckets[key.Bucket(d.bucketCount)]
}

func (d *Database) getIndex(name string) (indexes.Index, bool) {
	d.indexLock.RLock()
	defer d.indexLock.RUnlock()
	index, exists := d.indexes[name]
	return index, exists
}

// Loads documents and indexes from the storage engine
func (d *Database) restore() {
	d.loading = true

	if d.iFactory != nil {
		iter := d.dStorage.Iterator()
		for iter.Next() {
			id, typedValue := iter.Current()
			t, value := deserializeValue(typedValue)
			d.Update(d.iFactory(key.Deserialize(id), t, value))
		}
		iter.Close()
	} else {
		lookup := make(map[uint]string)
		iter := d.mStorage.Iterator()
		for iter.Next() {
			stringId, id := iter.Current()
			lookup[key.Deserialize(id)] = string(stringId)
		}
		iter.Close()
		if err := iter.Error(); err != nil {
			log.Println(err)
		}
		d.idMap.load(lookup)

		iter = d.dStorage.Iterator()
		for iter.Next() {
			rawId, typedValue := iter.Current()
			id := key.Deserialize(rawId)
			t, value := deserializeValue(typedValue)
			d.Update(d.sFactory(lookup[id], id, t, value))
		}
		iter.Close()
		if err := iter.Error(); err != nil {
			log.Println(err)
		}
	}
	d.loading = false
}

// Callback used to load indexes from index names
func (d *Database) LoadIndexes(conditions Conditions) {
	d.indexLock.RLock()
	defer d.indexLock.RUnlock()
	for _, condition := range conditions {
		if multi, ok := condition.(MultiCondition); ok {
			for _, indexName := range multi.IndexNames() {
				d.associateIndexWithCondition(condition, indexName)
			}
		} else {
			d.associateIndexWithCondition(condition, condition.IndexName())
		}
	}
}

func (d *Database) associateIndexWithCondition(condition Condition, indexName string) {
	if index, exists := d.indexes[indexName]; exists {
		condition.On(index)
	} else {
		condition.On(EmptyIndex)
	}
}

// Serialize type + values to be passed to the storage engine
func serializeValue(t string, value interface{}) []byte {
	serialized, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	bt := []byte(t)
	l := len(bt)
	final := make([]byte, 1+l+len(serialized))
	final[0] = byte(l)
	copy(final[1:], bt)
	copy(final[l+1:], serialized)
	return final
}

// Deserialize type  + value from the storage engine
func deserializeValue(data []byte) (string, []byte) {
	index := bytes.Index(data, []byte{'|'})
	if index == -1 {
		return "", data
	}
	t := string(data[:index])
	return t, data[index+1:]
}

// Serialize values to be passed to the storage engine
func serializeSort(ids []key.Type, ranged bool) []byte {
	sort := &SerializedSort{ids, ranged}
	serialized, err := json.Marshal(sort)
	if err != nil {
		panic(err)
	}
	return serialized
}

// Deserializes an indexes from the storage engine
func deserializeSort(raw []byte) *SerializedSort {
	sort := new(SerializedSort)
	if err := json.Unmarshal(raw, sort); err != nil {
		panic(err)
	}
	return sort
}

func removeValue(values []string, target string) ([]string, bool) {
	length := len(values)
	for index, value := range values {
		if target == value {
			for i := index + 1; i < length; i++ {
				values[i-1] = values[i]
			}
			return values[0 : length-1], true
		}
	}
	return values, false
}

func (db *Database) safeDelete(indexName string, id key.Type) {
	if index, exists := db.getIndex(indexName); exists {
		index.Remove(id)
	}
}

func (db *Database) getOrCreateSortedIntIndex(indexName string) indexes.WithIntScores {
	return db.getOrCreateIndex(indexName, func() indexes.Index {
		return indexes.NewSortedInts(indexName)
	}).(indexes.WithIntScores)
}

func (db *Database) getOrCreateSortedStringIndex(indexName string) indexes.WithStringScores {
	return db.getOrCreateIndex(indexName, func() indexes.Index {
		return indexes.NewSortedStrings(indexName)
	}).(indexes.WithStringScores)
}

func (db *Database) getOrCreateSetStringIndex(indexName string) indexes.Index {
	return db.getOrCreateIndex(indexName, func() indexes.Index {
		return indexes.NewSetString(indexName)
	})
}

func (db *Database) getOrCreateBigSetStringIndex(indexName string) indexes.Index {
	return db.getOrCreateIndex(indexName, func() indexes.Index {
		return indexes.NewSetString(indexName)
	})
}

func (db *Database) getOrCreateIndex(indexName string, factory func() indexes.Index) indexes.Index {
	if index, exists := db.getIndex(indexName); exists {
		return index
	}
	index := factory()
	db.indexLock.Lock()
	defer db.indexLock.Unlock()
	if index, exists := db.indexes[indexName]; exists {
		return index
	}
	db.indexes[indexName] = index
	return index
}

type SerializedSort struct {
	Ids    []key.Type
	Ranged bool
}
