// An in-memory set-based document database
package nabu

import (
	"fmt"
	"encoding/json"
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"github.com/karlseguin/nabu/storage"
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
type StringFactory func(id string, t string, data []byte) Document

// Database is the primary point of interaction with Nabu
type Database struct {
	loading bool
	*Configuration
	queryPool       chan *Query
	buckets         map[int]*Bucket
	dStorage        storage.Storage
	mStorage        storage.Storage
	indexLock       sync.RWMutex
	idMap           *IdMap
	sortedResults   chan *SortedResult
	indexes         map[string]indexes.Index
	unsortedResults chan *UnsortedResult
}

// Creates a new Database instance. Unless configured to SkipLoad, data from
// the storage path will be restored
func New(c *Configuration) *Database {
	db := &Database{
		Configuration:   c,
		indexes:         make(map[string]indexes.Index),
		dStorage:        storage.New(c.dbPath + "documents"),
		mStorage:        storage.New(c.dbPath + "idmap"),
		queryPool:       make(chan *Query, c.queryPoolSize),
		buckets:         make(map[int]*Bucket, c.bucketCount),
		sortedResults:   make(chan *SortedResult, c.sortedResultPoolSize),
		unsortedResults: make(chan *UnsortedResult, c.unsortedResultPoolSize),
		idMap:           newIdMap(),
	}
	for i := 0; i < int(c.bucketCount); i++ {
		db.buckets[i] = &Bucket{lookup: make(map[key.Type]Document)}
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
	}
	return db
}

// Generate a Query object against the specified sort index
func (d *Database) Query(indexName string) *Query {
	d.indexLock.RLock()
	index, exists := d.indexes[indexName]
	d.indexLock.RUnlock()
	if exists == false {
		panic(fmt.Sprintf("unknown index %q", indexName))
	}
	q := <-d.queryPool
	q.sort = index
	return q
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

// Inserts or updates the document
func (d *Database) Update(doc Document) {
	meta := newMeta()
	doc.ReadMeta(meta)

	id, stringId := meta.getId(d.idMap)
	bucket := d.getBucket(id)
	bucket.Lock()
	old, isUpdate := bucket.lookup[id]
	bucket.lookup[id] = doc
	bucket.Unlock()

	oldMeta := newMeta()
	if isUpdate {
		old.ReadMeta(oldMeta)
	}
	for name, score := range meta.iIndexes {
		delete(oldMeta.iIndexes, name)
		index := d.getOrCreateIndex(name)
		index.SetInt(id, score)
	}
	for name, _ := range oldMeta.iIndexes {
		d.getOrCreateIndex(name).Remove(id)
	}

	if d.loading == false {
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
	meta := newMeta()
	doc.ReadMeta(meta)
	id, stringId := meta.getId(d.idMap)
	for name, _ := range meta.iIndexes {
		d.indexLock.RLock()
		index, exists := d.indexes[name]
		d.indexLock.RUnlock()
		if exists == false {
			continue
		}
		index.Remove(id)
	}

	bucket := d.getBucket(id)
	bucket.Lock()
	delete(bucket.lookup, id)
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
	return bucket.lookup[id]
}

// Gets the document's bucket
func (d *Database) getBucket(key key.Type) *Bucket {
	return d.buckets[key.Bucket(d.bucketCount)]
}

// Gets the sort index, or creates it if it doesn't already exists
func (d *Database) getOrCreateIndex(name string) indexes.Index {
	d.indexLock.RLock()
	index, exists := d.indexes[name]
	d.indexLock.RUnlock()
	if exists {
		return index
	}

	d.indexLock.Lock()
	defer d.indexLock.Unlock()
	index, exists = d.indexes[name]
	if exists == false {
		index = indexes.NewIndex(name)
		d.indexes[name] = index
	}
	return index
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
	} else {
		lookup := make(map[uint]string)
		iter := d.mStorage.Iterator()
		for iter.Next() {
			stringId, id := iter.Current()
			lookup[key.Deserialize(id)] = string(stringId)
		}

		iter = d.dStorage.Iterator()
		for iter.Next() {
			id, typedValue := iter.Current()
			t, value := deserializeValue(typedValue)
			d.Update(d.sFactory(lookup[key.Deserialize(id)], t, value))
		}
	}
	d.loading = false
}

// Callback used to load indexes from index names
func (d *Database) LookupIndexes(indexNames []string, target indexes.Indexes) bool {
	ok := true
	d.indexLock.RLock()
	d.indexLock.RUnlock()
	for i, name := range indexNames {
		index, exists := d.indexes[name]
		target[i] = index
		if exists == false {
			ok = false
		}
	}
	return ok
}

// Serialize type + values to be passed to the storage engine
func serializeValue(t string, value interface{}) []byte {
	serialized, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	bt := []byte(t)
	l := len(bt)
	final := make([]byte, 1 + l + len(serialized))
	final[0] = byte(l)
	copy(final[1:], bt)
	copy(final[l+1:], serialized)
	return final
}

// Deserialize type  + value from the storage engine
func deserializeValue(data []byte) (string, []byte) {
	typeLength := data[0]
	if typeLength == 0 {
		return "", data[1:]
	}
	t := string(data[1:typeLength+1])
	return t, data[typeLength+1:]
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

type SerializedSort struct {
	Ids    []key.Type
	Ranged bool
}
