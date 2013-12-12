// An in-memory set-based document database
package nabu

import (
	"encoding/json"
	"fmt"
	"github.com/karlseguin/nabu/cache"
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

   func factory(id key.Type, data []byte) nabu.Document {
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
type Factory func(id key.Type, data []byte) Document

// Database is the primary point of interaction with Nabu
type Database struct {
	loading bool
	*Configuration
	cache           *cache.Cache
	queryPool       chan *Query
	sortLock        sync.RWMutex
	buckets         map[int]*Bucket
	iStorage        storage.Storage
	dStorage        storage.Storage
	indexLock       sync.RWMutex
	indexValueLock  sync.RWMutex
	idMap *IdMap
	sorts           map[string]indexes.Sort
	indexValues     map[string][]string
	sortedResults   chan *SortedResult
	indexes         map[string]*indexes.Index
	unsortedResults chan *UnsortedResult
}

// Creates a new Database instance. Unless configured to SkipLoad, data from
// the storage path will be restored
func New(c *Configuration) *Database {
	db := &Database{
		Configuration:   c,
		sorts:           make(map[string]indexes.Sort),
		indexValues:     make(map[string][]string),
		indexes:         make(map[string]*indexes.Index),
		iStorage:        storage.New(c.dbPath + "indexes"),
		dStorage:        storage.New(c.dbPath + "documents"),
		queryPool:       make(chan *Query, c.queryPoolSize),
		buckets:         make(map[int]*Bucket, c.bucketCount),
		sortedResults:   make(chan *SortedResult, c.sortedResultPoolSize),
		unsortedResults: make(chan *UnsortedResult, c.unsortedResultPoolSize),
		idMap: newIdMap(),
	}
	db.cache = cache.New(db, db.cacheWorkers, db.maxCacheStaleness)
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
func (d *Database) Query(sortName string) *Query {
	d.sortLock.RLock()
	sort, exists := d.sorts[sortName]
	d.sortLock.RUnlock()
	if exists == false {
		panic(fmt.Sprintf("unknown sort index %q", sortName))
	}
	q := <-d.queryPool
	q.sort = sort
	return q
}

// Generate a Query object against the specified sort index
// inclusive of the specified range
func (d *Database) RangeQuery(sortName string, from, to int) *Query {
	q := d.Query(sortName)
	q.from = from
	q.to = to
	q.ranged = true
	return q
}

// Loads a complete sort index. This is useful for updating sorting
// in a background process. For example, a sort based on trending tags
// probably has a background task responsible for updating it.
// Specifying ranged will allow ranged queries on the index (it creates
// a dynamic index rather than a static index)
func (d *Database) LoadSort(sortName string, ids []key.Type, ranged bool) {
	d.getOrCreateSort(sortName, len(ids)).Load(ids)
	if d.loading == false {
		d.iStorage.Put([]byte(sortName), serializeSort(ids, ranged))
	}
}

// Appends a value to the specified sort. This can be used against
// either a dynamic or static sort. This isn't particularly efficient
// when used against a static sort, though occasional use is encouraged.
func (d *Database) AppendSort(sortName string, id uint) {
	d.getOrCreateSort(sortName, -1).Append(key.Type(id))
}

// Prepends a value to the specified sort. See the notes on AppendSort
// for more details
func (d *Database) PrependSort(sortName string, id uint) {
	d.getOrCreateSort(sortName, -1).Prepend(key.Type(id))
}

// Retrieves a document by id
func (d *Database) Get(id uint) Document {
	typed := key.Type(id)
	return d.getFromBucket(typed, d.getBucket(typed))
}

// Retrieves a document by id
func (d *Database) StringGet(id string) Document {
	typed := d.idMap.get(id, false)
	if typed == key.NULL {
		return nil
	}
	return d.getFromBucket(typed, d.getBucket(typed))
}

// Inserts or updates the document
func (d *Database) Update(doc Document) {
	meta := newMeta()
	doc.ReadMeta(meta)
	id := meta.getId(d.idMap)
	bucket := d.getBucket(id)
	if old := d.getMeta(id, bucket); old == nil {
		d.insert(doc, id, meta, bucket)
	} else {
		d.update(doc, id, meta, old, bucket)
	}
	for sort, score := range meta.sorts {
		d.addDocumentSort(sort, id, score)
	}
	if d.loading == false {
		idBuffer := id.Serialize()
		defer idBuffer.Close()
		d.dStorage.Put(idBuffer.Bytes(), serializeValue(doc))
	}
}

// Removes the document. Safe to call even if the document
// does not exists.
func (d *Database) Remove(doc Document) {
	meta := newMeta()
	doc.ReadMeta(meta)
	id := meta.getId(d.idMap)
	for baseName, values := range meta.indexes {
		d.removeDocumentIndex(baseName, values, id)
	}
	for sort, _ := range meta.sorts {
		d.removeDocumentSort(sort, id)
	}
	d.removeDocument(doc, id)
	if d.loading == false {
		idBuffer := id.Serialize()
		defer idBuffer.Close()
		d.dStorage.Remove(idBuffer.Bytes())
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

// Removes to the document by id. Safe to call even if the
// id doesn't exist
func (d *Database) removeByTypedId(id key.Type) {
	bucket := d.getBucket(id)
	doc := d.getFromBucket(id, bucket)
	if doc != nil {
		d.Remove(doc)
	}
}

// Returns the distinct values contained in an index
func (d *Database) Distinct(indexName string) []string {
	d.indexValueLock.RLock()
	defer d.indexValueLock.RUnlock()
	return d.indexValues[indexName]
}

// Returns the distinct values in an index include the number
// of documents
func (d *Database) DistinctCount(indexName string) map[string]int {
	d.indexValueLock.RLock()
	values := d.indexValues[indexName]
	d.indexValueLock.RUnlock()

	results := make(map[string]int, len(values))
	d.indexLock.RLock()
	defer d.indexLock.RUnlock()
	for _, value := range values {
		fullName := indexName + "$" + value
		if index, ok := d.indexes[fullName]; ok {
			results[value] = index.Len()
		}
	}
	return results
}

// Closes the database
func (d *Database) Close() error {
	derr := d.dStorage.Close()
	ierr := d.iStorage.Close()
	if derr != nil {
		return derr
	}
	return ierr
}

// Gets a document's meta details based on an id
func (d *Database) getMeta(id key.Type, bucket int) *Meta {
	doc := d.getFromBucket(id, bucket)
	if doc == nil {
		return nil
	}
	meta := newMeta()
	doc.ReadMeta(meta)
	return meta
}

// Gets a document from the given bucket
func (d *Database) getFromBucket(id key.Type, index int) Document {
	bucket := d.buckets[index]
	bucket.RLock()
	defer bucket.RUnlock()
	return bucket.lookup[id]
}

// Gets the document's bucket
func (d *Database) getBucket(key key.Type) int {
	return key.Bucket(d.bucketCount)
}

// Inserts a new document
func (d *Database) insert(doc Document, id key.Type, meta *Meta, bucket int) {
	for baseName, values := range meta.indexes {
		d.addDocumentIndex(baseName, values, id)
	}
	d.addDocument(doc, id, bucket)
}

// Updates an existing document
func (d *Database) update(doc Document, id key.Type, meta *Meta, old *Meta, bucket int) {
	for baseName, values := range meta.indexes {
		length := len(values)

		//do a diff of the two values
		if oldValues, exists := old.indexes[baseName]; exists {
			length = 0
			for _, value := range values {
				var removed bool
				if oldValues, removed = removeValue(oldValues, value); !removed {
					values[length] = value
					length++
				}
			}
			if len(oldValues) == 0 {
				delete(old.indexes, baseName)
			}
		}

		if length > 0 {
			d.addDocumentIndex(baseName, values[0:length], id)
		}
	}

	for baseName, values := range old.indexes {
		d.removeDocumentIndex(baseName, values, id)
	}

	d.addDocument(doc, id, bucket)
}

// Indexes the document
func (d *Database) addDocumentIndex(baseName string, values []string, id key.Type) {
	shouldIndexValue := d.aggregatable[baseName]
	for _, value := range values {
		indexName := baseName + "$" + value
		d.indexLock.RLock()
		index, exists := d.indexes[indexName]
		d.indexLock.RUnlock()
		if exists == false {
			d.indexLock.Lock()
			index, exists = d.indexes[indexName]
			if exists == false {
				index = indexes.New(indexName)
				d.indexes[indexName] = index
			}
			d.indexLock.Unlock()
			if exists == false && shouldIndexValue {
				d.addIndexValue(baseName, value)
			}
		}
		index.Add(id)
		d.changed(indexName, id, true)
	}
}

// Sort indexes the document
func (d *Database) addDocumentSort(sortName string, id key.Type, score int) {
	d.getOrCreateSort(sortName, -1).(indexes.DynamicSort).Set(id, score)
}

// Tracks unique value belonging to an index
func (d *Database) addIndexValue(baseName, value string) {
	d.indexValueLock.Lock()
	defer d.indexValueLock.Unlock()
	container, exists := d.indexValues[baseName]
	if exists == false {
		d.indexValues[baseName] = []string{value}
		return
	}
	l := len(container)
	newContainer := make([]string, l+1)
	copy(newContainer, container)
	newContainer[l] = value
	d.indexValues[baseName] = newContainer
}

// Gets the sort index, or creates it if it doesn't already exists
func (d *Database) getOrCreateSort(sortName string, length int) indexes.Sort {
	d.sortLock.RLock()
	sort, exists := d.sorts[sortName]
	d.sortLock.RUnlock()
	if exists {
		return sort
	}

	d.sortLock.Lock()
	defer d.sortLock.Unlock()
	sort, exists = d.sorts[sortName]
	if exists == false {
		sort = indexes.NewSort(length, d.maxUnsortedSize)
		d.sorts[sortName] = sort
	}
	return sort
}

// Unindexes the document
func (d *Database) removeDocumentIndex(baseName string, values []string, id key.Type) {
	shouldIndexValue := d.aggregatable[baseName]
	for _, value := range values {
		indexName := baseName + "$" + value
		d.indexLock.RLock()
		index, exists := d.indexes[indexName]
		d.indexLock.RUnlock()
		if exists == false {
			return
		}
		if index.Remove(id) == 0 && shouldIndexValue {
			d.removeIndexValue(baseName, value)
		}
		d.changed(indexName, id, false)
	}
}

// Removes the sort indexes for the document
func (d *Database) removeDocumentSort(sortName string, id key.Type) {
	d.sortLock.RLock()
	sort, exists := d.sorts[sortName]
	d.sortLock.RUnlock()
	if exists == false {
		return
	}
	sort.(indexes.DynamicSort).Remove(id)
}

// Removes unique value belonging to an index
func (d *Database) removeIndexValue(baseName, value string) {
	d.indexValueLock.Lock()
	defer d.indexValueLock.Unlock()
	if container, exists := d.indexValues[baseName]; exists {
		newContainer := make([]string, len(container)-1)
		i := 0
		for _, v := range container {
			if v == value {
				continue
			}
			newContainer[i] = v
			i++
		}
		d.indexValues[baseName] = newContainer
	}
}

// Adds the document to the bucket
func (d *Database) addDocument(doc Document, id key.Type, index int) {
	bucket := d.buckets[index]
	bucket.Lock()
	defer bucket.Unlock()
	bucket.lookup[id] = doc
}

// Removes the document from the bucket
func (d *Database) removeDocument(doc Document, id key.Type) {
	index := d.getBucket(id)
	bucket := d.buckets[index]
	bucket.Lock()
	defer bucket.Unlock()
	delete(bucket.lookup, id)
}

// Signal the cache that an index was updated with a specific id
func (d *Database) changed(indexName string, id key.Type, updated bool) {
	if d.loading == false {
		d.cache.Changed(indexName, id, updated)
	}
}

// Loads documents and indexes from the storage engine
func (d *Database) restore() {
	d.loading = true
	iter := d.dStorage.Iterator()
	for iter.Next() {
		id, value := iter.Current()
		d.Update(d.factory(key.Deserialize(id), value))
	}
	iter = d.iStorage.Iterator()
	for iter.Next() {
		id, value := iter.Current()
		sort := deserializeSort(value)
		d.LoadSort(string(id), sort.Ids, sort.Ranged)
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

// Serialize values to be passed to the storage engine
func serializeValue(value interface{}) []byte {
	serialized, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return serialized
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
