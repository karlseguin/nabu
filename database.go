package nabu

import (
  "fmt"
  "sync"
  "encoding/json"
  "github.com/karlseguin/nabu/key"
  "github.com/karlseguin/nabu/cache"
  "github.com/karlseguin/nabu/indexes"
  "github.com/karlseguin/nabu/storage"
)

type Factory func(id key.Type, data []byte) Document

type Database struct {
  loading bool
  *Configuration
  cache *cache.Cache
  queryPool chan *Query
  sortLock sync.RWMutex
  buckets map[int]*Bucket
  iStorage storage.Storage
  dStorage storage.Storage
  indexLock sync.RWMutex
  sorts map[string]indexes.Sort
  sortedResults chan *SortedResult
  indexes map[string]*indexes.Index
  unsortedResults chan *UnsortedResult
}

func New(c *Configuration) *Database {
  db := &Database {
    Configuration: c,
    sorts: make(map[string]indexes.Sort),
    indexes: make(map[string]*indexes.Index),
    iStorage: storage.New(c.dbPath + "indexes"),
    dStorage: storage.New(c.dbPath + "documents"),
    queryPool: make(chan *Query, c.queryPoolSize),
    buckets: make(map[int]*Bucket, c.bucketCount),
    sortedResults: make(chan *SortedResult, c.sortedResultPoolSize),
    unsortedResults: make(chan *UnsortedResult, c.unsortedResultPoolSize),
  }
  db.cache = cache.New(db, db.cacheWorkers)
  for i := 0; i < int(c.bucketCount); i++ {
    db.buckets[i] = &Bucket{lookup: make(map[key.Type]Document),}
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

func (d *Database) Query(name string) *Query {
  d.sortLock.RLock()
  sort, exists := d.sorts[name]
  d.sortLock.RUnlock()
  if exists == false {
    panic(fmt.Sprintf("unknown sort index %q", name))
  }
  q := <-d.queryPool
  q.sort = sort
  return q
}

func (d *Database) LoadSort(sortName string, ids []key.Type) {
  d.getOrCreateSort(sortName, len(ids)).Load(ids)
  if d.loading == false {
    d.iStorage.Put([]byte(sortName), serializeValue(ids))
  }
}

func (d *Database) AppendSort(sortName string, id key.Type) {
  d.getOrCreateSort(sortName, -1).Append(id)
}

func (d *Database) PrependSort(sortName string, id key.Type) {
  d.getOrCreateSort(sortName, -1).Prepend(id)
}

func (d *Database) Get(id key.Type) Document {
  return d.getFromBucket(id, d.getBucket(id))
}

func (d *Database) Update(doc Document) {
  meta := newMeta()
  doc.ReadMeta(meta)
  bucket := d.getBucket(meta.id)
  if old := d.getMeta(meta.id, bucket); old == nil {
    d.insert(doc, meta, bucket)
  } else {
    d.replace(doc, meta, old, bucket)
  }
  for sort, rank := range meta.sorts {
    d.addDocumentSort(sort, meta.id, rank)
  }
  if d.loading == false {
    idBuffer := meta.id.Serialize()
    defer idBuffer.Close()
    d.dStorage.Put(idBuffer.Bytes(), serializeValue(doc))
  }
}

func (d *Database) Remove(doc Document) {
  meta := newMeta()
  doc.ReadMeta(meta)
  id := meta.id
  for index, _ := range meta.indexes {
    d.removeDocumentIndex(index, id)
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

func (d *Database) RemoveById(id key.Type) {
  bucket := d.getBucket(id)
  doc := d.getFromBucket(id, bucket)
  if doc != nil {
    d.Remove(doc)
  }
}

func (d *Database) BeginLoad() {
  d.loading = true
}

func (d *Database) EndLoad() {
  d.loading = false
}

func (d *Database) Close() error {
  derr := d.dStorage.Close()
  ierr := d.iStorage.Close()
  if derr != nil { return derr }
  return ierr
}

func (d *Database) getMeta(id key.Type, bucket int) *Meta {
  doc := d.getFromBucket(id, bucket)
  if doc == nil { return nil }
  meta := newMeta()
  doc.ReadMeta(meta)
  return meta
}

func (d *Database) getFromBucket(id key.Type, index int) Document {
  bucket := d.buckets[index]
  bucket.RLock()
  defer bucket.RUnlock()
  return bucket.lookup[id]
}

func (d *Database) getBucket(key key.Type) int {
  return key.Bucket(d.bucketCount)
}

func (d *Database) insert(doc Document, meta *Meta, bucket int) {
  id := meta.id
  for index, _ := range meta.indexes {
    d.addDocumentIndex(index, id)
  }
  d.addDocument(doc, id, bucket)
}

func (d *Database) replace(doc Document, meta *Meta, old *Meta, bucket int) {
  id := meta.id

  for index, _ := range meta.indexes {
    if _, exists := old.indexes[index]; exists {
      delete(old.indexes, index)
    } else {
      d.addDocumentIndex(index, id)
    }
  }
  for index, _ := range old.indexes {
    d.removeDocumentIndex(index, id)
  }
  d.addDocument(doc, id, bucket)
}

func (d *Database) addDocumentIndex(indexName string, id key.Type) {
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
  }
  index.Add(id)
  d.changed(indexName, id, true)
}

func (d *Database) addDocumentSort(sortName string, id key.Type, rank int) {
  d.getOrCreateSort(sortName, -1).(indexes.DynamicSort).Set(id, rank)
}

func (d *Database) getOrCreateSort(sortName string, length int) indexes.Sort {
  d.sortLock.RLock()
  sort, exists := d.sorts[sortName]
  d.sortLock.RUnlock()
  if exists { return sort }

  d.sortLock.Lock()
  defer d.sortLock.Unlock()
  sort, exists = d.sorts[sortName]
  if exists == false {
    sort = indexes.NewSort(length, d.maxUnsortedSize)
    d.sorts[sortName] = sort
  }
  return sort
}

func (d *Database) removeDocumentIndex(indexName string, id key.Type) {
  d.indexLock.RLock()
  index, exists := d.indexes[indexName]
  d.indexLock.RUnlock()
  if exists == false { return }
  index.Remove(id)
  d.changed(indexName, id, false)
}

func (d *Database) removeDocumentSort(sortName string, id key.Type) {
  d.sortLock.RLock()
  sort, exists := d.sorts[sortName]
  d.sortLock.RUnlock()
  if exists == false { return }
  sort.(indexes.DynamicSort).Remove(id)
}

func (d *Database) addDocument(doc Document, id key.Type, index int) {
  bucket := d.buckets[index]
  bucket.Lock()
  defer bucket.Unlock()
  bucket.lookup[id] = doc
}

func (d *Database) removeDocument(doc Document, id key.Type) {
  index := d.getBucket(id)
  bucket := d.buckets[index]
  bucket.Lock()
  defer bucket.Unlock()
  delete(bucket.lookup, id)
}

func (d *Database) changed(indexName string, id key.Type, updated bool) {
  if d.loading == false {
    d.cache.Changed(indexName, id, updated)
  }
}

func (d *Database) restore() {
  d.BeginLoad()
  iter := d.dStorage.Iterator()
  for iter.Next() {
    id, value := iter.Current()
    d.Update(d.factory(key.Deserialize(id), value))
  }
  iter = d.iStorage.Iterator()
  for iter.Next() {
    id, value := iter.Current()
    d.LoadSort(string(id), deserializeIndex(value))
  }
  d.EndLoad()
}

func (d *Database) LookupIndexes(indexNames []string, target indexes.Indexes) bool {
  ok := true
  d.indexLock.RLock()
  d.indexLock.RUnlock()
  for i, name := range indexNames {
    index, exists := d.indexes[name]
    target[i] = index
    if exists == false { ok = false }
  }
  return ok
}

func serializeValue(value interface{}) []byte {
  serialized, err := json.Marshal(value)
  if err != nil { panic(err) }
  return serialized
}

func deserializeIndex(raw []byte) []key.Type {
  var index []key.Type
  if err := json.Unmarshal(raw, &index); err != nil {
    panic(err)
  }
  return index
}
