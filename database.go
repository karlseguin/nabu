package nabu

import (
  "fmt"
  "sync"
  // "hash/fnv"
  "nabu/key"
  "nabu/cache"
  "nabu/indexes"
)

type Database struct {
  *Configuration
  cache *cache.Cache
  queryPool chan *Query
  sortLock sync.RWMutex
  buckets map[int]*Bucket
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
  return db
}

func (db *Database) Query(name string) *Query {
  sort, exists := db.sorts[name]
  if exists == false {
    panic(fmt.Sprintf("unknown sort index %q", name))
  }
  q := <-db.queryPool
  q.sort = sort
  q.sortLength = sort.Len()
  return q
}

func (db *Database) LoadSort(name string, ids []key.Type) {
  db.sortLock.RLock()
  s, exists := db.sorts[name]
  db.sortLock.RUnlock()
  if exists {
    s.Load(ids)
    return
  }

  db.sortLock.Lock()
  s, exists = db.sorts[name]
  if exists == false {
    s = indexes.NewSort(len(ids), db.maxUnsortedSize)
    db.sorts[name] = s
  }
  db.sortLock.Unlock()
  s.Load(ids)
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
}

func (d *Database) Remove(doc Document) {
  meta := newMeta()
  doc.ReadMeta(meta)
  id := meta.id
  for index, _ := range meta.indexes {
    d.removeDocumentIndex(index, id)
  }
  d.removeDocument(doc, id)
}

func (d *Database) RemoveById(id key.Type) {
  bucket := d.getBucket(id)
  doc := d.getFromBucket(id, bucket)
  if doc != nil {
    d.Remove(doc)
  }
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
  d.cache.Changed(indexName, id, true)
}

func (d *Database) removeDocumentIndex(indexName string, id key.Type) {
  d.indexLock.RLock()
  index, exists := d.indexes[indexName]
  d.indexLock.RUnlock()
  if exists == false { return }
  index.Remove(id)
  d.cache.Changed(indexName, id, false)
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
