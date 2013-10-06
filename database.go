package nabu

import (
  "fmt"
  "sync"
  "hash/fnv"
)

type Database struct {
  cache *Cache
  *Configuration
  queryPool chan *Query
  sortLock sync.RWMutex
  sorts map[string]*Sort
  buckets map[int]*Bucket
  indexLock sync.RWMutex
  indexes map[string]*Index
  sortedResults chan *SortedResult
  unsortedResults chan *UnsortedResult
}

func New(c *Configuration) *Database {
  db := &Database {
    Configuration: c,
    cache: newCache(c),
    sorts: make(map[string]*Sort),
    indexes: make(map[string]*Index),
    queryPool: make(chan *Query, c.queryPoolSize),
    buckets: make(map[int]*Bucket, c.bucketCount),
    sortedResults: make(chan *SortedResult, c.sortedResultPoolSize),
    unsortedResults: make(chan *UnsortedResult, c.unsortedResultPoolSize),
  }

  for i := 0; i < int(c.bucketCount); i++ {
    db.buckets[i] = &Bucket{lookup: make(map[string]Document),}
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

func (db *Database) AddSort(name string, list []string) {
  s := &Sort {
    list: list,
    lookup: make(map[string]int, len(list)),
  }
  for i := 0; i < len(list); i++ {
    s.lookup[list[i]] = i
  }
  db.sortLock.Lock()
  defer db.sortLock.Unlock()
  db.sorts[name] = s
}

func (d *Database) Get(id string) Document {
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

func (d *Database) RemoveById(id string) {
  bucket := d.getBucket(id)
  doc := d.getFromBucket(id, bucket)
  if doc != nil {
    d.Remove(doc)
  }
}

func (d *Database) getMeta(id string, bucket int) *Meta {
  doc := d.getFromBucket(id, bucket)
  if doc == nil { return nil }
  meta := newMeta()
  doc.ReadMeta(meta)
  return meta
}

func (d *Database) getFromBucket(id string, index int) Document {
  bucket := d.buckets[index]
  bucket.RLock()
  defer bucket.RUnlock()
  return bucket.lookup[id]
}

func (d *Database) getBucket(key string) int {
  h := fnv.New32a()
  h.Write([]byte(key))
  return int(h.Sum32() % d.bucketCount)
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

func (d *Database) addDocumentIndex(indexName string, id string) {
  d.indexLock.RLock()
  index, exists := d.indexes[indexName]
  d.indexLock.RUnlock()
  if exists == false {
    d.indexLock.Lock()
    index, exists = d.indexes[indexName]
    if exists == false {
      index = newIndex(indexName)
      d.indexes[indexName] = index
    }
    d.indexLock.Unlock()
  }
  index.Add(id)
}

func (d *Database) removeDocumentIndex(indexName string, id string) {
  d.indexLock.RLock()
  index, exists := d.indexes[indexName]
  d.indexLock.RUnlock()
  if exists == false { return }
  index.Remove(id)
}

func (d *Database) addDocument(doc Document, id string, index int) {
  bucket := d.buckets[index]
  bucket.Lock()
  defer bucket.Unlock()
  bucket.lookup[id] = doc
}

func (d *Database) removeDocument(doc Document, id string) {
  index := d.getBucket(id)
  bucket := d.buckets[index]
  bucket.Lock()
  defer bucket.Unlock()
  delete(bucket.lookup, id)
}
