package storage

import (
  "encoding/json"
  "github.com/karlseguin/nabu/key"
  "github.com/karlseguin/nabu/bytepool"
  "github.com/syndtr/goleveldb/leveldb"
  "github.com/syndtr/goleveldb/leveldb/iterator"
)

var encodingPool = bytepool.New(32, 1 * 1024 * 1024)

type Leveldb struct {
  db *leveldb.DB
}

func newLeveldb(path string) *Leveldb {
  db, err := leveldb.OpenFile(path, nil)
  if err != nil { panic(err) }
  return &Leveldb{
    db: db,
  }
}

func (l *Leveldb) Put(id key.Type, value interface{}) {
  encodedValue, err := json.Marshal(value)
  if err != nil { panic(err) }

  idBuffer := id.Serialize()
  defer idBuffer.Close()
  l.db.Put(idBuffer.Bytes(), encodedValue, nil)
}

func (l *Leveldb) Remove(id key.Type) {
  idBuffer := id.Serialize()
  defer idBuffer.Close()
  l.db.Delete(idBuffer.Bytes(), nil)
}

func (l *Leveldb) Iterator() Iterator {
  return &LeveldbIterator {
    inner: l.db.NewIterator(nil),
  }
}

func (l *Leveldb) Close() error {
  return l.db.Close()
}

type LeveldbIterator struct {
  inner iterator.Iterator
}

func (i *LeveldbIterator) Next() bool {
  return i.inner.Next()
}

func (i *LeveldbIterator) Current() (key.Type, []byte) {
  return key.Deserialize(i.inner.Key()), i.inner.Value()
}

func (i *LeveldbIterator) Close() {
  i.inner.Release()
}
