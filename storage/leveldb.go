package storage

import (
  "nabu/key"
  "encoding/gob"
  "nabu/bytepool"
  "github.com/syndtr/goleveldb/leveldb"
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
  buffer := encodingPool.Checkout()
  defer buffer.Close()
  encoder := gob.NewEncoder(buffer)
  encoder.Encode(value)
  idBuffer := id.Serialize()
  defer idBuffer.Close()
  l.db.Put(idBuffer.Bytes(), buffer.Bytes(), nil)
}

func (l *Leveldb) Remove(id key.Type) {
  idBuffer := id.Serialize()
  defer idBuffer.Close()
  l.db.Delete(idBuffer.Bytes(), nil)
}
