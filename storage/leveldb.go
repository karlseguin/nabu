package storage

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

// LevelDb based storage
type Leveldb struct {
	db *leveldb.DB
}

func newLeveldb(path string) *Leveldb {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		panic(err)
	}
	return &Leveldb{
		db: db,
	}
}

func (l *Leveldb) Put(id, value []byte) {
	l.db.Put(id, value, nil)
}

func (l *Leveldb) Remove(id []byte) {
	l.db.Delete(id, nil)
}

func (l *Leveldb) Iterator() Iterator {
	return &LeveldbIterator{
		inner: l.db.NewIterator(nil, nil),
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

func (i *LeveldbIterator) Current() ([]byte, []byte) {
	return i.inner.Key(), i.inner.Value()
}

func (i *LeveldbIterator) Close() {
	i.inner.Release()
}

func (i *LeveldbIterator) Error() error {
	return i.inner.Error()
}
