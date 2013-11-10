package storage

import (
  "nabu/key"
)

type Storage interface {
  Close() error
  Remove(id key.Type)
  Put(id key.Type, value interface{})
  Iterator() Iterator
}

type Iterator interface {
  Close()
  Next() bool
  Current() (key.Type, []byte)
}

func New(path string) Storage {
  return newLeveldb(path)
}
