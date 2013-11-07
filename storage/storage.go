package storage

import (
  "nabu/key"
)

type Storage interface {
  Put(id key.Type, value interface{})
  Remove(id key.Type)
}

func New(path string) Storage {
  return newLeveldb(path)
}
