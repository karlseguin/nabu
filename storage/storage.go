package storage

type Storage interface {
  Close() error
  Remove(id []byte)
  Put(id, value []byte)
  Iterator() Iterator
}

type Iterator interface {
  Close()
  Next() bool
  Current() ([]byte, []byte)
}

func New(path string) Storage {
  return newLeveldb(path)
}
