package indexes

import (
  "nabu/key"
)

type Sort interface {
  Len() int
  CanRank() bool
  Load(ids []key.Type)
  Rank(id key.Type) (int, bool)
  Forwards(offset int) Iterator
  Backwards(offset int) Iterator
}

type Iterator interface {
  Next() key.Type
  Current() key.Type
  Close()
}

func NewSort(length, maxUnsortedSize int) Sort {
  if length < maxUnsortedSize {
    return &StaticSort{}
  }
  return &StaticRankSort{}
}

type EmptyIterator struct {}

func (i *EmptyIterator) Next() key.Type { return key.NULL }
func (i *EmptyIterator) Current() key.Type { return key.NULL }
func (i *EmptyIterator) Close() {}

var emptyIterator = &EmptyIterator{}
