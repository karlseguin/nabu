package indexes

import (
  "sort"
  "testing"
  "strconv"
  "github.com/karlseguin/gspec"
)

func TestIndexAddAnItem(t *testing.T) {
  spec := gspec.New(t)
  index := New("_")
  index.Add("leto")
  spec.Expect(len(index.Ids)).ToEqual(1)
  spec.Expect(index.Contains("leto")).ToEqual(true)
}

func TestIndexCanRemoveNonExistingItem(t *testing.T) {
  spec := gspec.New(t)
  index := New("_")
  index.Add("a")
  index.Remove("b")
  spec.Expect(len(index.Ids)).ToEqual(1)
  spec.Expect(index.Contains("a")).ToEqual(true)
}

func TestIndexCanRemoveAnItem(t *testing.T) {
  spec := gspec.New(t)
  index := New("_")
  index.Add("a")
  index.Remove("a")
  spec.Expect(len(index.Ids)).ToEqual(0)
}

func TestIndexesAreSortedFromSmallestToLargest(t *testing.T) {
  spec := gspec.New(t)
  index1 := New("_")
  for i := 0; i < 4; i++ { index1.Add(strconv.Itoa(i)) }
  index2 := New("_")
  for i := 0; i < 7; i++ { index2.Add(strconv.Itoa(i)) }
  index3 := New("_")
  for i := 0; i < 13; i++ { index3.Add(strconv.Itoa(i)) }

  indexes := Indexes{index2, index1, index3}
  sort.Sort(indexes)
  spec.Expect(len(indexes[0].Ids)).ToEqual(4)
  spec.Expect(len(indexes[1].Ids)).ToEqual(7)
  spec.Expect(len(indexes[2].Ids)).ToEqual(13)
}
