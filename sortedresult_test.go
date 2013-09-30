package nabu

import (
  "testing"
  "github.com/karlseguin/gspec"
)

func TestSortedResultAddsValues(t *testing.T) {
  spec := gspec.New(t)
  result := newSortedResult(SmallDB())
  spec.Expect(result.add("its")).ToEqual(1)
  spec.Expect(result.add("over")).ToEqual(2)
  spec.Expect(result.add("9000")).ToEqual(3)
  spec.Expect(result.Len()).ToEqual(3)
  spec.Expect(result.Data()[0]).ToEqual("its")
  spec.Expect(result.Data()[1]).ToEqual("over")
  spec.Expect(result.Data()[2]).ToEqual("9000")
}

func TestSortedResultIsReleasedBackToTheDatabase(t *testing.T) {
  spec := gspec.New(t)
  db := SmallDB()
  result := <-db.sortedResults
  spec.Expect(len(db.sortedResults)).ToEqual(0)
  result.Close()
  spec.Expect(len(db.sortedResults)).ToEqual(1)
}

func TestSortedResultCanBeSafelyReused(t *testing.T) {
  spec := gspec.New(t)
  db := SmallDB()
  result := <-db.sortedResults
  spec.Expect(result.add("its")).ToEqual(1)
  spec.Expect(result.add("over")).ToEqual(2)
  spec.Expect(result.add("9000")).ToEqual(3)
  result.Close()
  spec.Expect(result.Len()).ToEqual(0)
  spec.Expect(result.add("ok")).ToEqual(1)
  spec.Expect(result.Data()[0]).ToEqual("ok")
  spec.Expect(len(result.Data())).ToEqual(1)
  spec.Expect(result.Len()).ToEqual(1)
}
