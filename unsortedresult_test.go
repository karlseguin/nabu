package nabu

import (
  "testing"
  "github.com/karlseguin/gspec"
)

func TestUnsortedResultAddsValues(t *testing.T) {
  spec := gspec.New(t)
  result := newUnsortedResult(SmallDB())
  result.add("its", 43)
  result.add("over", 2)
  result.add("9000", 9001)
  spec.Expect(result.Len()).ToEqual(3)

  //todo the behavior of UnsortedResult
  //when fialized hasn't been called is undefined
  spec.Expect(result.Data()[0]).ToEqual("its")
  spec.Expect(result.Data()[1]).ToEqual("over")
  spec.Expect(result.Data()[2]).ToEqual("9000")
}

func TestUnsortedResultIsReleasedBackToTheDatabase(t *testing.T) {
  spec := gspec.New(t)
  db := SmallDB()
  result := <-db.unsortedResults
  spec.Expect(len(db.unsortedResults)).ToEqual(0)
  result.Close()
  spec.Expect(len(db.unsortedResults)).ToEqual(1)
}

func TestUnsortedResultCanBeSorted(t *testing.T) {
  spec := gspec.New(t)
  result := newUnsortedResult(SmallDB())
  result.add("its", 43)
  result.add("over", 2)
  result.add("9000", 9001)
  result.finalize(&Query{limit:3,})
  spec.Expect(result.Len()).ToEqual(3)
  spec.Expect(result.Data()[0]).ToEqual("over")
  spec.Expect(result.Data()[1]).ToEqual("its")
  spec.Expect(result.Data()[2]).ToEqual("9000")
}

func TestUnsortedResultCanBeSortedInDescendingOrder(t *testing.T) {
  spec := gspec.New(t)
  result := newUnsortedResult(SmallDB())
  result.add("its", 43)
  result.add("over", 2)
  result.add("9000", 9001)
  result.finalize(&Query{limit:3,desc:true,})
  spec.Expect(result.Len()).ToEqual(3)
  spec.Expect(result.Data()[0]).ToEqual("9000")
  spec.Expect(result.Data()[1]).ToEqual("its")
  spec.Expect(result.Data()[2]).ToEqual("over")
}

func TestUnsortedResultCanBeSortedToALimit(t *testing.T) {
  spec := gspec.New(t)
  result := newUnsortedResult(SmallDB())
  result.add("its", 43)
  result.add("over", 2)
  result.add("9000", 9001)
  result.finalize(&Query{limit:2,})
  spec.Expect(result.Len()).ToEqual(2)
  spec.Expect(len(result.Data())).ToEqual(2)
  spec.Expect(result.Data()[0]).ToEqual("over")
  spec.Expect(result.Data()[1]).ToEqual("its")
}

func TestUnsortedResultCanBeSafelyReused(t *testing.T) {
  spec := gspec.New(t)
  db := SmallDB()
  result := <-db.unsortedResults
  result.add("its", 43)
  result.add("over", 2)
  result.add("9000", 9001)
  result.finalize(&Query{limit:3,})
  result.Close()
  spec.Expect(result.Len()).ToEqual(0)
  result.add("flow", 4)
  result.add("must", 2)
  result.finalize(&Query{limit:10,})
  spec.Expect(result.Data()[0]).ToEqual("must")
  spec.Expect(result.Data()[1]).ToEqual("flow")
  spec.Expect(len(result.Data())).ToEqual(2)
  spec.Expect(result.Len()).ToEqual(2)
}
