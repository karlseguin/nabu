package nabu

import (
  "testing"
  "github.com/karlseguin/gspec"
)

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
  spec.Expect(string(result.Ids()[0])).ToEqual("over")
  spec.Expect(string(result.Ids()[1])).ToEqual("its")
  spec.Expect(string(result.Ids()[2])).ToEqual("9000")
}

func TestUnsortedResultCanBeSortedInDescendingOrder(t *testing.T) {
  spec := gspec.New(t)
  result := newUnsortedResult(SmallDB())
  result.add("its", 43)
  result.add("over", 2)
  result.add("9000", 9001)
  result.finalize(&Query{limit:3,desc:true,})
  spec.Expect(result.Len()).ToEqual(3)
  spec.Expect(string(result.Ids()[0])).ToEqual("9000")
  spec.Expect(string(result.Ids()[1])).ToEqual("its")
  spec.Expect(string(result.Ids()[2])).ToEqual("over")
}

func TestUnsortedResultCanBeSortedToALimit(t *testing.T) {
  spec := gspec.New(t)
  result := newUnsortedResult(SmallDB())
  result.add("its", 43)
  result.add("over", 2)
  result.add("9000", 9001)
  result.finalize(&Query{limit:2,})
  spec.Expect(result.Len()).ToEqual(2)
  spec.Expect(len(result.Ids())).ToEqual(2)
  spec.Expect(string(result.Ids()[0])).ToEqual("over")
  spec.Expect(string(result.Ids()[1])).ToEqual("its")
}

func TestUnsortedResultCanBeSortedToALimitWithOffset(t *testing.T) {
  spec := gspec.New(t)
  result := newUnsortedResult(SmallDB())
  result.add("its", 43)
  result.add("over", 2)
  result.add("9000", 9001)
  result.finalize(&Query{limit:2,offset:1})
  spec.Expect(result.Len()).ToEqual(1)
  spec.Expect(len(result.Ids())).ToEqual(1)
  spec.Expect(string(result.Ids()[0])).ToEqual("its")
}

func TestUnsortedResultCanBeSortedToALimitWithOffsetDesc(t *testing.T) {
  spec := gspec.New(t)
  result := newUnsortedResult(SmallDB())
  result.add("its", 43)
  result.add("over", 2)
  result.add("9000", 9001)
  result.finalize(&Query{limit:2,offset:1,desc:true,})
  spec.Expect(result.Len()).ToEqual(2)
  spec.Expect(len(result.Ids())).ToEqual(2)
  spec.Expect(string(result.Ids()[0])).ToEqual("its")
  spec.Expect(string(result.Ids()[1])).ToEqual("over")
}

func TestUnsortedResultCanBeSortedWithOffsetBeyondLength(t *testing.T) {
  spec := gspec.New(t)
  result := newUnsortedResult(SmallDB())
  result.add("its", 43)
  result.add("over", 2)
  result.add("9000", 9001)
  result.finalize(&Query{limit:2,offset:3,})
  spec.Expect(result.Len()).ToEqual(0)
  spec.Expect(len(result.Ids())).ToEqual(0)
}

func TestUnsortedResultCanBeSortedWithOffsetBeyondLengthDesc(t *testing.T) {
  spec := gspec.New(t)
  result := newUnsortedResult(SmallDB())
  result.add("its", 43)
  result.add("over", 2)
  result.add("9000", 9001)
  result.finalize(&Query{limit:2,offset:3,desc:true,})
  spec.Expect(result.Len()).ToEqual(0)
  spec.Expect(len(result.Ids())).ToEqual(0)
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
  spec.Expect(string(result.Ids()[0])).ToEqual("must")
  spec.Expect(string(result.Ids()[1])).ToEqual("flow")
  spec.Expect(len(result.Ids())).ToEqual(2)
  spec.Expect(result.Len()).ToEqual(2)
  spec.Expect(cap(result.ids)).ToEqual(2500)
}
