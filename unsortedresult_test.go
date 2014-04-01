package nabu

import (
	"github.com/karlseguin/gspec"
	"testing"
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
	result.add(1, 43)
	result.add(2, 2)
	result.add(3, 9001)
	result.finalize(&NormalQuery{limit: 3})
	spec.Expect(result.Len()).ToEqual(3)
	spec.Expect(result.Ids()[0]).ToEqual(uint(2))
	spec.Expect(result.Ids()[1]).ToEqual(uint(1))
	spec.Expect(result.Ids()[2]).ToEqual(uint(3))
}

func TestUnsortedResultCanBeSortedInDescendingOrder(t *testing.T) {
	spec := gspec.New(t)
	result := newUnsortedResult(SmallDB())
	result.add(1, 43)
	result.add(2, 2)
	result.add(3, 9001)
	result.finalize(&NormalQuery{limit: 3, desc: true})
	spec.Expect(result.Len()).ToEqual(3)
	spec.Expect(result.Ids()[0]).ToEqual(uint(3))
	spec.Expect(result.Ids()[1]).ToEqual(uint(1))
	spec.Expect(result.Ids()[2]).ToEqual(uint(2))
}

func TestUnsortedResultCanBeSortedToALimit(t *testing.T) {
	spec := gspec.New(t)
	result := newUnsortedResult(SmallDB())
	result.add(1, 43)
	result.add(2, 2)
	result.add(3, 9001)
	result.finalize(&NormalQuery{limit: 2})
	spec.Expect(result.Len()).ToEqual(2)
	spec.Expect(len(result.Ids())).ToEqual(2)
	spec.Expect(result.Ids()[0]).ToEqual(uint(2))
	spec.Expect(result.Ids()[1]).ToEqual(uint(1))
}

func TestUnsortedResultCanBeSortedToALimitWithOffset(t *testing.T) {
	spec := gspec.New(t)
	result := newUnsortedResult(SmallDB())
	result.add(1, 43)
	result.add(2, 2)
	result.add(3, 9001)
	result.finalize(&NormalQuery{limit: 2, offset: 1})
	spec.Expect(result.Len()).ToEqual(2)
	spec.Expect(len(result.Ids())).ToEqual(2)
	spec.Expect(result.Ids()[0]).ToEqual(uint(1))
	spec.Expect(result.Ids()[1]).ToEqual(uint(3))
}

func TestUnsortedResultCanBeSortedToALimitWithOffsetDesc(t *testing.T) {
	spec := gspec.New(t)
	result := newUnsortedResult(SmallDB())
	result.add(1, 43)
	result.add(2, 2)
	result.add(3, 9001)
	result.finalize(&NormalQuery{limit: 2, offset: 1, desc: true})
	spec.Expect(result.Len()).ToEqual(2)
	spec.Expect(len(result.Ids())).ToEqual(2)
	spec.Expect(result.Ids()[0]).ToEqual(uint(1))
	spec.Expect(result.Ids()[1]).ToEqual(uint(2))
}

func TestUnsortedResultCanBeSortedWithOffsetBeyondLength(t *testing.T) {
	spec := gspec.New(t)
	result := newUnsortedResult(SmallDB())
	result.add(1, 43)
	result.add(2, 2)
	result.add(3, 9001)
	result.finalize(&NormalQuery{limit: 2, offset: 3})
	spec.Expect(result.Len()).ToEqual(0)
	spec.Expect(len(result.Ids())).ToEqual(0)
}

func TestUnsortedResultCanBeSortedWithOffsetBeyondLengthDesc(t *testing.T) {
	spec := gspec.New(t)
	result := newUnsortedResult(SmallDB())
	result.add(1, 43)
	result.add(2, 2)
	result.add(3, 9001)
	result.finalize(&NormalQuery{limit: 2, offset: 3, desc: true})
	spec.Expect(result.Len()).ToEqual(0)
	spec.Expect(len(result.Ids())).ToEqual(0)
}

func TestUnsortedResultCanBeSafelyReused(t *testing.T) {
	spec := gspec.New(t)
	db := SmallDB()
	result := <-db.unsortedResults
	result.add(1, 43)
	result.add(2, 2)
	result.add(3, 9001)
	result.finalize(&NormalQuery{limit: 3})
	result.Close()
	spec.Expect(result.Len()).ToEqual(0)
	result.add(4, 4)
	result.add(5, 2)
	result.finalize(&NormalQuery{limit: 10})
	spec.Expect(result.Ids()[0]).ToEqual(uint(5))
	spec.Expect(result.Ids()[1]).ToEqual(uint(4))
	spec.Expect(len(result.Ids())).ToEqual(2)
	spec.Expect(result.Len()).ToEqual(2)
	spec.Expect(cap(result.ids)).ToEqual(5000)
}
