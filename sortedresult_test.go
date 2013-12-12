package nabu

import (
	"github.com/karlseguin/gspec"
	"testing"
)

func TestSortedResultAddsValues(t *testing.T) {
	spec := gspec.New(t)
	result := newSortedResult(SmallDB())
	spec.Expect(result.add(5)).ToEqual(1)
	spec.Expect(result.add(6)).ToEqual(2)
	spec.Expect(result.add(7)).ToEqual(3)
	spec.Expect(result.Len()).ToEqual(3)
	spec.Expect(result.Ids()[0]).ToEqual(uint(5))
	spec.Expect(result.Ids()[1]).ToEqual(uint(6))
	spec.Expect(result.Ids()[2]).ToEqual(uint(7))
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
	spec.Expect(result.add(7)).ToEqual(1)
	spec.Expect(result.add(8)).ToEqual(2)
	spec.Expect(result.add(9)).ToEqual(3)
	result.total = 44
	result.Close()
	spec.Expect(result.total).ToEqual(0)
	spec.Expect(result.Len()).ToEqual(0)
	spec.Expect(result.add(2)).ToEqual(1)
	spec.Expect(result.Ids()[0]).ToEqual(uint(2))
	spec.Expect(len(result.Ids())).ToEqual(1)
	spec.Expect(result.Len()).ToEqual(1)
}
