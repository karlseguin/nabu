package nabu

import (
	"github.com/karlseguin/gspec"
	"testing"
)

func TestSortedResultAddsValues(t *testing.T) {
	spec := gspec.New(t)
	result := newSortedResult(SmallDB())
	spec.Expect(result.add("its")).ToEqual(1)
	spec.Expect(result.add("over")).ToEqual(2)
	spec.Expect(result.add("9000")).ToEqual(3)
	spec.Expect(result.Len()).ToEqual(3)
	spec.Expect(string(result.Ids()[0])).ToEqual("its")
	spec.Expect(string(result.Ids()[1])).ToEqual("over")
	spec.Expect(string(result.Ids()[2])).ToEqual("9000")
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
	result.total = 44
	result.Close()
	spec.Expect(result.total).ToEqual(0)
	spec.Expect(result.Len()).ToEqual(0)
	spec.Expect(result.add("ok")).ToEqual(1)
	spec.Expect(string(result.Ids()[0])).ToEqual("ok")
	spec.Expect(len(result.Ids())).ToEqual(1)
	spec.Expect(result.Len()).ToEqual(1)
}
