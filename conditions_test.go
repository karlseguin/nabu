package nabu

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/conditions"
	"sort"
	"testing"
)

func TestConditionsSortNonIterableHandling(t *testing.T) {
	spec := gspec.New(t)
	a, b, c := LT("A", 10), GTE("B", 100), conditions.NewUnion("c", []string{"a", "b"})
	a.On(makeIndex(nil, "a", 1, 2, 3, 4, 5, 6, 7, 8))
	b.On(makeIndex(nil, "b", 1, 2, 3, 4, 5))
	conditions := Conditions{a, b, c}
	sort.Sort(conditions)

	spec.Expect(conditions[0].Key()).ToEqual("B>=100")
	spec.Expect(conditions[1].Key()).ToEqual("A<10")
	spec.Expect(conditions[2].Key()).ToEqual("c in (a,b)")
}
