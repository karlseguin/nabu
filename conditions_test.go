package nabu

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/conditions"
	"sort"
	"testing"
)

func TestConditionsSortNonIterableHandling(t *testing.T) {
	spec := gspec.New(t)
	a, b, c := LT(10), GTE(100), conditions.NewUnion([]string{"a", "b"})
	a.On(makeIndex(nil, "a", 1, 2, 3, 4, 5, 6, 7, 8))
	b.On(makeIndex(nil, "b", 1, 2, 3, 4, 5))
	conditions := Conditions{a, b, c}
	sort.Sort(conditions)

	spec.Expect(conditions[0].Key()).ToEqual(">=100")
	spec.Expect(conditions[1].Key()).ToEqual("<10")
	spec.Expect(conditions[2].Key()).ToEqual("in(a,b)")
}
