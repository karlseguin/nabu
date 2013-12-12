package conditions

import (
	"testing"
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"github.com/karlseguin/nabu/indexes"
)

func TestGreaterThanReturnsTheLength(t *testing.T) {
	spec := gspec.New(t)
	gt := &GreaterThan{Value: 10}
	gt.On(makeIndex(1,7,8,10,11,12,13,20))
	spec.Expect(gt.Len()).ToEqual(4)
}

func TestGreaterThanDoesNotContainANonExistantId(t *testing.T) {
	spec := gspec.New(t)
	gt := &GreaterThan{Value: 10}
	gt.On(makeIndex(1,7,8,10,11,12,13,20))
	spec.Expect(gt.Contains(key.Type(22))).ToEqual(false)
}

func TestGreaterThanDoesNotContainAnIdWithAScoreEqualToOurtarget(t *testing.T) {
	spec := gspec.New(t)
	gt := &GreaterThan{Value: 10}
	gt.On(makeIndex(1,7,8,10,11,12,13,20))
	spec.Expect(gt.Contains(key.Type(3))).ToEqual(false)
}

func TestGreaterThanDoesNotContainAnIdWithAScoreLessThanOurtarget(t *testing.T) {
	spec := gspec.New(t)
	gt := &GreaterThan{Value: 10}
	gt.On(makeIndex(1,7,8,10,11,12,13,20))
	spec.Expect(gt.Contains(key.Type(2))).ToEqual(false)
}

func TestGreaterThanContainsAnIdWithAScoreGreaterThanOurTarget(t *testing.T) {
	spec := gspec.New(t)
	gt := &GreaterThan{Value: 10}
	gt.On(makeIndex(1,7,8,10,11,12,13,20))
	spec.Expect(gt.Contains(key.Type(4))).ToEqual(true)
}

func makeIndex(scores ...int) indexes.Index {
	m := make(map[key.Type]int, len(scores))
	for index, score := range scores {
		m[key.Type(index)] = score
	}
	return indexes.LoadIndex("test", m)
}
