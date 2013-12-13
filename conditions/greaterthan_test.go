package conditions

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"testing"
)

func TestGreaterThanReturnsTheLength(t *testing.T) {
	spec := gspec.New(t)
	gt := NewGreaterThan(10)
	gt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(gt.Len()).ToEqual(4)
}

func TestGreaterThanDoesNotContainANonExistantId(t *testing.T) {
	spec := gspec.New(t)
	gt := NewGreaterThan(10)
	gt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	_, exists := gt.Contains(key.Type(22))
	spec.Expect(exists).ToEqual(false)
}

func TestGreaterThanDoesNotContainAnIdWithAScoreEqualToOurtarget(t *testing.T) {
	spec := gspec.New(t)
	gt := NewGreaterThan(10)
	gt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	_, exists := gt.Contains(key.Type(3))
	spec.Expect(exists).ToEqual(false)
}

func TestGreaterThanDoesNotContainAnIdWithAScoreLessThanOurtarget(t *testing.T) {
	spec := gspec.New(t)
	gt := NewGreaterThan(10)
	gt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	_, exists := gt.Contains(key.Type(2))
	spec.Expect(exists).ToEqual(false)
}

func TestGreaterThanContainsAnIdWithAScoreGreaterThanOurTarget(t *testing.T) {
	spec := gspec.New(t)
	gt := NewGreaterThan(10)
	gt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	score, exists := gt.Contains(key.Type(4))
	spec.Expect(score).ToEqual(11)
	spec.Expect(exists).ToEqual(true)
}

func makeIndex(scores ...int) indexes.Index {
	m := make(map[key.Type]int, len(scores))
	for index, score := range scores {
		m[key.Type(index)] = score
	}
	return indexes.LoadIndex("test", m)
}
