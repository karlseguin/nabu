package conditions

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"testing"
)

func TestGreaterThanReturnsTheLength(t *testing.T) {
	spec := gspec.New(t)
	gt := NewGreaterThan("x", 10)
	gt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(gt.Len()).ToEqual(4)
}

func TestGreaterThanDoesNotContainANonExistantId(t *testing.T) {
	spec := gspec.New(t)
	gt := NewGreaterThan("x", 10)
	gt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(gt.Contains(key.Type(22))).ToEqual(false)
}

func TestGreaterThanDoesNotContainAnIdWithAScoreEqualToOurtarget(t *testing.T) {
	spec := gspec.New(t)
	gt := NewGreaterThan("x", 10)
	gt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(gt.Contains(key.Type(3))).ToEqual(false)
}

func TestGreaterThanDoesNotContainAnIdWithAScoreLessThanOurtarget(t *testing.T) {
	spec := gspec.New(t)
	gt := NewGreaterThan("x", 10)
	gt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(gt.Contains(key.Type(2))).ToEqual(false)
}

func TestGreaterThanContainsAnIdWithAScoreGreaterThanOurTarget(t *testing.T) {
	spec := gspec.New(t)
	gt := NewGreaterThan("x", 10)
	gt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	score, exists := gt.Score(key.Type(4))
	spec.Expect(score).ToEqual(11)
	spec.Expect(exists).ToEqual(true)
}

func makeIndex(scores ...int) indexes.Ranked {
	index := indexes.NewSortedInts("test")
	for i, score := range scores {
		index.SetInt(key.Type(i), score)
	}
	return index
}
