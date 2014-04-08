package conditions

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"testing"
)

func TestLessThanOrEqualReturnsTheLength(t *testing.T) {
	spec := gspec.New(t)
	lte := NewLessThanOrEqual("x", 10)
	lte.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(lte.Len()).ToEqual(4)
}

func TestLessThanOrEqualDoesNotContainANonExistantId(t *testing.T) {
	spec := gspec.New(t)
	lte := NewLessThanOrEqual("x", 10)
	lte.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	_, exists := lte.Contains(key.Type(22))

	spec.Expect(exists).ToEqual(false)
}

func TestLessThanOrEqualDoesNotContainAnIdWithAScoreGreaterThanOurtarget(t *testing.T) {
	spec := gspec.New(t)
	lte := NewLessThanOrEqual("x", 10)
	lte.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	_, exists := lte.Contains(key.Type(4))
	spec.Expect(exists).ToEqual(false)
}

func TestLessThanOrEqualContainsAnIdWithAScoreLessThanOrEqualOurTarget(t *testing.T) {
	spec := gspec.New(t)
	lte := NewLessThanOrEqual("x", 10)
	lte.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	score, exists := lte.Contains(key.Type(2))
	spec.Expect(score).ToEqual(8)
	spec.Expect(exists).ToEqual(true)
}

func TestLessThanOrEqualContainAnIdWithAScoreEqualteoOurtarget(t *testing.T) {
	spec := gspec.New(t)
	lte := NewLessThanOrEqual("x", 10)
	lte.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	_, exists := lte.Contains(key.Type(3))
	spec.Expect(exists).ToEqual(true)
}
