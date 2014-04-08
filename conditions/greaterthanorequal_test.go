package conditions

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"testing"
)

func TestGreaterThanOrEqualReturnsTheLength(t *testing.T) {
	spec := gspec.New(t)
	gte := NewGreaterThanOrEqual("x", 10)
	gte.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(gte.Len()).ToEqual(5)
}

func TestGreaterThanOrEqualDoesNotContainANonExistantId(t *testing.T) {
	spec := gspec.New(t)
	gte := NewGreaterThanOrEqual("x", 10)
	gte.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	_, exists := gte.Contains(key.Type(22))
	spec.Expect(exists).ToEqual(false)
}

func TestGreaterThanOrEqualDoesNotContainAnIdWithAScoreLessThanOurtarget(t *testing.T) {
	spec := gspec.New(t)
	gte := NewGreaterThanOrEqual("x", 10)
	gte.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	_, exists := gte.Contains(key.Type(2))
	spec.Expect(exists).ToEqual(false)
}

func TestGreaterThanOrEqualContainsAnIdWithAScoreGreaterThanOurTarget(t *testing.T) {
	spec := gspec.New(t)
	gte := NewGreaterThanOrEqual("x", 10)
	gte.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	score, exists := gte.Contains(key.Type(4))
	spec.Expect(score).ToEqual(11)
	spec.Expect(exists).ToEqual(true)
}

func TestGreaterThanOrEqualContainsAnIdWithAScoreEqualToOurTarget(t *testing.T) {
	spec := gspec.New(t)
	gte := NewGreaterThanOrEqual("x", 10)
	gte.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	score, exists := gte.Contains(key.Type(3))
	spec.Expect(score).ToEqual(10)
	spec.Expect(exists).ToEqual(true)
}
