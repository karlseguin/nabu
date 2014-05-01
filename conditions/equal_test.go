package conditions

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"github.com/karlseguin/nabu/indexes"
	"testing"
)

func TestEqualReturnsTheLength(t *testing.T) {
	spec := gspec.New(t)
	eq := NewEqual("x", 10)
	idx := makeIndex(1, 7, 8, 10, 11, 12, 13, 20)
	idx.(indexes.WithIntScores).SetInt(key.Type(22), 10)
	eq.On(idx)
	spec.Expect(eq.Len()).ToEqual(2)
}

func TestEqualReturnsTheLengthWhenNone(t *testing.T) {
	spec := gspec.New(t)
	eq := NewEqual("x", 223)
	eq.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(eq.Len()).ToEqual(0)
}

func TestEqualDoesNotContainANonExistantId(t *testing.T) {
	spec := gspec.New(t)
	eq := NewEqual("x", 10)
	eq.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(eq.Contains(key.Type(22))).ToEqual(false)
}

func TestEqualDoesNotContainAnIdWithAScoreGreaterThanOurtarget(t *testing.T) {
	spec := gspec.New(t)
	eq := NewEqual("x", 10)
	eq.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(eq.Contains(key.Type(4))).ToEqual(false)
}

func TestEqualDoesNotContainAnIdWithAScoreLessThanOurtarget(t *testing.T) {
	spec := gspec.New(t)
	eq := NewEqual("x", 10)
	eq.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(eq.Contains(key.Type(2))).ToEqual(false)
}

func TestEqualContainsAnIdWithAScoreEqualOurTarget(t *testing.T) {
	spec := gspec.New(t)
	eq := NewEqual("x", 10)
	eq.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	score, exists := eq.Score(key.Type(3))
	spec.Expect(score).ToEqual(10)
	spec.Expect(exists).ToEqual(true)
}
