package conditions

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"testing"
)

func TestBetweenReturnsTheLength(t *testing.T) {
	spec := gspec.New(t)
	bt := NewBetween("x", 7, 11)
	idx := makeIndex(1, 7, 8, 10, 11, 12, 13, 20)
	idx.(indexes.WithIntScores).SetInt(key.Type(22), 10)
	bt.On(idx)
	spec.Expect(bt.Len()).ToEqual(5)
}

func TestBetweenReturnsTheLengthWhenNone(t *testing.T) {
	spec := gspec.New(t)
	bt := NewBetween("x", 223, 233)
	bt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(bt.Len()).ToEqual(0)
}

func TestBetweenDoesNotContainANonExistantId(t *testing.T) {
	spec := gspec.New(t)
	bt := NewBetween("x", 10, 15)
	bt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(bt.Contains(key.Type(22))).ToEqual(false)
}

func TestBetweenDoesNotContainAnIdWithAScoreGreaterThanOurtarget(t *testing.T) {
	spec := gspec.New(t)
	bt := NewBetween("x", 20, 250)
	bt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(bt.Contains(key.Type(4))).ToEqual(false)
}

func TestBetweenDoesNotContainAnIdWithAScoreLessThanOurtarget(t *testing.T) {
	spec := gspec.New(t)
	bt := NewBetween("x", -1, 0)
	bt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(bt.Contains(key.Type(2))).ToEqual(false)
}

func TestBetweenContainsAnIdWithAScoreWithinOurTarget(t *testing.T) {
	spec := gspec.New(t)
	bt := NewBetween("x", 7, 11)
	bt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	score, exists := bt.Score(key.Type(3))
	spec.Expect(score).ToEqual(10)
	spec.Expect(exists).ToEqual(true)
}
