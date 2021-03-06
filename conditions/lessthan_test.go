package conditions

import (
	"github.com/karlseguin/gspec"
	"github.com/karlseguin/nabu/key"
	"testing"
)

func TestLessThanReturnsTheLength(t *testing.T) {
	spec := gspec.New(t)
	lt := NewLessThan("x", 10)
	lt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(lt.Len()).ToEqual(3)
}

func TestLessThanDoesNotContainANonExistantId(t *testing.T) {
	spec := gspec.New(t)
	lt := NewLessThan("x", 10)
	lt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(lt.Contains(key.Type(22))).ToEqual(false)
}

func TestLessThanDoesNotContainAnIdWithAScoreEqualToOurtarget(t *testing.T) {
	spec := gspec.New(t)
	lt := NewLessThan("x", 10)
	lt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(lt.Contains(key.Type(3))).ToEqual(false)
}

func TestLessThanDoesNotContainAnIdWithAScoreGreaterThanOurtarget(t *testing.T) {
	spec := gspec.New(t)
	lt := NewLessThan("x", 10)
	lt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	spec.Expect(lt.Contains(key.Type(4))).ToEqual(false)
}

func TestLessThanContainsAnIdWithAScoreLessThanOurTarget(t *testing.T) {
	spec := gspec.New(t)
	lt := NewLessThan("x", 10)
	lt.On(makeIndex(1, 7, 8, 10, 11, 12, 13, 20))
	score, exists := lt.Score(key.Type(2))
	spec.Expect(score).ToEqual(8)
	spec.Expect(exists).ToEqual(true)
}
