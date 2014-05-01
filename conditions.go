package nabu

import (
	"github.com/karlseguin/nabu/conditions"
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
)

type RankedContainer interface {
	Score(id key.Type) (int, bool)
}

// A condition to apply to an index
type Condition interface {
	Key() string
	Len() int
	IndexName() string
	On(index indexes.Index)
	Contains(id key.Type) bool
	CanIterate() bool
	Iterator() indexes.Iterator
	RLock()
	RUnlock()
}

type RankedCondition interface {
	Condition
	RankedContainer
	Range() (int, int)
}

type MultiCondition interface {
	Condition
	IndexNames() []string
}

// An array of condition
type Conditions []Condition

// Read locks all the conditions
func (conditions Conditions) RLock() {
	for _, condition := range conditions {
		condition.RLock()
	}
}

// Read unlocks all the conditions
func (conditions Conditions) RUnlock() {
	for _, condition := range conditions {
		condition.RUnlock()
	}
}

// The number of items in our array of set
func (c Conditions) Len() int {
	return len(c)
}

// Used to sort an array based on length
func (c Conditions) Less(i, j int) bool {
	return c[i].CanIterate() && c[i].Len() < c[j].Len()
}

// Used to sort an array based on length
func (c Conditions) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func GT(indexName string, value int) Condition {
	return conditions.NewGreaterThan(indexName, value)
}

func GTE(indexName string, value int) Condition {
	return conditions.NewGreaterThanOrEqual(indexName, value)
}

func LT(indexName string, value int) Condition {
	return conditions.NewLessThan(indexName, value)
}

func LTE(indexName string, value int) Condition {
	return conditions.NewLessThanOrEqual(indexName, value)
}

func EQ(indexName string, value int) Condition {
	return conditions.NewEqual(indexName, value)
}

func Between(indexName string, from, to int) Condition {
	return conditions.NewBetween(indexName, from, to)
}
