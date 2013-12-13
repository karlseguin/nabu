package nabu

import (
	"github.com/karlseguin/nabu/conditions"
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
)

type Container interface {
	Contains(id key.Type) (int, bool)
}

// A condition to apply to an index
type Condition interface {
	Key() string
	Len() int
	On(index indexes.Index)
	Contains(id key.Type) (int, bool)
	Range() (int, int)
	Iterator() indexes.Iterator
}

// An array of condition
type Conditions []Condition

// The number of items in our array of set
func (c Conditions) Len() int {
	return len(c)
}

// Used to sort an array based on length
func (c Conditions) Less(i, j int) bool {
	return c[i].Len() < c[j].Len()
}

// Used to sort an array based on length
func (c Conditions) Swap(i, j int) {
	x := c[i]
	c[i] = c[j]
	c[j] = x
}

func GT(value int) Condition {
	return conditions.NewGreaterThan(value)
}

func LT(value int) Condition {
	return conditions.NewLessThan(value)
}

func EQ(value int) Condition {
	return conditions.NewEqual(value)
}
