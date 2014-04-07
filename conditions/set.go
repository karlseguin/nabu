package conditions

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
)

type Set struct {
	value string
	index indexes.Index
}

func NewSet(value string) *Set {
	return &Set{
		value: value,
	}
}

func (c *Set) Key() string {
	return "set(" + c.value + ")"
}

func (c *Set) On(index indexes.Index) {
	c.index = index
}

func (c *Set) Range() (int, int) {
	return 0, c.Len()
}

func (c *Set) Len() int {
	return c.index.Len()
}

func (c *Set) Contains(id key.Type) (int, bool) {
	if score, exists := c.index.Contains(id); exists {
		return score, true
	}
	return 0, false
}

func (c *Set) CanIterate() bool {
	return true
}

func (c *Set) Iterator() indexes.Iterator {
	return c.index.Forwards()
}
