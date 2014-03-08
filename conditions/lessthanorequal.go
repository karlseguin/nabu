package conditions

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"strconv"
)

type LessThanOrEqual struct {
	value  int
	length int
	index  indexes.Index
}

func NewLessThanOrEqual(value int) *LessThanOrEqual {
	return &LessThanOrEqual{
		length: -1,
		value:  value,
	}
}

func (c *LessThanOrEqual) Key() string {
	return "<=" + strconv.Itoa(c.value)
}

func (c *LessThanOrEqual) On(index indexes.Index) {
	c.index = index
}

func (c *LessThanOrEqual) Range() (int, int) {
	return indexes.MIN, c.value
}

func (c *LessThanOrEqual) Len() int {
	if c.length == -1 {
		c.length = c.index.GetRank(c.value, false) + 1
	}
	return c.length
}

func (c *LessThanOrEqual) Contains(id key.Type) (int, bool) {
	if score, exists := c.index.Contains(id); exists && score <= c.value {
		return score, true
	}
	return 0, false
}

func (c *LessThanOrEqual) CanIterate() bool {
	return true
}

func (c *LessThanOrEqual) Iterator() indexes.Iterator {
	iterator := c.index.Forwards()
	return iterator.Range(c.Range()).Offset(0)
}
