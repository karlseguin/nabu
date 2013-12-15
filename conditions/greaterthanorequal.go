package conditions

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"strconv"
)

type GreaterThanOrEqual struct {
	length int
	value  int
	index  indexes.Index
}

func NewGreaterThanOrEqual(value int) *GreaterThanOrEqual {
	return &GreaterThanOrEqual{
		length: -1,
		value:  value,
	}
}

func (c *GreaterThanOrEqual) Key() string {
	return ">=" + strconv.Itoa(c.value)
}

func (c *GreaterThanOrEqual) On(index indexes.Index) {
	c.index = index
}

func (c *GreaterThanOrEqual) Range() (int, int) {
	return c.value, indexes.MAX
}

func (c *GreaterThanOrEqual) Len() int {
	if c.length == -1 {
		c.length = c.index.Len() - c.index.GetRank(c.value, true)
	}
	return c.length
}

func (c *GreaterThanOrEqual) Contains(id key.Type) (int, bool) {
	if score, exists := c.index.Contains(id); exists && score >= c.value {
		return score, true
	}
	return 0, false
}

func (c *GreaterThanOrEqual) Iterator() indexes.Iterator {
	iterator := c.index.Forwards()
	return iterator.Range(c.Range()).Offset(0)
}
