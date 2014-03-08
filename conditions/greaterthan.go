package conditions

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"strconv"
)

type GreaterThan struct {
	length int
	value  int
	index  indexes.Index
}

func NewGreaterThan(value int) *GreaterThan {
	return &GreaterThan{
		length: -1,
		value:  value,
	}
}

func (c *GreaterThan) Key() string {
	return ">" + strconv.Itoa(c.value)
}

func (c *GreaterThan) On(index indexes.Index) {
	c.index = index
}

func (c *GreaterThan) Range() (int, int) {
	return c.value + 1, indexes.MAX
}

func (c *GreaterThan) Len() int {
	if c.length == -1 {
		c.length = c.index.Len() - c.index.GetRank(c.value+1, true)
	}
	return c.length
}

func (c *GreaterThan) Contains(id key.Type) (int, bool) {
	if score, exists := c.index.Contains(id); exists && score > c.value {
		return score, true
	}
	return 0, false
}

func (c *GreaterThan) CanIterate() bool {
	return true
}

func (c *GreaterThan) Iterator() indexes.Iterator {
	iterator := c.index.Forwards()
	return iterator.Range(c.Range()).Offset(0)
}
