package conditions

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"strconv"
)

type LessThan struct {
	value  int
	length int
	index  indexes.Index
}

func NewLessThan(value int) *LessThan {
	return &LessThan{
		length: -1,
		value:  value,
	}
}

func (c *LessThan) Key() string {
	return "<" + strconv.Itoa(c.value)
}

func (c *LessThan) On(index indexes.Index) {
	c.index = index
}

func (c *LessThan) Range() (int, int) {
	return indexes.MIN, c.value - 1
}

func (c *LessThan) Len() int {
	if c.length == -1 {
		c.length = c.index.GetRank(c.value, false)
	}
	return c.length
}

func (c *LessThan) Contains(id key.Type) (int, bool) {
	if score, exists := c.index.Contains(id); exists && score < c.value {
		return score, true
	}
	return 0, false
}

func (c *LessThan) CanIterate() bool {
	return true
}

func (c *LessThan) Iterator() indexes.Iterator {
	iterator := c.index.Forwards()
	return iterator.Range(c.Range()).Offset(0)
}
