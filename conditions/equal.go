package conditions

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"strconv"
)

type Equal struct {
	length int
	value  int
	index  indexes.Index
}

func NewEqual(value int) *Equal {
	return &Equal{
		length: -1,
		value:  value,
	}
}

func (c *Equal) Key() string {
	return "=" + strconv.Itoa(c.value)
}

func (c *Equal) On(index indexes.Index) {
	c.index = index
}

func (c *Equal) Range() (int, int) {
	return c.value, c.value
}

//can optimize this
func (c *Equal) Len() int {
	if c.length == -1 {
		c.length = c.index.GetRank(c.value, false) - c.index.GetRank(c.value, true) + 1
	}
	return c.length
}

func (c *Equal) Contains(id key.Type) bool {
	if score, exists := c.index.Contains(id); exists && score == c.value {
		return true
	}
	return false
}
