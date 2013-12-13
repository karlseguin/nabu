package conditions

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"strconv"
)

type Equal struct {
	Value int
	index indexes.Index
}

func (c *Equal) Key() string {
	return "=" + strconv.Itoa(c.Value)
}

func (c *Equal) On(index indexes.Index) {
	c.index = index
}

func (c *Equal) Range() (int, int) {
	return c.Value, c.Value
}

//can optimize this
func (c *Equal) Len() int {
	return c.index.GetRank(c.Value, false) - c.index.GetRank(c.Value, true) + 1
}

func (c *Equal) Contains(id key.Type) bool {
	if score, exists := c.index.Contains(id); exists && score == c.Value {
		return true
	}
	return false
}
