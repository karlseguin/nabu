package conditions

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"strconv"
)

type GreaterThan struct {
	Value int
	index indexes.Index
}

func (c *GreaterThan) Key() string {
	return ">" + strconv.Itoa(c.Value)
}

func (c *GreaterThan) On(index indexes.Index) {
	c.index = index
}

func (c *GreaterThan) Len() int {
	return c.index.Len() - c.index.GetRank(c.Value+1, true)
}

func (c *GreaterThan) Contains(id key.Type) bool {
	if score, exists := c.index.Contains(id); exists && score > c.Value {
		return true
	}
	return false
}
