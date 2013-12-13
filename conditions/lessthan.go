package conditions

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"strconv"
)

type LessThan struct {
	Value int
	index indexes.Index
}

func (c *LessThan) Key() string {
	return "<" + strconv.Itoa(c.Value)
}

func (c *LessThan) On(index indexes.Index) {
	c.index = index
}

func (c *LessThan) Len() int {
	return c.index.GetRank(c.Value, false)
}

func (c *LessThan) Contains(id key.Type) bool {
	if score, exists := c.index.Contains(id); exists && score < c.Value {
		return true
	}
	return false
}
