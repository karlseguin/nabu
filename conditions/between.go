package conditions

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"strconv"
)

type Between struct {
	length int
	from   int
	to     int
	index  indexes.Index
}

func NewBetween(from, to int) *Between {
	return &Between{
		length: -1,
		from:   from,
		to:     to,
	}
}

func (c *Between) Key() string {
	return strconv.Itoa(c.from) + "<->" + strconv.Itoa(c.to)
}

func (c *Between) On(index indexes.Index) {
	c.index = index
}

func (c *Between) Range() (int, int) {
	return c.from, c.to
}

//can optimize this
func (c *Between) Len() int {
	if c.length == -1 {
		c.length = c.index.GetRank(c.to, false) - c.index.GetRank(c.from, true) + 1
	}
	return c.length
}

func (c *Between) Contains(id key.Type) (int, bool) {
	if score, exists := c.index.Contains(id); exists && score >= c.from && score <= c.to {
		return score, true
	}
	return 0, false
}

func (c *Between) Iterator() indexes.Iterator {
	iterator := c.index.Forwards()
	return iterator.Range(c.Range()).Offset(0)
}
