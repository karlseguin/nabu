package conditions

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"strconv"
)

type Between struct {
	key       string
	indexName string
	length    int
	from      int
	to        int
	index     indexes.Index
}

func NewBetween(indexName string, from, to int) *Between {
	return &Between{
		length:    -1,
		from:      from,
		to:        to,
		indexName: indexName,
		key:       strconv.Itoa(from) + "<" + indexName + "<" + strconv.Itoa(to),
	}
}

func (c *Between) Key() string {
	return c.key
}

func (c *Between) IndexName() string {
	return c.indexName
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

func (c *Between) CanIterate() bool {
	return true
}

func (c *Between) Iterator() indexes.Iterator {
	iterator := c.index.Forwards()
	return iterator.Range(c.Range()).Offset(0)
}

func (c *Between) RLock() {
	c.index.RLock()
}

func (c *Between) RUnlock() {
	c.index.RUnlock()
}
