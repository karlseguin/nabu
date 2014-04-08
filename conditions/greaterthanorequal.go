package conditions

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"strconv"
)

type GreaterThanOrEqual struct {
	key       string
	indexName string
	length    int
	value     int
	index     indexes.Index
}

func NewGreaterThanOrEqual(indexName string, value int) *GreaterThanOrEqual {
	return &GreaterThanOrEqual{
		length:    -1,
		value:     value,
		indexName: indexName,
		key:       indexName + ">=" + strconv.Itoa(value),
	}
}

func (c *GreaterThanOrEqual) Key() string {
	return c.key
}

func (c *GreaterThanOrEqual) IndexName() string {
	return c.indexName
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

func (c *GreaterThanOrEqual) CanIterate() bool {
	return true
}

func (c *GreaterThanOrEqual) Iterator() indexes.Iterator {
	iterator := c.index.Forwards()
	return iterator.Range(c.Range()).Offset(0)
}

func (c *GreaterThanOrEqual) RLock() {
	c.index.RLock()
}

func (c *GreaterThanOrEqual) RUnlock() {
	c.index.RUnlock()
}
