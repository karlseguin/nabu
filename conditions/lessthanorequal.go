package conditions

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"strconv"
)

type LessThanOrEqual struct {
	key       string
	indexName string
	value     int
	length    int
	index     indexes.Ranked
}

func NewLessThanOrEqual(indexName string, value int) *LessThanOrEqual {
	return &LessThanOrEqual{
		length:    -1,
		value:     value,
		indexName: indexName,
		key:       indexName + "<=" + strconv.Itoa(value),
	}
}

func (c *LessThanOrEqual) Key() string {
	return c.key
}

func (c *LessThanOrEqual) IndexName() string {
	return c.indexName
}

func (c *LessThanOrEqual) On(index indexes.Index) {
	c.index = index.(indexes.Ranked)
}

func (c *LessThanOrEqual) Range() (int, int) {
	return indexes.MIN, c.value
}

func (c *LessThanOrEqual) Len() int {
	if c.length == -1 {
		c.length = c.index.GetRank(c.value, false) + 1
	}
	return c.length
}

func (c *LessThanOrEqual) Contains(id key.Type) bool {
	_, exists := c.Score(id)
	return exists
}

func (c *LessThanOrEqual) Score(id key.Type) (int, bool) {
	if score, exists := c.index.Score(id); exists && score <= c.value {
		return score, true
	}
	return 0, false
}

func (c *LessThanOrEqual) CanIterate() bool {
	return true
}

func (c *LessThanOrEqual) Iterator() indexes.Iterator {
	iterator := c.index.Forwards()
	return iterator.Range(c.Range()).Offset(0)
}

func (c *LessThanOrEqual) RLock() {
	c.index.RLock()
}

func (c *LessThanOrEqual) RUnlock() {
	c.index.RUnlock()
}
