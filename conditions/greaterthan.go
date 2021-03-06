package conditions

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"strconv"
)

type GreaterThan struct {
	key       string
	indexName string
	length    int
	value     int
	index     indexes.Ranked
}

func NewGreaterThan(indexName string, value int) *GreaterThan {
	return &GreaterThan{
		length:    -1,
		value:     value,
		indexName: indexName,
		key:       indexName + ">" + strconv.Itoa(value),
	}
}

func (c *GreaterThan) Key() string {
	return c.key
}

func (c *GreaterThan) IndexName() string {
	return c.indexName
}

func (c *GreaterThan) On(index indexes.Index) {
	c.index = index.(indexes.Ranked)
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

func (c *GreaterThan) Contains(id key.Type) bool {
	_, exists := c.Score(id)
	return exists
}

func (c *GreaterThan) Score(id key.Type) (int, bool) {
	if score, exists := c.index.Score(id); exists && score > c.value {
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

func (c *GreaterThan) RLock() {
	c.index.RLock()
}

func (c *GreaterThan) RUnlock() {
	c.index.RUnlock()
}
