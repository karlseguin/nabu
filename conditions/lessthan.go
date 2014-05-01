package conditions

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"strconv"
)

type LessThan struct {
	key       string
	indexName string
	value     int
	length    int
	index     indexes.Ranked
}

func NewLessThan(indexName string, value int) *LessThan {
	return &LessThan{
		length:    -1,
		value:     value,
		indexName: indexName,
		key:       indexName + "<" + strconv.Itoa(value),
	}
}

func (c *LessThan) Key() string {
	return c.key
}

func (c *LessThan) IndexName() string {
	return c.indexName
}

func (c *LessThan) On(index indexes.Index) {
	c.index = index.(indexes.Ranked)
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

func (c *LessThan) Contains(id key.Type) bool {
	_, exists := c.Score(id)
	return exists
}

func (c *LessThan) Score(id key.Type) (int, bool) {
	if score, exists := c.index.Score(id); exists && score < c.value {
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

func (c *LessThan) RLock() {
	c.index.RLock()
}

func (c *LessThan) RUnlock() {
	c.index.RUnlock()
}
