package conditions

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
)

type Set struct {
	key       string
	indexName string
	value     string
	index     indexes.Index
}

func NewSet(indexName string, value string) *Set {
	return &Set{
		value:     value,
		indexName: indexName,
		key:       indexName + "=s=" + value,
	}
}

func (c *Set) Key() string {
	return c.key
}

func (c *Set) IndexName() string {
	return c.indexName
}

func (c *Set) On(index indexes.Index) {
	c.index = index
}

func (c *Set) Range() (int, int) {
	return 0, c.Len()
}

func (c *Set) Len() int {
	return c.index.Len()
}

func (c *Set) Contains(id key.Type) (int, bool) {
	if score, exists := c.index.Contains(id); exists {
		return score, true
	}
	return 0, false
}

func (c *Set) CanIterate() bool {
	return true
}

func (c *Set) Iterator() indexes.Iterator {
	return c.index.Forwards()
}

func (c *Set) RLock() {
	c.index.RLock()
}

func (c *Set) RUnlock() {
	c.index.RUnlock()
}
