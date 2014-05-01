package conditions

import (
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/key"
	"sort"
	"strings"
)

type Union struct {
	key        string
	indexCount int
	values     []string
	indexes    indexes.Indexes
}

func NewUnion(indexName string, values []string) *Union {
	u := &Union{
		values:  values,
		indexes: make(indexes.Indexes, len(values)),
		key:     indexName + " in (" + strings.Join(values, ",") + ")",
	}
	for i, l := 0, len(values); i < l; i++ {
		values[i] = indexName + "=" + values[i]
	}
	return u
}

func (c *Union) Key() string {
	return c.key
}

func (c *Union) IndexName() string {
	return ""
}

func (c *Union) IndexNames() []string {
	return c.values
}

func (c *Union) On(index indexes.Index) {
	c.indexes[c.indexCount] = index
	c.indexCount++
	if c.indexCount == len(c.values) {
		sort.Sort(c.indexes)
	}
}

func (c *Union) Range() (int, int) {
	return 0, c.Len()
}

//This is wrong. The length should be the sum of all unique values across
//all indexes. However, since CanIterate() return false, it doesn't matter.
//Still, would be nice to have this be right and be able to iterate!
func (c *Union) Len() int {
	return c.indexes[c.indexCount-1].Len()
}

func (c *Union) Contains(id key.Type) bool {
	for _, index := range c.indexes {
		if index.Contains(id) {
			return true
		}
	}
	return false
}

func (c *Union) CanIterate() bool {
	return false
}

func (c *Union) Iterator() indexes.Iterator {
	return nil
}

func (c *Union) RLock() {
	c.indexes.RLock()
}

func (c *Union) RUnlock() {
	c.indexes.RUnlock()
}
