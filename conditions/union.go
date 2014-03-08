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

func NewUnion(values []string) *Union {
	return &Union{
		values:  values,
		indexes: make(indexes.Indexes, len(values)),
		key:     "in (" + strings.Join(values, ",") + ")",
	}
}

func (c *Union) Key() string {
	return c.key
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

//our longest index is the length of this composite
func (c *Union) Len() int {
	return c.indexes[c.indexCount-1].Len()
}

func (c *Union) Contains(id key.Type) (int, bool) {
	for _, index := range c.indexes {
		if score, exists := index.Contains(id); exists {
			return score, true
		}
	}
	return 0, false
}

func (c *Union) CanIterate() bool {
	return false
}

func (c *Union) Iterator() indexes.Iterator {
	return nil
}
