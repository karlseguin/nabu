package conditions

import (
	"strconv"
	"github.com/karlseguin/nabu/key"
	"github.com/karlseguin/nabu/indexes"
)

type GreaterThan struct {
	Value int
}

func (c *GreaterThan) Key() string {
	return "gt" + strconv.Itoa(c.Value)
}

func (c *GreaterThan) Apply(index indexes.Index) map[key.Type]interface{} {
	m := make(map[key.Type]interface{})
	iterator := index.Forwards()
	defer iterator.Close()
	iterator.Range(c.Value+1, 4294967296).Offset(0)
	for id := iterator.Current(); id != key.NULL; id = iterator.Next() {
		m[id] = struct{}{}
	}
	return m
}
