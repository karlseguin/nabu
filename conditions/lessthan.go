package conditions

import (
	"strconv"
	"github.com/karlseguin/nabu/key"
	"github.com/karlseguin/nabu/indexes"
)

type LessThan struct {
	Value int
}

func (c *LessThan) Key() string {
	return "lt" + strconv.Itoa(c.Value)
}

func (c *LessThan) Apply(index indexes.Index) map[key.Type]interface{} {
	m := make(map[key.Type]interface{})
	iterator := index.Backwards()
	defer iterator.Close()
	iterator.Range(-4294967296, c.Value-1).Offset(0)
	for id := iterator.Current(); id != key.NULL; id = iterator.Next() {
		m[id] = struct{}{}
	}
	return m
}
