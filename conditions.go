package nabu

import (
	"github.com/karlseguin/nabu/key"
	"github.com/karlseguin/nabu/indexes"
	"github.com/karlseguin/nabu/conditions"
)

type Condition interface {
	Key() string
	Apply(index indexes.Index) map[key.Type]interface{}
}

func GT(value int) Condition {
	return &conditions.GreaterThan{value}
}

func LT(value int) Condition {
	return &conditions.LessThan{value}
}
