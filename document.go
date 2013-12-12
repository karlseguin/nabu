package nabu

import (
	"github.com/karlseguin/nabu/key"
)

/*
Any document stored in nabu must implement this interface:

    func (t *Tree) ReadMeta(m *nabu.Meta) {
      m.Id(key.Type(t.Id))
      m.Index("tree:borough", t.Borough)
      m.Index("tree:species", t.Species)
      m.Sort("tree:age", t.Age)
    }
*/
type Document interface {
	ReadMeta(meta *Meta)
}

// Meta describes a document
type Meta struct {
	uintId      uint
	stringId string
	sorts   map[string]int
	indexes map[string][]string
}

func newMeta() *Meta {
	return &Meta{
		sorts:   make(map[string]int),
		indexes: make(map[string][]string),
	}
}

// The document's Id
func (m *Meta) Id(id uint) *Meta {
	m.uintId = id
	return m
}

// The document's Id
func (m *Meta) StringId(id string) *Meta {
	m.stringId = id
	return m
}

func (m *Meta) getId(idMap *IdMap) key.Type {
	if len(m.stringId) == 0 {
		return key.Type(m.uintId)
	}
	return idMap.get(m.stringId, true)
}

// A document's index and value. Can be called multiple times
func (m *Meta) Index(indexName, value string) *Meta {
	index, exists := m.indexes[indexName]
	if exists == false {
		index = make([]string, 0, 1)
	}
	m.indexes[indexName] = append(index, value)
	return m
}

// A document's sort and score. Can be called Multiple times
func (m *Meta) Sort(name string, score int) *Meta {
	m.sorts[name] = score
	return m
}
