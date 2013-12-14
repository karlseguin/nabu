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
	GetType() string
}

// Meta describes a document
type Meta struct {
	uintId   uint
	stringId string
	t string

	iIndexes map[string]int
}

func newMeta() *Meta {
	return &Meta{
		iIndexes: make(map[string]int),
	}
}

// The document's Id
func (m *Meta) IntId(id uint) *Meta {
	m.uintId = id
	return m
}

// The document's type
func (m *Meta) Type(t string) *Meta {
	m.t = t
	return m
}

// The document's Id
func (m *Meta) StringId(id string) *Meta {
	m.stringId = id
	return m
}

func (m *Meta) getId(idMap *IdMap) (key.Type, string) {
	if len(m.stringId) == 0 {
		return key.Type(m.uintId), ""
	}
	return idMap.get(m.stringId, true), m.stringId
}

// Add an int-based index
func (m *Meta) IndexInt(name string, score int) *Meta {
	m.iIndexes[name] = score
	return m
}
