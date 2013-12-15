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
	id       key.Type
	stringId string
	database *Database
	IsUpdate bool
	t        string

	iIndexes map[string]int
}

func newMeta(database *Database, isUpdate bool) *Meta {
	return &Meta{
		iIndexes: make(map[string]int),
		database: database,
		IsUpdate: isUpdate,
	}
}

// The document's Id
func (m *Meta) IntId(id uint) *Meta {
	m.id = key.Type(id)
	return m
}

// The document's type
func (m *Meta) Type(t string) *Meta {
	m.t = t
	return m
}

// The document's Id
func (m *Meta) StringId(stringId string) uint {
	m.id = m.database.idMap.get(stringId, true)
	m.stringId = stringId
	return uint(m.id)
}

func (m *Meta) getId() (key.Type, string) {
	return m.id, m.stringId
}

// Add an int-based index
func (m *Meta) IndexInt(name string, score int) *Meta {
	m.iIndexes[name] = score
	return m
}
