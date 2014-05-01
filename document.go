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
	id       key.Type
	stringId string
	database *Database
	IsUpdate bool
	t        string

	sortedInts     map[string]int
	sortedStrings  map[string]string
	setStrings     map[string]struct{}
	bigSetStrings  map[string]struct{}
}

func newMeta(database *Database, isUpdate bool) *Meta {
	return &Meta{
		sortedInts:    make(map[string]int),
		sortedStrings: make(map[string]string),
		setStrings:    make(map[string]struct{}),
		bigSetStrings: make(map[string]struct{}),
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
func (m *Meta) SortedInt(name string, score int) *Meta {
	m.sortedInts[name] = score
	return m
}

// Add an int-based index
func (m *Meta) SortedString(name string, score string) *Meta {
	m.sortedStrings[name] = score
	return m
}

func (m *Meta) Set(name, value string, big bool) *Meta {
	name = name + "=" + value
	if big {
		m.bigSetStrings[name] = struct{}{}
	} else {
		m.setStrings[name] = struct{}{}
	}
	return m
}
