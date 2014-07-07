// Persistence layer for Nabu
package storage

var NullStorage = new(nullStorage)

// storage engine interface
type Storage interface {
	// Closes the storage engine
	Close() error

	// Removes a document
	RemoveDocument(id []byte)
	RemoveMapping(id string)

	// Inserts or updates a document
	PutDocument(id, value []byte)
	PutMapping(id string, value []byte)

	// Iterate through all rows
	IterateDocuments(handler func(id, value []byte))
	IterateMappings(handler func(id string, value []byte))
}

// Creates a new storage isntance
func New(path string) Storage {
	return newSQLite(path)
}

type nullStorage struct {
}

func (s *nullStorage) Close() error {
	return nil
}

func (s *nullStorage) RemoveDocument(id []byte) {}
func (s *nullStorage) RemoveMapping(id string) {}
func (s *nullStorage) PutDocument(id, value []byte) {}
func (s *nullStorage) PutMapping(id string, value []byte) {}

func (s *nullStorage) IterateDocuments(handler func(id, value []byte)){}
func (s *nullStorage) IterateMappings(handler func(id string, value []byte)){}
