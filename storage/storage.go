// Persistence layer for Nabu
package storage

var NullStorage = new(nullStorage)

// storage engine interface
type Storage interface {
	// Closes the storage engine
	Close() error

	// Removes a document
	Remove(id []byte)

	// Inserts or updates a document
	Put(id, value []byte)

	// Returns an iterator used to load all documents
	Iterator() Iterator
}

// Iterator to loop through all persisted key=>values
type Iterator interface {
	Close()
	Next() bool
	Current() ([]byte, []byte)
}

// Creates a new storage isntance
func New(path string) Storage {
	return newLeveldb(path)
}

type nullStorage struct {
}

func (s *nullStorage) Close() error {
	return nil
}

func (s *nullStorage) Remove(id []byte) {

}

func (s *nullStorage) Put(id, value []byte) {

}

func (s *nullStorage) Iterator() Iterator {
	return nil
}
