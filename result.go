package nabu

import (
	"github.com/karlseguin/nabu/key"
)

// A query's result. Close must be called once you are done with it
type Result interface {
	// The number of documents in the current results
	Len() int

	//The total number of results. -1 unless IncludeTotal was specified on the query
	Total() int

	// Whether more results are available
	HasMore() bool

	// The actual documents
	Docs() []Document

	// The document ids
	Ids() []key.Type

	// Releases the result
	Close()
}

// An empty result
var EmptyResult = &emptyResult{}

type emptyResult struct {
	ids       []key.Type
	documents []Document
}

func (r *emptyResult) Len() int {
	return 0
}

func (r *emptyResult) Total() int {
	return 0
}

func (r *emptyResult) HasMore() bool {
	return false
}

func (r *emptyResult) Ids() []key.Type {
	return r.ids
}

func (r *emptyResult) Docs() []Document {
	return r.documents
}

func (r *emptyResult) Close() {}
