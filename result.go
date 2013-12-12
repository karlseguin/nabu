package nabu

// A query's result. Close must be called once you are done with it
type Result interface {
	// The number of documents in the current results
	Len() int

	//The total number of results. -1 unless IncludeTotal was specified on the query
	Total() int

	// Whether more results are available
	HasMore() bool

	// The actual documents
	Documents() []Document

	// The document ids
	Ids() []uint

	// Releases the result
	Close()
}

// An empty result
var EmptyResult = &emptyResult{}

type emptyResult struct {
	ids       []uint
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

func (r *emptyResult) Ids() []uint {
	return r.ids
}

func (r *emptyResult) Documents() []Document {
	return r.documents
}

func (r *emptyResult) Close() {}
