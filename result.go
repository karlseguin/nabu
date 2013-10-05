package nabu

type Result interface {
  Len() int
  Total() int
  HasMore() bool
  Ids() []string
  Docs() []Document
  Close()
}

var EmptyResult = &emptyResult{}

type emptyResult struct {
  ids []string
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

func (r *emptyResult) Ids() []string {
  return r.ids
}

func (r *emptyResult) Docs() []Document {
  return r.documents
}

func (r *emptyResult) Close() {}
