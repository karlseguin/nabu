package nabu

import (
  "github.com/karlseguin/nabu/key"
)

type Result interface {
  Len() int
  Total() int
  HasMore() bool
  Docs() []Document
  Ids() []key.Type
  Close()
}

var EmptyResult = &emptyResult{}

type emptyResult struct {
  ids []key.Type
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
