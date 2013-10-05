package nabu

type Result interface {
  Len() int
  Total() int
  HasMore() bool
  Ids() []string
  Close()
}

var EmptyResult = &emptyResult{}

type emptyResult struct {
  empty []string
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
  return r.empty
}

func (r *emptyResult) Close() {}
