package nabu

type Result interface {
  Len() int
  Ids() []string
  Close()
}

var EmptyResult = &emptyResult{}

type emptyResult struct {
  empty []string
}

 func (r *emptyResult) Ids() []string {
  return r.empty
}

func (r *emptyResult) Len() int {
  return 0
}

func (r *emptyResult) Close() {}
