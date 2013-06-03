package nabu

type Resource interface {
  GetId() string
  GetIndexes() []string
  GetSorts() map[string]int
}