package nabu

type Index map[string]struct{}

type Indexes []Index

func (indexes Indexes) Len() int {
  return len(indexes)
}

func (indexes Indexes) Less(i, j int) bool {
  return len(indexes[i]) < len(indexes[j])
}

func (indexes Indexes) Swap(i, j int) {
  x := indexes[i]
  indexes[i] = indexes[j]
  indexes[j] = x
}
