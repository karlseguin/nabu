package nabu

import (
  "sort"
  "testing"
)

func TestIgnoresInvalidIndex(t *testing.T) {
  spec := Spec(t)
  query := newQuery([]string{"a", "b", "c", "d", "e", "f"}, []string{"b", "d", "e", "g"})
  query.indexes = append(query.indexes, nil)
  ids := query.Result()
  sort.Strings(ids)
  assertQueryResult(spec, ids, "b", "d", "e")
}

func TestFiltersTwoIndexes(t *testing.T) {
  spec := Spec(t)
  query := newQuery([]string{"a", "b", "c", "d", "e", "f"}, []string{"b", "d", "e", "g"})
  ids := query.Result()
  sort.Strings(ids)
  assertQueryResult(spec, ids, "b", "d", "e")
}

// impossible to test until the sorting is in
// func TestAppliesALimit(t *testing.T) {
//   spec := Spec(t)
//   query := newQuery([]string{"a", "b", "c", "d", "e", "f"}, []string{"b", "d", "e", "g"})
//   ids := query.Limit(2).Result()
//   sort.Strings(ids)
//   assertQueryResult(spec, ids, "b", "d")
// }

func newQuery(indexes ...[]string) *Query {
  query := &Query {
    indexes: make([]Index, len(indexes)),
  }
  for i, index := range indexes {
    idx := NewIndex()
    for _, id := range index {
      idx.Add(id)
    }
    query.indexes[i] = idx
  }
  return query
}

func assertQueryResult(spec *S, actuals []string, expected ...string) {
  spec.Expect(len(actuals)).ToEqual(len(expected))
  for i, actual := range actuals {
    spec.Expect(actual).ToEqual(expected[i])
  }
}