package nabu

import (
  "testing"
)

func TestASingleFilter(t *testing.T) {
  spec := Spec(t)
  query := newQuery([]string{"a", "b", "c", "d", "e"}, []string{"a", "b", "c"})
  ids := query.Result()
  assertQueryResult(spec, ids, "a", "b", "c")
}

func TestFiltersTwoIndexes(t *testing.T) {
  spec := Spec(t)
  query := newQuery([]string{"a", "b", "c", "d", "e"}, []string{"a", "b", "c", "d", "e", "f"}, []string{"b", "d", "e", "g"})
  ids := query.Result()
  assertQueryResult(spec, ids, "b", "d", "e")
}

func TestAppliesALimit(t *testing.T) {
  spec := Spec(t)
  query := newQuery([]string{"a", "b", "c", "d", "e"}, []string{"a", "b", "c", "d", "e", "f"}, []string{"b", "d", "e", "g"})
  ids := query.Limit(2).Result()
  assertQueryResult(spec, ids, "b", "d")
}

func newQuery(order []string, indexNames ...[]string) *Query {
  indexes := make([]*Set, len(indexNames))
  for i, ids := range indexNames {
    index := NewIndex()
    for _, id := range ids {
      index.Add(id)
    }
    indexes[i] = index.(*Set)
  }
  sort := NewSortedIndex()
  for i, id := range order {
    sort.Set(i, id)
  }
  return NewQuery(nil, sort, indexes)
}

func assertQueryResult(spec *S, actuals []string, expected ...string) {
  spec.Expect(len(actuals)).ToEqual(len(expected))
  for i, actual := range actuals {
    spec.Expect(actual).ToEqual(expected[i])
  }
}