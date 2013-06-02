package nabu

import (
  "testing"
)

type S struct {
  t *testing.T
}

func Spec(t *testing.T) *S {
  return &S {
    t: t,
  }
}

func (s *S) Expect(expected ... interface{}) (expectation *Expectation) {
  return &Expectation{t: s.t, expected: expected,}
}


type Expectation struct {
  t *testing.T
  expected []interface{}
}

func (e *Expectation) ToEqual(actuals ... interface{}) {
  for index, actual := range(actuals) {
    if e.expected[index] != actual {
      e.t.Errorf("expected %+v to equal %+v", e.expected[index], actual)
    }
  }
}
