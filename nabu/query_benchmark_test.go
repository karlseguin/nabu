package nabu

import (
  "strconv"
  "testing"
  "math/rand"
)

func BenchmarkQueryResultOfTwoLargeIndexes(b *testing.B) {
  var index1 []string
  var index2 []string
  for i := 0; i < 250000; i++ {
    id := strconv.Itoa(i)
    if rand.Int31n(2) == 0 { index1 = append(index1, id)}
    if rand.Int31n(3) == 0 { index2 = append(index2, id)}
  }
  query := newQuery(index1, index2)
  b.ResetTimer()
  for i := 0; i < b.N; i++ {
    query.Result()
  }
}