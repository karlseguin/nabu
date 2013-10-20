package key

// UINT keys
// type Type uint
// const NULL = 0
// func (t Type) Bucket(count int) int {
//   return int(uint(t) % uint(count))
// }

// String Keys
import (
  "hash/fnv"
)
type Type string
const NULL = ""
func (t Type) Bucket(count int) int {
  h := fnv.New32a()
  h.Write([]byte(t))
  return int(h.Sum32() % uint32(count))
}
