package key

import (
  "hash/fnv"
)

type Type string

const NULL Type = ""

func (t Type) Bucket(count int) int {
  h := fnv.New32a()
  h.Write([]byte(t))
  return int(h.Sum32() % uint32(count))
}

func (t Type) Serialize() BytesCloser {
  return ByteWrapper(t)
}

func Deserialize(raw []byte) Type {
  return Type(raw)
}
