package key

import (
	"hash/fnv"
)

// String type key
type Type string

const NULL Type = ""

// Calculates which bucket the key is in
func (t Type) Bucket(count int) int {
	h := fnv.New32a()
	h.Write([]byte(t))
	return int(h.Sum32() % uint32(count))
}

// Serializes the key for storage
func (t Type) Serialize() BytesCloser {
	return ByteWrapper(t)
}

// Deserializes the key from storage
func Deserialize(raw []byte) Type {
	return Type(raw)
}
