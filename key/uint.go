package key

import (
	"encoding/binary"
	"strconv"
	"github.com/karlseguin/nabu/bytepool"
)

var serializationPool = bytepool.New(256, binary.MaxVarintLen64)

// Integer based key
type Type uint64

//ugh
const NULL Type = 18446744073709551615

// Determines which bucket a key belongs to
func (t Type) Bucket(count int) int {
	return int(uint(t) % uint(count))
}

// Serializes a key for storage
func (t Type) Serialize() BytesCloser {
	buffer := serializationPool.Checkout()
	binary.PutUvarint(buffer.Bytes(), uint64(t))
	return buffer
}

// Deserializes the key from storage
func Deserialize(raw []byte) uint {
	if raw[0] == '|' {
		value, _ := strconv.Atoi(string(raw[1:]))
		return uint(value)
	}
	value, _ := binary.Uvarint(raw)
	return uint(value)
}
