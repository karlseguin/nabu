package key

// import (
//   "encoding/binary"
//   "github.com/karlseguin/nabu/bytepool"
// )

// var serializationPool = bytepool.New(256, binary.MaxVarintLen32)

// // Integer based key
// type Type uint64

// const NULL Type = 0

// // Determines which bucket a key belongs to
// func (t Type) Bucket(count int) int {
//   return int(uint(t) % uint(count))
// }

// // Serializes a key for storage
// func (t Type) Serialize() BytesCloser {
//   buffer := serializationPool.Checkout()
//   binary.PutUvarint(buffer.Bytes(), uint64(t))
//   return buffer
// }

// // Deserializes the key from storage
// func Deserialize(raw []byte) Type {
//   value, _ := binary.Uvarint(raw)
//   return Type(value)
// }
