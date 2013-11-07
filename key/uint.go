package key

// import (
//   "nabu/bytepool"
//   "encoding/binary"
// )

// var serializationPool = bytepool.New(256, binary.MaxVarintLen32)

// type Type uint

// const NULL Type = 0

// func (t Type) Bucket(count int) int {
//   return int(uint(t) % uint(count))
// }

// func (t Type) Serialize() BytesCloser {
//   buffer := serializationPool.Checkout()
//   binary.PutUvarint(buffer.Bytes(), uint64(t))
//   return buffer
// }
