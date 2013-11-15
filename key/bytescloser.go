package key

// Interface used by key serialization
type BytesCloser interface {
  Close()
  Bytes() []byte
}

// Wraps a string key in a dummy ByteCloser implementation
type ByteWrapper []byte

// The serialized bytes
func (bw ByteWrapper) Bytes() []byte {
  return bw
}

func (bw ByteWrapper) Close() {}
