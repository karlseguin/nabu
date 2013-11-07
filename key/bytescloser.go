package key

type BytesCloser interface {
  Close()
  Bytes() []byte
}

type ByteWrapper []byte

func (bw ByteWrapper) Bytes() []byte {
  return bw
}
func (bw ByteWrapper) Close() {}
