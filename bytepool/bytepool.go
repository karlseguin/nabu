package bytepool

type Pool struct {
  buffers chan*Buffer
}

func New(count, size int) *Pool {
  p := &Pool{
    buffers: make(chan*Buffer, count),
  }
  for i := 0; i < count; i++ {
    p.buffers <- &Buffer{
      pool: p,
      bytes: make([]byte, size),
    }
  }
  return p
}

func (p *Pool) Checkout() *Buffer {
  return <- p.buffers
}

type Buffer struct {
  pool *Pool
  length int
  bytes []byte
}

func (b *Buffer) Write(p []byte) (int, error) {
  n := copy(b.bytes[b.length:], p)
  b.length += n
  return n, nil
}

func (b *Buffer) Bytes() []byte {
  return b.bytes[0:b.length]
}

func (b *Buffer) Close() {
  b.length = 0
  b.pool.buffers <- b
}
