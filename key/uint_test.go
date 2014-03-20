package key

import (
	// "github.com/karlseguin/nabu"
	"github.com/karlseguin/gspec"
	"testing"
)

func TestGetsTheIntsBucket(t *testing.T) {
	spec := gspec.New(t)
	id1, id2, id3 := Type(234), Type(233), Type(34004)
	for i := 0; i < 101; i++ {
		spec.Expect(id1.Bucket(102)).ToEqual(30)
		spec.Expect(id2.Bucket(102)).ToEqual(29)
		spec.Expect(id3.Bucket(102)).ToEqual(38)
	}
}

func TestSerializesAnInt(t *testing.T) {
	spec := gspec.New(t)
	buffer := Type(43449).Serialize()
	defer buffer.Close()
	spec.ExpectBytes(buffer.Bytes()).ToEqual([]byte{185, 211, 2, 0, 0, 0, 0, 0, 0, 0})
}

func TestDeserializesAnint(t *testing.T) {
	spec := gspec.New(t)
	spec.Expect(Deserialize([]byte{185, 211, 2, 0, 0, 0, 0, 0, 0, 0})).ToEqual(uint(43449))
}
