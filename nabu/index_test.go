package nabu

import (
  "testing"
)

func TestAddsAnIdToTheIndex(t *testing.T) {
  spec := Spec(t)
  idx := NewIndex()
  idx.Add("9001")
  spec.Expect(idx.Exists("9001")).ToEqual(true)
}

func TestCanAddTheIdTwice(t *testing.T) {
  spec := Spec(t)
  idx := NewIndex()
  idx.Add("9002")
  idx.Add("9002")
  spec.Expect(idx.Exists("9002")).ToEqual(true)
  spec.Expect(idx.Count()).ToEqual(1)
}

func TestRemovesTheId(t *testing.T) {
  spec := Spec(t)
  idx := NewIndex()
  idx.Add("9001")
  idx.Remove("9001")
  spec.Expect(idx.Exists("9001")).ToEqual(false)
  spec.Expect(idx.Count()).ToEqual(0)
}

func TestRemoveAnInvalidId(t *testing.T) {
  spec := Spec(t)
  idx := NewIndex()
  idx.Remove("9004")
  spec.Expect(idx.Exists("9001")).ToEqual(false)
  spec.Expect(idx.Count()).ToEqual(0)
}
