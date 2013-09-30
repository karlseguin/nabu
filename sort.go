package nabu

type Sort struct {
  list []string
  lookup map[string]int
}

func (s *Sort) Len() int {
  return len(s.list)
}
