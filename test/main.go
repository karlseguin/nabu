package main

import (
  "nabu/nabu"
  "time"
  "strconv"
  "math/rand"
)

func main() {
  db := nabu.DB()
  for i := 0; i < 250000; i++ {
    var indexes []string
    for j := 1; j < 5; j++ {
      if rand.Int31n(4) == 0 {
        indexes = append(indexes, "x:" + strconv.Itoa(j))
      }
    }
    db.Update(New(strconv.Itoa(i), indexes))
  }
  start := time.Now()
  query := db.Find("x:1", "x:2")
  println(len(query.Limit(10).Result()))
  println(time.Since(start).Nanoseconds() / 1000)
}

func New(id string, indexes []string) *Resource{
  return &Resource {
    id: id,
    indexes: indexes,
  }
}

type Resource struct {
  id string
  indexes []string
}

func (r *Resource) GetId() string {
  return r.id
}

func (r *Resource) GetIndexes() []string {
  return r.indexes
}