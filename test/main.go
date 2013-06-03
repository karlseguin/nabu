package main

import (
  "fmt"
  "time"
  "strconv"
  "nabu/nabu"
  "math/rand"
)

func main() {
  db := nabu.DB([]string{"x:1", "x:2", "x:3", "x:4"}, []string{"s:1", "s:2"})

  for i := 0; i < 250000; i++ {
    var indexes []string
    for j := 1; j < 5; j++ {
      if rand.Int31n(4) == 0 {
        indexes = append(indexes, "x:" + strconv.Itoa(j))
      }
    }
    sorts := map[string]int {
      "s:1": i,
      "s:2": rand.Int(),
    }
    db.Update(New(strconv.Itoa(i), indexes, sorts))
  }
  start := time.Now()
  query := db.Find("s:1", "x:1", "x:2")
  fmt.Println(query.Limit(10).Result())
  println(time.Since(start).Nanoseconds() / 1000)
}

func New(id string, indexes []string, sorts map[string]int) *Resource{
  return &Resource {
    id: id,
    indexes: indexes,
    sorts: sorts,
  }
}

type Resource struct {
  id string
  indexes []string
  sorts map[string]int
}

func (r *Resource) GetId() string {
  return r.id
}

func (r *Resource) GetIndexes() []string {
  return r.indexes
}

func (r *Resource) GetSorts() map[string]int {
  return r.sorts
}