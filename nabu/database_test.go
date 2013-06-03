package nabu

import (
  "strconv"
  "testing"
  "math/rand"
)

func TestIndexesAResource(t *testing.T) {
  spec := Spec(t)
  db := DB([]string{"x1", "x2"}, []string{})
  resource := newFakeResource([]string{"1", "x1", "x2"}, nil)
  db.Update(resource)
  assertDBIndex(spec, db, "x1", resource)
  assertDBIndex(spec, db, "x2", resource)
  assertResources(spec, db, resource)
}

func TestRemovesAResource(t *testing.T) {
  spec := Spec(t)
  db := DB([]string{"x1", "x2"}, []string{})
  resource := newFakeResource([]string{"1", "x1", "x2"}, nil)
  db.Update(resource)
  db.Remove(resource.GetId())
  assertDBIndex(spec, db, "x1")
  assertDBIndex(spec, db, "x2")
  assertResources(spec, db)
}

func TestUpdatesAnIndex(t *testing.T) {
  spec := Spec(t)
  db := DB([]string{"x1", "x2", "x3"}, []string{})
  resource := newFakeResource([]string{"1", "x1", "x2"}, nil)
  resource.id = "11"
  db.Update(resource)
  resource = newFakeResource([]string{"1", "x1", "x3"}, nil)
  resource.id = "11"
  db.Update(resource)
  assertDBIndex(spec, db, "x1", resource)
  assertDBIndex(spec, db, "x2")
  assertDBIndex(spec, db, "x3", resource)
  assertResources(spec, db, resource)
}

func TestIndexesMultipleResources(t *testing.T) {
  spec := Spec(t)
  db := DB([]string{"x1", "x2", "x3", "x4"}, []string{})
  resource1 := newFakeResource([]string{"1", "x1", "x2"}, nil)
  resource2 := newFakeResource([]string{"2", "x1", "x3", "x4"}, nil)
  db.Update(resource1)
  db.Update(resource2)
  assertDBIndex(spec, db, "x1", resource1, resource2)
  assertDBIndex(spec, db, "x2", resource1)
  assertDBIndex(spec, db, "x3", resource2)
  assertDBIndex(spec, db, "x4", resource2)
  assertResources(spec, db, resource1, resource2)
}

func TestSilentlyIgnoresUnknownIndexes(t *testing.T) {
  spec := Spec(t)
  db := DB([]string{"x1"}, []string{})
  resource := newFakeResource([]string{"1", "x1", "x2"}, nil)
  db.Update(resource)
  assertDBIndex(spec, db, "x1", resource)
  spec.Expect(len(db.indexes)).ToEqual(1)
}

func TestAddsSortedIndexes(t *testing.T) {
  spec := Spec(t)
  db := DB([]string{}, []string{"s1"})
  resource := newFakeResource(nil, map[string]int{"s1": 4})
  db.Update(resource)
  assertDBSortIndex(spec, db, "s1", resource)
}

func TestAddsMultipleSortedIndexes(t *testing.T) {
  spec := Spec(t)
  db := DB([]string{}, []string{"s1"})
  resource1 := newFakeResource(nil, map[string]int{"s1": 4})
  resource2 := newFakeResource(nil, map[string]int{"s1": 2})
  db.Update(resource1)
  db.Update(resource2)
  assertDBSortIndex(spec, db, "s1", resource2, resource1)
}

func TestRemovesASortIndex(t *testing.T) {
  spec := Spec(t)
  db := DB([]string{}, []string{"s1"})
  resource1 := newFakeResource(nil, map[string]int{"s1": 4})
  resource2 := newFakeResource(nil, map[string]int{"s1": 2})
  db.Update(resource1)
  db.Update(resource2)
  db.Remove(resource1.GetId())
  assertDBSortIndex(spec, db, "s1", resource2)
}

func TestUpdatesASortIndex(t *testing.T) {
  spec := Spec(t)
  db := DB([]string{}, []string{"s1", "s2", "s3"})
  resource := newFakeResource(nil, map[string]int{"s1": 4, "s2": 5})
  resource.id = "1"
  db.Update(resource)

  resource = newFakeResource(nil, map[string]int{"s1": 3, "s3": 5})
  resource.id = "1"
  db.Update(resource)

  assertDBSortIndex(spec, db, "s1", resource)
  assertDBSortIndex(spec, db, "s2")
  assertDBSortIndex(spec, db, "s3", resource)
}

func assertDBIndex(spec *S, db *Database, index string, resources ...Resource) {
  idx := db.indexes[index]
  for _, resource := range resources {
    spec.Expect(idx.Exists(resource.GetId())).ToEqual(true)
  }
  spec.Expect(idx.Count()).ToEqual(len(resources))
}

func assertDBSortIndex(spec *S, db *Database, index string, resources ...Resource) {
  iterator := db.sorts[index].Forward()
  for _, resource := range resources {
    _, id := iterator.Current()
    spec.Expect(id).ToEqual(resource.GetId())
    iterator.Next()
  }
  iterator.Close()
}

func assertResources(spec *S, db *Database, resources ...Resource) {
  for _, resource := range resources {
    spec.Expect(db.resources[resource.GetId()]).ToEqual(resource)
  }
}

type fakeResource struct {
  id string
  indexes []string
  sorts map[string]int
}

func newFakeResource(indexes []string, sorts map[string]int) *fakeResource {
  return &fakeResource {
    id: strconv.Itoa(rand.Int()),
    indexes: indexes,
    sorts: sorts,
  }
}

func (r *fakeResource) GetId() string {
  return r.id
}

func (r *fakeResource) GetIndexes() []string {
  if r.indexes == nil { return make([]string, 0) }
  return r.indexes
}

func (r *fakeResource) GetSorts() map[string]int {
  if r.sorts == nil { return make(map[string]int, 0) }
  return r.sorts
}