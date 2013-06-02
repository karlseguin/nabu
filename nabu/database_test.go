package nabu

import (
  "testing"
)

func TestIndexesAResource(t *testing.T) {
  spec := Spec(t)
  db := DB([]string{"x1", "x2"})
  resource := newFakeResource("1", "x1", "x2")
  db.Update(resource)
  assertDBIndex(spec, db, "x1", resource)
  assertDBIndex(spec, db, "x2", resource)
  assertResources(spec, db, resource)
}

func TestRemovesAResource(t *testing.T) {
  spec := Spec(t)
  db := DB([]string{"x1", "x2"})
  resource := newFakeResource("1", "x1", "x2")
  db.Update(resource)
  db.Remove(resource.GetId())
  assertDBIndex(spec, db, "x1")
  assertDBIndex(spec, db, "x2")
  assertResources(spec, db)
}

func TestUpdatesAnIndex(t *testing.T) {
  spec := Spec(t)
  db := DB([]string{"x1", "x2", "x3"})
  db.Update(newFakeResource("1", "x1", "x2"))
  resource := newFakeResource("1", "x1", "x3")
  db.Update(resource)
  assertDBIndex(spec, db, "x1", resource)
  assertDBIndex(spec, db, "x2")
  assertDBIndex(spec, db, "x3", resource)
  assertResources(spec, db, resource)
}

func TestIndexesMultipleResources(t *testing.T) {
  spec := Spec(t)
  db := DB([]string{"x1", "x2", "x3", "x4"})
  resource1 := newFakeResource("1", "x1", "x2")
  resource2 := newFakeResource("2", "x1", "x3", "x4")
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
  db := DB([]string{"x1"})
  resource := newFakeResource("1", "x1", "x2")
  db.Update(resource)
  assertDBIndex(spec, db, "x1", resource)
  spec.Expect(len(db.indexes)).ToEqual(1)
}

func assertDBIndex(spec *S, db *Database, index string, resources ...Resource) {
  idx := db.indexes[index]
  for _, resource := range resources {
    spec.Expect(idx.Exists(resource.GetId())).ToEqual(true)
  }
  spec.Expect(idx.Count()).ToEqual(len(resources))
}

func assertResources(spec *S, db *Database, resources ...Resource) {
  for _, resource := range resources {
    spec.Expect(db.resources[resource.GetId()]).ToEqual(resource)
  }
}

type fakeResource struct {
  id string
  indexes []string
}

func newFakeResource(id string, indexes ...string) *fakeResource {
  return &fakeResource {
    id: id,
    indexes: indexes,
  }
}

func (r *fakeResource) GetId() string {
  return r.id
}

func (r *fakeResource) GetIndexes() []string {
  return r.indexes
}