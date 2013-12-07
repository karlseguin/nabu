package cache

import (
	"github.com/karlseguin/gspec"
	"testing"
	"time"
)

func TestBuildsTheCachedItem(t *testing.T) {
	spec := gspec.New(t)
	item := newItem(newFetcher(), "testItem", []string{"a", "b"})
	item.build()
	spec.Expect(len(item.index[0].Ids)).ToEqual(2)
	spec.Expect(item.index[0].Contains("b")).ToEqual(true)
	spec.Expect(item.index[0].Contains("d")).ToEqual(true)
}

func TestANewItemIsNotReadyUntilBuilt(t *testing.T) {
	spec := gspec.New(t)
	item := newItem(newFetcher(), "testItem", []string{"a", "b"})
	spec.Expect(item.readyAndPromotable()).ToEqual(false)
	item.build()
	spec.Expect(item.readyAndPromotable()).ToEqual(true)
}

func TestAddedItemWhichIsNotAMatch(t *testing.T) {
	spec := gspec.New(t)
	item := newItem(newFetcher(), "testItem", []string{"a", "b"})
	item.build()
	item.change(&Change{id: "z", added: true, indexName: "B"})
	spec.Expect(len(item.index[0].Ids)).ToEqual(2)
}

func TestAddedItemWhichIsAMatch(t *testing.T) {
	spec := gspec.New(t)
	item := newItem(newFetcher(), "testItem", []string{"a", "b"})
	item.build()
	item.sources[0].Add("a")
	item.change(&Change{id: "a", added: true, indexName: "B"})
	spec.Expect(len(item.index[0].Ids)).ToEqual(3)
}

func TestRemoveItemWhichDoesNotExist(t *testing.T) {
	spec := gspec.New(t)
	item := newItem(newFetcher(), "testItem", []string{"a", "b"})
	item.build()
	item.change(&Change{id: "y", added: false, indexName: "B"})
	spec.Expect(len(item.index[0].Ids)).ToEqual(2)
}

func TestRemoveItemWhichExists(t *testing.T) {
	spec := gspec.New(t)
	item := newItem(newFetcher(), "testItem", []string{"a", "b"})
	item.build()
	item.change(&Change{id: "b", added: false, indexName: "B"})
	spec.Expect(len(item.index[0].Ids)).ToEqual(1)
}

func TestRecentlyPromotedItemShouldNotBePromoted(t *testing.T) {
	spec := gspec.New(t)
	item := newItem(newFetcher(), "testItem", []string{"a", "b"})
	promoted := time.Now().Add(time.Second * -58)
	item.promoted = promoted
	_, promotable := item.readyAndPromotable()
	spec.Expect(promotable).ToEqual(false)
	spec.Expect(item.promoted).ToEqual(promoted)
}

func TestStaleItemShouldBePromoted(t *testing.T) {
	spec := gspec.New(t)
	item := newItem(newFetcher(), "testItem", []string{"a", "b"})
	promoted := time.Now().Add(time.Second * -62)
	item.promoted = promoted
	now := time.Now()
	_, promotable := item.readyAndPromotable()
	spec.Expect(promotable).ToEqual(true)
	spec.Expect(item.promoted.After(now)).ToEqual(true)
}
