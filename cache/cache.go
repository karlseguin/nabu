package cache

import (
	"github.com/karlseguin/nabu/indexes"
	"sync"
)

type Query interface {
	Id() string
	Reduce() indexes.Index
	Close()
}

type BuildData struct {
	id    string
	query Query
	wait  *sync.WaitGroup
}

type Cache struct {
	sync.RWMutex
	lookup      map[string]*CachedIndex
	buildQueue  chan *BuildData
	promotables chan *CachedIndex
}

func New(workerCount int) *Cache {
	c := &Cache{
		lookup:      make(map[string]*CachedIndex),
		buildQueue:  make(chan *BuildData, 128),
		promotables: make(chan *CachedIndex, 128),
	}

	for i := 0; i < workerCount; i++ {
		go c.workers()
	}
	return c
	// if workerCount > 0  { go c.maintenance() }
}

func (c *Cache) Get(query Query) (indexes.Index, bool, *sync.WaitGroup) {
	if cached, exists, wait := c.get(query); exists {
		return cached, true, wait
	}
	return nil, false, nil
}

func (c *Cache) get(query Query) (indexes.Index, bool, *sync.WaitGroup) {
	id := query.Id()
	c.RLock()
	ci, exists := c.lookup[id]
	c.RUnlock()
	if exists {
		return ci, true, nil
	}

	data := &BuildData{id, query, new(sync.WaitGroup)}
	select {
	case c.buildQueue <- data:
		data.wait.Add(1)
		return nil, false, data.wait
	default:
		return nil, false, nil
	}
}

func (c *Cache) workers() {
	for {
		select {
		case data := <-c.buildQueue:
			c.build(data)
		}
	}
}

func (c *Cache) build(data *BuildData) {
	index := data.query.Reduce()
	data.wait.Wait()
	data.query.Close()

	cachedIndex := newCachedIndex(index)
	c.Lock()
	c.lookup[data.id] = cachedIndex
	c.Unlock()
	c.promotables <- cachedIndex
}

func (c *Cache) promote(index *CachedIndex) {
	select {
	case c.promotables <- index:
	default:
	}
}
