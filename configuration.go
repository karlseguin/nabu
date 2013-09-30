package nabu

type Configuration struct {
  maxLimit int
  defaultLimit int
  queryPoolSize int
  maxUnsortedSize int
  sortedResultPoolSize int
  unsortedResultPoolSize int
}

func Configure() *Configuration {
  return &Configuration {
    maxLimit: 100,
    defaultLimit: 10,
    queryPoolSize: 1024,
    maxUnsortedSize: 100,
    sortedResultPoolSize: 512,
    unsortedResultPoolSize: 512,
  }
}

func (c *Configuration) QueryPoolSize(size int) *Configuration {
  c.queryPoolSize = size
  return c
}

func (c *Configuration) DefaultLimit(limit int) *Configuration {
  c.defaultLimit = limit
  return c
}

func (c *Configuration) MaxLimit(max int) *Configuration {
  c.maxLimit = max
  return c
}

func (c *Configuration) MaxUnsortedSize(max int) *Configuration {
  c.maxUnsortedSize = max
  return c
}

func (c *Configuration) ResultsPoolSize(sorted, unsorted int) *Configuration {
  c.sortedResultPoolSize = sorted
  c.unsortedResultPoolSize = unsorted
  return c
}
