package nabu

type Configuration struct {
  maxLimit int
  defaultLimit int
  queryPoolSize int
  bucketCount uint32
  maxUnsortedSize int
  sortedResultPoolSize int
  unsortedResultPoolSize int
}

func Configure() *Configuration {
  return &Configuration {
    maxLimit: 100,
    bucketCount: 25,
    defaultLimit: 10,
    queryPoolSize: 512,
    maxUnsortedSize: 100,
    sortedResultPoolSize: 512,
    unsortedResultPoolSize: 512,
  }
}

func (c *Configuration) DefaultLimit(limit int) *Configuration {
  c.defaultLimit = limit
  return c
}

func (c *Configuration) MaxLimit(max int) *Configuration {
  c.maxLimit = max
  return c
}

func (c *Configuration) BucketCount(bucketCount int) *Configuration {
  c.bucketCount = uint32(bucketCount)
  return c
}

func (c *Configuration) QueryPoolSize(size int) *Configuration {
  c.queryPoolSize = size
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


