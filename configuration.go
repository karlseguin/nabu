package nabu

type Configuration struct {
  maxLimit int
  maxTotal int
  dbPath string
  bucketCount int
  defaultLimit int
  cacheWorkers int
  queryPoolSize int
  maxUnsortedSize int
  maxIndexesPerQuery int
  sortedResultPoolSize int
  unsortedResultPoolSize int
}

func Configure() *Configuration {
  return &Configuration {
    maxLimit: 100,
    maxTotal: 1000,
    bucketCount: 25,
    cacheWorkers: 2,
    defaultLimit: 10,
    dbPath: "./data/",
    queryPoolSize: 512,
    maxUnsortedSize: 2500,
    maxIndexesPerQuery: 10,
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

func (c *Configuration) MaxTotal(max int) *Configuration {
  c.maxTotal = max
  return c
}

func (c *Configuration) BucketCount(bucketCount int) *Configuration {
  c.bucketCount = bucketCount
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

func (c *Configuration) DbPath(path string) *Configuration {
  c.dbPath = path
  return c
}

func (c *Configuration) ResultsPoolSize(sorted, unsorted int) *Configuration {
  c.sortedResultPoolSize = sorted
  c.unsortedResultPoolSize = unsorted
  return c
}

func (c *Configuration) MaxIndexesPerQuery(max int) *Configuration {
  c.maxIndexesPerQuery = max
  return c
}

func (c *Configuration) CacheWorkers(workers int) *Configuration {
  c.cacheWorkers = workers
  return c
}
