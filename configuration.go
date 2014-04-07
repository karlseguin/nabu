package nabu

// Configuration option for a Database. Exposes as a fluent-interface
// which begins by calling nabu.Configure()
type Configuration struct {
	maxLimit               int
	maxTotal               int
	skipLoad               bool
	dbPath                 string
	persist                bool
	iFactory               IntFactory
	sFactory               StringFactory
	bucketCount            int
	defaultLimit           int
	queryPoolSize          int
	maxUnsortedSize        int
	maxIndexesPerQuery     int
	sortedResultPoolSize   int
	unsortedResultPoolSize int
}

// Begins the configuration process.
func Configure() *Configuration {
	return &Configuration{
		maxLimit:               100,
		maxTotal:               1000,
		skipLoad:               false,
		bucketCount:            25,
		defaultLimit:           10,
		dbPath:                 "./data/",
		queryPoolSize:          512,
		maxUnsortedSize:        5000,
		maxIndexesPerQuery:     10,
		sortedResultPoolSize:   512,
		unsortedResultPoolSize: 512,
		persist:                true,
	}
}

// The default number of document to return
func (c *Configuration) DefaultLimit(limit int) *Configuration {
	c.defaultLimit = limit
	return c
}

// The maximum number of documents which can ever be returned
// from a single query
func (c *Configuration) MaxLimit(max int) *Configuration {
	c.maxLimit = max
	return c
}

// The maximum number of results to count towards a results Total
// If you are showing 10 records per page, does it make sense
// to count more than 1000 matching documents (100 pages)?
func (c *Configuration) MaxTotal(max int) *Configuration {
	c.maxTotal = max
	return c
}

// The number of buckets to use for sharding documents
func (c *Configuration) BucketCount(bucketCount int) *Configuration {
	c.bucketCount = bucketCount
	return c
}

// The number of concurrent queries the database can support
func (c *Configuration) QueryPoolSize(size int) *Configuration {
	c.queryPoolSize = size
	return c
}

// The maximum set size to consider an index-first filtering query as
// opposed to a sort-first. Index-first filters require upfront memory
// and aren't likely to be efficient past a certain threshold
func (c *Configuration) MaxUnsortedSize(max int) *Configuration {
	c.maxUnsortedSize = max
	return c
}

// Where to persist the database. Indexes will be stored in
// path/indexes/  while documents will be stored in path/documents/
func (c *Configuration) DbPath(path string) *Configuration {
	c.dbPath = path
	return c
}

// The maximum number of concurrent results which can be open for
// index-first and sorted-first driven results
func (c *Configuration) ResultsPoolSize(sorted, unsorted int) *Configuration {
	c.sortedResultPoolSize = sorted
	c.unsortedResultPoolSize = unsorted
	return c
}

// The maximum number of indexes allowed for a given query
func (c *Configuration) MaxIndexesPerQuery(max int) *Configuration {
	c.maxIndexesPerQuery = max
	return c
}

// Instructs the database to not load data from disk on startup
func (c *Configuration) SkipLoad() *Configuration {
	c.skipLoad = true
	return c
}

// Does not persist changes to disk
func (c *Configuration) NoPersistence() *Configuration {
	c.persist = false
	return c
}

// The factory used to rehydrate objects on startup (when the id is a string)
func (c *Configuration) StringFactory(factory StringFactory) *Configuration {
	c.sFactory = factory
	return c
}

// The factory used to rehydrate objects on startup (when the id is an int)
func (c *Configuration) IntFactory(factory IntFactory) *Configuration {
	c.iFactory = factory
	return c
}
