# Nabu
Nabu is a thread-safe in-process database. It's targeted at read-heavy workloads and is particularly well suited for datasets with a large number of small indexes. 

Nabu is not meant to be an authoritative database. Lacking the ability to fetch missing data, it's also not a cache. 

Where does it fit then? Imagine that you're building an API. Like many APIs, yours is read-heavy. Various endpoints will expose paging, sorting and filtering capabilities. Maybe your API is geographically distributed. <strong>You want single digit uncached response times.</strong> A good solution is to send all writes to a centralized, authoritative system and asynchronously propagates changes to your read-optimized edge servers.

Nabu is designed for systems that want to benefit from a distinct read data model.

## Sacrifices
Nabu isn't where business analysts go to gain insight, nor where developers go to debug issues. It's made to answer a specific set of queries within a predefined range of inputs. Flexibility is happily sacrificed. 

For example, most systems have an upper bound on the number of results a query can return (the page size). On startup, this limit is configured within Nabu and subsequently enforced. It's simply not possible to return a larger set of results. Why? Because a pool of results with fixed-length arrays are preallocated on startup. This reduces the amount of garbage collection the system must do.

## Genesis
Nabu stems from building a similar system out-of-process, with Redis. [I love Redis](http://openmymind.net/2012/1/23/The-Little-Redis-Book/), but, in this case, it had two flaws. First, while blazing fast, it didn't escape my attention that the system spent most of its time chatting with Redis over TCP, parsing responses and unmarshaling bytes into objects. 

Secondly, Redis is single threaded. Scaling it for higher concurrency meant launching new instances, yet each new instance linearly increased the storage space. A 10GB database leveraging 12 cores takes 120GB of memory. 

Nabu attempts to solve both these problems. By being in-process, there is no network communication, no message parsing and no unmarshaling (which also all adds up to less garbage collection). By being thread-safe, a single copy of the data exists, allowing for better and more flexible resource usage.

## State
Nabu is in early development. These are the core missing features:

* Management of sorted indexes (probably requiring a real implementation of a sortable index)
* Persistence
* Richer results (counts, and returning matching documents, not just ids)
* Richer querying (ORs, maybe)

## Usage
This is still being flushed out.

Nabu deals with objects fulfilling the `nabu.Document` interface through an instance of `*nabu.Database`. `nabu.Document` exposes a single method: `ReadMeta(m *nabu.Meta)`. This is the method where your custom type describes itself:

    type User struct {
      Id string
      Age int
      Name string
      Gender string
    }
    
    func (u *User) ReadMeta(m *nabu.Meta) {
      m.Id(u.Id)
      m.Index("user:age", strconv.Itoa(u.Age))
      m.Index("user:startswith", string.ToLower(u.Name[0:1]))
      m.Index("user:gender", u.Gender)
    }

It's important to note that `ReadMeta` is only called on startup or when a document is added to the database. Do not waste memory storing meta information about indexes.

With your documents defined, you can now interact with Nabu's database. First, create an instance:

    db := nabu.New(nabu.Configure())

Once created, you can call:

* `db.Update(doc Document)` either update or insert a document
* `db.Remove(doc Document)` remove the document
* `db.RemoveById(id string)` remove the document by id
* `db.Get(id) Document` get a document by id

### Querying
You can query for results by creating a new `Query`:

    query := db.Query("SORT_INDEX")

This is a chainable object. Available methods are:

* `query.Limit(count int)` the number of results to return
* `query.Offset(offset int)` the offset to start at
* `query.Desc()` return results in descending order
* `query.Where(index string, value string)` filter results 

Finally, results can be retrieved by calling the `Execute` method. The returned result *must* be closed after you're done with it:

    query := db.Query("created_at").Desc().Limit(10).
              Where("user:gender", gender, "user:startswith", "n")
    if len(age) != 0 {
      query.Where("user:age", age)
    }
    res := db.Query().Execute()
    defer res.Close()
    for _, id := range res.Ids() {
      ...
    }

### Configuration
The database is configured via the chainable configuration api:

    config := nabu.Configure().DefaultLimit(10).MaxLimit(100).QueryPoolSize(10).BucketSize(50)
    db := nabu.New(config)

Available options are:

* `DefaultLimit(limit int)` [10] The default number of results to return
* `MaxLimit(limit int)` [100] The maximum number of results to return
* `BucketCount(count int)` [25] The number of buckets to use to store documents
* `QueryPoolSize(size int)` [512] The number of concurrent queries to support
* `MaxUnsortedSize(size int)` [100] When an index smaller than the specified size is part of the query, an optimized query path is used
* `ResultsPoolSize(sorted int, unsorted int)` The pool size for sorted results as well as unsorted results

Pools are currently blocking. Hooks will eventually be provided to gauge the health and appropriateness of pool sizes.
