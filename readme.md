# Nabu
Nabu is an in-memory query engine. It's an explorating into addressing some of the limitations encountered building a similar engine directly into Redis.

## What
The approach relies on set operations (namely intersection) to filter results. Given two indexes of ids, we can intersect them to generate an AND filter:

        [a b c d e f]
    &&  [b c f g]
    -----------------
        [b c f]

`b`, `c` and `f` can then be looked up in a Hash. 

Futhermore, by including a sorted set, we can get efficient paging.

## Why Sets
We've found that this structure works well for specific workload: read-heavy application where small sets are often interesected with larger ones. Since the performance of set intersect is O(N+M) where N is the size of the smallest set and M is the total number of sets, it's easier to get microsecond perormance when a set of a few thousand is intersected with a set of millions. When you add sorted sets to the mix and a preference for early pages (page 1, 2, 3 vs page 100, 101, 102) you get good performance.

## Why Not Redis
The Redis approach has worked well, but a lot has been learnt. First, the Redis approach is over-specialized and would be hard to reuse as a general purpose solution (nothing to do with Redis, simply the common outcome of a first attempt). Secondly, Redis is single threaded which is unfortunate for read-heavy systems - why can't two or more threads read form the same index at the same time? Finally, TCP and serialization overhead are now the biggest bottleneck. By moving to an embedded database, both can be elimited.

Also, hacking Redis isn't as accessible as using a library.

## Status
Nabu is just an experiment and shouldn't be used in production. It'll likely never be taken so far, but the addition of persistence (dumping/loading the state of `db.resources`) would be a decent v1 milestone.