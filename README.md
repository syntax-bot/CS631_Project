# qcache: a basic query result cache for PostgreSQL

qcache saves the result of a read-only `SELECT` and, when the same query
arrives again, sends the saved rows to the client without executing it.

First working version: one cache per connection, kept in that connection's
own memory. Written for PostgreSQL 18 (tested on 18.6). Start reading at the
comment at the top of `qcache.c`.

## Build and test

```bash
export PATH=/usr/local/pgsql/bin:$PATH     # the pg_config of your own build
make && make install
# postgresql.conf:  shared_preload_libraries = 'qcache'   then restart
psql -d postgres -c "CREATE EXTENSION qcache;"
make installcheck
```

## Use

```sql
SELECT * FROM qcache_stats();     -- entries, bytes, hits, misses, ...
SELECT * FROM qcache_entries();   -- what is cached
SELECT qcache_reset();            -- empty the cache, zero the counters
SET qcache.enabled = off;         -- bypass the cache in this session
SET qcache.debug = on;            -- print a message at every step
```

## How it works (so far)

| Hook | What qcache does there |
| --- | --- |
| `planner_hook` | Decides from the query tree whether the query is safe to cache (no `now()`, `random()`, `FOR UPDATE`, ...) |
| `ExecutorStart_hook` | Builds the key (user + query text); notices write statements |
| `ExecutorRun_hook` | Hit: sends the saved rows. Miss: runs the query and copies each row on its way to the client |
| `ExecutorEnd_hook` | Cleans up |
| transaction callback | At COMMIT of a transaction that wrote anything, empties the whole cache |

Invalidation is deliberately coarse for now: any committed write empties the
cache. That is always correct, but a write to one table throws away results
of every other table too.

## Next

- Invalidate per table, and notice `ALTER`, `TRUNCATE`, `COPY`.
- A size limit with eviction.
- A shared cache (a write in another connection is not seen yet).
