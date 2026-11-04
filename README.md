# qcache: a basic query result cache for PostgreSQL

qcache saves the result of a read-only `SELECT` and, when the same query
arrives again, sends the saved rows to the client without executing it.
A saved result is thrown away when a transaction that wrote to one of its
tables commits.

This is the checkpoint-3 version: cached results, locks, and table version
counters are shared by all connections.
Written for PostgreSQL 18 (tested on 18.6).
The code is written to be read by beginners: start at the comment at the
top of `qcache.c`.

## Files

| File | What it is |
| --- | --- |
| `qcache.c` | The whole extension |
| `qcache--1.0.sql`, `qcache.control` | SQL functions and extension metadata |
| `Makefile` | Build with PGXS |
| `sql/qcache.sql`, `expected/qcache.out` | Regression test |
| `demo.sql` | Checkpoint demo |
| `tour.sql` | Short walk through hits, misses and invalidation with `qcache.debug` on |
| `bench/` | pgbench comparison, cache off vs on |

## Install

The steps assume your source build is installed under `/usr/local/pgsql`
(the default `--prefix`). Change the path if you used another one.

1. **Use your build's tools.** Every command must use the `pg_config` of
   the PostgreSQL you built, not a system one.

   ```bash
   export PATH=/usr/local/pgsql/bin:$PATH
   which pg_config          # should print /usr/local/pgsql/bin/pg_config
   pg_config --version
   ```

2. **Build and install the extension.** Run as the user that owns
   `/usr/local/pgsql` (use `sudo` with the full pg_config path if root owns it).

   ```bash
   cd qcache
   make
   make install
   ```

   This copies `qcache.so` into `$(pg_config --pkglibdir)` and the SQL and
   control files into `$(pg_config --sharedir)/extension`.

3. **Load it at server start.** In your data directory's
   `postgresql.conf`, set:

   ```
   shared_preload_libraries = 'qcache'
   ```

   then restart the server:

   ```bash
   pg_ctl -D /path/to/data restart
   ```

4. **Create the SQL functions** in each database you use:

   ```sql
   CREATE EXTENSION qcache;
   SHOW shared_preload_libraries;   -- should list qcache
   ```

5. **Run the tests** with the server running:

   ```bash
   make installcheck
   ```

   It should end with `# All 1 tests passed.` If it fails, the differences
   are in `regression.diffs`.

For a quick try without a restart, `LOAD 'qcache';` in a psql session
turns it on for that session only.

## Use

```sql
SELECT * FROM qcache_stats();     -- entries, bytes, hits, misses, ...
SELECT * FROM qcache_entries();   -- what is cached, most recent first
SELECT qcache_reset();            -- empty the cache, zero the counters
SET qcache.enabled = off;         -- bypass the cache in this session
SET qcache.debug = on;            -- print a message at every step
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `qcache.enabled` | `on` | Use and fill the cache |
| `qcache.debug` | `off` | Print a NOTICE at every step (hit, miss, saved, invalidated, why not cached) |
| `qcache.max_size` | `64MB` | Shared memory reserved for cached results; restart required |
| `qcache.max_result_size` | `1MB` | Larger results are not cached |
| `qcache.max_entries` | `65536` | Maximum entries across the shared hash tables; restart required |

Counters and entries are shared by all connections.

## Demo and benchmark

```bash
psql -d postgres -f demo.sql                 # 2M-row table, timing on
bench/run_bench.sh postgres 10 1             # database, seconds, clients (1 for a clean comparison)
```

On our test machine the demo query took 236 ms on the first run and
0.5 ms from the cache.

## How it works

PostgreSQL runs a query as parse → rewrite → plan → execute. qcache
uses the hooks PostgreSQL provides at these stages, so the server's own
code is not changed.

| Hook | What qcache does there |
| --- | --- |
| `planner_hook` | Decides from the query tree whether the query is safe to cache |
| `ExecutorStart_hook` | Builds the cache key; records tables that a write statement touches |
| `ExecutorRun_hook` | On a hit, sends the saved rows; on a miss, runs the query and copies each row as it goes to the client |
| `ExecutorFinish_hook`, `ExecutorEnd_hook` | Tracks nesting and cleans up |
| `ProcessUtility_hook` | Notices DDL and other utility commands |
| transaction callback | At commit, drops results that read a changed table; at abort, forgets pending changes |

The key is the user, the statement's position in the query string, the
tables the query resolved to, and the query text. Saved rows live in a
hash table with least-recently-used eviction.

**What is never cached:** anything but a plain `SELECT`; queries using
non-immutable functions such as `now()` or `random()`; `FOR UPDATE`;
temporary, partitioned, inherited, foreign and system tables; row-level
security; queries with parameters; cursors, `EXPLAIN` and queries inside
functions; transactions in `REPEATABLE READ` or `SERIALIZABLE`; and tables
the current transaction has already changed.

**Invalidation:** every `INSERT`, `UPDATE`, `DELETE` or `MERGE`, at any
level (including triggers and functions), records the tables it touches.
At commit, results that read those tables are removed. Any DDL, `TRUNCATE`
or `COPY` clears the whole cache at commit.

## Known limits of this version

- Only the simple query protocol benefits (psql, `pgbench -M simple`).
  Prepared statements and parameterized queries are not cached.
- A hit still pays for parsing, planning and executor startup.

## Checkpoint 2: shared table versions

Each table maps to one slot in a fixed shared-memory array. A writing
transaction marks its slots and bumps each version before its commit becomes
visible, then bumps again and clears the in-flight marker after commit or
abort. Readers take their version snapshot in the planner, before the query
snapshot is created. A cached result is used or saved only when every recorded
version is unchanged and no commit is in progress.

This closes the stale-read window between a commit becoming visible and the
cache noticing it. An abort also clears its in-flight markers, so later
queries are not permanently bypassed.

## Checkpoint 3: one shared cache

The cache is now shared by all PostgreSQL connections. At server startup,
qcache reserves one fixed shared-memory region: one hash table, one LRU list
and one free list of chunks, protected by one lock.

Results are stored as a chain of fixed-size shared-memory chunks. A cache hit
takes the lock, copies the result into the requesting connection's temporary
memory, and releases the lock before sending rows to the client, so no lock
is held during client output.

The lock is taken exclusively even by a hit, because a hit moves its entry to
the front of the LRU list. So readers wait for each other; this is the next
thing to fix (it is the global-lock problem of MySQL's query cache). The
shared settings `qcache.max_size` and `qcache.max_entries` are postmaster
settings and require a restart.
