CREATE EXTENSION qcache;
LOAD 'qcache';

CREATE TABLE t (id int PRIMARY KEY, v int);
INSERT INTO t SELECT g, g FROM generate_series(1, 1000) g;
SELECT qcache_reset();

-- 1. The second identical SELECT is served from the cache
SELECT count(*), sum(v) FROM t;
SELECT count(*), sum(v) FROM t;
SELECT hits, misses, stores FROM qcache_stats();

-- 2. A committed write to t invalidates it; the new result is correct
UPDATE t SET v = v + 1 WHERE id = 1;
SELECT count(*), sum(v) FROM t;
SELECT hits, misses, invalidations FROM qcache_stats();

-- 3. Inside a transaction that wrote to t, the cache is bypassed,
--    so the transaction sees its own change
BEGIN;
UPDATE t SET v = 0 WHERE id = 2;
SELECT count(*), sum(v) FROM t;
ROLLBACK;
-- after the rollback the saved result is still valid and is used
SELECT count(*), sum(v) FROM t;
SELECT hits, misses FROM qcache_stats();

-- 4. Multi-row results keep their order and row count
SELECT * FROM t WHERE id <= 3 ORDER BY id;
SELECT * FROM t WHERE id <= 3 ORDER BY id;

-- 5. Queries with non-immutable functions are never cached
SELECT qcache_reset();
SELECT count(*) FROM t WHERE random() >= 0;
SELECT count(*) FROM t WHERE random() >= 0;
SELECT count(*) > 0 AS ok FROM t WHERE now() > '2000-01-01';
SELECT count(*) > 0 AS ok FROM t WHERE now() > '2000-01-01';
SELECT hits, misses, stores FROM qcache_stats();

-- 6. What is in the cache
SELECT count(*) FROM t;
SELECT query, row_count, hits FROM qcache_entries();

DROP TABLE t;
DROP EXTENSION qcache;
