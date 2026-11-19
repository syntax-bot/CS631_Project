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

-- 6. Temporary tables are never cached
CREATE TEMP TABLE tmp AS SELECT 1 AS x;
SELECT * FROM tmp;
SELECT * FROM tmp;
SELECT hits, stores FROM qcache_stats();

-- 7. DDL clears the whole cache, and SELECT * shows the new column.
-- The key is a hash of the query tree, where * is expanded to the column
-- list: after ALTER the query has a new key, so the old entry is never
-- looked up and is not counted as an invalidation.
SELECT qcache_reset();
SELECT * FROM t WHERE id = 5;
SELECT * FROM t WHERE id = 5;
ALTER TABLE t ADD COLUMN w int DEFAULT 7;
SELECT * FROM t WHERE id = 5;
SELECT hits, misses, invalidations FROM qcache_stats();

-- 8. TRUNCATE invalidates
SELECT count(*) FROM t;
SELECT count(*) FROM t;
TRUNCATE t;
SELECT count(*) FROM t;

-- 9. Two statements in one query string get separate entries
SELECT qcache_reset();
SELECT 1 AS a\; SELECT 2 AS a;
SELECT 1 AS a\; SELECT 2 AS a;
SELECT hits, stores FROM qcache_stats();

-- 10. Large out-of-line (TOASTed) values are copied correctly
CREATE TABLE big (id int, doc text);
ALTER TABLE big ALTER COLUMN doc SET STORAGE EXTERNAL;
INSERT INTO big SELECT 1, string_agg(md5(g::text), '') FROM generate_series(1, 200) g;
SELECT qcache_reset();
SELECT md5(doc) AS expected FROM big \gset
-- fetch the whole value twice; the second copy comes from the cache
SELECT doc FROM big \gset first_
SELECT doc FROM big \gset second_
SELECT md5(:'first_doc') = :'expected' AS first_ok,
       md5(:'second_doc') = :'expected' AS second_ok;
SELECT hits FROM qcache_stats();

-- 11. Turning the cache off still tracks writes
SELECT qcache_reset();
SELECT count(*) FROM big;
SET qcache.enabled = off;
INSERT INTO big VALUES (2, 'x');
SET qcache.enabled = on;
SELECT count(*) FROM big;
SELECT hits, invalidations FROM qcache_stats();

-- 12. What is in the cache
SELECT query, row_count, hits FROM qcache_entries();

DROP TABLE t, big;
DROP EXTENSION qcache;
