\echo Use "CREATE EXTENSION qcache" to load this file. \quit

-- Counters for the current connection
CREATE FUNCTION qcache_stats(
    OUT entries bigint,
    OUT total_bytes bigint,
    OUT hits bigint,
    OUT misses bigint,
    OUT stores bigint,
    OUT invalidations bigint,
    OUT evictions bigint)
RETURNS record
AS 'MODULE_PATHNAME', 'qcache_stats'
LANGUAGE C STRICT VOLATILE;

-- What the current connection has cached, most recently used first
CREATE FUNCTION qcache_entries(
    OUT query text,
    OUT row_count bigint,
    OUT bytes bigint,
    OUT hits bigint)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'qcache_entries'
LANGUAGE C STRICT VOLATILE;

-- Empty the cache and zero the counters for the current connection
CREATE FUNCTION qcache_reset()
RETURNS void
AS 'MODULE_PATHNAME', 'qcache_reset'
LANGUAGE C STRICT VOLATILE;
