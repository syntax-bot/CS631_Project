setup
{
    CREATE TABLE acc (id int PRIMARY KEY, v int);
    INSERT INTO acc VALUES (1, 100);
}

teardown
{
    DROP TABLE acc;
}

session r
setup       { SELECT qcache_reset(); }
step r_read  { SELECT v FROM acc WHERE id = 1; }
step r_stats { SELECT hits, misses, invalidations FROM qcache_stats(); }

session w
step w_begin    { BEGIN; }
step w_update   { UPDATE acc SET v = v + 1 WHERE id = 1; }
step w_commit   { COMMIT; }
step w_rollback { ROLLBACK; }
step w_truncate { TRUNCATE acc; }

permutation r_read r_read w_update r_read r_stats

permutation r_read w_begin w_update r_read w_commit r_read r_stats

permutation r_read w_begin w_update w_rollback r_read r_stats

permutation r_read w_truncate r_read r_stats
