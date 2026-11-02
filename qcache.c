/*
 * qcache.c - a basic query result cache for PostgreSQL 18  (step 0 version)
 * =========================================================================
 *
 * WHAT IT DOES
 *   When a SELECT finishes, we keep a copy of its result rows in memory.
 *   When the exact same SELECT is sent again, we send the saved rows back
 *   to the client instead of running the query.
 *
 *   Every table has a VERSION NUMBER.  When a transaction that changed a
 *   table COMMITs, that table's version goes up by one.  A saved result
 *   remembers the versions of the tables it read, and is used only while
 *   they are unchanged.  So a write never has to search the cache: it only
 *   bumps a counter, and out-of-date results are thrown away the next time
 *   someone looks them up (or when the cache is full).
 *
 * HOW IT PLUGS INTO POSTGRESQL
 *   PostgreSQL handles every query in stages:
 *
 *       parse  ->  plan  ->  executor start  ->  executor run  ->  executor end
 *
 *   At several stages PostgreSQL checks a global "hook" variable.  If an
 *   extension has put a function there, PostgreSQL calls that function
 *   instead of doing the stage itself.  Our function does a little extra
 *   work and then calls the normal PostgreSQL function for that stage.
 *   We use these hooks (each one is a section below):
 *
 *     1. planner_hook        is this query safe to cache?  read table versions
 *     2. ExecutorStart_hook  build the cache key; notice INSERT/UPDATE/DELETE
 *     3. ExecutorRun_hook    cache hit: send saved rows
 *                            cache miss: run the query and save its rows
 *     4. ProcessUtility_hook notice commands like ALTER TABLE and TRUNCATE
 *     5. a transaction callback, called at COMMIT and ROLLBACK: bump versions
 *
 * WHAT CHANGED IN STEP 0 (preparing for the shared cache)
 *   - A saved result is ONE block of memory with no pointers inside (rows,
 *     key and query text together), instead of one allocation per row in a
 *     memory context of its own.  Such a block can later be copied into
 *     shared memory unchanged, and its size is the memory it really uses.
 *     (PART B, PART 3)
 *   - Out-of-date results are found with version numbers, not by scanning the
 *     whole cache at every COMMIT.  (PART D, PART 6)
 *   - Table versions are read in the planner hook, which runs before the
 *     query's snapshot is taken.  With a shared cache this order is what keeps
 *     results correct.  (PART 1)
 *   - The key also contains the database and the settings that change what a
 *     query means (DateStyle, TimeZone, search_path, ...).  (PART 2)
 *
 * WHAT CHANGED IN STEP 1 (shared memory)
 *   - The version counters live in SHARED MEMORY, one copy for the whole
 *     server, instead of one copy per connection.  So when ANY connection
 *     commits a write, the saved results of EVERY connection that read that
 *     table go out of date.  (PART 8)
 *   - The counters are atomic variables: connections update and read them
 *     at the same time without a lock.
 *   - Shared memory can only be set up at server start, so qcache must now
 *     be in shared_preload_libraries.  (PART 9)
 *
 * IMPORTANT LIMITS OF THIS VERSION
 *   - The saved results themselves are still private to each connection
 *     (each connection fills its own cache).  Moving them into shared
 *     memory is step 3.
 *   - A commit becomes visible to other connections a moment BEFORE our
 *     transaction callback bumps the counters.  In that short window another
 *     connection can still get the old result.  Step 2 closes it.
 *   - Only plain SELECTs sent directly by the client are cached (not queries
 *     inside functions, not prepared statements with parameters).
 *
 * TIP FOR LEARNING
 *   SET qcache.debug = on;   prints a message at every step we take.
 */

/* "postgres.h" must always be the first include in PostgreSQL C code. */
#include "postgres.h"

#include <limits.h>

#include "access/detoast.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits.h"
#include "common/hashfn.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/ilist.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"

/* The hook functions below are written for PostgreSQL 18. */
#if PG_VERSION_NUM < 180000 || PG_VERSION_NUM >= 190000
#error "This version of qcache is written for PostgreSQL 18"
#endif

/* Every loadable module must contain this line. */
PG_MODULE_MAGIC;


/* ======================================================================
 * PART A. SETTINGS AND LIMITS
 *
 * These can be changed with SET, for example:  SET qcache.enabled = off;
 * They are registered with PostgreSQL in _PG_init() at the end of the file.
 * ====================================================================== */

static bool setting_enabled = true;		/* qcache.enabled */
static bool setting_debug = false;		/* qcache.debug */
static int	setting_max_size_kb = 64 * 1024;	/* qcache.max_size: 64 MB */
static int	setting_max_result_kb = 1024;	/* qcache.max_result_size: 1 MB */

/*
 * A query that reads more tables than this is not cached.  A fixed limit
 * lets every entry keep its table versions in a small array of its own,
 * which the shared-memory version will need (no pointers in shared memory).
 */
#define MAX_TABLES		8

/*
 * Number of version counters.  A table's counter is chosen by hashing its
 * database and table OID (table_slot() below).  Two tables may share a
 * counter; that only makes some results look out of date too early, it can
 * never make an old result look current.  Must be a power of two.
 */
#define VERSION_SLOTS	65536

/*
 * Counter 0 is not used by any table.  It is the "epoch": it goes up when a
 * transaction runs ALTER, TRUNCATE, COPY or another command whose tables we
 * do not track.  Every saved result remembers it too, so such a command
 * makes every saved result out of date at once.
 */
#define EPOCH_SLOT		0

/*
 * Settings that change what the same query text MEANS, so they are part of
 * the cache key (see build_cache_key):
 *
 *   DateStyle, IntervalStyle, TimeZone
 *       change how literals such as date '01/02/03' are read, and the
 *       result of some functions labelled IMMUTABLE (jsonpath datetime).
 *   search_path
 *       changes which function or operator a name refers to.  (The table
 *       OIDs in the key already cover tables.)
 *   standard_conforming_strings, array_nulls, transform_null_equals
 *       change how string literals, array literals and "= NULL" are read.
 *
 * Settings that only change how a value is PRINTED (extra_float_digits,
 * bytea_output, ...) are not needed: saved rows hold the values themselves,
 * and they are printed with the current settings when sent to the client.
 */
static const char *const key_settings[] = {
	"DateStyle",
	"IntervalStyle",
	"TimeZone",
	"search_path",
	"standard_conforming_strings",
	"array_nulls",
	"transform_null_equals",
};

/* Print a debug message, only when qcache.debug is on. */
#define DEBUG_MSG(...) \
	do { \
		if (setting_debug) \
			ereport(NOTICE, (errmsg(__VA_ARGS__))); \
	} while (0)


/* ======================================================================
 * PART B. DATA STRUCTURES
 * ====================================================================== */

/*
 * One saved result is a single block of memory:
 *
 *   +--------------+------------------+-----+-------+-----+-------+-----
 *   | ResultHeader | column type OIDs | pad | row 1 | pad | row 2 | ...
 *   +--------------+------------------+-----+-------+-----+-------+-----
 *
 * Each row is a MinimalTuple (PostgreSQL's compact row format; its first
 * field t_len is the row's length in bytes) and starts at a MAXALIGNed
 * offset, because PostgreSQL reads the row's fields in place.
 *
 * The block holds no pointers, only offsets, so it means the same thing at
 * any address.  That is what will let us copy it into shared memory later.
 */
typedef struct ResultHeader
{
	Size		total_len;		/* bytes in the whole block */
	uint64		num_rows;
	uint32		num_columns;
	uint32		rows_offset;	/* where row 1 starts */
} ResultHeader;

/* The column type OIDs come right after the header. */
#define RESULT_COLUMN_TYPES(header) \
	((Oid *) ((char *) (header) + MAXALIGN(sizeof(ResultHeader))))

/* A table a query reads, and that table's version when it was read. */
typedef struct TableVersion
{
	Oid			oid;
	uint32		slot;			/* which version counter, see table_slot() */
	uint64		version;		/* value of that counter */
} TableVersion;

/*
 * One saved result.  All saved results live in a hash table, so that
 * finding one by its key is fast.
 */
typedef struct CachedResult
{
	uint64		key_hash;		/* the hash table's key (must be the first field) */

	/*
	 * Everything variable-sized is in ONE allocation, "block":
	 *
	 *   [ saved rows (ResultHeader ...) | full key | query text ]
	 *
	 * One palloc per saved result keeps the memory use honest and freeing
	 * simple (one pfree), and it is the shape the shared cache will copy.
	 */
	char	   *block;
	ResultHeader *result;		/* = block: the saved rows */
	char	   *key;			/* inside block: the full key, see build_cache_key() */
	int			key_len;
	char	   *query_text;		/* inside block: the SELECT, for qcache_entries() */
	Size		size_bytes;		/* memory really used by block */

	/* The tables this result was read from, and their versions then. */
	int			num_tables;
	TableVersion tables[MAX_TABLES];
	uint64		epoch;			/* the epoch counter then */

	uint64		hits;			/* how many times it was reused */
	dlist_node	lru_link;		/* position in the "recently used" list */
} CachedResult;

/*
 * The SELECT that is running right now, if we may cache it.
 * Filled in by the ExecutorStart hook, used by the ExecutorRun hook.
 */
typedef struct CurrentQuery
{
	QueryDesc  *query_desc;		/* PostgreSQL's description of the query */
	MemoryContext memory;		/* everything below is allocated in here */

	uint64		key_hash;
	char	   *key;
	int			key_len;
	char	   *query_text;

	/* Table versions, as the planner hook read them. */
	int			num_tables;
	TableVersion tables[MAX_TABLES];
	uint64		epoch;

	bool		already_ran;	/* ExecutorRun was already called once */
} CurrentQuery;

/*
 * A "row collector".  PostgreSQL sends every result row to a receiver
 * object (a DestReceiver) that passes it on to the client.  On a cache miss
 * we put our collector in front of the real receiver: it copies each row
 * into one growing block (see ResultHeader), then hands the row on to the
 * real receiver.
 */
typedef struct RowCollector
{
	DestReceiver base;			/* must be first: PostgreSQL sees only this part */
	DestReceiver *real_receiver;	/* where the rows really go (the client) */

	MemoryContext memory;		/* holds the block */
	char	   *block;			/* starts with a ResultHeader */
	Size		used;			/* bytes of the block filled so far */
	Size		capacity;		/* bytes allocated for the block */
	uint64		num_rows;
	bool		too_big;		/* the result passed qcache.max_result_size */
} RowCollector;


/* ======================================================================
 * PART C. GLOBAL VARIABLES
 *
 * Each connection is a separate PostgreSQL process, so every connection
 * gets its own copy of these variables, and so its own cache.
 * ====================================================================== */

static MemoryContext cache_memory = NULL;	/* parent of all our memory */
static HTAB *cache_table = NULL;	/* key_hash -> CachedResult */
static dlist_head lru_list = DLIST_STATIC_INIT(lru_list);	/* most recent first */
static Size cache_size_bytes = 0;	/* total size of all saved results */

/*
 * What lives in shared memory: one block, the same for every connection.
 * For now it holds only the version counters (VERSION_SLOTS of them, 512 kB;
 * counter 0 is the epoch).  Step 3 adds the saved results.
 *
 * pg_atomic_uint64 is a 64-bit number that many processes can update at the
 * same time without a lock: pg_atomic_fetch_add_u64() adds to it as one
 * indivisible step, so no increment is ever lost.
 */
typedef struct QcacheShared
{
	pg_atomic_uint64 version[VERSION_SLOTS];
} QcacheShared;

/* Points into shared memory; set by qcache_shmem_startup(). */
static QcacheShared *shared = NULL;

/* Read and bump one counter. */
#define VERSION(slot)		pg_atomic_read_u64(&shared->version[(slot)])
#define BUMP_VERSION(slot)	pg_atomic_fetch_add_u64(&shared->version[(slot)], 1)

static CurrentQuery current;	/* the SELECT running now (zeroed if none) */

/*
 * How deep we are in nested queries.  0 means a query sent by the client.
 * A query run inside a function (or EXPLAIN, a cursor...) is at level 1+.
 * We only cache level-0 queries.
 */
static int	nesting_level = 0;

/* What the planner hook found out about the plan it made most recently. */
static PlannedStmt *plan_just_made = NULL;
static const char *plan_just_made_problem = NULL;	/* NULL means safe */
static int	plan_just_made_num_tables = 0;
static TableVersion plan_just_made_tables[MAX_TABLES];
static uint64 plan_just_made_epoch = 0;

/* What the current transaction changed; acted on at COMMIT. */
static List *tables_written = NIL;	/* list of table OIDs */
static bool structure_changed = false;	/* ALTER, TRUNCATE, COPY, ... */

/* Counters shown by qcache_stats(). */
static uint64 count_hits = 0;
static uint64 count_misses = 0;
static uint64 count_stores = 0;
static uint64 count_invalidations = 0;	/* out-of-date results thrown away */
static uint64 count_evictions = 0;

/* The hook functions that were installed before ours (usually NULL). */
static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;

void		_PG_init(void);
PG_FUNCTION_INFO_V1(qcache_stats);
PG_FUNCTION_INFO_V1(qcache_entries);
PG_FUNCTION_INFO_V1(qcache_reset);


/* ======================================================================
 * PART D. SMALL HELPERS
 * ====================================================================== */

/*
 * Which version counter belongs to this table?  We mix in the database OID,
 * because two databases can contain tables with the same OID.  The result
 * is never EPOCH_SLOT.
 */
static uint32
table_slot(Oid table_oid)
{
	uint32		h = hash_combine(murmurhash32((uint32) MyDatabaseId),
								 murmurhash32((uint32) table_oid));

	return 1 + h % (VERSION_SLOTS - 1);
}

/*
 * Are the tables still at the versions in the list, and the epoch unchanged?
 * If yes, a result computed at those versions is still correct.
 */
static bool
versions_unchanged(const TableVersion *tables, int num_tables, uint64 epoch)
{
	if (VERSION(EPOCH_SLOT) != epoch)
		return false;
	for (int i = 0; i < num_tables; i++)
	{
		if (VERSION(tables[i].slot) != tables[i].version)
			return false;
	}
	return true;
}

/* Find a saved result by key.  Returns NULL if there is none. */
static CachedResult *
find_cached_result(uint64 key_hash, const char *key, int key_len)
{
	CachedResult *entry;

	entry = (CachedResult *) hash_search(cache_table, &key_hash, HASH_FIND, NULL);
	if (entry == NULL)
		return NULL;

	/* Two different keys can have the same hash; compare the full keys. */
	if (entry->key_len != key_len || memcmp(entry->key, key, key_len) != 0)
		return NULL;

	return entry;
}

/* Delete one saved result and free its memory. */
static void
remove_cached_result(CachedResult *entry)
{
	char	   *block = entry->block;
	uint64		key_hash = entry->key_hash;

	dlist_delete(&entry->lru_link);
	cache_size_bytes -= entry->size_bytes;
	hash_search(cache_table, &key_hash, HASH_REMOVE, NULL);

	pfree(block);				/* rows, key and text in one go */
}

/* Remove every saved result (only qcache_reset() needs this now). */
static void
remove_all_cached_results(void)
{
	while (!dlist_is_empty(&lru_list))
		remove_cached_result(dlist_head_element(CachedResult, lru_link, &lru_list));
}

/* Forget the current query and free its memory. */
static void
forget_current_query(void)
{
	if (current.memory != NULL)
		MemoryContextDelete(current.memory);
	memset(&current, 0, sizeof(current));
}

/* Forget what the current transaction changed. */
static void
forget_transaction_changes(void)
{
	list_free(tables_written);
	tables_written = NIL;
	structure_changed = false;
}


/* ======================================================================
 * PART 1. PLANNER HOOK: IS THIS QUERY SAFE TO CACHE?  READ TABLE VERSIONS.
 *
 * The planner receives the query as a tree (a "Query" struct) and turns it
 * into a plan.  We look at the tree first, because some questions are easy
 * to answer there, then let PostgreSQL plan as usual.
 * ====================================================================== */

/* Returns NULL if the query is safe, or a short reason why it is not. */
static const char *
check_query_tree(Query *query)
{
	if (query->commandType != CMD_SELECT || query->utilityStmt != NULL)
		return "not a SELECT";
	if (query->hasModifyingCTE)
		return "contains a WITH clause that changes data";
	if (query->rowMarks != NIL)
		return "uses FOR UPDATE or FOR SHARE";
	if (query->hasRowSecurity)
		return "reads a table with row-level security";

	/*
	 * A "mutable" function can return a different answer without any table
	 * changing: now(), random(), nextval(), ...  Such a query is never safe.
	 */
	if (contain_mutable_functions((Node *) query))
		return "calls a function that is not IMMUTABLE (like now(), random() or qcache_stats())";

	return NULL;
}

/*
 * Note the tables the plan reads and the current version of each.
 *
 * WHY HERE, AND NOT IN ExecutorStart?  PostgreSQL takes the snapshot (the
 * moment in time whose data the query sees) AFTER planning, in
 * PortalStart(), and only then calls ExecutorStart.  We want to read the
 * versions BEFORE the snapshot:
 *
 *   - if a write commits after we read the versions, its version bump is
 *     newer than what we noted, so we will not save this (possibly old)
 *     result, see save_result();
 *   - if we read the versions after the snapshot instead, a write committed
 *     in between would be in the versions but not in the result, and we
 *     would save an old result under new versions.
 *
 * With one private cache per connection no other connection bumps our
 * counters, so this does not matter yet.  It matters for the shared cache.
 */
static const char *
record_table_versions(PlannedStmt *plan)
{
	ListCell   *cell;
	int			n = 0;

	plan_just_made_epoch = VERSION(EPOCH_SLOT);

	foreach(cell, plan->rtable)
	{
		RangeTblEntry *table = (RangeTblEntry *) lfirst(cell);
		bool		seen = false;

		if (table->rtekind != RTE_RELATION)
			continue;			/* not a table (a subquery, a join, ...) */

		/* A table can appear several times (a self-join); note it once. */
		for (int i = 0; i < n; i++)
		{
			if (plan_just_made_tables[i].oid == table->relid)
				seen = true;
		}
		if (seen)
			continue;

		if (n == MAX_TABLES)
			return "reads more than 8 tables";

		plan_just_made_tables[n].oid = table->relid;
		plan_just_made_tables[n].slot = table_slot(table->relid);
		plan_just_made_tables[n].version = VERSION(plan_just_made_tables[n].slot);
		n++;
	}
	plan_just_made_num_tables = n;
	return NULL;
}

static PlannedStmt *
qcache_planner_hook(Query *query, const char *query_string, int cursor_options,
					ParamListInfo bound_params)
{
	const char *problem;
	PlannedStmt *plan;

	/* Check the tree before planning: the planner may change it. */
	problem = check_query_tree(query);

	/* Let PostgreSQL (or a hook installed before ours) make the plan. */
	if (prev_planner_hook != NULL)
		plan = prev_planner_hook(query, query_string, cursor_options, bound_params);
	else
		plan = standard_planner(query, query_string, cursor_options, bound_params);

	/* Read the versions right away, before the snapshot is taken. */
	plan_just_made_num_tables = 0;
	if (problem == NULL)
		problem = record_table_versions(plan);

	/* Remember our answer; the ExecutorStart hook asks for it. */
	plan_just_made = plan;
	plan_just_made_problem = problem;

	return plan;
}


/* ======================================================================
 * PART 2. EXECUTOR START HOOK: PREPARE TO CACHE, AND NOTICE WRITES
 *
 * ExecutorStart sets up the plan for running.  Here we know which tables
 * the query uses (the "range table", plan->rtable).
 * ====================================================================== */

/* Remember the tables an INSERT/UPDATE/DELETE/MERGE touches. */
static void
remember_written_tables(PlannedStmt *plan)
{
	ListCell   *cell;
	MemoryContext old_memory;

	/* tables_written must survive until COMMIT: use our own memory. */
	old_memory = MemoryContextSwitchTo(cache_memory);

	foreach(cell, plan->rtable)
	{
		RangeTblEntry *table = (RangeTblEntry *) lfirst(cell);

		if (table->rtekind != RTE_RELATION)
			continue;			/* not a table (a subquery, a join, ...) */

		if (!list_member_oid(tables_written, table->relid))
		{
			tables_written = lappend_oid(tables_written, table->relid);
			DEBUG_MSG("qcache: this transaction changes table \"%s\"; results that "
					  "read it go out of date at COMMIT", get_rel_name(table->relid));
		}
	}

	MemoryContextSwitchTo(old_memory);
}

/*
 * Checks that need the plan.  Returns NULL if we may cache the query, or a
 * reason why not.
 */
static const char *
check_plan(QueryDesc *query_desc, int eflags)
{
	PlannedStmt *plan = query_desc->plannedstmt;
	ListCell   *cell;

	if (nesting_level > 0)
		return "runs inside another command (a function, EXPLAIN, a cursor, ...)";

	/* Only a plan our planner hook has just checked. */
	if (plan != plan_just_made)
		return "uses a saved plan (prepared statement)";
	if (plan_just_made_problem != NULL)
		return plan_just_made_problem;

	if (query_desc->operation != CMD_SELECT || plan->hasModifyingCTE ||
		plan->rowMarks != NIL || plan->dependsOnRole)
		return "not a plain SELECT";
	if ((eflags & EXEC_FLAG_EXPLAIN_ONLY) || query_desc->instrument_options != 0)
		return "EXPLAIN";
	if (query_desc->params != NULL && query_desc->params->numParams > 0)
		return "has parameters";
	if (IsolationUsesXactSnapshot())
		return "transaction is REPEATABLE READ or SERIALIZABLE";
	if (structure_changed)
		return "this transaction ran ALTER, TRUNCATE or a similar command";

	/* Now look at every table the query reads. */
	foreach(cell, plan->rtable)
	{
		RangeTblEntry *table = (RangeTblEntry *) lfirst(cell);
		Oid			oid = table->relid;
		char		kind;

		if (table->rtekind != RTE_RELATION)
			continue;

		if (table->tablesample != NULL)
			return "uses TABLESAMPLE, which returns random rows";

		/* System catalogs change through commands we do not track. */
		if (oid < FirstNormalObjectId)
			return "reads a system catalog";

		kind = get_rel_relkind(oid);
		if (kind != RELKIND_RELATION && kind != RELKIND_VIEW && kind != RELKIND_MATVIEW)
			return "reads a partitioned or foreign table";

		if (get_rel_persistence(oid) == RELPERSISTENCE_TEMP)
			return "reads a temporary table";

		/* Rows can reach these tables through a different table name. */
		if (get_rel_relispartition(oid) || has_subclass(oid))
			return "reads a partition or an inheritance parent";

		/* We cannot see this transaction's own uncommitted changes. */
		if (list_member_oid(tables_written, oid))
			return "this transaction already changed a table it reads";
	}

	return NULL;
}

/*
 * Build the cache key for the current query.  Two queries share a key only
 * if all of these are equal:
 *   - the database                   (table OIDs repeat across databases)
 *   - the user running it            (users can have different permissions)
 *   - the settings in key_settings[] (they change what the text means)
 *   - the position of the statement in the query string
 *                                     ("SELECT 1; SELECT 2" holds two statements)
 *   - the tables it reads            (search_path can change what "orders" means)
 *   - the full query text
 */
static void
build_cache_key(QueryDesc *query_desc)
{
	PlannedStmt *plan = query_desc->plannedstmt;
	const char *text = query_desc->sourceText;
	int			text_len;
	Oid			database = MyDatabaseId;
	Oid			user = GetUserId();
	int			start = plan->stmt_location;
	int			length = plan->stmt_len;
	StringInfoData key;

	if (text == NULL)
		text = "";
	text_len = strlen(text);

	/* Append each part as raw bytes. */
	initStringInfo(&key);
	appendBinaryStringInfo(&key, &database, sizeof(database));
	appendBinaryStringInfo(&key, &user, sizeof(user));

	/*
	 * Each setting's current value, as text, ended by a zero byte so that
	 * two neighbouring values cannot run into each other.  GetConfigOption
	 * may reuse its buffer, so we copy each value before asking for the next.
	 */
	for (int i = 0; i < lengthof(key_settings); i++)
	{
		const char *value = GetConfigOption(key_settings[i], true, false);

		appendStringInfoString(&key, value ? value : "");
		appendStringInfoChar(&key, '\0');
	}

	appendBinaryStringInfo(&key, &start, sizeof(start));
	appendBinaryStringInfo(&key, &length, sizeof(length));
	for (int i = 0; i < current.num_tables; i++)
		appendBinaryStringInfo(&key, &current.tables[i].oid, sizeof(Oid));
	appendBinaryStringInfo(&key, text, text_len);

	current.key = key.data;
	current.key_len = key.len;
	current.key_hash = hash_bytes_extended((const unsigned char *) key.data, key.len, 0);

	/* Keep just this statement's text, for qcache_entries(). */
	if (start < 0 || start > text_len)
		start = 0;
	if (length <= 0 || start + length > text_len)
		length = text_len - start;
	current.query_text = pnstrdup(text + start, length);
}

static void
qcache_ExecutorStart_hook(QueryDesc *query_desc, int eflags)
{
	const char *problem;
	MemoryContext old_memory;

	/*
	 * First let PostgreSQL start the query as usual.  This also checks the
	 * user's permissions on every table, so a cache hit never skips them.
	 */
	if (prev_ExecutorStart_hook != NULL)
		prev_ExecutorStart_hook(query_desc, eflags);
	else
		standard_ExecutorStart(query_desc, eflags);

	/*
	 * Is this a write?  We remember writes at every nesting level, so that a
	 * write inside a trigger or function counts too, and even when the cache
	 * is turned off, so that turning it on later never shows old data.
	 */
	if (query_desc->operation != CMD_SELECT || query_desc->plannedstmt->hasModifyingCTE)
	{
		remember_written_tables(query_desc->plannedstmt);
		return;
	}

	if (!setting_enabled)
		return;

	/* Is this SELECT safe to cache? */
	problem = check_plan(query_desc, eflags);
	if (problem != NULL)
	{
		if (nesting_level == 0)
			DEBUG_MSG("qcache: not cached: %s", problem);
		return;
	}

	/* Yes: remember it as the current query, with its tables and key. */
	forget_current_query();
	current.query_desc = query_desc;
	current.memory = AllocSetContextCreate(cache_memory, "qcache current query",
										   ALLOCSET_DEFAULT_SIZES);

	current.num_tables = plan_just_made_num_tables;
	memcpy(current.tables, plan_just_made_tables,
		   sizeof(TableVersion) * plan_just_made_num_tables);
	current.epoch = plan_just_made_epoch;

	old_memory = MemoryContextSwitchTo(current.memory);
	build_cache_key(query_desc);
	MemoryContextSwitchTo(old_memory);
}


/* ======================================================================
 * PART 3. EXECUTOR RUN HOOK: HIT OR MISS
 *
 * ExecutorRun runs the plan and sends each result row to the query's
 * receiver, query_desc->dest.  This is the heart of the cache.
 * ====================================================================== */

/* ---- 3a. The row collector (used on a miss) ---- */

/*
 * Copy a row into a new MinimalTuple.  Large values (over about 2 kB) may
 * be stored outside the row ("TOAST") and the row only points at them; we
 * copy the values themselves, so a saved row never points into a table.
 */
static MinimalTuple
copy_row(TupleTableSlot *slot)
{
	TupleDesc	columns = slot->tts_tupleDescriptor;
	int			num_columns = columns->natts;
	Datum	   *values = palloc(sizeof(Datum) * Max(num_columns, 1));
	bool	   *is_null = palloc(sizeof(bool) * Max(num_columns, 1));
	bool	   *was_copied = palloc0(sizeof(bool) * Max(num_columns, 1));
	MinimalTuple row;

	/* Make sure every column's value is available in the slot. */
	slot_getallattrs(slot);

	for (int i = 0; i < num_columns; i++)
	{
		bool		variable_length = (TupleDescAttr(columns, i)->attlen == -1);

		values[i] = slot->tts_values[i];
		is_null[i] = slot->tts_isnull[i];

		if (!is_null[i] && variable_length &&
			VARATT_IS_EXTERNAL(DatumGetPointer(values[i])))
		{
			struct varlena *stored = (struct varlena *) DatumGetPointer(values[i]);

			values[i] = PointerGetDatum(detoast_external_attr(stored));
			was_copied[i] = true;
		}
	}

	/* Pack the values into one compact row. */
	row = heap_form_minimal_tuple(columns, values, is_null, 0);

	for (int i = 0; i < num_columns; i++)
	{
		if (was_copied[i])
			pfree(DatumGetPointer(values[i]));
	}
	pfree(values);
	pfree(is_null);
	pfree(was_copied);

	return row;
}

/*
 * Make sure the block has room for "needed" bytes in total.  The block
 * doubles in size when it grows (8 kB, 16 kB, 32 kB, ...), so copying it
 * on growth costs at most as much again as the rows themselves.
 */
static void
collector_reserve(RowCollector *collector, Size needed)
{
	Size		new_capacity;

	if (needed <= collector->capacity)
		return;

	new_capacity = Max(collector->capacity * 2, (Size) 8192);
	while (new_capacity < needed)
		new_capacity *= 2;

	if (collector->block == NULL)
		collector->block = MemoryContextAlloc(collector->memory, new_capacity);
	else
		collector->block = repalloc(collector->block, new_capacity);
	collector->capacity = new_capacity;
}

/* Called by PostgreSQL once for every result row. */
static bool
collector_receive_row(TupleTableSlot *slot, DestReceiver *self)
{
	RowCollector *collector = (RowCollector *) self;

	if (!collector->too_big && collector->block != NULL)
	{
		MemoryContext old_memory = MemoryContextSwitchTo(collector->memory);
		MinimalTuple row = copy_row(slot);
		Size		start = MAXALIGN(collector->used);	/* rows start aligned */
		Size		end = start + row->t_len;

		if (end > (Size) setting_max_result_kb * 1024)
		{
			/* Too big to cache: free the block and stop copying. */
			collector->too_big = true;
			MemoryContextReset(collector->memory);
			collector->block = NULL;
			collector->used = collector->capacity = 0;
			collector->num_rows = 0;
		}
		else
		{
			collector_reserve(collector, end);
			memcpy(collector->block + start, row, row->t_len);
			collector->used = end;
			collector->num_rows++;
			pfree(row);
		}
		MemoryContextSwitchTo(old_memory);
	}

	/* Hand the row on to the client, unchanged. */
	return collector->real_receiver->receiveSlot(slot, collector->real_receiver);
}

/*
 * Called before the first row, with the result's columns.  We start the
 * block here: the header and the column types come first.
 */
static void
collector_startup(DestReceiver *self, int operation, TupleDesc columns)
{
	RowCollector *collector = (RowCollector *) self;
	Size		types_offset = MAXALIGN(sizeof(ResultHeader));
	Size		rows_offset = MAXALIGN(types_offset + sizeof(Oid) * columns->natts);
	ResultHeader *header;

	collector_reserve(collector, rows_offset);
	header = (ResultHeader *) collector->block;
	memset(header, 0, rows_offset);
	header->num_columns = columns->natts;
	header->rows_offset = rows_offset;
	for (int i = 0; i < columns->natts; i++)
		RESULT_COLUMN_TYPES(header)[i] = TupleDescAttr(columns, i)->atttypid;
	collector->used = rows_offset;

	collector->real_receiver->rStartup(collector->real_receiver, operation, columns);
}

static void
collector_shutdown(DestReceiver *self)
{
	RowCollector *collector = (RowCollector *) self;

	collector->real_receiver->rShutdown(collector->real_receiver);
}

static void
collector_destroy(DestReceiver *self)
{
	/* Nothing to do: the collector is freed with the current query. */
}

static RowCollector *
create_row_collector(DestReceiver *real_receiver)
{
	RowCollector *collector;

	collector = MemoryContextAllocZero(current.memory, sizeof(RowCollector));
	collector->base.receiveSlot = collector_receive_row;
	collector->base.rStartup = collector_startup;
	collector->base.rShutdown = collector_shutdown;
	collector->base.rDestroy = collector_destroy;
	collector->base.mydest = DestNone;
	collector->real_receiver = real_receiver;
	collector->memory = AllocSetContextCreate(current.memory, "qcache rows",
											  ALLOCSET_DEFAULT_SIZES);
	return collector;
}

/* ---- 3b. Saving a result (after a miss) ---- */

static void
save_result(RowCollector *collector)
{
	Size		max_size = (Size) setting_max_size_kb * 1024;
	Size		rows_len;
	Size		text_len;
	Size		block_len;
	char	   *block;
	ResultHeader *header;
	CachedResult *entry;
	bool		found;

	if (collector->too_big || collector->block == NULL)
	{
		DEBUG_MSG("qcache: result is larger than qcache.max_result_size; not saved");
		return;
	}

	/*
	 * Did a table change while the query ran?  Then the rows may be older
	 * than the current versions, so we must not save them under those
	 * versions.  (With a private cache this happens only if the query itself
	 * caused a write; with a shared cache, any connection can.)
	 */
	if (!versions_unchanged(current.tables, current.num_tables, current.epoch))
	{
		DEBUG_MSG("qcache: a table changed while the query ran; not saved");
		return;
	}

	/* Finish the header. */
	header = (ResultHeader *) collector->block;
	header->num_rows = collector->num_rows;
	header->total_len = collector->used;

	/* The entry's block: [ rows | key | text ], see CachedResult. */
	rows_len = MAXALIGN(collector->used);
	text_len = strlen(current.query_text) + 1;
	block_len = rows_len + current.key_len + text_len;
	if (block_len > max_size)
	{
		DEBUG_MSG("qcache: result is larger than qcache.max_size; not saved");
		return;
	}

	/* Make room: remove the least recently used results first. */
	while (cache_size_bytes + block_len + sizeof(CachedResult) > max_size &&
		   !dlist_is_empty(&lru_list))
	{
		CachedResult *oldest = dlist_tail_element(CachedResult, lru_link, &lru_list);

		remove_cached_result(oldest);
		count_evictions++;
	}

	/*
	 * Copy rows, key and text into one new allocation in cache_memory.  The
	 * collector's own memory is freed with the current query.  (Copying
	 * costs one memcpy of the rows, small next to running the query.)
	 */
	block = MemoryContextAlloc(cache_memory, block_len);
	memcpy(block, collector->block, collector->used);
	memcpy(block + rows_len, current.key, current.key_len);
	memcpy(block + rows_len + current.key_len, current.query_text, text_len);

	/* Add an entry to the hash table. */
	entry = (CachedResult *) hash_search(cache_table, &current.key_hash, HASH_ENTER, &found);
	if (found)
	{
		/* Another key with the same hash: replace it. */
		dlist_delete(&entry->lru_link);
		cache_size_bytes -= entry->size_bytes;
		pfree(entry->block);
	}

	entry->block = block;
	entry->result = (ResultHeader *) block;
	entry->key = block + rows_len;
	entry->key_len = current.key_len;
	entry->query_text = block + rows_len + current.key_len;

	/*
	 * Count what the entry really uses: the allocation including palloc's
	 * overhead, plus the entry's slot in the hash table.
	 */
	entry->size_bytes = GetMemoryChunkSpace(block) + sizeof(CachedResult);

	entry->num_tables = current.num_tables;
	memcpy(entry->tables, current.tables, sizeof(TableVersion) * current.num_tables);
	entry->epoch = current.epoch;
	entry->hits = 0;

	dlist_push_head(&lru_list, &entry->lru_link);
	cache_size_bytes += entry->size_bytes;
	count_stores++;

	DEBUG_MSG("qcache: saved %llu row(s), %zu bytes",
			  (unsigned long long) header->num_rows, entry->size_bytes);
}

/* ---- 3c. Sending saved rows (on a hit) ---- */

/* The saved rows must have the same columns the client now expects. */
static bool
same_columns(ResultHeader *result, TupleDesc columns)
{
	if (result->num_columns != columns->natts)
		return false;
	for (int i = 0; i < columns->natts; i++)
	{
		if (RESULT_COLUMN_TYPES(result)[i] != TupleDescAttr(columns, i)->atttypid)
			return false;
	}
	return true;
}

/* Do what standard_ExecutorRun would do, using the saved rows. */
static void
send_saved_rows(QueryDesc *query_desc, ResultHeader *result)
{
	EState	   *estate = query_desc->estate;
	DestReceiver *receiver = query_desc->dest;
	MemoryContext old_memory = MemoryContextSwitchTo(estate->es_query_cxt);
	TupleTableSlot *slot;
	char	   *position = (char *) result + result->rows_offset;

	estate->es_processed = 0;
	receiver->rStartup(receiver, CMD_SELECT, query_desc->tupDesc);

	/* A slot holds one row at a time while it is passed to the receiver. */
	slot = MakeSingleTupleTableSlot(query_desc->tupDesc, &TTSOpsMinimalTuple);
	for (uint64 i = 0; i < result->num_rows; i++)
	{
		MinimalTuple row = (MinimalTuple) position;

		/* false: the slot must not free the row, the cache owns it. */
		ExecStoreMinimalTuple(row, slot, false);
		if (!receiver->receiveSlot(slot, receiver))
			break;
		estate->es_processed++;		/* becomes the "SELECT n" row count */

		position += MAXALIGN(row->t_len);	/* the next row starts aligned */
	}
	ExecDropSingleTupleTableSlot(slot);

	estate->es_total_processed += estate->es_processed;
	receiver->rShutdown(receiver);
	MemoryContextSwitchTo(old_memory);
}

/* ---- 3d. The hook itself ---- */

static void
qcache_ExecutorRun_hook(QueryDesc *query_desc, ScanDirection direction, uint64 count)
{
	bool		use_cache = false;
	RowCollector *collector = NULL;

	/*
	 * We use the cache only for the current query, only on its first run,
	 * and only when all rows are wanted at once (count == 0).  Cursors that
	 * fetch a few rows at a time are left alone.
	 */
	if (current.query_desc == query_desc && !current.already_ran &&
		count == 0 && ScanDirectionIsForward(direction))
		use_cache = true;

	if (current.query_desc == query_desc)
		current.already_ran = true;

	if (use_cache)
	{
		CachedResult *entry = find_cached_result(current.key_hash, current.key,
												 current.key_len);

		/*
		 * Is the saved result out of date?  Then throw it away now.  This
		 * is where a write's effect on the cache actually happens: the write
		 * only bumped a version counter.
		 */
		if (entry != NULL &&
			!versions_unchanged(entry->tables, entry->num_tables, entry->epoch))
		{
			DEBUG_MSG("qcache: saved result is out of date (a table it read has changed); removed");
			remove_cached_result(entry);
			count_invalidations++;
			entry = NULL;
		}

		if (entry != NULL && same_columns(entry->result, query_desc->tupDesc))
		{
			/* HIT: send the saved rows and skip running the plan. */
			DEBUG_MSG("qcache: HIT, sending %llu saved row(s)",
					  (unsigned long long) entry->result->num_rows);
			send_saved_rows(query_desc, entry->result);
			entry->hits++;
			count_hits++;
			dlist_move_head(&lru_list, &entry->lru_link);	/* recently used */
			forget_current_query();
			return;
		}

		/* MISS: put the row collector in front of the real receiver. */
		DEBUG_MSG("qcache: MISS, running the query and saving its rows");
		count_misses++;
		collector = create_row_collector(query_desc->dest);
		query_desc->dest = &collector->base;
	}

	/*
	 * Run the query as usual.  PG_TRY/PG_FINALLY is PostgreSQL's way of
	 * saying "run the FINALLY part even if an error happens", so the
	 * nesting level and the receiver are always put back.
	 */
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun_hook != NULL)
			prev_ExecutorRun_hook(query_desc, direction, count);
		else
			standard_ExecutorRun(query_desc, direction, count);
	}
	PG_FINALLY();
	{
		nesting_level--;
		if (collector != NULL)
			query_desc->dest = collector->real_receiver;
	}
	PG_END_TRY();

	/* After a miss, the collector holds every row: save them. */
	if (collector != NULL)
	{
		save_result(collector);
		forget_current_query();
	}
}


/* ======================================================================
 * PART 4. EXECUTOR FINISH AND END HOOKS: BOOKKEEPING
 * ====================================================================== */

static void
qcache_ExecutorFinish_hook(QueryDesc *query_desc)
{
	/* Anything run while finishing (for example triggers) is nested. */
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish_hook != NULL)
			prev_ExecutorFinish_hook(query_desc);
		else
			standard_ExecutorFinish(query_desc);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();
}

static void
qcache_ExecutorEnd_hook(QueryDesc *query_desc)
{
	/* If the query ended without running, forget it. */
	if (current.query_desc == query_desc)
		forget_current_query();

	if (prev_ExecutorEnd_hook != NULL)
		prev_ExecutorEnd_hook(query_desc);
	else
		standard_ExecutorEnd(query_desc);
}


/* ======================================================================
 * PART 5. UTILITY HOOK: ALTER, TRUNCATE, COPY AND FRIENDS
 *
 * Commands that are not SELECT/INSERT/UPDATE/DELETE/MERGE are "utility
 * commands", and they do not go through the executor hooks above.
 * ====================================================================== */

/* Utility commands that cannot change any table's data or structure. */
static bool
is_harmless_command(Node *command)
{
	switch (nodeTag(command))
	{
		case T_TransactionStmt:		/* BEGIN, COMMIT, ROLLBACK, ... */

			/*
			 * ... except COMMIT PREPARED: it makes the changes of a transaction
			 * prepared earlier visible, maybe one from another connection,
			 * and we do not know which tables that one wrote.
			 */
			return ((TransactionStmt *) command)->kind != TRANS_STMT_COMMIT_PREPARED;
		case T_VariableSetStmt:		/* SET */
		case T_VariableShowStmt:	/* SHOW */
		case T_ExplainStmt:
		case T_PrepareStmt:
		case T_ExecuteStmt:
		case T_DeallocateStmt:
		case T_DeclareCursorStmt:
		case T_FetchStmt:
		case T_ClosePortalStmt:
		case T_CallStmt:		/* writes inside procedures and DO blocks */
		case T_DoStmt:			/* still pass through the executor hooks */
		case T_ListenStmt:
		case T_UnlistenStmt:
		case T_NotifyStmt:
		case T_CheckPointStmt:
		case T_VacuumStmt:
			return true;
		default:
			return false;
	}
}

static void
qcache_ProcessUtility_hook(PlannedStmt *plan, const char *query_string,
						   bool read_only_tree, ProcessUtilityContext context,
						   ParamListInfo params, QueryEnvironment *query_env,
						   DestReceiver *dest, QueryCompletion *completion)
{
	/* Anything else might change data: bump the epoch at COMMIT. */
	if (!is_harmless_command(plan->utilityStmt))
	{
		if (!structure_changed)
			DEBUG_MSG("qcache: this command may change tables; every saved result "
					  "goes out of date at COMMIT");
		structure_changed = true;
	}

	nesting_level++;
	PG_TRY();
	{
		if (prev_ProcessUtility_hook != NULL)
			prev_ProcessUtility_hook(plan, query_string, read_only_tree, context,
									 params, query_env, dest, completion);
		else
			standard_ProcessUtility(plan, query_string, read_only_tree, context,
									params, query_env, dest, completion);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();
}


/* ======================================================================
 * PART 6. TRANSACTION CALLBACK: COMMIT AND ROLLBACK
 *
 * PostgreSQL calls this at the end of every transaction.  Outside BEGIN ...
 * COMMIT, every single statement is its own transaction, so this also runs
 * after a lone UPDATE.
 *
 * A COMMIT only bumps version counters; it never looks at the cache.  The
 * cost of a COMMIT therefore no longer grows with the number of saved
 * results (it used to scan all of them).
 *
 * Note for the shared cache (step 2): PostgreSQL makes a commit visible to
 * other connections BEFORE it calls us with XACT_EVENT_COMMIT.  The shared
 * version must therefore also bump at XACT_EVENT_PRE_COMMIT.
 * ====================================================================== */

static void
qcache_transaction_callback(XactEvent event, void *arg)
{
	ListCell   *cell;

	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PREPARE:
			/*
			 * The changes are now permanent: make old results out of date,
			 * in every connection (the counters are shared).
			 */
			foreach(cell, tables_written)
				BUMP_VERSION(table_slot(lfirst_oid(cell)));
			if (structure_changed)
				BUMP_VERSION(EPOCH_SLOT);

			if (structure_changed)
				DEBUG_MSG("qcache: COMMIT: every saved result is now out of date");
			else if (tables_written != NIL)
				DEBUG_MSG("qcache: COMMIT: %d table(s) changed; saved results that "
						  "read them are now out of date", list_length(tables_written));

			forget_transaction_changes();
			forget_current_query();
			plan_just_made = NULL;
			break;

		case XACT_EVENT_ABORT:
			/* ROLLBACK or an error: the data did not change, keep the cache. */
			forget_transaction_changes();
			forget_current_query();
			plan_just_made = NULL;
			break;

		default:
			break;
	}
}


/* ======================================================================
 * PART 7. SQL FUNCTIONS: qcache_stats(), qcache_entries(), qcache_reset()
 *
 * These are declared in qcache--1.0.sql, which CREATE EXTENSION runs.
 * ====================================================================== */

Datum
qcache_stats(PG_FUNCTION_ARGS)
{
	TupleDesc	result_columns;
	Datum		values[7];
	bool		nulls[7] = {false, false, false, false, false, false, false};

	if (get_call_result_type(fcinfo, NULL, &result_columns) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	values[0] = Int64GetDatum(hash_get_num_entries(cache_table));
	values[1] = Int64GetDatum((int64) cache_size_bytes);
	values[2] = Int64GetDatum((int64) count_hits);
	values[3] = Int64GetDatum((int64) count_misses);
	values[4] = Int64GetDatum((int64) count_stores);
	values[5] = Int64GetDatum((int64) count_invalidations);
	values[6] = Int64GetDatum((int64) count_evictions);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(result_columns, values, nulls)));
}

Datum
qcache_entries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *result = (ReturnSetInfo *) fcinfo->resultinfo;
	dlist_iter	iter;

	/* Prepare to return many rows at once. */
	InitMaterializedSRF(fcinfo, 0);

	/* One row per saved result, most recently used first. */
	dlist_foreach(iter, &lru_list)
	{
		CachedResult *entry = dlist_container(CachedResult, lru_link, iter.cur);
		Datum		values[4];
		bool		nulls[4] = {false, false, false, false};

		values[0] = CStringGetTextDatum(entry->query_text);
		values[1] = Int64GetDatum((int64) entry->result->num_rows);
		values[2] = Int64GetDatum((int64) entry->size_bytes);
		values[3] = Int64GetDatum((int64) entry->hits);
		tuplestore_putvalues(result->setResult, result->setDesc, values, nulls);
	}
	return (Datum) 0;
}

Datum
qcache_reset(PG_FUNCTION_ARGS)
{
	remove_all_cached_results();
	count_hits = 0;
	count_misses = 0;
	count_stores = 0;
	count_invalidations = 0;
	count_evictions = 0;
	PG_RETURN_VOID();
}


/* ======================================================================
 * PART 8. SHARED MEMORY
 *
 * PostgreSQL creates its shared memory once, when the server starts, and
 * every connection process inherits it.  An extension gets a piece of it in
 * two steps, through two hooks:
 *
 *   1. shmem_request_hook: "I will need this many bytes."  Called while the
 *      server computes how much shared memory to create.
 *   2. shmem_startup_hook: "Now set up my bytes."  Called once the memory
 *      exists.  ShmemInitStruct() finds our piece by name, or creates it.
 *
 * The same pattern is used by contrib/pg_stat_statements.
 * ====================================================================== */

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static void
qcache_shmem_request(void)
{
	if (prev_shmem_request_hook != NULL)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(sizeof(QcacheShared));
}

static void
qcache_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook != NULL)
		prev_shmem_startup_hook();

	/*
	 * AddinShmemInitLock makes sure only one process at a time sets up
	 * extension memory.  "found" is false the first time, when the block
	 * has just been created and must be initialized.
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	shared = ShmemInitStruct("qcache", sizeof(QcacheShared), &found);
	if (!found)
	{
		for (int i = 0; i < VERSION_SLOTS; i++)
			pg_atomic_init_u64(&shared->version[i], 0);
	}
	LWLockRelease(AddinShmemInitLock);
}


/* ======================================================================
 * PART 9. _PG_init: CALLED ONCE WHEN POSTGRESQL LOADS THIS LIBRARY
 *
 * With shared_preload_libraries = 'qcache' this runs when the server
 * starts, and every connection then starts with our hooks in place.
 * ====================================================================== */

void
_PG_init(void)
{
	HASHCTL		table_options;

	/*
	 * 0. Shared memory can only be requested while the server starts, so
	 * refuse to be loaded any other way (for example with LOAD).
	 */
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errmsg("qcache must be loaded via shared_preload_libraries"),
				 errhint("Add qcache to shared_preload_libraries in postgresql.conf "
						 "and restart the server.")));

	/* 1. Register the settings. */
	DefineCustomBoolVariable("qcache.enabled",
							 "Use and fill the query result cache.",
							 NULL, &setting_enabled, true,
							 PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("qcache.debug",
							 "Print a message at every step qcache takes.",
							 NULL, &setting_debug, false,
							 PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("qcache.max_size",
							"Memory for cached results, per connection.",
							NULL, &setting_max_size_kb, 64 * 1024,
							0, INT_MAX / 1024, PGC_USERSET, GUC_UNIT_KB,
							NULL, NULL, NULL);
	DefineCustomIntVariable("qcache.max_result_size",
							"Largest single result that is cached.",
							NULL, &setting_max_result_kb, 1024,
							0, INT_MAX / 1024, PGC_USERSET, GUC_UNIT_KB,
							NULL, NULL, NULL);
	MarkGUCPrefixReserved("qcache");

	/* 2. Ask for our piece of shared memory (PART 8). */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = qcache_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = qcache_shmem_startup;

	/* 3. Create our private memory context and hash table. */
	cache_memory = AllocSetContextCreate(TopMemoryContext, "qcache",
										 ALLOCSET_DEFAULT_SIZES);

	memset(&table_options, 0, sizeof(table_options));
	table_options.keysize = sizeof(uint64);
	table_options.entrysize = sizeof(CachedResult);
	table_options.hcxt = cache_memory;
	cache_table = hash_create("qcache results", 1024, &table_options,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/*
	 * 4. Install the hooks.  We save the previous value of each hook first,
	 * so that another extension using the same hook keeps working: our
	 * function calls the previous one instead of the standard function.
	 */
	prev_planner_hook = planner_hook;
	planner_hook = qcache_planner_hook;

	prev_ExecutorStart_hook = ExecutorStart_hook;
	ExecutorStart_hook = qcache_ExecutorStart_hook;

	prev_ExecutorRun_hook = ExecutorRun_hook;
	ExecutorRun_hook = qcache_ExecutorRun_hook;

	prev_ExecutorFinish_hook = ExecutorFinish_hook;
	ExecutorFinish_hook = qcache_ExecutorFinish_hook;

	prev_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = qcache_ExecutorEnd_hook;

	prev_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = qcache_ProcessUtility_hook;

	/* 5. Ask to be told when transactions end. */
	RegisterXactCallback(qcache_transaction_callback, NULL);
}
