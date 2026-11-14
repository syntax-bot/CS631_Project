/*
 * qcache.c - a basic query result cache for PostgreSQL 18
 * =======================================================
 *
 * WHAT IT DOES
 *   When a SELECT finishes, we keep a copy of its result rows in memory.
 *   When the exact same SELECT is sent again, we send the saved rows back
 *   to the client instead of running the query.
 *
 *   When a transaction that changed a table COMMITs, we throw away every
 *   saved result that was read from that table, so we never return old data.
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
 *     1. planner_hook        is this query safe to cache?
 *     2. ExecutorStart_hook  build the cache key; notice INSERT/UPDATE/DELETE
 *     3. ExecutorRun_hook    cache hit: send saved rows
 *                            cache miss: run the query and save its rows
 *     4. ProcessUtility_hook notice commands like ALTER TABLE and TRUNCATE
 *     5. a transaction callback, called at COMMIT and ROLLBACK
 *
 * The cache entries and their table versions are shared by all connections.
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
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "catalog/pg_authid.h"
#include "utils/acl.h"
#include "utils/injection_point.h"

/* The hook functions below are written for PostgreSQL 18. */
#if PG_VERSION_NUM < 180000 || PG_VERSION_NUM >= 190000
#error "This version of qcache is written for PostgreSQL 18"
#endif

/* Every loadable module must contain this line. */
PG_MODULE_MAGIC;


/* ======================================================================
 * PART A. SETTINGS
 *
 * These can be changed with SET, for example:  SET qcache.enabled = off;
 * They are registered with PostgreSQL in _PG_init() at the end of the file.
 * ====================================================================== */

static bool setting_enabled = true;		/* qcache.enabled */
static bool setting_debug = false;		/* qcache.debug */
static int	setting_max_size_kb = 64 * 1024;	/* qcache.max_size: 64 MB */
static int	setting_max_result_kb = 1024;	/* qcache.max_result_size: 1 MB */
#define MAX_TABLES 8
#define VERSION_SLOTS 65536
#define EPOCH_SLOT 0
#define QC_MAX_PARTITIONS 32
#define QC_CHUNK_SIZE 1024
#define QC_MAX_ENTRIES 65536
static int setting_partitions = QC_MAX_PARTITIONS;
static int setting_max_entries = QC_MAX_ENTRIES;
/*
 * qcache.test_skip_precommit: FOR TESTS ONLY.  Skip the PRE_COMMIT bump and
 * bump once at COMMIT, as step 1 did.  The race test uses it to show the
 * stale read that the PRE_COMMIT bump prevents.
 */
static bool setting_test_skip_precommit = false;

static const char *const key_settings[] ={
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
typedef struct ResultHeader{
	Size total_len;
	uint64 num_rows;
	uint32 num_columns;
	uint32 rows_offset;
}ResultHeader;

#define RESULT_COLUMN_TYPES(header) ((Oid *)((char *)(header) + MAXALIGN(sizeof(ResultHeader))))

typedef struct TableVersion{
	Oid oid;
	uint32 slot;
	uint64 version;
}TableVersion;

typedef struct QcChunk
{
	struct QcChunk *next;
	char data[QC_CHUNK_SIZE - sizeof(struct QcChunk *)];
} QcChunk;
#define QC_CHUNK_DATA (sizeof(((QcChunk *) 0)->data))

/*
 * One saved query result.  All saved results live in a hash table, so that
 * finding one by its key is fast.
 */
typedef struct CachedResult
{
	uint64		key_hash;		/* the hash table's key (must be the first field) */

	QcChunk *first;
	uint32 block_len;
	uint32 nchunks;
	uint32 key_len;
	uint32 query_text_len;
	Oid dbid;
	Oid userid;
	int16 num_tables;
	TableVersion tables[MAX_TABLES];
	uint64 epoch;

	pg_atomic_uint32 referenced;
	pg_atomic_uint64 hits;
	dlist_node	lru_link;		/* position in the "recently used" list */
} CachedResult;

typedef struct QcPartition
{
	LWLock *lock;
	dlist_head lru;
	QcChunk *free;
	uint32 nfree;
	uint64 bytes_used;
} QcPartition;

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
	int			num_tables;
	TableVersion tables[MAX_TABLES];
	uint64 epoch;
	char *hit_block;
	Size hit_block_len;

	bool		already_ran;	/* ExecutorRun was already called once */
} CurrentQuery;

/*
 * A "row collector".  PostgreSQL sends every result row to a receiver
 * object (a DestReceiver) that passes it on to the client.  On a cache miss
 * we put our collector in front of the real receiver: it copies each row
 * for the cache, then hands the row on to the real receiver.
 */
typedef struct RowCollector
{
	DestReceiver base;			/* must be first: PostgreSQL sees only this part */
	DestReceiver *real_receiver;	/* where the rows really go (the client) */

	MemoryContext memory;
	char *block;
	Size used;
	Size capacity;
	uint64		num_rows;
	
	bool		too_big;		/* the result passed qcache.max_result_size */
} RowCollector;


/* ======================================================================
 * PART C. GLOBAL VARIABLES
 *
 * The cache entries live in shared memory.  Each backend keeps only local
 * handles for the shared hash tables and temporary state for its query.
 * ====================================================================== */

static MemoryContext cache_memory = NULL;	/* parent of current-query state */
static HTAB *part_hash[QC_MAX_PARTITIONS];	/* local handles for shared hashes */
typedef struct QcacheShared{
	pg_atomic_uint64 version[VERSION_SLOTS];
	pg_atomic_uint32 inflight[VERSION_SLOTS];
	QcPartition parts[QC_MAX_PARTITIONS];
	pg_atomic_uint64 hits;
	pg_atomic_uint64 misses;
	pg_atomic_uint64 stores;
	pg_atomic_uint64 invalidations;
	pg_atomic_uint64 evictions;
}QcacheShared;

static QcacheShared *shared = NULL;

#define VERSION(slot) pg_atomic_read_membarrier_u64(&shared->version[(slot)])
#define INFLIGHT(slot) pg_atomic_read_membarrier_u32(&shared->inflight[(slot)])
#define BUMP_VERSION(slot) pg_atomic_fetch_add_u64(&shared->version[(slot)], 1)

static CurrentQuery current;	/* the SELECT running now (zeroed if none) */

/*
 * How deep we are in nested queries.  0 means a query sent by the client.
 * A query run inside a function (or EXPLAIN, a cursor...) is at level 1+.
 * We only cache level-0 queries.
 */
static int	nesting_level = 0;

/* What the planner hook decided about the plan it made most recently. */
static PlannedStmt *plan_just_made = NULL;
static const char *plan_just_made_problem = NULL;	/* NULL means safe */
static int	plan_just_made_num_tables = 0;
static TableVersion plan_just_made_tables[MAX_TABLES];
static uint64 plan_just_made_epoch = 0;

/* What the current transaction changed; acted on at COMMIT. */
static List *tables_written = NIL;	/* list of table OIDs */
static bool structure_changed = false;	/* ALTER, TRUNCATE, COPY, ... */
static bool precommit_done = false;

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
 * PART D. SMALL HELPERS FOR THE CACHE TABLE
 * ====================================================================== */

static uint32 table_slot(Oid table_oid){
	uint32 h = hash_combine(murmurhash32((uint32) MyDatabaseId), murmurhash32((uint32) table_oid));
	return 1 + h % (VERSION_SLOTS - 1);
}

/*
 * Are the tables still at the versions in the list, and the epoch unchanged?
 * If yes, a result computed at those versions is still correct.
 */
static bool versions_unchanged(const TableVersion *tables, int num_tables, uint64 epoch){
	if(INFLIGHT(EPOCH_SLOT) != 0 || VERSION(EPOCH_SLOT) != epoch) return false;
	for(int i = 0; i < num_tables; i++){
		if(INFLIGHT(tables[i].slot) != 0 ||
		   VERSION(tables[i].slot) != tables[i].version)
			return false;
	}
	return true;
}

static int
partition_index(uint64 key_hash)
{
	return key_hash % setting_partitions;
}

/*
 * Sizes derived from the settings.  qcache.max_size, qcache.partitions and
 * qcache.max_entries can only change at server start, so every process
 * computes the same values.
 */
static Size
total_chunks(void)
{
	return ((Size) setting_max_size_kb * 1024) / QC_CHUNK_SIZE;
}

static Size
chunks_per_partition(void)
{
	return total_chunks() / setting_partitions;
}

static long
entries_per_partition(void)
{
	return Max(1, (setting_max_entries + setting_partitions - 1) / setting_partitions);
}

/* How many chunks a block of this length needs. */
static uint32
chunks_for(Size block_len)
{
	return (block_len + QC_CHUNK_DATA - 1) / QC_CHUNK_DATA;
}

static bool
entry_key_matches(const CachedResult *entry, const char *key, int key_len)
{
	QcChunk    *chunk = entry->first;
	Size		offset;
	Size		done = 0;

	if (entry->key_len != (uint32) key_len)
		return false;

	offset = MAXALIGN((Size) ((ResultHeader *) entry->first->data)->total_len);
	while (offset >= QC_CHUNK_DATA)
	{
		chunk = chunk->next;
		offset -= QC_CHUNK_DATA;
	}

	while (done < (Size) key_len)
	{
		Size		n = Min((Size) key_len - done, QC_CHUNK_DATA - offset);

		if (memcmp(chunk->data + offset, key + done, n) != 0)
			return false;
		done += n;
		offset = 0;
		chunk = chunk->next;
	}
	return true;
}

static void
copy_entry_block(const CachedResult *entry, char *destination)
{
	QcChunk *chunk = entry->first;
	Size copied = 0;

	while (copied < entry->block_len)
	{
		Size amount = Min((Size) QC_CHUNK_DATA, (Size) entry->block_len - copied);

		memcpy(destination + copied, chunk->data, amount);
		copied += amount;
		chunk = chunk->next;
	}
}

static void
free_entry_chunks(QcPartition *partition, CachedResult *entry)
{
	QcChunk *chunk = entry->first;

	while (chunk != NULL)
	{
		QcChunk *next = chunk->next;

		chunk->next = partition->free;
		partition->free = chunk;
		partition->nfree++;
		chunk = next;
	}
	entry->first = NULL;
}

static bool
allocate_entry_chunks(QcPartition *partition, CachedResult *entry, Size block_len)
{
	QcChunk *last = NULL;
	Size needed = chunks_for(block_len);

	if (needed > partition->nfree)
		return false;

	entry->nchunks = needed;
	entry->first = NULL;
	for (Size i = 0; i < needed; i++)
	{
		QcChunk *chunk = partition->free;

		partition->free = chunk->next;
		partition->nfree--;
		chunk->next = NULL;
		if (last == NULL)
			entry->first = chunk;
		else
			last->next = chunk;
		last = chunk;
	}
	return true;
}

static void
remove_cached_result(QcPartition *partition, CachedResult *entry, bool count_invalidation)
{
	uint64 key_hash = entry->key_hash;

	dlist_delete(&entry->lru_link);
	partition->bytes_used -= (uint64) entry->nchunks * QC_CHUNK_SIZE;
	free_entry_chunks(partition, entry);
	hash_search(part_hash[partition - shared->parts], &key_hash, HASH_REMOVE, NULL);
	if (count_invalidation)
		pg_atomic_fetch_add_u64(&shared->invalidations, 1);
}

static void
remove_all_cached_results(void)
{
	for (int i = 0; i < setting_partitions; i++)
	{
		QcPartition *partition = &shared->parts[i];

		LWLockAcquire(partition->lock, LW_EXCLUSIVE);
		while (!dlist_is_empty(&partition->lru))
		{
			CachedResult *entry =
				dlist_tail_element(CachedResult, lru_link, &partition->lru);
			remove_cached_result(partition, entry, false);
		}
		LWLockRelease(partition->lock);
	}
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
 * PART 1. PLANNER HOOK: IS THIS QUERY SAFE TO CACHE?
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

static const char * record_table_versions(PlannedStmt *plan){
	ListCell *cell;
	int n = 0;

	plan_just_made_epoch = VERSION(EPOCH_SLOT);
	if (INFLIGHT(EPOCH_SLOT) != 0)
		return "a commit is in progress";

	foreach(cell, plan->rtable)
	{
		RangeTblEntry *table = (RangeTblEntry *) lfirst(cell);
		bool		seen = false;

		if (table->rtekind != RTE_RELATION)
			continue;
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
		if (INFLIGHT(plan_just_made_tables[n].slot) != 0)
			return "a commit is in progress";
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
					  "read it will be removed at COMMIT", get_rel_name(table->relid));
		}
	}

	MemoryContextSwitchTo(old_memory);
}

/*
 * Checks that need the plan.  Returns NULL if we may cache the query, or a
 * reason why not.  On success, *table_oids is the list of tables it reads.
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
 *   - the user running it            (users can have different permissions)
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
	for(int i = 0; i < lengthof(key_settings); i++){
		const char *value = GetConfigOption(key_settings[i], true, false);
		appendStringInfoString(&key, value ? value : "");
		appendStringInfoChar(&key, '\0');
	}
	appendBinaryStringInfo(&key, &start, sizeof(start));
	appendBinaryStringInfo(&key, &length, sizeof(length));
	for(int i = 0; i < current.num_tables; i++){
		appendBinaryStringInfo(&key, &current.tables[i].oid, sizeof(Oid));
	}
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

	/* Yes: remember it as the current query, with its key. */
	forget_current_query();
	current.query_desc = query_desc;
	current.memory = AllocSetContextCreate(cache_memory, "qcache current query",
										   ALLOCSET_DEFAULT_SIZES);
	
	current.num_tables = plan_just_made_num_tables;
	memcpy(current.tables, plan_just_made_tables, sizeof(TableVersion) * plan_just_made_num_tables);
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
 * Copy a row.  Large values (over about 2 kB) may be stored outside the
 * row ("TOAST") and the row only points at them; we copy the values
 * themselves, so a saved row never points into a table.
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
static void collector_reserve(RowCollector *collector, Size needed){
	Size new_capacity;

	if(needed <= collector->capacity) return;

	new_capacity = Max(collector->capacity * 2, (Size) 8192);
	while(new_capacity < needed){
		new_capacity *= 2;
	}

	if(collector->block == NULL){
		collector->block = MemoryContextAlloc(collector->memory, new_capacity);
	}else{
		collector->block = repalloc(collector->block, new_capacity);
	}
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
		Size start = MAXALIGN(collector->used);
		Size end = start + row->t_len;

		if (end > (Size) setting_max_result_kb * 1024)
		{
			/* Too big to cache: free what we copied and stop copying. */
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

/* Called before the first row and after the last: just pass them on. */
static void
collector_startup(DestReceiver *self, int operation, TupleDesc columns)
{
	RowCollector *collector = (RowCollector *) self;
	Size types_offset = MAXALIGN(sizeof(ResultHeader));
	Size rows_offset = MAXALIGN(types_offset + sizeof(Oid) * columns->natts);
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
	uint32		needed;
	char	   *block;
	ResultHeader *header;
	CachedResult *entry;
	QcPartition *partition;
	int partition_no;
	bool		found;

	if (collector->too_big || collector->block == NULL)
	{
		DEBUG_MSG("qcache: result is larger than qcache.max_result_size; not saved");
		return;
	}

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
	needed = chunks_for(block_len);
	if (needed > chunks_per_partition())
	{
		DEBUG_MSG("qcache: result does not fit in its partition; not saved");
		return;
	}

	partition_no = partition_index(current.key_hash);
	partition = &shared->parts[partition_no];

	block = MemoryContextAlloc(current.memory, block_len);
	memcpy(block, collector->block, collector->used);
	memcpy(block + rows_len, current.key, current.key_len);
	memcpy(block + rows_len + current.key_len, current.query_text, text_len);

	LWLockAcquire(partition->lock, LW_EXCLUSIVE);
	entry = (CachedResult *) hash_search(part_hash[partition_no],
										 &current.key_hash, HASH_FIND, NULL);
	if (entry != NULL)
		remove_cached_result(partition, entry, false);
	while ((partition->nfree < needed ||
			hash_get_num_entries(part_hash[partition_no]) >= entries_per_partition()) &&
		   !dlist_is_empty(&partition->lru))
	{
		CachedResult *oldest =
			dlist_tail_element(CachedResult, lru_link, &partition->lru);

		if (pg_atomic_exchange_u32(&oldest->referenced, 0) != 0)
			dlist_move_head(&partition->lru, &oldest->lru_link);
		else
		{
			remove_cached_result(partition, oldest, false);
			pg_atomic_fetch_add_u64(&shared->evictions, 1);
		}
	}

	/* Add an entry to the shared hash table. */
	entry = (CachedResult *) hash_search(part_hash[partition_no],
										 &current.key_hash,
										 HASH_ENTER_NULL, &found);
	if (entry == NULL)
	{
		LWLockRelease(partition->lock);
		DEBUG_MSG("qcache: partition hash table is full; not saved");
		return;
	}

	memset(entry, 0, sizeof(*entry));
	entry->key_len = current.key_len;
	entry->query_text_len = text_len;
	entry->block_len = block_len;
	entry->key_hash = current.key_hash;
	entry->dbid = MyDatabaseId;
	entry->userid = GetUserId();
	if (!allocate_entry_chunks(partition, entry, block_len))
	{
		hash_search(part_hash[partition_no], &current.key_hash, HASH_REMOVE, NULL);
		LWLockRelease(partition->lock);
		DEBUG_MSG("qcache: shared arena is full; not saved");
		return;
	}
	{
		QcChunk *chunk = entry->first;
		Size copied = 0;

		while (copied < block_len)
		{
			Size amount = Min((Size) QC_CHUNK_DATA, block_len - copied);

			memcpy(chunk->data, block + copied, amount);
			copied += amount;
			chunk = chunk->next;
		}
	}

	entry->num_tables = current.num_tables;
	memcpy(entry->tables, current.tables, sizeof(TableVersion) * current.num_tables);
	entry->epoch = current.epoch;
	pg_atomic_init_u32(&entry->referenced, 0);
	pg_atomic_init_u64(&entry->hits, 0);

	dlist_push_head(&partition->lru, &entry->lru_link);
	partition->bytes_used += (uint64) entry->nchunks * QC_CHUNK_SIZE;
	pg_atomic_fetch_add_u64(&shared->stores, 1);
	LWLockRelease(partition->lock);

	DEBUG_MSG("qcache: saved %llu row(s), %zu bytes in %u chunk(s)",
			  (unsigned long long) header->num_rows, block_len, needed);
}

/* ---- 3c. Sending saved rows (on a hit) ---- */

/* The saved rows must have the same columns the client now expects. */
static bool
same_columns(ResultHeader *result, TupleDesc columns)
{
	if(result->num_columns != columns->natts) return false;
	for(int i = 0; i < columns->natts; i++){
		if(RESULT_COLUMN_TYPES(result)[i] != TupleDescAttr(columns, i)->atttypid) return false;
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
	char *position = (char *) result + result->rows_offset;

	estate->es_processed = 0;
	receiver->rStartup(receiver, CMD_SELECT, query_desc->tupDesc);

	/* A slot holds one row at a time while it is passed to the receiver. */
	slot = MakeSingleTupleTableSlot(query_desc->tupDesc, &TTSOpsMinimalTuple);
	for (uint64 i = 0; i < result->num_rows; i++)
	{
		MinimalTuple row = (MinimalTuple) position;
		ExecStoreMinimalTuple(row, slot, false);
		if (!receiver->receiveSlot(slot, receiver))
			break;
		estate->es_processed++;		/* becomes the "SELECT n" row count */
		position += MAXALIGN(row->t_len);
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
		int partition_no = partition_index(current.key_hash);
		QcPartition *partition = &shared->parts[partition_no];
		CachedResult *entry;
		bool hit = false;
		bool stale = false;

		LWLockAcquire(partition->lock, LW_SHARED);
		entry = (CachedResult *) hash_search(part_hash[partition_no],
											 &current.key_hash,
											 HASH_FIND, NULL);
		if (entry != NULL && entry_key_matches(entry, current.key, current.key_len) &&
			versions_unchanged(entry->tables, entry->num_tables, entry->epoch))
		{
			ResultHeader *result;

			current.hit_block = MemoryContextAlloc(current.memory, entry->block_len);
			current.hit_block_len = entry->block_len;
			copy_entry_block(entry, current.hit_block);
			result = (ResultHeader *) current.hit_block;
			hit = same_columns(result, query_desc->tupDesc);
			if (hit)
			{
				pg_atomic_write_u32(&entry->referenced, 1);
				pg_atomic_fetch_add_u64(&entry->hits, 1);
				pg_atomic_fetch_add_u64(&shared->hits, 1);
			}
		}
		else if (entry != NULL &&
				 entry_key_matches(entry, current.key, current.key_len))
			stale = true;
		LWLockRelease(partition->lock);

		if (stale)
		{
			DEBUG_MSG("qcache: saved result is out of date (a table it read has changed)");
			LWLockAcquire(partition->lock, LW_EXCLUSIVE);
			entry = (CachedResult *) hash_search(part_hash[partition_no],
												 &current.key_hash,
												 HASH_FIND, NULL);
			if (entry != NULL &&
				entry_key_matches(entry, current.key, current.key_len) &&
				!versions_unchanged(entry->tables, entry->num_tables,
									entry->epoch))
				remove_cached_result(partition, entry, true);
			LWLockRelease(partition->lock);
		}

		if (hit)
		{
			/* HIT: send the saved rows and skip running the plan. */
			DEBUG_MSG("qcache: HIT, sending %llu saved row(s)",
					  (unsigned long long) ((ResultHeader *) current.hit_block)->num_rows);
			send_saved_rows(query_desc, (ResultHeader *) current.hit_block);
			forget_current_query();
			return;
		}

		/* MISS: put the row collector in front of the real receiver. */
		DEBUG_MSG("qcache: MISS, running the query and saving its rows");
		pg_atomic_fetch_add_u64(&shared->misses, 1);
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
	bool		commit_prepared = IsA(plan->utilityStmt, TransactionStmt) &&
		((TransactionStmt *) plan->utilityStmt)->kind == TRANS_STMT_COMMIT_PREPARED;
	/* Anything else might change data: clear the whole cache at COMMIT. */
	if (!is_harmless_command(plan->utilityStmt))
	{
		if (!structure_changed)
			DEBUG_MSG("qcache: this command may change tables; the whole cache "
					  "will be cleared at COMMIT");
		structure_changed = true;
	}
	if (commit_prepared)
	{
		pg_atomic_fetch_add_u32(&shared->inflight[EPOCH_SLOT], 1);
		BUMP_VERSION(EPOCH_SLOT);
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
		if (commit_prepared)
		{
			BUMP_VERSION(EPOCH_SLOT);
			pg_atomic_fetch_sub_u32(&shared->inflight[EPOCH_SLOT], 1);
		}
	}
	PG_END_TRY();
}


/* ======================================================================
 * PART 6. TRANSACTION CALLBACK: COMMIT AND ROLLBACK
 *
 * PostgreSQL calls this at the end of every transaction.  Outside BEGIN ...
 * COMMIT, every single statement is its own transaction, so this also runs
 * after a lone UPDATE.
 * ====================================================================== */

static void
bump_written(bool begin_inflight, bool end_inflight)
{
	ListCell   *cell;

	foreach(cell, tables_written)
	{
		uint32		slot = table_slot(lfirst_oid(cell));

		if (begin_inflight)
			pg_atomic_fetch_add_u32(&shared->inflight[slot], 1);
		BUMP_VERSION(slot);
		if (end_inflight)
			pg_atomic_fetch_sub_u32(&shared->inflight[slot], 1);
	}
	if (structure_changed)
	{
		if (begin_inflight)
			pg_atomic_fetch_add_u32(&shared->inflight[EPOCH_SLOT], 1);
		BUMP_VERSION(EPOCH_SLOT);
		if (end_inflight)
			pg_atomic_fetch_sub_u32(&shared->inflight[EPOCH_SLOT], 1);
	}
}

 static void
qcache_transaction_callback(XactEvent event, void *arg)
{
	bool		wrote = (tables_written != NIL || structure_changed);

	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
			if (!wrote || setting_test_skip_precommit)
				break;
			bump_written(true, false);
			precommit_done = true;
			break;

		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PREPARE:
			/*
			 * The changes are now permanent.  The second version bump
			 * prevents a reader from treating the in-flight window as stable.
			 */
			if (wrote)
				INJECTION_POINT("qcache-before-commit-bump", NULL);
			if (precommit_done)
			{
				bump_written(false, true);
				precommit_done = false;
			}
			else if (wrote)
			{
				/*
				 * No PRE_COMMIT bump happened: PREPARE TRANSACTION (which has
				 * no PRE_COMMIT event), or qcache.test_skip_precommit.  Bump
				 * once now; for PREPARE the data becomes visible only at
				 * COMMIT PREPARED, which bumps the epoch again.
				 */
				bump_written(false, false);
			}

			if(structure_changed){
				DEBUG_MSG("qcache: COMMIT: every saved result is now out of date");
			}else if(tables_written != NIL){
				DEBUG_MSG("qcache: COMMIT: %d table(s) changed; saved results that "
						  "read them are now out of date", list_length(tables_written));
			}
			forget_transaction_changes();
			forget_current_query();
			plan_just_made = NULL;
			break;

		case XACT_EVENT_ABORT:
			/* ROLLBACK or an error: the data did not change, keep the cache. */
			if (precommit_done)
			{
				bump_written(false, true);
				precommit_done = false;
			}
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
	int64		entries = 0;
	int64		bytes = 0;

	if (get_call_result_type(fcinfo, NULL, &result_columns) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	for (int i = 0; i < setting_partitions; i++)
	{
		QcPartition *partition = &shared->parts[i];

		LWLockAcquire(partition->lock, LW_SHARED);
		entries += hash_get_num_entries(part_hash[i]);
		bytes += partition->bytes_used;
		LWLockRelease(partition->lock);
	}

	values[0] = Int64GetDatum(entries);
	values[1] = Int64GetDatum(bytes);
	values[2] = Int64GetDatum((int64) pg_atomic_read_u64(&shared->hits));
	values[3] = Int64GetDatum((int64) pg_atomic_read_u64(&shared->misses));
	values[4] = Int64GetDatum((int64) pg_atomic_read_u64(&shared->stores));
	values[5] = Int64GetDatum((int64) pg_atomic_read_u64(&shared->invalidations));
	values[6] = Int64GetDatum((int64) pg_atomic_read_u64(&shared->evictions));

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(result_columns, values, nulls)));
}

Datum
qcache_entries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *result = (ReturnSetInfo *) fcinfo->resultinfo;
	bool		see_all = has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS);
	/* Prepare to return many rows at once. */
	InitMaterializedSRF(fcinfo, 0);

	for (int i = 0; i < setting_partitions; i++)
	{
		QcPartition *partition = &shared->parts[i];
		dlist_iter iter;

		LWLockAcquire(partition->lock, LW_SHARED);
		dlist_foreach(iter, &partition->lru)
		{
			CachedResult *entry =
				dlist_container(CachedResult, lru_link, iter.cur);
			if (entry->dbid != MyDatabaseId)
				continue;
			Datum values[4];
			bool nulls[4] = {false, false, false, false};
			char *block = MemoryContextAlloc(CurrentMemoryContext, entry->block_len);
			ResultHeader *header;
			Size text_offset;

			copy_entry_block(entry, block);
			header = (ResultHeader *) block;
			text_offset = MAXALIGN(header->total_len) + entry->key_len;
			if (see_all || entry->userid == GetUserId())
				values[0] = CStringGetTextDatum(block + text_offset);
			else
				values[0] = CStringGetTextDatum("<insufficient privilege>");
			values[1] = Int64GetDatum((int64) header->num_rows);
			values[2] = Int64GetDatum((int64) entry->nchunks * QC_CHUNK_SIZE);
			values[3] = Int64GetDatum((int64) pg_atomic_read_u64(&entry->hits));
			tuplestore_putvalues(result->setResult, result->setDesc, values, nulls);
		}
		LWLockRelease(partition->lock);
	}
	return (Datum) 0;
}

Datum
qcache_reset(PG_FUNCTION_ARGS)
{
	remove_all_cached_results();
	pg_atomic_write_u64(&shared->hits, 0);
	pg_atomic_write_u64(&shared->misses, 0);
	pg_atomic_write_u64(&shared->stores, 0);
	pg_atomic_write_u64(&shared->invalidations, 0);
	pg_atomic_write_u64(&shared->evictions, 0);
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

static Size
qcache_hash_size(void)
{
	Size total = 0;

	for (int i = 0; i < setting_partitions; i++)
		total = add_size(total, hash_estimate_size(entries_per_partition(),
												   sizeof(CachedResult)));
	return total;
}

static void
qcache_shmem_request(void)
{
	Size size = add_size(sizeof(QcacheShared),
						 mul_size(total_chunks(), sizeof(QcChunk)));

	if (prev_shmem_request_hook != NULL)
		prev_shmem_request_hook();
	RequestAddinShmemSpace(add_size(size, qcache_hash_size()));
	RequestNamedLWLockTranche("qcache", setting_partitions);
}

static void
qcache_shmem_startup(void)
{
	bool found;
	LWLockPadded *locks;

	if (prev_shmem_startup_hook != NULL)
		prev_shmem_startup_hook();
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	shared = ShmemInitStruct("qcache state", sizeof(QcacheShared), &found);
	locks = GetNamedLWLockTranche("qcache");
	for (int i = 0; i < setting_partitions; i++)
		shared->parts[i].lock = &locks[i].lock;

	if(!found){
		for(int i = 0; i < VERSION_SLOTS; i++){
			pg_atomic_init_u64(&shared->version[i], 0);
			pg_atomic_init_u32(&shared->inflight[i], 0);
		}
		pg_atomic_init_u64(&shared->hits, 0);
		pg_atomic_init_u64(&shared->misses, 0);
		pg_atomic_init_u64(&shared->stores, 0);
		pg_atomic_init_u64(&shared->invalidations, 0);
		pg_atomic_init_u64(&shared->evictions, 0);
		for (int i = 0; i < setting_partitions; i++)
		{
			dlist_init(&shared->parts[i].lru);
			shared->parts[i].free = NULL;
			shared->parts[i].nfree = 0;
			shared->parts[i].bytes_used = 0;
		}
	}

	for (int i = 0; i < setting_partitions; i++)
	{
		HASHCTL ctl;
		char name[32];

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(uint64);
		ctl.entrysize = sizeof(CachedResult);
		snprintf(name, sizeof(name), "qcache part %d", i);
		part_hash[i] = ShmemInitHash(name, entries_per_partition(),
									 entries_per_partition(), &ctl,
									 HASH_ELEM | HASH_BLOBS);
	}

	if (!found)
	{
		Size per_partition = chunks_per_partition();
		QcChunk *all_chunks = ShmemAlloc(mul_size(total_chunks(), sizeof(QcChunk)));

		for (int i = 0; i < setting_partitions; i++)
		{
			QcPartition *partition = &shared->parts[i];

			for (Size j = 0; j < per_partition; j++)
			{
				QcChunk *chunk =
					&all_chunks[i * per_partition + j];

				chunk->next = partition->free;
				partition->free = chunk;
				partition->nfree++;
			}
		}
	}
	LWLockRelease(AddinShmemInitLock);
}


/* ======================================================================
 * PART 9. _PG_init: CALLED ONCE WHEN POSTGRESQL LOADS THIS LIBRARY
 *
 * With shared_preload_libraries = 'qcache' this runs when the server
 * starts, and every connection then starts with our hooks in place.
 * ====================================================================== */

void _PG_init(void){
	if(!process_shared_preload_libraries_in_progress){
		ereport(ERROR,
				(errmsg("qcache must be loaded via shared_preload_libraries"),
				 errhint("Add qcache to shared_preload_libraries in postgresql.conf "
						 "and restart the server.")));
	}
	DefineCustomBoolVariable("qcache.enabled",
							 "Use and fill the query result cache.",
							 NULL, &setting_enabled, true,
							 PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("qcache.debug",
							 "Print a message at every step qcache takes.",
							 NULL, &setting_debug, false,
							 PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("qcache.max_size",
							"Shared memory reserved for cached results.",
							NULL, &setting_max_size_kb, 64 * 1024,
							1024, INT_MAX / 1024, PGC_POSTMASTER, GUC_UNIT_KB,
							NULL, NULL, NULL);
	DefineCustomIntVariable("qcache.max_result_size",
							"Largest single result that is cached.",
							NULL, &setting_max_result_kb, 1024,
							0, INT_MAX / 1024, PGC_USERSET, GUC_UNIT_KB,
							NULL, NULL, NULL);
	DefineCustomIntVariable("qcache.partitions",
							"Number of independent shared cache partitions.",
							NULL, &setting_partitions, QC_MAX_PARTITIONS,
							1, QC_MAX_PARTITIONS, PGC_POSTMASTER, 0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("qcache.max_entries",
							"Maximum number of shared cache entries.",
							NULL, &setting_max_entries, QC_MAX_ENTRIES,
							1, QC_MAX_ENTRIES, PGC_POSTMASTER, 0,
							NULL, NULL, NULL);
	DefineCustomBoolVariable("qcache.test_skip_precommit",
							 "For tests only: skip the PRE_COMMIT version bump.",
							 NULL, &setting_test_skip_precommit, false,
							 PGC_SUSET, 0, NULL, NULL, NULL);
	MarkGUCPrefixReserved("qcache");

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = qcache_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = qcache_shmem_startup;
	cache_memory = AllocSetContextCreate(TopMemoryContext, "qcache",
										 ALLOCSET_DEFAULT_SIZES);

	/*
	 * 3. Install the hooks.  We save the previous value of each hook first,
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

	/* 4. Ask to be told when transactions end. */
	RegisterXactCallback(qcache_transaction_callback, NULL);
}
