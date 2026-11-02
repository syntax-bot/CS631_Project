// Keep completed SELECT results in a small, per-connection cache.
// The cache records a version for each table a query reads.  A write bumps
// the relevant version at commit, and old entries are discarded on lookup.
// Only simple, direct SELECTs are cached; nested queries and parameterized
// statements are left to PostgreSQL.

// Keep postgres.h first, as required by PostgreSQL.
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

// This file targets PostgreSQL 18.
#if PG_VERSION_NUM < 180000 || PG_VERSION_NUM >= 190000
#error "This version of qcache is written for PostgreSQL 18"
#endif

// Required by PostgreSQL loadable modules.
PG_MODULE_MAGIC;


// Cache settings.  They are registered in _PG_init().

static bool setting_enabled = true;		// qcache.enabled
static bool setting_debug = false;		// qcache.debug
static int	setting_max_size_kb = 64 * 1024;	// qcache.max_size: 64 MB
static int	setting_max_result_kb = 1024;	// qcache.max_result_size: 1 MB

// Keep the table-version list small and pointer-free.
#define MAX_TABLES		8

// Table versions are assigned to hashed slots.
#define VERSION_SLOTS	65536

// Slot zero is the epoch for changes we cannot track by table.
#define EPOCH_SLOT		0

//
// These settings affect how query text is interpreted, so they belong in
// the key.  Output-only settings do not: rows are formatted when returned.
//
static const char *const key_settings[] = {
	"DateStyle",
	"IntervalStyle",
	"TimeZone",
	"search_path",
	"standard_conforming_strings",
	"array_nulls",
	"transform_null_equals",
};

// Emit notices only when debugging is enabled.
#define DEBUG_MSG(...) \
	do { \
		if (setting_debug) \
			ereport(NOTICE, (errmsg(__VA_ARGS__))); \
	} while (0)


// Cached result data.

//
// A saved result is one contiguous block:
//
// +--------------+------------------+-----+-------+-----+-------+-----
// | ResultHeader | column type OIDs | pad | row 1 | pad | row 2 | ...
// +--------------+------------------+-----+-------+-----+-------+-----
//
// Rows are MinimalTuples and start at MAXALIGNed offsets.  The block uses
// offsets rather than pointers, which also makes it suitable for shared
// memory later.
//
typedef struct ResultHeader
{
	Size		total_len;		// bytes in the whole block
	uint64		num_rows;
	uint32		num_columns;
	uint32		rows_offset;	// where row 1 starts
} ResultHeader;

// Column type OIDs follow the header.
#define RESULT_COLUMN_TYPES(header) \
	((Oid *) ((char *) (header) + MAXALIGN(sizeof(ResultHeader))))

// A table and the version seen when it was read.
typedef struct TableVersion
{
	Oid			oid;
	uint32		slot;			// version counter selected by table_slot()
	uint64		version;		// counter value when the row was read
} TableVersion;

// One entry in the result hash table.
typedef struct CachedResult
{
	uint64		key_hash;		// hash-table key; must remain first

	//
// Variable-sized data lives in one allocation:
//
// [ saved rows (ResultHeader ...) | full key | query text ]
//
// This keeps freeing simple and matches the future shared-cache layout.
//
	char	   *block;
	ResultHeader *result;		// points at the start of block
	char	   *key;			// full key stored inside block
	int			key_len;
	char	   *query_text;		// SELECT text stored inside block
	Size		size_bytes;		// space used by this entry

	// Tables read by this result and their saved versions.
	int			num_tables;
	TableVersion tables[MAX_TABLES];
	uint64		epoch;			// epoch value at lookup time

	uint64		hits;			// number of cache hits
	dlist_node	lru_link;		// position in the LRU list
} CachedResult;

// State for the SELECT currently being considered for caching.
typedef struct CurrentQuery
{
	QueryDesc  *query_desc;		// query being prepared
	MemoryContext memory;		// storage for this query's state

	uint64		key_hash;
	char	   *key;
	int			key_len;
	char	   *query_text;

	// Versions captured by the planner hook.
	int			num_tables;
	TableVersion tables[MAX_TABLES];
	uint64		epoch;

	bool		already_ran;	// ExecutorRun has been called
} CurrentQuery;

//
// On a miss, this receiver copies rows into a growing block and forwards
// them to the normal receiver.
//
typedef struct RowCollector
{
	DestReceiver base;			// must be first for PostgreSQL
	DestReceiver *real_receiver;	// receiver that sends rows to the client

	MemoryContext memory;		// owns the block
	char	   *block;			// begins with a ResultHeader
	Size		used;			// bytes currently filled
	Size		capacity;		// allocated block size
	uint64		num_rows;
	bool		too_big;		// the result passed qcache.max_result_size
} RowCollector;


// Per-connection cache state.

static MemoryContext cache_memory = NULL;	// owns cache allocations
static HTAB *cache_table = NULL;	// results indexed by hash
static dlist_head lru_list = DLIST_STATIC_INIT(lru_list);	// newest first
static Size cache_size_bytes = 0;	// total cached bytes

// Counter zero is reserved for the epoch.
static uint64 *version_counters = NULL;

static CurrentQuery current;	// current SELECT, or zeroed when idle

// Only top-level queries are eligible for caching.
static int	nesting_level = 0;

// Result of the most recent planner hook call.
static PlannedStmt *plan_just_made = NULL;
static const char *plan_just_made_problem = NULL;	// NULL means safe
static int	plan_just_made_num_tables = 0;
static TableVersion plan_just_made_tables[MAX_TABLES];
static uint64 plan_just_made_epoch = 0;

// Changes made by the current transaction, handled at commit.
static List *tables_written = NIL;	// table OIDs
static bool structure_changed = false;	// ALTER, TRUNCATE, COPY, etc.

// Counters returned by qcache_stats().
static uint64 count_hits = 0;
static uint64 count_misses = 0;
static uint64 count_stores = 0;
static uint64 count_invalidations = 0;	// stale entries removed
static uint64 count_evictions = 0;

// Hooks that were already installed when qcache loaded.
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


// Small helpers.

// Pick a version slot for a table in this database.
static uint32
table_slot(Oid table_oid)
{
	uint32		h = hash_combine(murmurhash32((uint32) MyDatabaseId),
								 murmurhash32((uint32) table_oid));

	return 1 + h % (VERSION_SLOTS - 1);
}

// Check whether the saved table versions are still current.
static bool
versions_unchanged(const TableVersion *tables, int num_tables, uint64 epoch)
{
	if (version_counters[EPOCH_SLOT] != epoch)
		return false;
	for (int i = 0; i < num_tables; i++)
	{
		if (version_counters[tables[i].slot] != tables[i].version)
			return false;
	}
	return true;
}

// Look up a result by its complete key.
static CachedResult *
find_cached_result(uint64 key_hash, const char *key, int key_len)
{
	CachedResult *entry;

	entry = (CachedResult *) hash_search(cache_table, &key_hash, HASH_FIND, NULL);
	if (entry == NULL)
		return NULL;

	// Hash collisions still need a full-key comparison.
	if (entry->key_len != key_len || memcmp(entry->key, key, key_len) != 0)
		return NULL;

	return entry;
}

// Remove one result from the cache.
static void
remove_cached_result(CachedResult *entry)
{
	char	   *block = entry->block;
	uint64		key_hash = entry->key_hash;

	dlist_delete(&entry->lru_link);
	cache_size_bytes -= entry->size_bytes;
	hash_search(cache_table, &key_hash, HASH_REMOVE, NULL);

	pfree(block);				// rows, key and text in one go
}

// Empty the result cache.
static void
remove_all_cached_results(void)
{
	while (!dlist_is_empty(&lru_list))
		remove_cached_result(dlist_head_element(CachedResult, lru_link, &lru_list));
}

// Release the current query state.
static void
forget_current_query(void)
{
	if (current.memory != NULL)
		MemoryContextDelete(current.memory);
	memset(&current, 0, sizeof(current));
}

// Clear the transaction's pending changes.
static void
forget_transaction_changes(void)
{
	list_free(tables_written);
	tables_written = NIL;
	structure_changed = false;
}


// PART 1: planner hook and table versions.

// Return a reason when the query should not be cached.
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

	// Mutable functions can change their answer without a table write.
	if (contain_mutable_functions((Node *) query))
		return "calls a function that is not IMMUTABLE (like now(), random() or qcache_stats())";

	return NULL;
}

//
// Capture the versions before the executor takes its snapshot.  That keeps
// a result from being saved with versions newer than the data it contains.
//
static const char *
record_table_versions(PlannedStmt *plan)
{
	ListCell   *cell;
	int			n = 0;

	plan_just_made_epoch = version_counters[EPOCH_SLOT];

	foreach(cell, plan->rtable)
	{
		RangeTblEntry *table = (RangeTblEntry *) lfirst(cell);
		bool		seen = false;

		if (table->rtekind != RTE_RELATION)
			continue;			// not a table (a subquery, a join, ...)

		// A table can appear several times (a self-join); note it once.
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
		plan_just_made_tables[n].version =
			version_counters[plan_just_made_tables[n].slot];
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

	// Check the original tree before planning changes it.
	problem = check_query_tree(query);

	// Let PostgreSQL and any earlier hook build the plan.
	if (prev_planner_hook != NULL)
		plan = prev_planner_hook(query, query_string, cursor_options, bound_params);
	else
		plan = standard_planner(query, query_string, cursor_options, bound_params);

	// Record versions before the executor takes its snapshot.
	plan_just_made_num_tables = 0;
	if (problem == NULL)
		problem = record_table_versions(plan);

	// ExecutorStart uses this result.
	plan_just_made = plan;
	plan_just_made_problem = problem;

	return plan;
}


// PART 2: executor start hook.

// Remember tables modified by a plan.
static void
remember_written_tables(PlannedStmt *plan)
{
	ListCell   *cell;
	MemoryContext old_memory;

	// Keep this list alive until commit.
	old_memory = MemoryContextSwitchTo(cache_memory);

	foreach(cell, plan->rtable)
	{
		RangeTblEntry *table = (RangeTblEntry *) lfirst(cell);

		if (table->rtekind != RTE_RELATION)
			continue;			// not a table (a subquery, a join, ...)

		if (!list_member_oid(tables_written, table->relid))
		{
			tables_written = lappend_oid(tables_written, table->relid);
			DEBUG_MSG("qcache: this transaction changes table \"%s\"; results that "
					  "read it go out of date at COMMIT", get_rel_name(table->relid));
		}
	}

	MemoryContextSwitchTo(old_memory);
}

// Check plan-level conditions that rule out caching.
static const char *
check_plan(QueryDesc *query_desc, int eflags)
{
	PlannedStmt *plan = query_desc->plannedstmt;
	ListCell   *cell;

	if (nesting_level > 0)
		return "runs inside another command (a function, EXPLAIN, a cursor, ...)";

	// Do not cache a plan we did not inspect.
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

	// Check each relation referenced by the plan.
	foreach(cell, plan->rtable)
	{
		RangeTblEntry *table = (RangeTblEntry *) lfirst(cell);
		Oid			oid = table->relid;
		char		kind;

		if (table->rtekind != RTE_RELATION)
			continue;

		if (table->tablesample != NULL)
			return "uses TABLESAMPLE, which returns random rows";

		// System catalogs are changed outside this bookkeeping.
		if (oid < FirstNormalObjectId)
			return "reads a system catalog";

		kind = get_rel_relkind(oid);
		if (kind != RELKIND_RELATION && kind != RELKIND_VIEW && kind != RELKIND_MATVIEW)
			return "reads a partitioned or foreign table";

		if (get_rel_persistence(oid) == RELPERSISTENCE_TEMP)
			return "reads a temporary table";

		// Partitions and inheritance can hide other sources of rows.
		if (get_rel_relispartition(oid) || has_subclass(oid))
			return "reads a partition or an inheritance parent";

		// The cache cannot include this transaction's uncommitted writes.
		if (list_member_oid(tables_written, oid))
			return "this transaction already changed a table it reads";
	}

	return NULL;
}

// Build the key used to find this statement in the cache.
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

	// Add the fixed parts first.
	initStringInfo(&key);
	appendBinaryStringInfo(&key, &database, sizeof(database));
	appendBinaryStringInfo(&key, &user, sizeof(user));

	// Separate setting values so adjacent values cannot be confused.
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

	// Keep the statement text for qcache_entries().
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

	// Start normally so PostgreSQL still performs its permission checks.
	if (prev_ExecutorStart_hook != NULL)
		prev_ExecutorStart_hook(query_desc, eflags);
	else
		standard_ExecutorStart(query_desc, eflags);

	// Track writes even when caching is disabled or the write is nested.
	if (query_desc->operation != CMD_SELECT || query_desc->plannedstmt->hasModifyingCTE)
	{
		remember_written_tables(query_desc->plannedstmt);
		return;
	}

	if (!setting_enabled)
		return;

	// Check whether this SELECT is eligible.
	problem = check_plan(query_desc, eflags);
	if (problem != NULL)
	{
		if (nesting_level == 0)
			DEBUG_MSG("qcache: not cached: %s", problem);
		return;
	}

	// Save the information needed by ExecutorRun.
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


// PART 3: executor run hook, including cache hits and misses.

// 3a. Collecting rows on a miss.

// Copy a slot into a self-contained MinimalTuple.
static MinimalTuple
copy_row(TupleTableSlot *slot)
{
	TupleDesc	columns = slot->tts_tupleDescriptor;
	int			num_columns = columns->natts;
	Datum	   *values = palloc(sizeof(Datum) * Max(num_columns, 1));
	bool	   *is_null = palloc(sizeof(bool) * Max(num_columns, 1));
	bool	   *was_copied = palloc0(sizeof(bool) * Max(num_columns, 1));
	MinimalTuple row;

	// Materialize all attributes before copying them.
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

	// Build the compact row representation.
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

// Grow the row buffer when needed.
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

// PostgreSQL calls this once for each result row.
static bool
collector_receive_row(TupleTableSlot *slot, DestReceiver *self)
{
	RowCollector *collector = (RowCollector *) self;

	if (!collector->too_big && collector->block != NULL)
	{
		MemoryContext old_memory = MemoryContextSwitchTo(collector->memory);
		MinimalTuple row = copy_row(slot);
		Size		start = MAXALIGN(collector->used);	// rows start aligned
		Size		end = start + row->t_len;

		if (end > (Size) setting_max_result_kb * 1024)
		{
			// Stop collecting once the result exceeds the configured limit.
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

	// The client still receives the original slot.
	return collector->real_receiver->receiveSlot(slot, collector->real_receiver);
}

// Initialize the block before the first row arrives.
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
	// The current query context owns the collector.
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

// 3b. Saving a result after a miss.

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

	// Do not save rows if any recorded version changed during execution.
	if (!versions_unchanged(current.tables, current.num_tables, current.epoch))
	{
		DEBUG_MSG("qcache: a table changed while the query ran; not saved");
		return;
	}

	// Fill in the final row count and size.
	header = (ResultHeader *) collector->block;
	header->num_rows = collector->num_rows;
	header->total_len = collector->used;

	// Store rows, key, and query text in one block.
	rows_len = MAXALIGN(collector->used);
	text_len = strlen(current.query_text) + 1;
	block_len = rows_len + current.key_len + text_len;
	if (block_len > max_size)
	{
		DEBUG_MSG("qcache: result is larger than qcache.max_size; not saved");
		return;
	}

	// Evict old entries until the new one fits.
	while (cache_size_bytes + block_len + sizeof(CachedResult) > max_size &&
		   !dlist_is_empty(&lru_list))
	{
		CachedResult *oldest = dlist_tail_element(CachedResult, lru_link, &lru_list);

		remove_cached_result(oldest);
		count_evictions++;
	}

	// Move the result into the cache's memory context.
	block = MemoryContextAlloc(cache_memory, block_len);
	memcpy(block, collector->block, collector->used);
	memcpy(block + rows_len, current.key, current.key_len);
	memcpy(block + rows_len + current.key_len, current.query_text, text_len);

	// Add or replace the hash entry.
	entry = (CachedResult *) hash_search(cache_table, &current.key_hash, HASH_ENTER, &found);
	if (found)
	{
		// The hash matched an existing entry, so replace that entry.
		dlist_delete(&entry->lru_link);
		cache_size_bytes -= entry->size_bytes;
		pfree(entry->block);
	}

	entry->block = block;
	entry->result = (ResultHeader *) block;
	entry->key = block + rows_len;
	entry->key_len = current.key_len;
	entry->query_text = block + rows_len + current.key_len;

	// Include allocator overhead and the hash-table entry itself.
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

// 3c. Sending rows from a cache hit.

// Make sure the cached row layout still matches the query.
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

// Send cached rows through the normal receiver.
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

	// Reuse one slot while sending the rows.
	slot = MakeSingleTupleTableSlot(query_desc->tupDesc, &TTSOpsMinimalTuple);
	for (uint64 i = 0; i < result->num_rows; i++)
	{
		MinimalTuple row = (MinimalTuple) position;

		// The cache owns the tuple memory.
		ExecStoreMinimalTuple(row, slot, false);
		if (!receiver->receiveSlot(slot, receiver))
			break;
		estate->es_processed++;		// becomes the "SELECT n" row count

		position += MAXALIGN(row->t_len);	// the next row starts aligned
	}
	ExecDropSingleTupleTableSlot(slot);

	estate->es_total_processed += estate->es_processed;
	receiver->rShutdown(receiver);
	MemoryContextSwitchTo(old_memory);
}

// 3d. The executor hook.

static void
qcache_ExecutorRun_hook(QueryDesc *query_desc, ScanDirection direction, uint64 count)
{
	bool		use_cache = false;
	RowCollector *collector = NULL;

	// Cursors and repeated runs go through the normal executor path.
	if (current.query_desc == query_desc && !current.already_ran &&
		count == 0 && ScanDirectionIsForward(direction))
		use_cache = true;

	if (current.query_desc == query_desc)
		current.already_ran = true;

	if (use_cache)
	{
		CachedResult *entry = find_cached_result(current.key_hash, current.key,
												 current.key_len);

		// Remove an entry as soon as its recorded versions are stale.
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
			// Hit: send the saved rows without running the plan.
			DEBUG_MSG("qcache: HIT, sending %llu saved row(s)",
					  (unsigned long long) entry->result->num_rows);
			send_saved_rows(query_desc, entry->result);
			entry->hits++;
			count_hits++;
			dlist_move_head(&lru_list, &entry->lru_link);	// refresh LRU order
			forget_current_query();
			return;
		}

		// Miss: collect rows while forwarding them to the client.
		DEBUG_MSG("qcache: MISS, running the query and saving its rows");
		count_misses++;
		collector = create_row_collector(query_desc->dest);
		query_desc->dest = &collector->base;
	}

	// Restore nesting state and the receiver even when execution fails.
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

	// Save the rows collected during a miss.
	if (collector != NULL)
	{
		save_result(collector);
		forget_current_query();
	}
}


// PART 4: executor finish and end hooks.

static void
qcache_ExecutorFinish_hook(QueryDesc *query_desc)
{
	// Work done during finish can run nested queries.
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
	// Clean up if ExecutorRun was never called.
	if (current.query_desc == query_desc)
		forget_current_query();

	if (prev_ExecutorEnd_hook != NULL)
		prev_ExecutorEnd_hook(query_desc);
	else
		standard_ExecutorEnd(query_desc);
}


// PART 5: utility commands that can invalidate the cache.

// Return true for utility commands that cannot change table contents.
static bool
is_harmless_command(Node *command)
{
	switch (nodeTag(command))
	{
		case T_TransactionStmt:		// BEGIN, COMMIT, ROLLBACK, ...

			// COMMIT PREPARED may publish changes from another connection.
			return ((TransactionStmt *) command)->kind != TRANS_STMT_COMMIT_PREPARED;
		case T_VariableSetStmt:		// SET
		case T_VariableShowStmt:	// SHOW
		case T_ExplainStmt:
		case T_PrepareStmt:
		case T_ExecuteStmt:
		case T_DeallocateStmt:
		case T_DeclareCursorStmt:
		case T_FetchStmt:
		case T_ClosePortalStmt:
		case T_CallStmt:		// writes inside procedures and DO blocks
		case T_DoStmt:			// still pass through the executor hooks
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
	// Unknown utility commands invalidate the cache at commit.
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


// PART 6: update versions when transactions finish.

static void
qcache_transaction_callback(XactEvent event, void *arg)
{
	ListCell   *cell;

	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PREPARE:
			// Committed writes make matching entries stale.
			foreach(cell, tables_written)
				version_counters[table_slot(lfirst_oid(cell))]++;
			if (structure_changed)
				version_counters[EPOCH_SLOT]++;

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
			// Rolled-back writes do not invalidate anything.
			forget_transaction_changes();
			forget_current_query();
			plan_just_made = NULL;
			break;

		default:
			break;
	}
}


// PART 7: SQL functions exposed by the extension.

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

	// Set up the materialized result.
	InitMaterializedSRF(fcinfo, 0);

	// Return entries in LRU order.
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


// PART 8: initialize the extension.

void
_PG_init(void)
{
	HASHCTL		table_options;

	// Register settings.
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

	// Set up memory, version counters, and the result table.
	cache_memory = AllocSetContextCreate(TopMemoryContext, "qcache",
										 ALLOCSET_DEFAULT_SIZES);

	version_counters = MemoryContextAllocZero(cache_memory,
											  sizeof(uint64) * VERSION_SLOTS);

	memset(&table_options, 0, sizeof(table_options));
	table_options.keysize = sizeof(uint64);
	table_options.entrysize = sizeof(CachedResult);
	table_options.hcxt = cache_memory;
	cache_table = hash_create("qcache results", 1024, &table_options,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	// Install our hooks after saving any hooks already in use.
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

	// Track transaction completion.
	RegisterXactCallback(qcache_transaction_callback, NULL);
}
