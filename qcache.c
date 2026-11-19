// qcache.c - query result cache for PostgreSQL 18
// Caches plain client SELECT results and invalidates them after writes.
// Enable qcache.debug to trace cache decisions.

// "postgres.h" must always be the first include in PostgreSQL C code.
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
#include "portability/instr_time.h"

// The hook functions below are written for PostgreSQL 18.
#if PG_VERSION_NUM < 180000 || PG_VERSION_NUM >= 190000
#error "This version of qcache is written for PostgreSQL 18"
#endif

// Every loadable module must contain this line.
PG_MODULE_MAGIC;


// PART A. SETTINGS

static bool setting_enabled = true;		// qcache.enabled
static bool setting_debug = false;		// qcache.debug
static int	setting_max_size_kb = 64 * 1024;	// qcache.max_size: 64 MB
static int	setting_max_result_kb = 1024;	// qcache.max_result_size: 1 MB
#define MAX_TABLES 8
#define VERSION_SLOTS 65536
#define EPOCH_SLOT 0
#define QC_MAX_PARTITIONS 32
#define QC_CHUNK_SIZE 1024
#define QC_MAX_ENTRIES 65536
static int setting_partitions = QC_MAX_PARTITIONS;
static int setting_max_entries = QC_MAX_ENTRIES;
typedef enum InvalidationMode
{
	INVALIDATION_LAZY,
	
	INVALIDATION_EAGER	// MySQL style
} InvalidationMode;

static const struct config_enum_entry invalidation_options[] = {
	{"lazy", INVALIDATION_LAZY, false},
	{"eager", INVALIDATION_EAGER, false},
	{NULL, 0, false}
};

static int	setting_invalidation = INVALIDATION_LAZY;
static double setting_min_exec_time_ms = 0.0;	// Skip results below this time.

// Test-only option to skip the PRE_COMMIT version bump.
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


// Print a debug message, only when qcache.debug is on.
#define DEBUG_MSG(...) \
	do { \
		if (setting_debug) \
			ereport(NOTICE, (errmsg(__VA_ARGS__))); \
	} while (0)


// PART B. DATA STRUCTURES
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

// A saved query result stored in the shared hash table.
typedef struct CachedResult
{
	uint64		key_hash;		// the hash table's key (must be the first field)

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
	dlist_node	lru_link;		// position in the "recently used" list
} CachedResult;

typedef struct QcPartition
{
	LWLock *lock;
	dlist_head lru;
	QcChunk *free;
	uint32 free_chunk_count;
	uint64 bytes_used;
} QcPartition;

// The current cacheable SELECT.
typedef struct CurrentQuery
{
	QueryDesc  *query_desc;		// PostgreSQL's description of the query
	MemoryContext memory;		// everything below is allocated in here

	uint64		key_hash;
	char	   *key;
	int			key_len;
	char	   *query_text;
	int			num_tables;
	TableVersion tables[MAX_TABLES];
	uint64 epoch;
	char *hit_block;
	Size hit_block_len;

	bool		already_ran;	// ExecutorRun was already called once
} CurrentQuery;

// Collects rows on a miss while forwarding them to the client.
typedef struct RowCollector
{
	DestReceiver base;			// must be first: PostgreSQL sees only this part
	DestReceiver *real_receiver;	// where the rows really go (the client)

	MemoryContext memory;
	char *block;
	Size used;
	Size capacity;
	uint64		num_rows;
	
	bool		too_big;		// the result passed qcache.max_result_size
} RowCollector;


// PART C. GLOBAL VARIABLES
// Cache entries are shared; query state and hash handles are local.

static MemoryContext cache_memory = NULL;	// parent of current-query state
static HTAB *partition_hashes[QC_MAX_PARTITIONS];	// local handles for shared hashes
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

static QcacheShared *shared_state = NULL;

#define VERSION(slot) pg_atomic_read_membarrier_u64(&shared_state->version[(slot)])
#define INFLIGHT(slot) pg_atomic_read_membarrier_u32(&shared_state->inflight[(slot)])
#define BUMP_VERSION(slot) pg_atomic_fetch_add_u64(&shared_state->version[(slot)], 1)

static CurrentQuery cur_query;	// the SELECT running now (zeroed if none)

// Nested queries are not cached; level 0 is a client query.
static int	nesting_level = 0;

// Planner hook result for the most recent plan.
static PlannedStmt *plan_just_made = NULL;
static const char *plan_just_made_problem = NULL;	// NULL means safe
static int	plan_just_made_num_tables = 0;
static TableVersion plan_just_made_tables[MAX_TABLES];
static uint64 plan_just_made_epoch = 0;

// Tables changed by the current transaction.
static List *tables_written = NIL;	// list of table OIDs
static bool structure_changed = false;	// ALTER, TRUNCATE, COPY, ...
static bool precommit_done = false;

// Hooks installed before ours.
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


// PART D. CACHE HELPERS

static uint32 table_slot(Oid table_oid){
	uint32 h = hash_combine(murmurhash32((uint32) MyDatabaseId), murmurhash32((uint32) table_oid));
	return 1 + h % (VERSION_SLOTS - 1);
}

// Check whether table versions and the epoch are unchanged.
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

// Compute shared-memory sizes from startup settings.
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

// How many chunks a block of this length needs.
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
		partition->free_chunk_count++;
		chunk = next;
	}
	entry->first = NULL;
}

static bool
allocate_entry_chunks(QcPartition *partition, CachedResult *entry, Size block_len)
{
	QcChunk *last = NULL;
	Size needed = chunks_for(block_len);

	if (needed > partition->free_chunk_count)
		return false;

	entry->nchunks = needed;
	entry->first = NULL;
	for (Size i = 0; i < needed; i++)
	{
		QcChunk *chunk = partition->free;

		partition->free = chunk->next;
		partition->free_chunk_count--;
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
	hash_search(partition_hashes[partition - shared_state->parts], &key_hash, HASH_REMOVE, NULL);
	if (count_invalidation)
		pg_atomic_fetch_add_u64(&shared_state->invalidations, 1);
}
// Did this saved result read one of the tables in the list?
static bool
entry_reads_any(const CachedResult *entry, List *table_oids)
{
	for (int i = 0; i < entry->num_tables; i++)
	{
		if (list_member_oid(table_oids, entry->tables[i].oid))
			return true;
	}
	return false;
}

// Eagerly remove results that read changed tables.
static void
remove_results_reading(List *table_oids, bool all)
{
	for (int i = 0; i < setting_partitions; i++)
	{
		QcPartition *partition = &shared_state->parts[i];
		dlist_mutable_iter iter;

		LWLockAcquire(partition->lock, LW_EXCLUSIVE);
		dlist_foreach_modify(iter, &partition->lru)
		{
			CachedResult *entry = dlist_container(CachedResult, lru_link, iter.cur);

			if (all || entry_reads_any(entry, table_oids))
				remove_cached_result(partition, entry, true);
		}
		LWLockRelease(partition->lock);
	}
}

static void
remove_all_cached_results(void)
{
	for (int i = 0; i < setting_partitions; i++)
	{
		QcPartition *partition = &shared_state->parts[i];

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

// Forget the current query and free its memory.
static void
forget_current_query(void)
{
	if (cur_query.memory != NULL)
		MemoryContextDelete(cur_query.memory);
	memset(&cur_query, 0, sizeof(cur_query));
}

// Forget what the current transaction changed.
static void
forget_transaction_changes(void)
{
	list_free(tables_written);
	tables_written = NIL;
	structure_changed = false;
}


// PART 1. PLANNER HOOK

// Returns NULL if the query is safe, or a short reason why it is not.
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

// A "mutable" function can return a different answer without any table
// changing: now(), random(), nextval(), ...  Such a query is never safe.
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

	// Check the tree before planning: the planner may change it.
	problem = check_query_tree(query);

	// Let PostgreSQL (or a hook installed before ours) make the plan.
	if (prev_planner_hook != NULL)
		plan = prev_planner_hook(query, query_string, cursor_options, bound_params);
	else
		plan = standard_planner(query, query_string, cursor_options, bound_params);

	plan_just_made_num_tables = 0;
	if (problem == NULL)
		problem = record_table_versions(plan);

	// Remember our answer; the ExecutorStart hook asks for it.
	plan_just_made = plan;
	plan_just_made_problem = problem;

	return plan;
}


// PART 2. EXECUTOR START HOOK

// Remember the tables an INSERT/UPDATE/DELETE/MERGE touches.
static void
remember_written_tables(PlannedStmt *plan)
{
	ListCell   *cell;
	MemoryContext old_memory;

	// tables_written must survive until COMMIT: use our own memory.
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
					  "read it will be removed at COMMIT", get_rel_name(table->relid));
		}
	}

	MemoryContextSwitchTo(old_memory);
}

// Checks that need the plan.  Returns NULL if we may cache the query, or a
// reason why not.  On success, *table_oids is the list of tables it reads.
static const char *
check_plan(QueryDesc *query_desc, int eflags)
{
	PlannedStmt *plan = query_desc->plannedstmt;
	ListCell   *cell;


	if (nesting_level > 0)
		return "runs inside another command (a function, EXPLAIN, a cursor, ...)";

	// Only a plan our planner hook has just checked.
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

	// Now look at every table the query reads.
	foreach(cell, plan->rtable)
	{
		RangeTblEntry *table = (RangeTblEntry *) lfirst(cell);
		Oid			oid = table->relid;
		char		kind;

		if (table->rtekind != RTE_RELATION)
			continue;

		if (table->tablesample != NULL)
			return "uses TABLESAMPLE, which returns random rows";

		// System catalogs change through commands we do not track.
		if (oid < FirstNormalObjectId)
			return "reads a system catalog";

		kind = get_rel_relkind(oid);
		if (kind != RELKIND_RELATION && kind != RELKIND_VIEW && kind != RELKIND_MATVIEW)
			return "reads a partitioned or foreign table";

		if (get_rel_persistence(oid) == RELPERSISTENCE_TEMP)
			return "reads a temporary table";

		// Rows can reach these tables through a different table name.
		if (get_rel_relispartition(oid) || has_subclass(oid))
			return "reads a partition or an inheritance parent";

		// We cannot see this transaction's own uncommitted changes.
		if (list_member_oid(tables_written, oid))
			return "this transaction already changed a table it reads";
	}

	return NULL;
}

// Build a key from the user, statement position, settings, tables, and text.
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

	// Append each part as raw bytes.
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
	for(int i = 0; i < cur_query.num_tables; i++){
		appendBinaryStringInfo(&key, &cur_query.tables[i].oid, sizeof(Oid));
	}
	appendBinaryStringInfo(&key, text, text_len);

	cur_query.key = key.data;
	cur_query.key_len = key.len;
	cur_query.key_hash = hash_bytes_extended((const unsigned char *) key.data, key.len, 0);

	// Keep just this statement's text, for qcache_entries().
	if (start < 0 || start > text_len)
		start = 0;
	if (length <= 0 || start + length > text_len)
		length = text_len - start;
	cur_query.query_text = pnstrdup(text + start, length);
}

static void
qcache_ExecutorStart_hook(QueryDesc *query_desc, int eflags)
{
	const char *problem;
	MemoryContext old_memory;

// Start normally first so permissions are still checked.
	if (prev_ExecutorStart_hook != NULL)
		prev_ExecutorStart_hook(query_desc, eflags);
	else
		standard_ExecutorStart(query_desc, eflags);

// Record writes at every nesting level, even when caching is disabled.
	if (query_desc->operation != CMD_SELECT || query_desc->plannedstmt->hasModifyingCTE)
	{
		remember_written_tables(query_desc->plannedstmt);
		return;
	}

	if (!setting_enabled)
		return;

	// Is this SELECT safe to cache?
	problem = check_plan(query_desc, eflags);
	if (problem != NULL)
	{
		if (nesting_level == 0)
			DEBUG_MSG("qcache: not cached: %s", problem);
		return;
	}

	// Yes: remember it as the current query, with its key.
	forget_current_query();
	cur_query.query_desc = query_desc;
	cur_query.memory = AllocSetContextCreate(cache_memory, "qcache current query",
										   ALLOCSET_DEFAULT_SIZES);
	
	cur_query.num_tables = plan_just_made_num_tables;
	memcpy(cur_query.tables, plan_just_made_tables, sizeof(TableVersion) * plan_just_made_num_tables);
	cur_query.epoch = plan_just_made_epoch;
	old_memory = MemoryContextSwitchTo(cur_query.memory);
	build_cache_key(query_desc);
	MemoryContextSwitchTo(old_memory);
}


// PART 3. EXECUTOR RUN HOOK

// ---- 3a. The row collector (used on a miss) ----

// Copy a row, including externally stored variable-length values.
static MinimalTuple
copy_row(TupleTableSlot *slot)
{
	TupleDesc	columns = slot->tts_tupleDescriptor;
	int			num_columns = columns->natts;
	Datum	   *values = palloc(sizeof(Datum) * Max(num_columns, 1));
	bool	   *is_null = palloc(sizeof(bool) * Max(num_columns, 1));
	bool	   *was_copied = palloc0(sizeof(bool) * Max(num_columns, 1));
	MinimalTuple row;

	// Make sure every column's value is available in the slot.
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

	// Pack the values into one compact row.
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

// Called by PostgreSQL once for every result row.
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
			// Too big to cache: free what we copied and stop copying.
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

	// Hand the row on to the client, unchanged.
	return collector->real_receiver->receiveSlot(slot, collector->real_receiver);
}

// Called before the first row and after the last: just pass them on.
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
	// Nothing to do: the collector is freed with the current query.
}

static RowCollector *
create_row_collector(DestReceiver *real_receiver)
{
	RowCollector *collector;

	collector = MemoryContextAllocZero(cur_query.memory, sizeof(RowCollector));
	collector->base.receiveSlot = collector_receive_row;
	collector->base.rStartup = collector_startup;
	collector->base.rShutdown = collector_shutdown;
	collector->base.rDestroy = collector_destroy;
	collector->base.mydest = DestNone;
	collector->real_receiver = real_receiver;
	collector->memory = AllocSetContextCreate(cur_query.memory, "qcache rows",
											  ALLOCSET_DEFAULT_SIZES);
	return collector;
}

// ---- 3b. Saving a result (after a miss) ----

static void
save_result(RowCollector *collector, double elapsed_ms)
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

	if (elapsed_ms < setting_min_exec_time_ms)
	{
		DEBUG_MSG("qcache: query took %.3f ms, less than qcache.min_exec_time; not saved",
				  elapsed_ms);
		return;
	}

	if (!versions_unchanged(cur_query.tables, cur_query.num_tables, cur_query.epoch))
	{
		DEBUG_MSG("qcache: a table changed while the query ran; not saved");
		return;
	}

	// Finish the header.
	header = (ResultHeader *) collector->block;
	header->num_rows = collector->num_rows;
	header->total_len = collector->used;

	// The entry's block: [ rows | key | text ], see CachedResult.
	rows_len = MAXALIGN(collector->used);
	text_len = strlen(cur_query.query_text) + 1;
	block_len = rows_len + cur_query.key_len + text_len;
	needed = chunks_for(block_len);
	if (needed > chunks_per_partition())
	{
		DEBUG_MSG("qcache: result does not fit in its partition; not saved");
		return;
	}

	partition_no = partition_index(cur_query.key_hash);
	partition = &shared_state->parts[partition_no];

	block = MemoryContextAlloc(cur_query.memory, block_len);
	memcpy(block, collector->block, collector->used);
	memcpy(block + rows_len, cur_query.key, cur_query.key_len);
	memcpy(block + rows_len + cur_query.key_len, cur_query.query_text, text_len);

	LWLockAcquire(partition->lock, LW_EXCLUSIVE);
	entry = (CachedResult *) hash_search(partition_hashes[partition_no],
										 &cur_query.key_hash, HASH_FIND, NULL);
	if (entry != NULL)
		remove_cached_result(partition, entry, false);
	while ((partition->free_chunk_count < needed ||
			hash_get_num_entries(partition_hashes[partition_no]) >= entries_per_partition()) &&
		   !dlist_is_empty(&partition->lru))
	{
		CachedResult *oldest =
			dlist_tail_element(CachedResult, lru_link, &partition->lru);

		if (pg_atomic_exchange_u32(&oldest->referenced, 0) != 0)
			dlist_move_head(&partition->lru, &oldest->lru_link);
		else
		{
			remove_cached_result(partition, oldest, false);
			pg_atomic_fetch_add_u64(&shared_state->evictions, 1);
		}
	}

	// Add an entry to the shared hash table.
	entry = (CachedResult *) hash_search(partition_hashes[partition_no],
										 &cur_query.key_hash,
										 HASH_ENTER_NULL, &found);
	if (entry == NULL)
	{
		LWLockRelease(partition->lock);
		DEBUG_MSG("qcache: partition hash table is full; not saved");
		return;
	}

	memset(entry, 0, sizeof(*entry));
	entry->key_len = cur_query.key_len;
	entry->query_text_len = text_len;
	entry->block_len = block_len;
	entry->key_hash = cur_query.key_hash;
	entry->dbid = MyDatabaseId;
	entry->userid = GetUserId();
	if (!allocate_entry_chunks(partition, entry, block_len))
	{
		hash_search(partition_hashes[partition_no], &cur_query.key_hash, HASH_REMOVE, NULL);
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

	entry->num_tables = cur_query.num_tables;
	memcpy(entry->tables, cur_query.tables, sizeof(TableVersion) * cur_query.num_tables);
	entry->epoch = cur_query.epoch;
	pg_atomic_init_u32(&entry->referenced, 0);
	pg_atomic_init_u64(&entry->hits, 0);

	dlist_push_head(&partition->lru, &entry->lru_link);
	partition->bytes_used += (uint64) entry->nchunks * QC_CHUNK_SIZE;
	pg_atomic_fetch_add_u64(&shared_state->stores, 1);
	LWLockRelease(partition->lock);

	DEBUG_MSG("qcache: saved %llu row(s), %zu bytes in %u chunk(s)",
			  (unsigned long long) header->num_rows, block_len, needed);
}

// ---- 3c. Sending saved rows (on a hit) ----

// The saved rows must have the same columns the client now expects.
static bool
same_columns(ResultHeader *result, TupleDesc columns)
{
	if(result->num_columns != columns->natts) return false;
	for(int i = 0; i < columns->natts; i++){
		if(RESULT_COLUMN_TYPES(result)[i] != TupleDescAttr(columns, i)->atttypid) return false;
	}
	return true;
}

// Do what standard_ExecutorRun would do, using the saved rows.
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

	// A slot holds one row at a time while it is passed to the receiver.
	slot = MakeSingleTupleTableSlot(query_desc->tupDesc, &TTSOpsMinimalTuple);
	for (uint64 i = 0; i < result->num_rows; i++)
	{
		MinimalTuple row = (MinimalTuple) position;
		ExecStoreMinimalTuple(row, slot, false);
		if (!receiver->receiveSlot(slot, receiver))
			break;
		estate->es_processed++;		// becomes the "SELECT n" row count
		position += MAXALIGN(row->t_len);
	}
	ExecDropSingleTupleTableSlot(slot);

	estate->es_total_processed += estate->es_processed;
	receiver->rShutdown(receiver);
	MemoryContextSwitchTo(old_memory);
}

// ---- 3d. The hook itself ----

static void
qcache_ExecutorRun_hook(QueryDesc *query_desc, ScanDirection direction, uint64 count)
{
	bool		use_cache = false;
	RowCollector *collector = NULL;
	instr_time	started;
	instr_time	duration;

// Cache only the first forward run that requests all rows.
	if (cur_query.query_desc == query_desc && !cur_query.already_ran &&
		count == 0 && ScanDirectionIsForward(direction))
		use_cache = true;

	if (cur_query.query_desc == query_desc)
		cur_query.already_ran = true;

	if (use_cache)
	{
		int partition_no = partition_index(cur_query.key_hash);
		QcPartition *partition = &shared_state->parts[partition_no];
		CachedResult *entry;
		bool hit = false;
		bool stale = false;

		LWLockAcquire(partition->lock, LW_SHARED);
		entry = (CachedResult *) hash_search(partition_hashes[partition_no],
											 &cur_query.key_hash,
											 HASH_FIND, NULL);
		if (entry != NULL && entry_key_matches(entry, cur_query.key, cur_query.key_len) &&
			versions_unchanged(entry->tables, entry->num_tables, entry->epoch))
		{
			ResultHeader *result;

			cur_query.hit_block = MemoryContextAlloc(cur_query.memory, entry->block_len);
			cur_query.hit_block_len = entry->block_len;
			copy_entry_block(entry, cur_query.hit_block);
			result = (ResultHeader *) cur_query.hit_block;
			hit = same_columns(result, query_desc->tupDesc);
			if (hit)
			{
				pg_atomic_write_u32(&entry->referenced, 1);
				pg_atomic_fetch_add_u64(&entry->hits, 1);
				pg_atomic_fetch_add_u64(&shared_state->hits, 1);
			}
		}
		else if (entry != NULL &&
				 entry_key_matches(entry, cur_query.key, cur_query.key_len))
			stale = true;
		LWLockRelease(partition->lock);

		if (stale)
		{
			DEBUG_MSG("qcache: saved result is out of date (a table it read has changed)");
			LWLockAcquire(partition->lock, LW_EXCLUSIVE);
			entry = (CachedResult *) hash_search(partition_hashes[partition_no],
												 &cur_query.key_hash,
												 HASH_FIND, NULL);
			if (entry != NULL &&
				entry_key_matches(entry, cur_query.key, cur_query.key_len) &&
				!versions_unchanged(entry->tables, entry->num_tables,
									entry->epoch))
				remove_cached_result(partition, entry, true);
			LWLockRelease(partition->lock);
		}

		if (hit)
		{
			// HIT: send the saved rows and skip running the plan.
			DEBUG_MSG("qcache: HIT, sending %llu saved row(s)",
					  (unsigned long long) ((ResultHeader *) cur_query.hit_block)->num_rows);
			send_saved_rows(query_desc, (ResultHeader *) cur_query.hit_block);
			forget_current_query();
			return;
		}

		// MISS: put the row collector in front of the real receiver.
		DEBUG_MSG("qcache: MISS, running the query and saving its rows");
		pg_atomic_fetch_add_u64(&shared_state->misses, 1);
		collector = create_row_collector(query_desc->dest);
		query_desc->dest = &collector->base;
	}

// Run normally and restore state even if execution fails.
	INSTR_TIME_SET_CURRENT(started);
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

	// After a miss, the collector holds every row: save them.
	if (collector != NULL)
	{
		INSTR_TIME_SET_CURRENT(duration);
		INSTR_TIME_SUBTRACT(duration, started);
		save_result(collector, INSTR_TIME_GET_MILLISEC(duration));
		forget_current_query();
	}
}


// PART 4. EXECUTOR FINISH AND END HOOKS

static void
qcache_ExecutorFinish_hook(QueryDesc *query_desc)
{
	// Anything run while finishing (for example triggers) is nested.
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
	// If the query ended without running, forget it.
	if (cur_query.query_desc == query_desc)
		forget_current_query();

	if (prev_ExecutorEnd_hook != NULL)
		prev_ExecutorEnd_hook(query_desc);
	else
		standard_ExecutorEnd(query_desc);
}


// PART 5. UTILITY HOOK

// Utility commands that cannot change any table's data or structure.
static bool
is_harmless_command(Node *command)
{
	switch (nodeTag(command))
	{
		case T_TransactionStmt:		// BEGIN, COMMIT, ROLLBACK, ...
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
	bool		commit_prepared = IsA(plan->utilityStmt, TransactionStmt) &&
		((TransactionStmt *) plan->utilityStmt)->kind == TRANS_STMT_COMMIT_PREPARED;
	// Anything else might change data: clear the whole cache at COMMIT.
	if (!is_harmless_command(plan->utilityStmt))
	{
		if (!structure_changed)
			DEBUG_MSG("qcache: this command may change tables; the whole cache "
					  "will be cleared at COMMIT");
		structure_changed = true;
	}
	if (commit_prepared)
	{
		pg_atomic_fetch_add_u32(&shared_state->inflight[EPOCH_SLOT], 1);
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
			pg_atomic_fetch_sub_u32(&shared_state->inflight[EPOCH_SLOT], 1);
		}
	}
	PG_END_TRY();
}


// PART 6. TRANSACTION CALLBACK

static void
bump_written(bool begin_inflight, bool end_inflight)
{
	ListCell   *cell;

	foreach(cell, tables_written)
	{
		uint32		slot = table_slot(lfirst_oid(cell));

		if (begin_inflight)
			pg_atomic_fetch_add_u32(&shared_state->inflight[slot], 1);
		BUMP_VERSION(slot);
		if (end_inflight)
			pg_atomic_fetch_sub_u32(&shared_state->inflight[slot], 1);
	}
	if (structure_changed)
	{
		if (begin_inflight)
			pg_atomic_fetch_add_u32(&shared_state->inflight[EPOCH_SLOT], 1);
		BUMP_VERSION(EPOCH_SLOT);
		if (end_inflight)
			pg_atomic_fetch_sub_u32(&shared_state->inflight[EPOCH_SLOT], 1);
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
			if (setting_invalidation == INVALIDATION_EAGER)
				remove_results_reading(tables_written, structure_changed);
			break;

		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PREPARE:
// The changes are now permanent.  The second version bump
// prevents a reader from treating the in-flight window as stable.
			if (wrote)
				INJECTION_POINT("qcache-before-commit-bump", NULL);
			if (precommit_done)
			{
				bump_written(false, true);
				precommit_done = false;
			}
			else if (wrote)
			{
// No PRE_COMMIT bump happened: PREPARE TRANSACTION (which has
// no PRE_COMMIT event), or qcache.test_skip_precommit.  Bump
// once now; for PREPARE the data becomes visible only at
// COMMIT PREPARED, which bumps the epoch again.
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
			// ROLLBACK or an error: the data did not change, keep the cache.
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


// PART 7. SQL FUNCTIONS

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
		QcPartition *partition = &shared_state->parts[i];

		LWLockAcquire(partition->lock, LW_SHARED);
		entries += hash_get_num_entries(partition_hashes[i]);
		bytes += partition->bytes_used;
		LWLockRelease(partition->lock);
	}

	values[0] = Int64GetDatum(entries);
	values[1] = Int64GetDatum(bytes);
	values[2] = Int64GetDatum((int64) pg_atomic_read_u64(&shared_state->hits));
	values[3] = Int64GetDatum((int64) pg_atomic_read_u64(&shared_state->misses));
	values[4] = Int64GetDatum((int64) pg_atomic_read_u64(&shared_state->stores));
	values[5] = Int64GetDatum((int64) pg_atomic_read_u64(&shared_state->invalidations));
	values[6] = Int64GetDatum((int64) pg_atomic_read_u64(&shared_state->evictions));

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(result_columns, values, nulls)));
}

Datum
qcache_entries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *result = (ReturnSetInfo *) fcinfo->resultinfo;
	bool		see_all = has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS);
	// Prepare to return many rows at once.
	InitMaterializedSRF(fcinfo, 0);

	for (int i = 0; i < setting_partitions; i++)
	{
		QcPartition *partition = &shared_state->parts[i];
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
	pg_atomic_write_u64(&shared_state->hits, 0);
	pg_atomic_write_u64(&shared_state->misses, 0);
	pg_atomic_write_u64(&shared_state->stores, 0);
	pg_atomic_write_u64(&shared_state->invalidations, 0);
	pg_atomic_write_u64(&shared_state->evictions, 0);
	PG_RETURN_VOID();
}
// PART 8. SHARED MEMORY
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
	shared_state = ShmemInitStruct("qcache state", sizeof(QcacheShared), &found);
	locks = GetNamedLWLockTranche("qcache");
	for (int i = 0; i < setting_partitions; i++)
		shared_state->parts[i].lock = &locks[i].lock;

	if(!found){
		for(int i = 0; i < VERSION_SLOTS; i++){
			pg_atomic_init_u64(&shared_state->version[i], 0);
			pg_atomic_init_u32(&shared_state->inflight[i], 0);
		}
		pg_atomic_init_u64(&shared_state->hits, 0);
		pg_atomic_init_u64(&shared_state->misses, 0);
		pg_atomic_init_u64(&shared_state->stores, 0);
		pg_atomic_init_u64(&shared_state->invalidations, 0);
		pg_atomic_init_u64(&shared_state->evictions, 0);
		for (int i = 0; i < setting_partitions; i++)
		{
			dlist_init(&shared_state->parts[i].lru);
			shared_state->parts[i].free = NULL;
			shared_state->parts[i].free_chunk_count = 0;
			shared_state->parts[i].bytes_used = 0;
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
		partition_hashes[i] = ShmemInitHash(name, entries_per_partition(),
									 entries_per_partition(), &ctl,
									 HASH_ELEM | HASH_BLOBS);
	}

	if (!found)
	{
		Size per_partition = chunks_per_partition();
		QcChunk *all_chunks = ShmemAlloc(mul_size(total_chunks(), sizeof(QcChunk)));

		for (int i = 0; i < setting_partitions; i++)
		{
			QcPartition *partition = &shared_state->parts[i];

			for (Size j = 0; j < per_partition; j++)
			{
				QcChunk *chunk =
					&all_chunks[i * per_partition + j];

				chunk->next = partition->free;
				partition->free = chunk;
				partition->free_chunk_count++;
			}
		}
	}
	LWLockRelease(AddinShmemInitLock);
}


// PART 9. _PG_init

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
	DefineCustomEnumVariable("qcache.invalidation",
							 "lazy: a commit only bumps version numbers; eager: it also "
							 "scans the cache and deletes (MySQL-style baseline).",
							 NULL, &setting_invalidation, INVALIDATION_LAZY,
							 invalidation_options, PGC_POSTMASTER, 0,
							 NULL, NULL, NULL);
	DefineCustomRealVariable("qcache.min_exec_time",
							 "Results of queries that ran faster than this (ms) are not saved.",
							 NULL, &setting_min_exec_time_ms, 0.0,
							 0.0, 1e9, PGC_USERSET, 0,
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

// Install hooks after saving the previous handlers.
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

	// 4. Ask to be told when transactions end.
	RegisterXactCallback(qcache_transaction_callback, NULL);
}
