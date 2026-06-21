# Architecture Issues & Bug Report

Six confirmed bugs and performance issues found by static code analysis.
All fixes are applied in the accompanying commits.

---

## 1. CRITICAL — N+1 subqueries in `cbm_store_search()` (`store.c:2447`)

**Severity**: Critical performance  
**File**: `src/store/store.c`

Every row returned by `cbm_store_search()` triggers two correlated COUNT
subqueries regardless of whether the caller requested degree filtering:

```c
"(SELECT COUNT(*) FROM edges e WHERE e.target_id = n.id AND e.type IN ('CALLS','USAGE')) AS in_deg,"
"(SELECT COUNT(*) FROM edges e WHERE e.source_id = n.id AND e.type IN ('CALLS','USAGE')) AS out_deg"
```

A search returning 50 nodes fires 100 extra queries. With the default
`LIMIT 10` this is 20 extra queries per call; with `LIMIT 1000` it becomes
2000. These are correlated subqueries re-executed for every outer row.

**Fix**: Compute `has_degree_filter` before building `select_cols`. When false,
use a stripped `select_cols_no_deg` that omits both COUNT subqueries. The
degree columns are only needed when the caller passes `min_degree`/`max_degree`;
`in_degree`/`out_degree` in the result struct are left as 0 otherwise (callers
needing degrees without filtering can use `cbm_store_node_degree()`).

---

## 2. HIGH — OR condition defeats both edge indexes (`store.c:2400`)

**Severity**: High performance  
**File**: `src/store/store.c`

The `relationship` filter in `search_where_advanced()` uses:

```c
"EXISTS(SELECT 1 FROM edges e WHERE (e.source_id = n.id OR e.target_id = n.id) AND e.type = ?%d)"
```

SQLite cannot satisfy `source_id = X OR target_id = X` with a single B-tree
index. Both `idx_edges_source_type (source_id, type)` and
`idx_edges_target_type (target_id, type)` become unusable; the planner falls
back to a full `edges` table scan for every node in the outer query.

On a 500K-edge graph, each outer row triggers a 500K-row scan instead of two
O(log N) index seeks.

**Fix**: Split into two indexed EXISTS clauses:

```c
"(EXISTS(SELECT 1 FROM edges e WHERE e.source_id = n.id AND e.type = ?%d)"
" OR EXISTS(SELECT 1 FROM edges e WHERE e.target_id = n.id AND e.type = ?%d))"
```

SQLite satisfies each half with the corresponding index. Bind the parameter
twice (two placeholders, same value).

---

## 3. HIGH — Predump passes run fully sequentially (`pipeline.c:516`)

**Severity**: High performance  
**File**: `src/pipeline/pipeline.c`, `src/graph_buffer/graph_buffer.c`

`run_predump_passes()` executes all six passes in a serial loop:

```c
for (int i = 0; i < PREDUMP_PASS_COUNT && !check_cancel(p); i++) {
    passes[i].fn(ctx);
}
```

The two expensive passes — `similarity` (SIMILAR_TO edges via MinHash LSH) and
`semantic_edges` (SEMANTICALLY_RELATED via TF-IDF + Random Indexing) — each
take minutes on a 50K-function codebase. They are mutually independent: both
only read nodes from `gbuf` during their compute phases and write edges during
their sequential merge phases.

**Fix**:
1. Add `cbm_mutex_t insert_mu` to `struct cbm_gbuf` and lock it inside
   `cbm_gbuf_insert_edge()` to make edge insertion thread-safe.
2. After the fast sequential passes complete, run `predump_sim` and
   `predump_sem` in two concurrent `cbm_thread_t` threads.

Expected wall-clock improvement: 40–50% on multi-core machines.

---

## 4. HIGH — BFS edge collection silently truncates at 4 KB (`store.c:2565`)

**Severity**: High correctness / data loss  
**File**: `src/store/store.c`

`bfs_collect_edges()` builds the visited-node IN clause into a fixed buffer:

```c
char id_set[CBM_SZ_4K];
int ilen = snprintf(id_set, sizeof(id_set), "%lld", (long long)start_id);
...
if (ilen >= (int)sizeof(id_set)) {
    ilen = (int)sizeof(id_set) - SKIP_ONE;  // silent truncation
}
```

At ~20 chars per int64 ID (including comma), the buffer overflows at ~200
nodes. The `if (ilen >= ...)` guard silently caps the buffer, emitting an
invalid SQL `IN (...)` clause with a truncated list. Edges whose endpoints
aren't in the truncated set are silently dropped.

**Impact**: `trace_path` and impact analysis return incomplete results on any
graph with more than ~200 reachable nodes, with no error or warning to the
caller.

**Fix**: Replace the fixed buffer with a `malloc`/`realloc` dynamic buffer,
growing by 4KB whenever the next ID would overflow. Return `CBM_STORE_OK`
with empty results on allocation failure (existing caller contract).

---

## 5. MEDIUM — Incremental indexing builds the same data into two separate hash tables (`pipeline_incremental.c:78`)

**Severity**: Medium performance  
**File**: `src/pipeline/pipeline_incremental.c`

The incremental pipeline calls two functions back-to-back on the same data:

```c
bool *is_changed = classify_files(files, file_count, stored, stored_count, ...);
int deleted_count = find_deleted_files(repo_path, files, file_count, stored, stored_count, ...);
```

`classify_files` builds `ht_stored` (rel_path → stored hash) then iterates
`files[]` to find changed entries.

`find_deleted_files` builds `ht_current` (rel_path → current file) then
iterates `stored[]` to find deleted/skipped entries.

Both functions iterate the same `stored[]` and `files[]` arrays. Building two
separate hash tables and iterating twice causes ~4 MB of allocator churn on a
100K-file repository before any parsing begins.

**Fix**: Merge the two functions into `classify_all_files()` that builds a
single `ht_stored` and processes both classifications in one sweep.

---

## 6. MEDIUM — Semantic embeddings recomputed for every node on every incremental run (`pass_semantic_edges.c:1196`)

**Severity**: Medium performance  
**File**: `src/pipeline/pass_semantic_edges.c`, `src/pipeline/pipeline_internal.h`

`cbm_pipeline_pass_semantic_edges()` calls `phase1_scan_functions()` which
collects ALL Function/Method nodes from the graph buffer — even in incremental
mode when only a handful of files changed:

```c
int func_count = phase1_scan_functions(gbuf, &funcs, &node_ptrs);
```

On a 50K-function codebase, running full embedding on every incremental reindex
(e.g., 10 files changed) wastes 99% of the work. The pipeline has already
identified which files changed (`changed_file_set`), but that information is not
threaded through to the semantic pass.

**Fix**:
1. Add `CBMHashTable *changed_file_set` to `cbm_pipeline_ctx_t`.
2. Populate it in `pipeline_incremental.c` (rel_path → `(void*)1`) before
   calling the semantic pass.
3. In `phase1_scan_functions`, skip nodes whose `file_path` is not in
   `changed_file_set` (when the set is non-NULL — full-index mode passes NULL
   so all nodes are processed as before).
