/*
 * cbm_library.h — Single-header entry point for embedding codebase-memory.
 *
 * Include this file (and only this file) in consumer code. It pulls in the
 * public API surface of the core engine — indexing, search, Cypher queries,
 * file discovery, and logging — without the MCP/CLI/HTTP transport layer.
 *
 * Quick-start:
 *
 *   #include "cbm_library.h"
 *
 *   int main(void) {
 *       cbm_library_init(NULL);                    // defaults: 50% RAM, env log level
 *
 *       cbm_pipeline_t *p = cbm_pipeline_new("/path/to/repo", "/tmp/repo.db", CBM_MODE_FAST);
 *       cbm_pipeline_run(p);
 *       cbm_pipeline_free(p);
 *
 *       cbm_store_t *store = cbm_store_open_path("/tmp/repo.db");
 *
 *       cbm_search_params_t params = { .project = "repo", .name_pattern = ".*Handler" };
 *       cbm_search_output_t out = {0};
 *       cbm_store_search(store, &params, &out);
 *       for (int i = 0; i < out.count; i++)
 *           printf("%s\n", out.results[i].node.qualified_name);
 *       cbm_store_search_free(&out);
 *
 *       cbm_store_close(store);
 *       cbm_library_fini();
 *   }
 *
 * Memory contract:
 *   - Strings returned from the library are owned by the library and freed
 *     through the paired cbm_*_free() call.  Never pass them to free() directly.
 *   - Opaque handles (cbm_store_t *, cbm_pipeline_t *) must be closed/freed via
 *     their lifecycle functions; do not pass them to free().
 *
 * Build note:
 *   When building libcbm.so / libcbm.a, define CBM_BUILDING_LIBRARY before
 *   including this header so CBM_PUBLIC expands to dllexport / visibility("default").
 *   Consumer code needs no extra define — CBM_PUBLIC becomes dllimport / default.
 *
 * Visibility annotation status:
 *   CBM_PUBLIC is declared here and in cbm_public.h. The core headers (store.h,
 *   pipeline.h, cypher.h, discover.h) do not yet annotate individual functions —
 *   the .so build therefore exports all symbols (default ELF visibility).
 *   A future pass should prefix each public function declaration with CBM_PUBLIC
 *   and add -fvisibility=hidden to CFLAGS_LIB to reduce the export surface.
 */
#ifndef CBM_LIBRARY_H
#define CBM_LIBRARY_H

/* Visibility + extern "C" */
#include "cbm_public.h"

/* ── Lifecycle ──────────────────────────────────────────────────── */
#include "cbm_library_init.h"

/* ── Core engine ────────────────────────────────────────────────── */

/* Graph store: open/close, CRUD, search, traversal, schema. */
#include "store/store.h"

/* Indexing pipeline: discover → extract → resolve → persist. */
#include "pipeline/pipeline.h"

/* Cypher query engine: MATCH/WHERE/RETURN over the graph store. */
#include "cypher/cypher.h"

/* File discovery and language detection. */
#include "discover/discover.h"

/* In-memory graph buffer (used internally by pipeline; exposed for testing). */
#include "graph_buffer/graph_buffer.h"

/* ── Utilities ──────────────────────────────────────────────────── */

/* Structured key-value logging to stderr (or a custom sink). */
#include "foundation/log.h"

/* RSS / memory budget tracking. */
#include "foundation/mem.h"

#endif /* CBM_LIBRARY_H */
