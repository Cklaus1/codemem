/*
 * cbm_library_init.h — Lifecycle API for embedding codebase-memory as a library.
 *
 * Call cbm_library_init() once at process startup before any other cbm_* call.
 * Call cbm_library_fini() once at process shutdown.
 *
 * Thread safety: init/fini must be called from a single thread; all other
 * cbm_* APIs are thread-safe per their own documentation after init returns.
 */
#ifndef CBM_LIBRARY_INIT_H
#define CBM_LIBRARY_INIT_H

#include "cbm_public.h"
#include <stdbool.h>

CBM_EXTERN_C_BEGIN

/* ── Init options ───────────────────────────────────────────────── */

typedef struct {
    /* Fraction of physical RAM to use as indexing budget (0.0 → default 0.5). */
    double ram_fraction;

    /* Minimum log level (cast from CBMLogLevel); -1 → read CBM_LOG_LEVEL env var. */
    int log_level;

    /* Custom log sink — receives each formatted log line. NULL → default stderr. */
    void (*log_sink)(const char *line);

    /* Suppress stderr output entirely (overrides log_sink). */
    bool silent;
} cbm_library_opts_t;

/* ── Lifecycle ──────────────────────────────────────────────────── */

/*
 * Initialize the library.
 *
 * Binds 3rd-party allocators (sqlite, tree-sitter) to mimalloc, sets memory
 * budget, and configures logging. Idempotent — subsequent calls are no-ops.
 *
 * opts may be NULL for all defaults.
 * Returns 0 on success, -1 on error (error message written to stderr).
 */
CBM_PUBLIC int cbm_library_init(const cbm_library_opts_t *opts);

/*
 * Tear down the library.
 *
 * Releases the global pipeline lock if held. Safe to call even if
 * cbm_library_init() was never called or returned an error.
 */
CBM_PUBLIC void cbm_library_fini(void);

/* Version string embedded at compile time (e.g. "1.4.2" or "dev"). */
CBM_PUBLIC const char *cbm_library_version(void);

/* Returns true if cbm_library_init() has been called successfully. */
CBM_PUBLIC bool cbm_library_ready(void);

CBM_EXTERN_C_END

#endif /* CBM_LIBRARY_INIT_H */
