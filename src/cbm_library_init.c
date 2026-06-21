/*
 * cbm_library_init.c — Library lifecycle implementation.
 *
 * Wires together the three mandatory startup steps that main.c normally
 * performs:  allocator binding, memory budget, and logging configuration.
 * Exposes them as a single cbm_library_init() call so embedding hosts
 * don't need to replicate main.c's startup sequence.
 */

/* Bind 3rd-party allocators to mimalloc before any other include pulls in
 * sqlite or tree-sitter headers (mirroring main.c's include order). */
#include "cbm.h"

#include "cbm_library_init.h"
#include "foundation/log.h"
#include "foundation/mem.h"
#include "pipeline/pipeline.h"

#ifndef CBM_VERSION
#define CBM_VERSION "dev"
#endif

#define CBM_LIB_DEFAULT_RAM_FRACTION 0.5

static int g_initialized = 0;

int cbm_library_init(const cbm_library_opts_t *opts) {
    if (g_initialized) {
        return 0;
    }

    /* 1. Bind sqlite3 / tree-sitter allocators to mimalloc. Must be first. */
    cbm_alloc_init();

    /* 2. Memory budget — caps RSS during indexing. */
    double ram = (opts && opts->ram_fraction > 0.0)
                     ? opts->ram_fraction
                     : CBM_LIB_DEFAULT_RAM_FRACTION;
    cbm_mem_init(ram);

    /* 3. Logging — sink before level so the sink sees all messages. */
    if (opts && opts->silent) {
        cbm_log_set_level(CBM_LOG_NONE);
    } else {
        if (opts && opts->log_sink) {
            cbm_log_set_sink(opts->log_sink);
        }
        if (opts && opts->log_level >= 0) {
            cbm_log_set_level((CBMLogLevel)opts->log_level);
        } else {
            cbm_log_init_from_env();
        }
    }

    g_initialized = 1;
    return 0;
}

void cbm_library_fini(void) {
    if (!g_initialized) {
        return;
    }
    cbm_pipeline_unlock();
    g_initialized = 0;
}

const char *cbm_library_version(void) {
    return CBM_VERSION;
}

bool cbm_library_ready(void) {
    return g_initialized != 0;
}
