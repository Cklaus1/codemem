/*
 * cbm_public.h — Symbol visibility and extern "C" guards for library consumers.
 *
 * Include this before any cbm_library.h include when building a shared library
 * or consuming one via FFI. It is a no-op in static builds and direct embedding.
 *
 * Usage in library source:
 *   Define CBM_BUILDING_LIBRARY before including any cbm header so that
 *   CBM_PUBLIC expands to dllexport (Windows) / visibility("default") (ELF).
 *
 * Usage in consumer code:
 *   Include cbm_library.h; CBM_PUBLIC expands to dllimport (Windows) / default.
 */
#ifndef CBM_PUBLIC_H
#define CBM_PUBLIC_H

/* ── Symbol export/import ───────────────────────────────────────── */

#ifdef _WIN32
#  ifdef CBM_BUILDING_LIBRARY
#    define CBM_PUBLIC __declspec(dllexport)
#  else
#    define CBM_PUBLIC __declspec(dllimport)
#  endif
#elif defined(__GNUC__) || defined(__clang__)
#  define CBM_PUBLIC __attribute__((visibility("default")))
#else
#  define CBM_PUBLIC
#endif

/* ── C linkage for C++ consumers ────────────────────────────────── */

#ifdef __cplusplus
#  define CBM_EXTERN_C       extern "C"
#  define CBM_EXTERN_C_BEGIN extern "C" {
#  define CBM_EXTERN_C_END   }
#else
#  define CBM_EXTERN_C
#  define CBM_EXTERN_C_BEGIN
#  define CBM_EXTERN_C_END
#endif

#endif /* CBM_PUBLIC_H */
