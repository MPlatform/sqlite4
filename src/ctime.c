/*
** 2010 February 23
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file implements routines used to report what compile-time options
** SQLite was built with.
*/

#ifndef SQLITE4_OMIT_COMPILEOPTION_DIAGS

#include "sqliteInt.h"

/*
** An array of names of all compile-time options.  This array should 
** be sorted A-Z.
**
** This array looks large, but in a typical installation actually uses
** only a handful of compile-time options, so most times this array is usually
** rather short and uses little memory space.
*/
static const char * const azCompileOpt[] = {

/* These macros are provided to "stringify" the value of the define
** for those options in which the value is meaningful. */
#define CTIMEOPT_VAL_(opt) #opt
#define CTIMEOPT_VAL(opt) CTIMEOPT_VAL_(opt)

#ifdef SQLITE4_32BIT_ROWID
  "32BIT_ROWID",
#endif
#ifdef SQLITE4_4_BYTE_ALIGNED_MALLOC
  "4_BYTE_ALIGNED_MALLOC",
#endif
#ifdef SQLITE4_CASE_SENSITIVE_LIKE
  "CASE_SENSITIVE_LIKE",
#endif
#ifdef SQLITE4_CHECK_PAGES
  "CHECK_PAGES",
#endif
#ifdef SQLITE4_COVERAGE_TEST
  "COVERAGE_TEST",
#endif
#ifdef SQLITE4_DEBUG
  "DEBUG",
#endif
#ifdef SQLITE4_DEFAULT_LOCKING_MODE
  "DEFAULT_LOCKING_MODE=" CTIMEOPT_VAL(SQLITE4_DEFAULT_LOCKING_MODE),
#endif
#ifdef SQLITE4_DISABLE_DIRSYNC
  "DISABLE_DIRSYNC",
#endif
#ifdef SQLITE4_DISABLE_LFS
  "DISABLE_LFS",
#endif
#ifdef SQLITE4_ENABLE_ATOMIC_WRITE
  "ENABLE_ATOMIC_WRITE",
#endif
#ifdef SQLITE4_ENABLE_CEROD
  "ENABLE_CEROD",
#endif
#ifdef SQLITE4_ENABLE_COLUMN_METADATA
  "ENABLE_COLUMN_METADATA",
#endif
#ifdef SQLITE4_ENABLE_EXPENSIVE_ASSERT
  "ENABLE_EXPENSIVE_ASSERT",
#endif
#ifdef SQLITE4_ENABLE_FTS1
  "ENABLE_FTS1",
#endif
#ifdef SQLITE4_ENABLE_FTS2
  "ENABLE_FTS2",
#endif
#ifdef SQLITE4_ENABLE_FTS3
  "ENABLE_FTS3",
#endif
#ifdef SQLITE4_ENABLE_FTS3_PARENTHESIS
  "ENABLE_FTS3_PARENTHESIS",
#endif
#ifdef SQLITE4_ENABLE_FTS4
  "ENABLE_FTS4",
#endif
#ifdef SQLITE4_ENABLE_ICU
  "ENABLE_ICU",
#endif
#ifdef SQLITE4_ENABLE_IOTRACE
  "ENABLE_IOTRACE",
#endif
#ifdef SQLITE4_ENABLE_LOAD_EXTENSION
  "ENABLE_LOAD_EXTENSION",
#endif
#ifdef SQLITE4_ENABLE_LOCKING_STYLE
  "ENABLE_LOCKING_STYLE=" CTIMEOPT_VAL(SQLITE4_ENABLE_LOCKING_STYLE),
#endif
#ifdef SQLITE4_ENABLE_MEMSYS3
  "ENABLE_MEMSYS3",
#endif
#ifdef SQLITE4_ENABLE_MEMSYS5
  "ENABLE_MEMSYS5",
#endif
#ifdef SQLITE4_ENABLE_OVERSIZE_CELL_CHECK
  "ENABLE_OVERSIZE_CELL_CHECK",
#endif
#ifdef SQLITE4_ENABLE_RTREE
  "ENABLE_RTREE",
#endif
#ifdef SQLITE4_ENABLE_STAT3
  "ENABLE_STAT3",
#endif
#ifdef SQLITE4_ENABLE_UNLOCK_NOTIFY
  "ENABLE_UNLOCK_NOTIFY",
#endif
#ifdef SQLITE4_ENABLE_UPDATE_DELETE_LIMIT
  "ENABLE_UPDATE_DELETE_LIMIT",
#endif
#ifdef SQLITE4_HAS_CODEC
  "HAS_CODEC",
#endif
#ifdef SQLITE4_HAVE_ISNAN
  "HAVE_ISNAN",
#endif
#ifdef SQLITE4_HOMEGROWN_RECURSIVE_MUTEX
  "HOMEGROWN_RECURSIVE_MUTEX",
#endif
#ifdef SQLITE4_IGNORE_AFP_LOCK_ERRORS
  "IGNORE_AFP_LOCK_ERRORS",
#endif
#ifdef SQLITE4_IGNORE_FLOCK_LOCK_ERRORS
  "IGNORE_FLOCK_LOCK_ERRORS",
#endif
#ifdef SQLITE4_INT64_TYPE
  "INT64_TYPE",
#endif
#ifdef SQLITE4_LOCK_TRACE
  "LOCK_TRACE",
#endif
#ifdef SQLITE4_MAX_SCHEMA_RETRY
  "MAX_SCHEMA_RETRY=" CTIMEOPT_VAL(SQLITE4_MAX_SCHEMA_RETRY),
#endif
#ifdef SQLITE4_MEMDEBUG
  "MEMDEBUG",
#endif
#ifdef SQLITE4_MIXED_ENDIAN_64BIT_FLOAT
  "MIXED_ENDIAN_64BIT_FLOAT",
#endif
#ifdef SQLITE4_NO_SYNC
  "NO_SYNC",
#endif
#ifdef SQLITE4_OMIT_ALTERTABLE
  "OMIT_ALTERTABLE",
#endif
#ifdef SQLITE4_OMIT_ANALYZE
  "OMIT_ANALYZE",
#endif
#ifdef SQLITE4_OMIT_ATTACH
  "OMIT_ATTACH",
#endif
#ifdef SQLITE4_OMIT_AUTHORIZATION
  "OMIT_AUTHORIZATION",
#endif
#ifdef SQLITE4_OMIT_AUTOINCREMENT
  "OMIT_AUTOINCREMENT",
#endif
#ifdef SQLITE4_OMIT_AUTOINIT
  "OMIT_AUTOINIT",
#endif
#ifdef SQLITE4_OMIT_AUTOMATIC_INDEX
  "OMIT_AUTOMATIC_INDEX",
#endif
#ifdef SQLITE4_OMIT_AUTORESET
  "OMIT_AUTORESET",
#endif
#ifdef SQLITE4_OMIT_BETWEEN_OPTIMIZATION
  "OMIT_BETWEEN_OPTIMIZATION",
#endif
#ifdef SQLITE4_OMIT_BLOB_LITERAL
  "OMIT_BLOB_LITERAL",
#endif
#ifdef SQLITE4_OMIT_BUILTIN_TEST
  "OMIT_BUILTIN_TEST",
#endif
#ifdef SQLITE4_OMIT_CAST
  "OMIT_CAST",
#endif
#ifdef SQLITE4_OMIT_CHECK
  "OMIT_CHECK",
#endif
/* // redundant
** #ifdef SQLITE4_OMIT_COMPILEOPTION_DIAGS
**   "OMIT_COMPILEOPTION_DIAGS",
** #endif
*/
#ifdef SQLITE4_OMIT_COMPLETE
  "OMIT_COMPLETE",
#endif
#ifdef SQLITE4_OMIT_COMPOUND_SELECT
  "OMIT_COMPOUND_SELECT",
#endif
#ifdef SQLITE4_OMIT_DATETIME_FUNCS
  "OMIT_DATETIME_FUNCS",
#endif
#ifdef SQLITE4_OMIT_DECLTYPE
  "OMIT_DECLTYPE",
#endif
#ifdef SQLITE4_OMIT_DEPRECATED
  "OMIT_DEPRECATED",
#endif
#ifdef SQLITE4_OMIT_DISKIO
  "OMIT_DISKIO",
#endif
#ifdef SQLITE4_OMIT_EXPLAIN
  "OMIT_EXPLAIN",
#endif
#ifdef SQLITE4_OMIT_FLAG_PRAGMAS
  "OMIT_FLAG_PRAGMAS",
#endif
#ifdef SQLITE4_OMIT_FLOATING_POINT
  "OMIT_FLOATING_POINT",
#endif
#ifdef SQLITE4_OMIT_FOREIGN_KEY
  "OMIT_FOREIGN_KEY",
#endif
#ifdef SQLITE4_OMIT_GET_TABLE
  "OMIT_GET_TABLE",
#endif
#ifdef SQLITE4_OMIT_INTEGRITY_CHECK
  "OMIT_INTEGRITY_CHECK",
#endif
#ifdef SQLITE4_OMIT_LIKE_OPTIMIZATION
  "OMIT_LIKE_OPTIMIZATION",
#endif
#ifdef SQLITE4_OMIT_LOAD_EXTENSION
  "OMIT_LOAD_EXTENSION",
#endif
#ifdef SQLITE4_OMIT_LOCALTIME
  "OMIT_LOCALTIME",
#endif
#ifdef SQLITE4_OMIT_LOOKASIDE
  "OMIT_LOOKASIDE",
#endif
#ifdef SQLITE4_OMIT_MEMORYDB
  "OMIT_MEMORYDB",
#endif
#ifdef SQLITE4_OMIT_OR_OPTIMIZATION
  "OMIT_OR_OPTIMIZATION",
#endif
#ifdef SQLITE4_OMIT_PAGER_PRAGMAS
  "OMIT_PAGER_PRAGMAS",
#endif
#ifdef SQLITE4_OMIT_PRAGMA
  "OMIT_PRAGMA",
#endif
#ifdef SQLITE4_OMIT_PROGRESS_CALLBACK
  "OMIT_PROGRESS_CALLBACK",
#endif
#ifdef SQLITE4_OMIT_QUICKBALANCE
  "OMIT_QUICKBALANCE",
#endif
#ifdef SQLITE4_OMIT_REINDEX
  "OMIT_REINDEX",
#endif
#ifdef SQLITE4_OMIT_SCHEMA_PRAGMAS
  "OMIT_SCHEMA_PRAGMAS",
#endif
#ifdef SQLITE4_OMIT_SCHEMA_VERSION_PRAGMAS
  "OMIT_SCHEMA_VERSION_PRAGMAS",
#endif
#ifdef SQLITE4_OMIT_SUBQUERY
  "OMIT_SUBQUERY",
#endif
#ifdef SQLITE4_OMIT_TCL_VARIABLE
  "OMIT_TCL_VARIABLE",
#endif
#ifdef SQLITE4_OMIT_TEMPDB
  "OMIT_TEMPDB",
#endif
#ifdef SQLITE4_OMIT_TRACE
  "OMIT_TRACE",
#endif
#ifdef SQLITE4_OMIT_TRIGGER
  "OMIT_TRIGGER",
#endif
#ifdef SQLITE4_OMIT_TRUNCATE_OPTIMIZATION
  "OMIT_TRUNCATE_OPTIMIZATION",
#endif
#ifdef SQLITE4_OMIT_UTF16
  "OMIT_UTF16",
#endif
#ifdef SQLITE4_OMIT_VIEW
  "OMIT_VIEW",
#endif
#ifdef SQLITE4_OMIT_VIRTUALTABLE
  "OMIT_VIRTUALTABLE",
#endif
#ifdef SQLITE4_OMIT_XFER_OPT
  "OMIT_XFER_OPT",
#endif
#ifdef SQLITE4_PERFORMANCE_TRACE
  "PERFORMANCE_TRACE",
#endif
#ifdef SQLITE4_PROXY_DEBUG
  "PROXY_DEBUG",
#endif
#ifdef SQLITE4_SECURE_DELETE
  "SECURE_DELETE",
#endif
#ifdef SQLITE4_SMALL_STACK
  "SMALL_STACK",
#endif
#ifdef SQLITE4_SOUNDEX
  "SOUNDEX",
#endif
#ifdef SQLITE4_TCL
  "TCL",
#endif
#ifdef SQLITE4_TEMP_STORE
  "TEMP_STORE=" CTIMEOPT_VAL(SQLITE4_TEMP_STORE),
#endif
#ifdef SQLITE4_TEST
  "TEST",
#endif
#ifdef SQLITE4_THREADSAFE
  "THREADSAFE=" CTIMEOPT_VAL(SQLITE4_THREADSAFE),
#endif
#ifdef SQLITE4_USE_ALLOCA
  "USE_ALLOCA",
#endif
#ifdef SQLITE4_ZERO_MALLOC
  "ZERO_MALLOC"
#endif
};

/*
** Given the name of a compile-time option, return true if that option
** was used and false if not.
**
** The name can optionally begin with "SQLITE4_" but the "SQLITE4_" prefix
** is not required for a match.
*/
int sqlite4_compileoption_used(const char *zOptName){
  int i, n;
  if( sqlite4_strnicmp(zOptName, "SQLITE4_", 8)==0 ) zOptName += 8;
  n = sqlite4Strlen30(zOptName);

  /* Since ArraySize(azCompileOpt) is normally in single digits, a
  ** linear search is adequate.  No need for a binary search. */
  for(i=0; i<ArraySize(azCompileOpt); i++){
    if(   (sqlite4_strnicmp(zOptName, azCompileOpt[i], n)==0)
       && ( (azCompileOpt[i][n]==0) || (azCompileOpt[i][n]=='=') ) ) return 1;
  }
  return 0;
}

/*
** Return the N-th compile-time option string.  If N is out of range,
** return a NULL pointer.
*/
const char *sqlite4_compileoption_get(int N){
  if( N>=0 && N<ArraySize(azCompileOpt) ){
    return azCompileOpt[N];
  }
  return 0;
}

#endif /* SQLITE4_OMIT_COMPILEOPTION_DIAGS */
