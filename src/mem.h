/*
** 2013-01-01
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This header file defines the interface that the SQLite4 memory
** management logic.
**
** Some of this will eventually fold into sqliteInt.h.
*/


/*
** object declarations
*/
typedef struct sqlite4_mm sqlite4_mm;
typedef struct sqlite4_mm_methods sqlite4_mm_methods;

/*
** Base class.  Each implementation extends this with additional
** fields specific to its own needs.  This needs to be public so that
** applications can supply their on customized memory allocators.
*/
struct sqlite4_mm {
  const sqlite4_mm_methods *pMethods;
};

/*
** An instance of the following object defines a BESPOKE memory alloator
*/
struct sqlite4_mm_methods {
  int iVersion;
  void *(*xMalloc)(sqlite4_mm*, sqlite4_int64);
  void *(*xRealloc)(sqlite4_mm*, void*, sqlite4_int64);
  void (*xFree)(sqlite4_mm*, void*);
  sqlite4_int64 (*xMsize)(sqlite4_mm*, void*);
  int (*xMember)(sqlite4_mm*, const void*);
  void (*xBenign)(sqlite4_mm*, int);
  void (*xFinal)(sqlite4_mm*);
};

/*
** Available memory management types:
*/
typedef enum {
  SQLITE4_MM_SYSTEM,         /* Use the system malloc() */
  SQLITE4_MM_ONESIZE,        /* All allocations map to a fixed size */
  SQLITE4_MM_OVERFLOW,       /* Two allocators. Use A first; failover to B */
  SQLITE4_MM_COMPACT,        /* Like memsys3 from SQLite3 */
  SQLITE4_MM_ROBSON,         /* Like memsys5 from SQLite3 */
  SQLITE4_MM_LINEAR,         /* Allocate from a fixed buffer w/o free */
  SQLITE4_MM_DEBUG,          /* Debugging memory allocator */
  SQLITE4_MM_STATS,          /* Keep memory statistics */
  SQLITE4_MM_BESPOKE         /* Caller-defined implementation */
} sqlite4_mm_type;

/*
** Allocate a new memory manager.  Return NULL if unable.
*/
sqlite4_mm *sqlite4_mm_new(sqlite4_mm_type, ...);

/*
** Free the sqlite4_mm object.
**
** All outstanding memory for the allocator must be freed prior to
** invoking this interface, or else the behavior is undefined.
*/
void sqlite4_mm_destroy(sqlite4_mm*);

/*
** Core memory allocation routines:
*/
void *sqlite4_mm_malloc(sqlite4_mm*, sqlite4_int64);
void *sqlite4_mm_realloc(sqlite4_mm*, void*, sqlite4_int64);
void sqlite4_mm_free(sqlite4_mm*, void*);

/*
** Return the size of a memory allocation.
**
** All memory allocators in SQLite4 must be able to report their size.
** When using system malloc() on system that lack the malloc_usable_size()
** routine or its equivalent, then the sqlite4_mm object allocates 8 extra
** bytes for each memory allocation and stores the allocation size in those
** initial 8 bytes.
*/
sqlite4_int64 sqlite4_mm_msize(sqlite4_mm*, void*);

/*
** Check to see if pOld is a memory allocation from pMM.  If it is, return
** 1.  If not, return 0.  If we cannot determine an answer, return -1.
**
** If pOld is not a valid memory allocation or is a memory allocation that
** has previously been freed, then the result of this routine is undefined.
*/
int sqlite4_mm_member(sqlite4_mm *pMM, const void *pOld);

/*
** Enable or disable benign failure mode.  Benign failure mode can be
** nested.  In benign failure mode, OOM errors do not necessarily propagate
** back out to the application but can be dealt with internally.  Memory
** allocations that occur in benign failure mode are considered "optional".
*/
void sqlite4_mm_benign_failures(sqlite4_mm*, int bEnable);
