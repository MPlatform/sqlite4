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
**
** This file defines the sqlite4_mm "SQLite4 Memory Manager" object and
** its interfaces.
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
** Memory statistics reporting
*/
typedef enum {
  SQLITE4_MMSTAT_OUT = 1,         /* Bytes of memory outstanding */
  SQLITE4_MMSTAT_UNITS = 2,       /* Separate allocations outstanding */
  SQLITE4_MMSTAT_SIZE = 3,        /* Size of the allocation */
  SQLITE4_MMSTAT_SZFAULT = 4,     /* Number of faults due to size */
  SQLITE4_MMSTAT_MEMFAULT = 5,    /* Number of faults due to out of space */
  SQLITE4_MMSTAT_FAULT = 6,       /* Total number of faults */
};

/*
** Bit flags for the 3rd parameter of xStat()
*/
#define SQLITE4_MMSTAT_HIGHWATER  0x01
#define SQLITE4_MMSTAT_RESET      0x02
#define SQLITE4_MMSTAT_HWRESET    0x03

/*
** An instance of the following object defines the methods on
** a BESPOKE memory allocator.
*/
struct sqlite4_mm_methods {
  int iVersion;
  void *(*xMalloc)(sqlite4_mm*, sqlite4_int64);
  void *(*xRealloc)(sqlite4_mm*, void*, sqlite4_int64);
  void (*xFree)(sqlite4_mm*, void*);
  sqlite4_int64 (*xMsize)(sqlite4_mm*, void*);
  int (*xMember)(sqlite4_mm*, const void*);
  void (*xBenign)(sqlite4_mm*, int);
  sqlite4_int64 (*xStat)(sqlite4_mm*, sqlite4_mm_stattype, unsigned flags);
  void (*xFinal)(sqlite4_mm*);
};

/*
** Available memory management types:
*/
typedef enum {
  SQLITE4_MM_SYSTEM = 1,     /* Use the system malloc() */
  SQLITE4_MM_ONESIZE = 2,    /* All allocations map to a fixed size */
  SQLITE4_MM_OVERFLOW = 3,   /* Two allocators. Use A first; failover to B */
  SQLITE4_MM_COMPACT = 4,    /* Like memsys3 from SQLite3 */
  SQLITE4_MM_ROBSON = 5,     /* Like memsys5 from SQLite3 */
  SQLITE4_MM_LINEAR = 6,     /* Allocate from a fixed buffer w/o free */
  SQLITE4_MM_BESPOKE = 7,    /* Caller-defined implementation */
  SQLITE4_MM_DEBUG,          /* Debugging memory allocator */
  SQLITE4_MM_STATS           /* Keep memory statistics */
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

/*
** Rest
