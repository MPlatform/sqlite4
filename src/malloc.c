/*
** 2001 September 15
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
** Memory allocation functions used throughout sqlite.
*/
#include "sqliteInt.h"
#include <stdarg.h>

/*
** State information local to the memory allocation subsystem.
*/
static SQLITE_WSD struct Mem0Global {
  sqlite4_mutex *mutex;         /* Mutex to serialize access */
} mem0 = { 0 };

/*
** Initialize the memory allocation subsystem.
*/
int sqlite4MallocInit(void){
  if( sqlite4DefaultEnv.m.xMalloc==0 ){
    sqlite4MemSetDefault(0);
  }
  memset(&mem0, 0, sizeof(mem0));
  if( sqlite4DefaultEnv.bCoreMutex ){
    mem0.mutex = sqlite4MutexAlloc(SQLITE_MUTEX_STATIC_MEM);
  }
  return sqlite4DefaultEnv.m.xInit(sqlite4DefaultEnv.m.pAppData);
}

/*
** Deinitialize the memory allocation subsystem.
*/
void sqlite4MallocEnd(void){
  if( sqlite4DefaultEnv.m.xShutdown ){
    sqlite4DefaultEnv.m.xShutdown(sqlite4DefaultEnv.m.pAppData);
  }
  memset(&mem0, 0, sizeof(mem0));
}

/*
** Return the amount of memory currently checked out.
*/
sqlite4_int64 sqlite4_memory_used(void){
  int n, mx;
  sqlite4_int64 res;
  sqlite4_status(SQLITE_STATUS_MEMORY_USED, &n, &mx, 0);
  res = (sqlite4_int64)n;  /* Work around bug in Borland C. Ticket #3216 */
  return res;
}

/*
** Return the maximum amount of memory that has ever been
** checked out since either the beginning of this process
** or since the most recent reset.
*/
sqlite4_int64 sqlite4_memory_highwater(int resetFlag){
  int n, mx;
  sqlite4_int64 res;
  sqlite4_status(SQLITE_STATUS_MEMORY_USED, &n, &mx, resetFlag);
  res = (sqlite4_int64)mx;  /* Work around bug in Borland C. Ticket #3216 */
  return res;
}

/*
** Allocate memory.  This routine is like sqlite4_malloc() except that it
** assumes the memory subsystem has already been initialized.
*/
void *sqlite4Malloc(int n){
  void *p;
  if( n<=0               /* IMP: R-65312-04917 */ 
   || n>=0x7fffff00
  ){
    /* A memory allocation of a number of bytes which is near the maximum
    ** signed integer value might cause an integer overflow inside of the
    ** xMalloc().  Hence we limit the maximum size to 0x7fffff00, giving
    ** 255 bytes of overhead.  SQLite itself will never use anything near
    ** this amount.  The only way to reach the limit is with sqlite4_malloc() */
    p = 0;
  }else if( sqlite4DefaultEnv.bMemstat ){
    int nFull;
    sqlite4_mutex_enter(mem0.mutex);
    nFull = sqlite4DefaultEnv.m.xRoundup(n);
    sqlite4StatusSet(SQLITE_STATUS_MALLOC_SIZE, n);
    p = sqlite4DefaultEnv.m.xMalloc(nFull);
    if( p ){
      nFull = sqlite4MallocSize(p);
      sqlite4StatusAdd(SQLITE_STATUS_MEMORY_USED, nFull);
      sqlite4StatusAdd(SQLITE_STATUS_MALLOC_COUNT, 1);
    }
    sqlite4_mutex_leave(mem0.mutex);
  }else{
    p = sqlite4DefaultEnv.m.xMalloc(n);
  }
  assert( EIGHT_BYTE_ALIGNMENT(p) );  /* IMP: R-04675-44850 */
  return p;
}

/*
** This version of the memory allocation is for use by the application.
** First make sure the memory subsystem is initialized, then do the
** allocation.
*/
void *sqlite4_malloc(int n){
#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlite4_initialize(0) ) return 0;
#endif
  return sqlite4Malloc(n);
}


/*
** TRUE if p is a lookaside memory allocation from db
*/
#ifndef SQLITE_OMIT_LOOKASIDE
static int isLookaside(sqlite4 *db, void *p){
  return p && p>=db->lookaside.pStart && p<db->lookaside.pEnd;
}
#else
#define isLookaside(A,B) 0
#endif

/*
** Return the size of a memory allocation previously obtained from
** sqlite4Malloc() or sqlite4_malloc().
*/
int sqlite4MallocSize(void *p){
  assert( sqlite4MemdebugHasType(p, MEMTYPE_HEAP) );
  assert( sqlite4MemdebugNoType(p, MEMTYPE_DB) );
  return sqlite4DefaultEnv.m.xSize(p);
}
int sqlite4DbMallocSize(sqlite4 *db, void *p){
  assert( db==0 || sqlite4_mutex_held(db->mutex) );
  if( db && isLookaside(db, p) ){
    return db->lookaside.sz;
  }else{
    assert( sqlite4MemdebugHasType(p, MEMTYPE_DB) );
    assert( sqlite4MemdebugHasType(p, MEMTYPE_LOOKASIDE|MEMTYPE_HEAP) );
    assert( db!=0 || sqlite4MemdebugNoType(p, MEMTYPE_LOOKASIDE) );
    return sqlite4DefaultEnv.m.xSize(p);
  }
}

/*
** Free memory previously obtained from sqlite4Malloc().
*/
void sqlite4_free(void *p){
  if( p==0 ) return;  /* IMP: R-49053-54554 */
  assert( sqlite4MemdebugNoType(p, MEMTYPE_DB) );
  assert( sqlite4MemdebugHasType(p, MEMTYPE_HEAP) );
  if( sqlite4DefaultEnv.bMemstat ){
    sqlite4_mutex_enter(mem0.mutex);
    sqlite4StatusAdd(SQLITE_STATUS_MEMORY_USED, -sqlite4MallocSize(p));
    sqlite4StatusAdd(SQLITE_STATUS_MALLOC_COUNT, -1);
    sqlite4DefaultEnv.m.xFree(p);
    sqlite4_mutex_leave(mem0.mutex);
  }else{
    sqlite4DefaultEnv.m.xFree(p);
  }
}

/*
** Free memory that might be associated with a particular database
** connection.
*/
void sqlite4DbFree(sqlite4 *db, void *p){
  assert( db==0 || sqlite4_mutex_held(db->mutex) );
  if( db ){
    if( db->pnBytesFreed ){
      *db->pnBytesFreed += sqlite4DbMallocSize(db, p);
      return;
    }
    if( isLookaside(db, p) ){
      LookasideSlot *pBuf = (LookasideSlot*)p;
      pBuf->pNext = db->lookaside.pFree;
      db->lookaside.pFree = pBuf;
      db->lookaside.nOut--;
      return;
    }
  }
  assert( sqlite4MemdebugHasType(p, MEMTYPE_DB) );
  assert( sqlite4MemdebugHasType(p, MEMTYPE_LOOKASIDE|MEMTYPE_HEAP) );
  assert( db!=0 || sqlite4MemdebugNoType(p, MEMTYPE_LOOKASIDE) );
  sqlite4MemdebugSetType(p, MEMTYPE_HEAP);
  sqlite4_free(p);
}

/*
** Change the size of an existing memory allocation
*/
void *sqlite4Realloc(void *pOld, int nBytes){
  int nOld, nNew;
  void *pNew;
  if( pOld==0 ){
    return sqlite4Malloc(nBytes); /* IMP: R-28354-25769 */
  }
  if( nBytes<=0 ){
    sqlite4_free(pOld); /* IMP: R-31593-10574 */
    return 0;
  }
  if( nBytes>=0x7fffff00 ){
    /* The 0x7ffff00 limit term is explained in comments on sqlite4Malloc() */
    return 0;
  }
  nOld = sqlite4MallocSize(pOld);
  /* IMPLEMENTATION-OF: R-46199-30249 SQLite guarantees that the second
  ** argument to xRealloc is always a value returned by a prior call to
  ** xRoundup. */
  nNew = sqlite4DefaultEnv.m.xRoundup(nBytes);
  if( nOld==nNew ){
    pNew = pOld;
  }else if( sqlite4DefaultEnv.bMemstat ){
    sqlite4_mutex_enter(mem0.mutex);
    sqlite4StatusSet(SQLITE_STATUS_MALLOC_SIZE, nBytes);
    assert( sqlite4MemdebugHasType(pOld, MEMTYPE_HEAP) );
    assert( sqlite4MemdebugNoType(pOld, ~MEMTYPE_HEAP) );
    pNew = sqlite4DefaultEnv.m.xRealloc(pOld, nNew);
    if( pNew ){
      nNew = sqlite4MallocSize(pNew);
      sqlite4StatusAdd(SQLITE_STATUS_MEMORY_USED, nNew-nOld);
    }
    sqlite4_mutex_leave(mem0.mutex);
  }else{
    pNew = sqlite4DefaultEnv.m.xRealloc(pOld, nNew);
  }
  assert( EIGHT_BYTE_ALIGNMENT(pNew) ); /* IMP: R-04675-44850 */
  return pNew;
}

/*
** The public interface to sqlite4Realloc.  Make sure that the memory
** subsystem is initialized prior to invoking sqliteRealloc.
*/
void *sqlite4_realloc(void *pOld, int n){
#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlite4_initialize(0) ) return 0;
#endif
  return sqlite4Realloc(pOld, n);
}


/*
** Allocate and zero memory.
*/ 
void *sqlite4MallocZero(int n){
  void *p = sqlite4Malloc(n);
  if( p ){
    memset(p, 0, n);
  }
  return p;
}

/*
** Allocate and zero memory.  If the allocation fails, make
** the mallocFailed flag in the connection pointer.
*/
void *sqlite4DbMallocZero(sqlite4 *db, int n){
  void *p = sqlite4DbMallocRaw(db, n);
  if( p ){
    memset(p, 0, n);
  }
  return p;
}

/*
** Allocate and zero memory.  If the allocation fails, make
** the mallocFailed flag in the connection pointer.
**
** If db!=0 and db->mallocFailed is true (indicating a prior malloc
** failure on the same database connection) then always return 0.
** Hence for a particular database connection, once malloc starts
** failing, it fails consistently until mallocFailed is reset.
** This is an important assumption.  There are many places in the
** code that do things like this:
**
**         int *a = (int*)sqlite4DbMallocRaw(db, 100);
**         int *b = (int*)sqlite4DbMallocRaw(db, 200);
**         if( b ) a[10] = 9;
**
** In other words, if a subsequent malloc (ex: "b") worked, it is assumed
** that all prior mallocs (ex: "a") worked too.
*/
void *sqlite4DbMallocRaw(sqlite4 *db, int n){
  void *p;
  assert( db==0 || sqlite4_mutex_held(db->mutex) );
  assert( db==0 || db->pnBytesFreed==0 );
#ifndef SQLITE_OMIT_LOOKASIDE
  if( db ){
    LookasideSlot *pBuf;
    if( db->mallocFailed ){
      return 0;
    }
    if( db->lookaside.bEnabled ){
      if( n>db->lookaside.sz ){
        db->lookaside.anStat[1]++;
      }else if( (pBuf = db->lookaside.pFree)==0 ){
        db->lookaside.anStat[2]++;
      }else{
        db->lookaside.pFree = pBuf->pNext;
        db->lookaside.nOut++;
        db->lookaside.anStat[0]++;
        if( db->lookaside.nOut>db->lookaside.mxOut ){
          db->lookaside.mxOut = db->lookaside.nOut;
        }
        return (void*)pBuf;
      }
    }
  }
#else
  if( db && db->mallocFailed ){
    return 0;
  }
#endif
  p = sqlite4Malloc(n);
  if( !p && db ){
    db->mallocFailed = 1;
  }
  sqlite4MemdebugSetType(p, MEMTYPE_DB |
         ((db && db->lookaside.bEnabled) ? MEMTYPE_LOOKASIDE : MEMTYPE_HEAP));
  return p;
}

/*
** Resize the block of memory pointed to by p to n bytes. If the
** resize fails, set the mallocFailed flag in the connection object.
*/
void *sqlite4DbRealloc(sqlite4 *db, void *p, int n){
  void *pNew = 0;
  assert( db!=0 );
  assert( sqlite4_mutex_held(db->mutex) );
  if( db->mallocFailed==0 ){
    if( p==0 ){
      return sqlite4DbMallocRaw(db, n);
    }
    if( isLookaside(db, p) ){
      if( n<=db->lookaside.sz ){
        return p;
      }
      pNew = sqlite4DbMallocRaw(db, n);
      if( pNew ){
        memcpy(pNew, p, db->lookaside.sz);
        sqlite4DbFree(db, p);
      }
    }else{
      assert( sqlite4MemdebugHasType(p, MEMTYPE_DB) );
      assert( sqlite4MemdebugHasType(p, MEMTYPE_LOOKASIDE|MEMTYPE_HEAP) );
      sqlite4MemdebugSetType(p, MEMTYPE_HEAP);
      pNew = sqlite4_realloc(p, n);
      if( !pNew ){
        sqlite4MemdebugSetType(p, MEMTYPE_DB|MEMTYPE_HEAP);
        db->mallocFailed = 1;
      }
      sqlite4MemdebugSetType(pNew, MEMTYPE_DB | 
            (db->lookaside.bEnabled ? MEMTYPE_LOOKASIDE : MEMTYPE_HEAP));
    }
  }
  return pNew;
}

/*
** Attempt to reallocate p.  If the reallocation fails, then free p
** and set the mallocFailed flag in the database connection.
*/
void *sqlite4DbReallocOrFree(sqlite4 *db, void *p, int n){
  void *pNew;
  pNew = sqlite4DbRealloc(db, p, n);
  if( !pNew ){
    sqlite4DbFree(db, p);
  }
  return pNew;
}

/*
** Make a copy of a string in memory obtained from sqliteMalloc(). These 
** functions call sqlite4MallocRaw() directly instead of sqliteMalloc(). This
** is because when memory debugging is turned on, these two functions are 
** called via macros that record the current file and line number in the
** ThreadData structure.
*/
char *sqlite4DbStrDup(sqlite4 *db, const char *z){
  char *zNew;
  size_t n;
  if( z==0 ){
    return 0;
  }
  n = sqlite4Strlen30(z) + 1;
  assert( (n&0x7fffffff)==n );
  zNew = sqlite4DbMallocRaw(db, (int)n);
  if( zNew ){
    memcpy(zNew, z, n);
  }
  return zNew;
}
char *sqlite4DbStrNDup(sqlite4 *db, const char *z, int n){
  char *zNew;
  if( z==0 ){
    return 0;
  }
  assert( (n&0x7fffffff)==n );
  zNew = sqlite4DbMallocRaw(db, n+1);
  if( zNew ){
    memcpy(zNew, z, n);
    zNew[n] = 0;
  }
  return zNew;
}

/*
** Create a string from the zFromat argument and the va_list that follows.
** Store the string in memory obtained from sqliteMalloc() and make *pz
** point to that string.
*/
void sqlite4SetString(char **pz, sqlite4 *db, const char *zFormat, ...){
  va_list ap;
  char *z;

  va_start(ap, zFormat);
  z = sqlite4VMPrintf(db, zFormat, ap);
  va_end(ap);
  sqlite4DbFree(db, *pz);
  *pz = z;
}


/*
** This function must be called before exiting any API function (i.e. 
** returning control to the user) that has called sqlite4_malloc or
** sqlite4_realloc.
**
** The returned value is normally a copy of the second argument to this
** function. However, if a malloc() failure has occurred since the previous
** invocation SQLITE_NOMEM is returned instead. 
**
** If the first argument, db, is not NULL and a malloc() error has occurred,
** then the connection error-code (the value returned by sqlite4_errcode())
** is set to SQLITE_NOMEM.
*/
int sqlite4ApiExit(sqlite4* db, int rc){
  /* If the db handle is not NULL, then we must hold the connection handle
  ** mutex here. Otherwise the read (and possible write) of db->mallocFailed 
  ** is unsafe, as is the call to sqlite4Error().
  */
  assert( !db || sqlite4_mutex_held(db->mutex) );
  if( db && (db->mallocFailed || rc==SQLITE_IOERR_NOMEM) ){
    sqlite4Error(db, SQLITE_NOMEM, 0);
    db->mallocFailed = 0;
    rc = SQLITE_NOMEM;
  }
  return rc;
}
