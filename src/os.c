/*
** 2005 November 29
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This file contains OS interface code that is common to all
** architectures.
*/
#define _SQLITE_OS_C_ 1
#include "sqliteInt.h"
#undef _SQLITE_OS_C_

/*
** The default SQLite sqlite4_vfs implementations do not allocate
** memory (actually, os_unix.c allocates a small amount of memory
** from within OsOpen()), but some third-party implementations may.
** So we test the effects of a malloc() failing and the sqlite4OsXXX()
** function returning SQLITE_IOERR_NOMEM using the DO_OS_MALLOC_TEST macro.
**
** The following functions are instrumented for malloc() failure 
** testing:
**
**     sqlite4OsRead()
**     sqlite4OsWrite()
**     sqlite4OsSync()
**     sqlite4OsFileSize()
**     sqlite4OsLock()
**     sqlite4OsCheckReservedLock()
**     sqlite4OsFileControl()
**     sqlite4OsShmMap()
**     sqlite4OsOpen()
**     sqlite4OsDelete()
**     sqlite4OsAccess()
**     sqlite4OsFullPathname()
**
*/
#if defined(SQLITE_TEST)
int sqlite4_memdebug_vfs_oom_test = 1;
  #define DO_OS_MALLOC_TEST(x)                                       \
  if (sqlite4_memdebug_vfs_oom_test && (!x || !sqlite4IsMemJournal(x))) {  \
    void *pTstAlloc = sqlite4Malloc(10);                             \
    if (!pTstAlloc) return SQLITE_IOERR_NOMEM;                       \
    sqlite4_free(pTstAlloc);                                         \
  }
#else
  #define DO_OS_MALLOC_TEST(x)
#endif

/*
** The following routines are convenience wrappers around methods
** of the sqlite4_file object.  This is mostly just syntactic sugar. All
** of this would be completely automatic if SQLite were coded using
** C++ instead of plain old C.
*/
int sqlite4OsClose(sqlite4_file *pId){
  int rc = SQLITE_OK;
  if( pId->pMethods ){
    rc = pId->pMethods->xClose(pId);
    pId->pMethods = 0;
  }
  return rc;
}
int sqlite4OsRead(sqlite4_file *id, void *pBuf, int amt, i64 offset){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xRead(id, pBuf, amt, offset);
}
int sqlite4OsWrite(sqlite4_file *id, const void *pBuf, int amt, i64 offset){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xWrite(id, pBuf, amt, offset);
}
int sqlite4OsTruncate(sqlite4_file *id, i64 size){
  return id->pMethods->xTruncate(id, size);
}
int sqlite4OsSync(sqlite4_file *id, int flags){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xSync(id, flags);
}
int sqlite4OsFileSize(sqlite4_file *id, i64 *pSize){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xFileSize(id, pSize);
}
int sqlite4OsLock(sqlite4_file *id, int lockType){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xLock(id, lockType);
}
int sqlite4OsUnlock(sqlite4_file *id, int lockType){
  return id->pMethods->xUnlock(id, lockType);
}
int sqlite4OsCheckReservedLock(sqlite4_file *id, int *pResOut){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xCheckReservedLock(id, pResOut);
}

/*
** Use sqlite4OsFileControl() when we are doing something that might fail
** and we need to know about the failures.  Use sqlite4OsFileControlHint()
** when simply tossing information over the wall to the VFS and we do not
** really care if the VFS receives and understands the information since it
** is only a hint and can be safely ignored.  The sqlite4OsFileControlHint()
** routine has no return value since the return value would be meaningless.
*/
int sqlite4OsFileControl(sqlite4_file *id, int op, void *pArg){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xFileControl(id, op, pArg);
}
void sqlite4OsFileControlHint(sqlite4_file *id, int op, void *pArg){
  (void)id->pMethods->xFileControl(id, op, pArg);
}

int sqlite4OsSectorSize(sqlite4_file *id){
  int (*xSectorSize)(sqlite4_file*) = id->pMethods->xSectorSize;
  return (xSectorSize ? xSectorSize(id) : SQLITE_DEFAULT_SECTOR_SIZE);
}
int sqlite4OsDeviceCharacteristics(sqlite4_file *id){
  return id->pMethods->xDeviceCharacteristics(id);
}
int sqlite4OsShmLock(sqlite4_file *id, int offset, int n, int flags){
  return id->pMethods->xShmLock(id, offset, n, flags);
}
void sqlite4OsShmBarrier(sqlite4_file *id){
  id->pMethods->xShmBarrier(id);
}
int sqlite4OsShmUnmap(sqlite4_file *id, int deleteFlag){
  return id->pMethods->xShmUnmap(id, deleteFlag);
}
int sqlite4OsShmMap(
  sqlite4_file *id,               /* Database file handle */
  int iPage,
  int pgsz,
  int bExtend,                    /* True to extend file if necessary */
  void volatile **pp              /* OUT: Pointer to mapping */
){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xShmMap(id, iPage, pgsz, bExtend, pp);
}

/*
** The next group of routines are convenience wrappers around the
** VFS methods.
*/
int sqlite4OsOpen(
  sqlite4_vfs *pVfs, 
  const char *zPath, 
  sqlite4_file *pFile, 
  int flags, 
  int *pFlagsOut
){
  int rc;
  DO_OS_MALLOC_TEST(0);
  /* 0x87f7f is a mask of SQLITE_OPEN_ flags that are valid to be passed
  ** down into the VFS layer.  Some SQLITE_OPEN_ flags (for example,
  ** SQLITE_OPEN_FULLMUTEX or SQLITE_OPEN_SHAREDCACHE) are blocked before
  ** reaching the VFS. */
  rc = pVfs->xOpen(pVfs, zPath, pFile, flags & 0x87f7f, pFlagsOut);
  assert( rc==SQLITE_OK || pFile->pMethods==0 );
  return rc;
}
int sqlite4OsDelete(sqlite4_vfs *pVfs, const char *zPath, int dirSync){
  DO_OS_MALLOC_TEST(0);
  assert( dirSync==0 || dirSync==1 );
  return pVfs->xDelete(pVfs, zPath, dirSync);
}
int sqlite4OsAccess(
  sqlite4_vfs *pVfs, 
  const char *zPath, 
  int flags, 
  int *pResOut
){
  DO_OS_MALLOC_TEST(0);
  return pVfs->xAccess(pVfs, zPath, flags, pResOut);
}
int sqlite4OsFullPathname(
  sqlite4_vfs *pVfs, 
  const char *zPath, 
  int nPathOut, 
  char *zPathOut
){
  DO_OS_MALLOC_TEST(0);
  zPathOut[0] = 0;
  return pVfs->xFullPathname(pVfs, zPath, nPathOut, zPathOut);
}
#ifndef SQLITE_OMIT_LOAD_EXTENSION
void *sqlite4OsDlOpen(sqlite4_vfs *pVfs, const char *zPath){
  return pVfs->xDlOpen(pVfs, zPath);
}
void sqlite4OsDlError(sqlite4_vfs *pVfs, int nByte, char *zBufOut){
  pVfs->xDlError(pVfs, nByte, zBufOut);
}
void (*sqlite4OsDlSym(sqlite4_vfs *pVfs, void *pHdle, const char *zSym))(void){
  return pVfs->xDlSym(pVfs, pHdle, zSym);
}
void sqlite4OsDlClose(sqlite4_vfs *pVfs, void *pHandle){
  pVfs->xDlClose(pVfs, pHandle);
}
#endif /* SQLITE_OMIT_LOAD_EXTENSION */
int sqlite4OsRandomness(sqlite4_vfs *pVfs, int nByte, char *zBufOut){
  return pVfs->xRandomness(pVfs, nByte, zBufOut);
}
int sqlite4OsSleep(sqlite4_vfs *pVfs, int nMicro){
  return pVfs->xSleep(pVfs, nMicro);
}
int sqlite4OsCurrentTimeInt64(sqlite4_vfs *pVfs, sqlite4_int64 *pTimeOut){
  int rc;
  /* IMPLEMENTATION-OF: R-49045-42493 SQLite will use the xCurrentTimeInt64()
  ** method to get the current date and time if that method is available
  ** (if iVersion is 2 or greater and the function pointer is not NULL) and
  ** will fall back to xCurrentTime() if xCurrentTimeInt64() is
  ** unavailable.
  */
  if( pVfs->iVersion>=2 && pVfs->xCurrentTimeInt64 ){
    rc = pVfs->xCurrentTimeInt64(pVfs, pTimeOut);
  }else{
    double r;
    rc = pVfs->xCurrentTime(pVfs, &r);
    *pTimeOut = (sqlite4_int64)(r*86400000.0);
  }
  return rc;
}

int sqlite4OsOpenMalloc(
  sqlite4_vfs *pVfs, 
  const char *zFile, 
  sqlite4_file **ppFile, 
  int flags,
  int *pOutFlags
){
  int rc = SQLITE_NOMEM;
  sqlite4_file *pFile;
  pFile = (sqlite4_file *)sqlite4MallocZero(pVfs->szOsFile);
  if( pFile ){
    rc = sqlite4OsOpen(pVfs, zFile, pFile, flags, pOutFlags);
    if( rc!=SQLITE_OK ){
      sqlite4_free(pFile);
    }else{
      *ppFile = pFile;
    }
  }
  return rc;
}
int sqlite4OsCloseFree(sqlite4_file *pFile){
  int rc = SQLITE_OK;
  assert( pFile );
  rc = sqlite4OsClose(pFile);
  sqlite4_free(pFile);
  return rc;
}

/*
** This function is a wrapper around the OS specific implementation of
** sqlite4_os_init(). The purpose of the wrapper is to provide the
** ability to simulate a malloc failure, so that the handling of an
** error in sqlite4_os_init() by the upper layers can be tested.
*/
int sqlite4OsInit(void){
  void *p = sqlite4_malloc(10);
  if( p==0 ) return SQLITE_NOMEM;
  sqlite4_free(p);
  return sqlite4_os_init();
}

/*
** The list of all registered VFS implementations.
*/
static sqlite4_vfs * SQLITE_WSD vfsList = 0;
#define vfsList GLOBAL(sqlite4_vfs *, vfsList)

/*
** Locate a VFS by name.  If no name is given, simply return the
** first VFS on the list.
*/
sqlite4_vfs *sqlite4_vfs_find(const char *zVfs){
  sqlite4_vfs *pVfs = 0;
#if SQLITE_THREADSAFE
  sqlite4_mutex *mutex;
#endif
#ifndef SQLITE_OMIT_AUTOINIT
  int rc = sqlite4_initialize();
  if( rc ) return 0;
#endif
#if SQLITE_THREADSAFE
  mutex = sqlite4MutexAlloc(SQLITE_MUTEX_STATIC_MASTER);
#endif
  sqlite4_mutex_enter(mutex);
  for(pVfs = vfsList; pVfs; pVfs=pVfs->pNext){
    if( zVfs==0 ) break;
    if( strcmp(zVfs, pVfs->zName)==0 ) break;
  }
  sqlite4_mutex_leave(mutex);
  return pVfs;
}

/*
** Unlink a VFS from the linked list
*/
static void vfsUnlink(sqlite4_vfs *pVfs){
  assert( sqlite4_mutex_held(sqlite4MutexAlloc(SQLITE_MUTEX_STATIC_MASTER)) );
  if( pVfs==0 ){
    /* No-op */
  }else if( vfsList==pVfs ){
    vfsList = pVfs->pNext;
  }else if( vfsList ){
    sqlite4_vfs *p = vfsList;
    while( p->pNext && p->pNext!=pVfs ){
      p = p->pNext;
    }
    if( p->pNext==pVfs ){
      p->pNext = pVfs->pNext;
    }
  }
}

/*
** Register a VFS with the system.  It is harmless to register the same
** VFS multiple times.  The new VFS becomes the default if makeDflt is
** true.
*/
int sqlite4_vfs_register(sqlite4_vfs *pVfs, int makeDflt){
  MUTEX_LOGIC(sqlite4_mutex *mutex;)
#ifndef SQLITE_OMIT_AUTOINIT
  int rc = sqlite4_initialize();
  if( rc ) return rc;
#endif
  MUTEX_LOGIC( mutex = sqlite4MutexAlloc(SQLITE_MUTEX_STATIC_MASTER); )
  sqlite4_mutex_enter(mutex);
  vfsUnlink(pVfs);
  if( makeDflt || vfsList==0 ){
    pVfs->pNext = vfsList;
    vfsList = pVfs;
  }else{
    pVfs->pNext = vfsList->pNext;
    vfsList->pNext = pVfs;
  }
  assert(vfsList);
  sqlite4_mutex_leave(mutex);
  return SQLITE_OK;
}

/*
** Unregister a VFS so that it is no longer accessible.
*/
int sqlite4_vfs_unregister(sqlite4_vfs *pVfs){
#if SQLITE_THREADSAFE
  sqlite4_mutex *mutex = sqlite4MutexAlloc(SQLITE_MUTEX_STATIC_MASTER);
#endif
  sqlite4_mutex_enter(mutex);
  vfsUnlink(pVfs);
  sqlite4_mutex_leave(mutex);
  return SQLITE_OK;
}
