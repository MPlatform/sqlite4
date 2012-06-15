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

int sqlite4_os_init(void){}
int sqlite4_os_end(void){}

int sqlite4OsRandomness(sqlite4_env *pEnv, int nByte, unsigned char *zBufOut){
  memset(zBufOut, 0, nByte);
  return SQLITE_OK;
}

/*
** The following variable, if set to a non-zero value, is interpreted as
** the number of seconds since 1970 and is used to set the result of
** sqlite4OsCurrentTime() during testing.
*/
unsigned int sqlite4_current_time = 0; /* Fake system time */
int sqlite4OsCurrentTime(sqlite4_env *pEnv, sqlite4_uint64 *pTimeOut){
  UNUSED_PARAMETER(pEnv);
  *pTimeOut = (sqlite4_uint64)sqlite4_current_time * 1000;
  return SQLITE_OK;
}

/*
** This function is a wrapper around the OS specific implementation of
** sqlite4_os_init(). The purpose of the wrapper is to provide the
** ability to simulate a malloc failure, so that the handling of an
** error in sqlite4_os_init() by the upper layers can be tested.
*/
int sqlite4OsInit(sqlite4_env *pEnv){
  void *p = sqlite4_malloc(10);
  if( p==0 ) return SQLITE_NOMEM;
  sqlite4_free(p);
  return sqlite4_os_init();
}

/*
** The list of all registered VFS implementations.
*/
static sqlite4_vfs * SQLITE_WSD vfsList = 0;

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
