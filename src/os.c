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
