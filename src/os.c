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
#define _SQLITE4_OS_C_ 1
#include "sqliteInt.h"
#undef _SQLITE4_OS_C_

#if SQLITE4_OS_UNIX
#include <sys/time.h>
#endif

/*
** The following variable, if set to a non-zero value, is interpreted as
** the number of seconds since 1970 and is used to set the result of
** sqlite4OsCurrentTime() during testing.
*/
unsigned int sqlite4_current_time = 0; /* Fake system time */
int sqlite4OsCurrentTime(sqlite4_env *pEnv, sqlite4_uint64 *pTimeOut){
  int rc = SQLITE4_OK;
  if( sqlite4_current_time ){
    *pTimeOut = (sqlite4_uint64)sqlite4_current_time * 1000;
    return SQLITE4_OK;
  }
#if SQLITE4_OS_UNIX
  static const sqlite4_int64 unixEpoch = 24405875*(sqlite4_int64)8640000;
  struct timeval sNow;
  if( gettimeofday(&sNow, 0)==0 ){
    *pTimeOut = unixEpoch + 1000*(sqlite4_int64)sNow.tv_sec + sNow.tv_usec/1000;
  }else{
    rc = SQLITE4_ERROR;
  }
  UNUSED_PARAMETER(pEnv);
#endif
#if SQLITE4_OS_WIN
  FILETIME ft;
  static const sqlite4_int64 winFiletimeEpoch =
                                 23058135*(sqlite4_int64)8640000;
  /* 2^32 - to avoid use of LL and warnings in gcc */
  static const sqlite4_int64 max32BitValue = 
      (sqlite4_int64)2000000000 + (sqlite4_int64)2000000000
         + (sqlite4_int64)294967296;
  GetSystemTimeAsFileTime( &ft );
  *pTimeOut = winFiletimeEpoch +
                ((((sqlite4_int64)ft.dwHighDateTime)*max32BitValue) + 
                   (sqlite4_int64)ft.dwLowDateTime)/(sqlite4_int64)10000;
  UNUSED_PARAMETER(pEnv);
#endif
  return rc;
}

/*
** Write nByte bytes of randomness into zBufOut[].  This is used to initialize
** the PRNGs.  nByte will always be 8.
*/
int sqlite4OsRandomness(sqlite4_env *pEnv, int nByte, unsigned char *zBufOut){
  static sqlite4_uint64 cnt = 0;
  unsigned char *p;
  int i;
  sqlite4_uint64 now;
  sqlite4_uint64 x = 0;

#if 0 && SQLITE4_OS_UNIX
  int fd = open("/dev/urandom", O_RDONLY, 0);
  if( fd>=0 ){
    read(fd, zBufOut, nByte);
    close(fd);
  }
  x = getpid();
#endif
  sqlite4OsCurrentTime(pEnv, &now);
  x ^= now;
  memset(zBufOut, 0, nByte);
  cnt++;
  x ^= cnt;
  p = (unsigned char*)&x;
  for(i=0; i<8; i++) zBufOut[i%nByte] ^= p[i];
    
  return SQLITE4_OK;
}

/*
** This function is a wrapper around the OS specific implementation of
** sqlite4_os_init(). The purpose of the wrapper is to provide the
** ability to simulate a malloc failure, so that the handling of an
** error in sqlite4_os_init() by the upper layers can be tested.
*/
int sqlite4OsInit(sqlite4_env *pEnv){
  void *p = sqlite4_malloc(pEnv, 10);
  if( p==0 ) return SQLITE4_NOMEM;
  sqlite4_free(pEnv, p);
  sqlite4OsRandomness(pEnv, 8, (unsigned char*)&pEnv->prngX);
  return SQLITE4_OK; /*sqlite4_os_init();*/
}
