/*
** 2007 August 14
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains the C functions that implement mutexes.
**
** This file contains code that is common across all mutex implementations.
*/
#include "sqliteInt.h"

#if defined(SQLITE_DEBUG) && !defined(SQLITE_MUTEX_OMIT)
/*
** For debugging purposes, record when the mutex subsystem is initialized
** and uninitialized so that we can assert() if there is an attempt to
** allocate a mutex while the system is uninitialized.
*/
static SQLITE_WSD int mutexIsInit = 0;
#endif /* SQLITE_DEBUG */


#ifndef SQLITE_MUTEX_OMIT
/*
** Initialize the mutex system.
*/
int sqlite4MutexInit(void){ 
  int rc = SQLITE_OK;
  if( !sqlite4DefaultEnv.mutex.xMutexAlloc ){
    /* If the xMutexAlloc method has not been set, then the user did not
    ** install a mutex implementation via sqlite4_config() prior to 
    ** sqlite4_initialize() being called. This block copies pointers to
    ** the default implementation into the sqlite4DefaultEnv structure.
    */
    sqlite4_mutex_methods const *pFrom;
    sqlite4_mutex_methods *pTo = &sqlite4DefaultEnv.mutex;

    if( sqlite4DefaultEnv.bCoreMutex ){
      pFrom = sqlite4DefaultMutex();
    }else{
      pFrom = sqlite4NoopMutex();
    }
    memcpy(pTo, pFrom, offsetof(sqlite4_mutex_methods, xMutexAlloc));
    memcpy(&pTo->xMutexFree, &pFrom->xMutexFree,
           sizeof(*pTo) - offsetof(sqlite4_mutex_methods, xMutexFree));
    pTo->xMutexAlloc = pFrom->xMutexAlloc;
  }
  rc = sqlite4DefaultEnv.mutex.xMutexInit();

#ifdef SQLITE_DEBUG
  mutexIsInit = 1;
#endif

  return rc;
}

/*
** Shutdown the mutex system. This call frees resources allocated by
** sqlite4MutexInit().
*/
int sqlite4MutexEnd(void){
  int rc = SQLITE_OK;
  if( sqlite4DefaultEnv.mutex.xMutexEnd ){
    rc = sqlite4DefaultEnv.mutex.xMutexEnd();
  }

#ifdef SQLITE_DEBUG
  mutexIsInit = 0;
#endif

  return rc;
}

/*
** Retrieve a pointer to a static mutex or allocate a new dynamic one.
*/
sqlite4_mutex *sqlite4_mutex_alloc(int id){
#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlite4_initialize() ) return 0;
#endif
  return sqlite4DefaultEnv.mutex.xMutexAlloc(id);
}

sqlite4_mutex *sqlite4MutexAlloc(int id){
  if( !sqlite4DefaultEnv.bCoreMutex ){
    return 0;
  }
  assert( mutexIsInit );
  return sqlite4DefaultEnv.mutex.xMutexAlloc(id);
}

/*
** Free a dynamic mutex.
*/
void sqlite4_mutex_free(sqlite4_mutex *p){
  if( p ){
    sqlite4DefaultEnv.mutex.xMutexFree(p);
  }
}

/*
** Obtain the mutex p. If some other thread already has the mutex, block
** until it can be obtained.
*/
void sqlite4_mutex_enter(sqlite4_mutex *p){
  if( p ){
    sqlite4DefaultEnv.mutex.xMutexEnter(p);
  }
}

/*
** Obtain the mutex p. If successful, return SQLITE_OK. Otherwise, if another
** thread holds the mutex and it cannot be obtained, return SQLITE_BUSY.
*/
int sqlite4_mutex_try(sqlite4_mutex *p){
  int rc = SQLITE_OK;
  if( p ){
    return sqlite4DefaultEnv.mutex.xMutexTry(p);
  }
  return rc;
}

/*
** The sqlite4_mutex_leave() routine exits a mutex that was previously
** entered by the same thread.  The behavior is undefined if the mutex 
** is not currently entered. If a NULL pointer is passed as an argument
** this function is a no-op.
*/
void sqlite4_mutex_leave(sqlite4_mutex *p){
  if( p ){
    sqlite4DefaultEnv.mutex.xMutexLeave(p);
  }
}

#ifndef NDEBUG
/*
** The sqlite4_mutex_held() and sqlite4_mutex_notheld() routine are
** intended for use inside assert() statements.
*/
int sqlite4_mutex_held(sqlite4_mutex *p){
  return p==0 || sqlite4DefaultEnv.mutex.xMutexHeld(p);
}
int sqlite4_mutex_notheld(sqlite4_mutex *p){
  return p==0 || sqlite4DefaultEnv.mutex.xMutexNotheld(p);
}
#endif

#endif /* !defined(SQLITE_MUTEX_OMIT) */
