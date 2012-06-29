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

#ifndef SQLITE4_MUTEX_OMIT
/*
** Initialize the mutex system.
*/
int sqlite4MutexInit(sqlite4_env *pEnv){
  int rc = SQLITE4_OK;
  if( !pEnv->mutex.xMutexAlloc ){
    if( pEnv->bCoreMutex ){
      pEnv->mutex = *sqlite4DefaultMutex();
    }else{
      pEnv->mutex = *sqlite4NoopMutex();
    }
    pEnv->mutex.pMutexEnv = pEnv;
  }
  rc = pEnv->mutex.xMutexInit(pEnv->mutex.pMutexEnv);
  return rc;
}

/*
** Shutdown the mutex system. This call frees resources allocated by
** sqlite4MutexInit().
*/
int sqlite4MutexEnd(sqlite4_env *pEnv){
  int rc = SQLITE4_OK;
  if( pEnv->mutex.xMutexEnd ){
    rc = pEnv->mutex.xMutexEnd(pEnv->mutex.pMutexEnv);
  }
  return rc;
}

/*
** Retrieve a pointer to a static mutex or allocate a new dynamic one.
*/
sqlite4_mutex *sqlite4_mutex_alloc(sqlite4_env *pEnv, int id){
  if( pEnv==0 ) pEnv = &sqlite4DefaultEnv;
#ifndef SQLITE4_OMIT_AUTOINIT
  if( sqlite4_initialize(pEnv) ) return 0;
#endif
  return pEnv->mutex.xMutexAlloc(pEnv->mutex.pMutexEnv, id);
}

sqlite4_mutex *sqlite4MutexAlloc(sqlite4_env *pEnv, int id){
  if( !pEnv->bCoreMutex ){
    return 0;
  }
  return pEnv->mutex.xMutexAlloc(pEnv->mutex.pMutexEnv, id);
}

/*
** Free a dynamic mutex.
*/
void sqlite4_mutex_free(sqlite4_mutex *p){
  if( p ){
    p->pMutexMethods->xMutexFree(p);
  }
}

/*
** Obtain the mutex p. If some other thread already has the mutex, block
** until it can be obtained.
*/
void sqlite4_mutex_enter(sqlite4_mutex *p){
  if( p ){
    p->pMutexMethods->xMutexEnter(p);
  }
}

/*
** Obtain the mutex p. If successful, return SQLITE4_OK. Otherwise, if another
** thread holds the mutex and it cannot be obtained, return SQLITE4_BUSY.
*/
int sqlite4_mutex_try(sqlite4_mutex *p){
  int rc = SQLITE4_OK;
  if( p ){
    return p->pMutexMethods->xMutexTry(p);
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
    p->pMutexMethods->xMutexLeave(p);
  }
}

#ifndef NDEBUG
/*
** The sqlite4_mutex_held() and sqlite4_mutex_notheld() routine are
** intended for use inside assert() statements.
*/
int sqlite4_mutex_held(sqlite4_mutex *p){
  return p==0 || p->pMutexMethods->xMutexHeld(p);
}
int sqlite4_mutex_notheld(sqlite4_mutex *p){
  return p==0 || p->pMutexMethods->xMutexNotheld(p);
}
#endif

#endif /* !defined(SQLITE4_MUTEX_OMIT) */
