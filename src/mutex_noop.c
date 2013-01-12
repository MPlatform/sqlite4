/*
** 2008 October 07
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
** This implementation in this file does not provide any mutual
** exclusion and is thus suitable for use only in applications
** that use SQLite in a single thread.  The routines defined
** here are place-holders.  Applications can substitute working
** mutex routines at start-time using the
**
**     sqlite4_env_config(pEnv, SQLITE4_ENVCONFIG_MUTEX,...)
**
** interface.
**
** If compiled with SQLITE4_DEBUG, then additional logic is inserted
** that does error checking on mutexes to make sure they are being
** called correctly.
*/
#include "sqliteInt.h"

#ifndef SQLITE4_MUTEX_OMIT

#ifndef SQLITE4_DEBUG
/*
** Stub routines for all mutex methods.
**
** This routines provide no mutual exclusion or error checking.
*/
static int noopMutexInit(void *p){ UNUSED_PARAMETER(p); return SQLITE4_OK; }
static int noopMutexEnd(void *p){ UNUSED_PARAMETER(p); return SQLITE4_OK; }
static sqlite4_mutex *noopMutexAlloc(void *p, int id){ 
  UNUSED_PARAMETER(p);
  UNUSED_PARAMETER(id);
  return (sqlite4_mutex*)8; 
}
static void noopMutexFree(sqlite4_mutex *p){ UNUSED_PARAMETER(p); return; }
static void noopMutexEnter(sqlite4_mutex *p){ UNUSED_PARAMETER(p); return; }
static int noopMutexTry(sqlite4_mutex *p){
  UNUSED_PARAMETER(p);
  return SQLITE4_OK;
}
static void noopMutexLeave(sqlite4_mutex *p){ UNUSED_PARAMETER(p); return; }

sqlite4_mutex_methods const *sqlite4NoopMutex(void){
  static const sqlite4_mutex_methods sMutex = {
    noopMutexInit,
    noopMutexEnd,
    noopMutexAlloc,
    noopMutexFree,
    noopMutexEnter,
    noopMutexTry,
    noopMutexLeave,
    0,
    0,
    0
  };
  return &sMutex;
}
#endif /* !SQLITE4_DEBUG */

#ifdef SQLITE4_DEBUG
/*
** In this implementation, error checking is provided for testing
** and debugging purposes.  The mutexes still do not provide any
** mutual exclusion.
*/

/*
** The mutex object
*/
typedef struct sqlite4DebugMutex {
  sqlite4_mutex base;    /* Base class. Must be first */
  sqlite4_env *pEnv;     /* Run-time environment */
  int id;                /* Type of mutex */
  int cnt;               /* Number of entries without a matching leave */
} sqlite4DebugMutex;

/*
** The sqlite4_mutex_held() and sqlite4_mutex_notheld() routine are
** intended for use inside assert() statements.
*/
static int debugMutexHeld(sqlite4_mutex *pX){
  sqlite4DebugMutex *p = (sqlite4DebugMutex*)pX;
  return p==0 || p->cnt>0;
}
static int debugMutexNotheld(sqlite4_mutex *pX){
  sqlite4DebugMutex *p = (sqlite4DebugMutex*)pX;
  return p==0 || p->cnt==0;
}

/*
** Initialize and deinitialize the mutex subsystem.
*/
static int debugMutexInit(void *p){ UNUSED_PARAMETER(p); return SQLITE4_OK; }
static int debugMutexEnd(void *p){ UNUSED_PARAMETER(p); return SQLITE4_OK; }

/*
** The sqlite4_mutex_alloc() routine allocates a new
** mutex and returns a pointer to it.  If it returns NULL
** that means that a mutex could not be allocated. 
*/
static sqlite4_mutex *debugMutexAlloc(sqlite4_env *pEnv, int id){
  sqlite4DebugMutex *pNew = 0;
  pNew = sqlite4Malloc(pEnv, sizeof(*pNew));
  if( pNew ){
    pNew->id = id;
    pNew->cnt = 0;
    pNew->pEnv = pEnv;
  }
  return (sqlite4_mutex*)pNew;
}

/*
** This routine deallocates a previously allocated mutex.
*/
static void debugMutexFree(sqlite4_mutex *pX){
  sqlite4DebugMutex *p = (sqlite4DebugMutex*)pX;
  assert( p->cnt==0 );
  sqlite4_free(p->pEnv, p);
}

/*
** The sqlite4_mutex_enter() and sqlite4_mutex_try() routines attempt
** to enter a mutex.  If another thread is already within the mutex,
** sqlite4_mutex_enter() will block and sqlite4_mutex_try() will return
** SQLITE4_BUSY.  The sqlite4_mutex_try() interface returns SQLITE4_OK
** upon successful entry.  Mutexes created using SQLITE4_MUTEX_RECURSIVE can
** be entered multiple times by the same thread.  In such cases the,
** mutex must be exited an equal number of times before another thread
** can enter.  If the same thread tries to enter any other kind of mutex
** more than once, the behavior is undefined.
*/
static void debugMutexEnter(sqlite4_mutex *pX){
  sqlite4DebugMutex *p = (sqlite4DebugMutex*)pX;
  assert( p->id==SQLITE4_MUTEX_RECURSIVE || debugMutexNotheld(pX) );
  p->cnt++;
}
static int debugMutexTry(sqlite4_mutex *pX){
  sqlite4DebugMutex *p = (sqlite4DebugMutex*)pX;
  assert( p->id==SQLITE4_MUTEX_RECURSIVE || debugMutexNotheld(pX) );
  p->cnt++;
  return SQLITE4_OK;
}

/*
** The sqlite4_mutex_leave() routine exits a mutex that was
** previously entered by the same thread.  The behavior
** is undefined if the mutex is not currently entered or
** is not currently allocated.  SQLite will never do either.
*/
static void debugMutexLeave(sqlite4_mutex *pX){
  sqlite4DebugMutex *p = (sqlite4DebugMutex*)pX;
  assert( debugMutexHeld(pX) );
  p->cnt--;
  assert( p->id==SQLITE4_MUTEX_RECURSIVE || debugMutexNotheld(pX) );
}

sqlite4_mutex_methods const *sqlite4NoopMutex(void){
  static const sqlite4_mutex_methods sMutex = {
    debugMutexInit,
    debugMutexEnd,
    debugMutexAlloc,
    debugMutexFree,
    debugMutexEnter,
    debugMutexTry,
    debugMutexLeave,
    debugMutexHeld,
    debugMutexNotheld, 
    0
  };

  return &sMutex;
}
#endif /* SQLITE4_DEBUG */

/*
** If compiled with SQLITE4_MUTEX_NOOP, then the no-op mutex implementation
** is used regardless of the run-time threadsafety setting.
*/
#ifdef SQLITE4_MUTEX_NOOP
sqlite4_mutex_methods const *sqlite4DefaultMutex(void){
  return sqlite4NoopMutex();
}
#endif /* defined(SQLITE4_MUTEX_NOOP) */
#endif /* !defined(SQLITE4_MUTEX_OMIT) */
