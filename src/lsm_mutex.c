/*
** 2012-01-30
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
** Mutex implementation for LSM.
*/
#include "lsmInt.h"

/*
** One of the following must be defined:
**
**   LSM_MUTEX_NONE
**     Default mutex methods are no-ops. For single threaded applications.
**
**   LSM_MUTEX_PTHREADS
**     Use pthreads mutex functions.
**
** If neither of the above are defined, throw a compile time error.
*/
#if !defined(LSM_MUTEX_NONE) && !defined(LSM_MUTEX_PTHREADS)
# error "One of LSM_MUTEX_NONE or LSM_MUTEX_PTHREADS must be defined"
#endif

/*
** Concatenate two symbols together.
*/
#define concat(a,b) a ## b

#ifdef LSM_MUTEX_PTHREADS 
/*************************************************************************
** Mutex methods for pthreads based systems.
*/
#define dflt_mutex_method(x)  concat(pthreads, x)
#include <pthread.h>

typedef struct PthreadMutex PthreadMutex;
struct PthreadMutex {
  pthread_mutex_t mutex;
#ifdef LSM_DEBUG
  pthread_t owner;
#endif
};

#ifdef LSM_DEBUG
# define LSM_PTHREAD_STATIC_MUTEX { PTHREAD_MUTEX_INITIALIZER, 0 }
#else
# define LSM_PTHREAD_STATIC_MUTEX { PTHREAD_MUTEX_INITIALIZER }
#endif

static int pthreads_mutex_static(int iMutex, lsm_mutex **ppStatic){
  static PthreadMutex sMutex[2] = {
    LSM_PTHREAD_STATIC_MUTEX,
    LSM_PTHREAD_STATIC_MUTEX
  };

  assert( iMutex==LSM_MUTEX_GLOBAL || iMutex==LSM_MUTEX_HEAP );
  assert( LSM_MUTEX_GLOBAL==1 && LSM_MUTEX_HEAP==2 );

  *ppStatic = (lsm_mutex *)&sMutex[iMutex-1];
  return LSM_OK;
}

static int pthreads_mutex_new(lsm_mutex **ppNew){
  PthreadMutex *pMutex;           /* Pointer to new mutex */
  pthread_mutexattr_t attr;       /* Attributes object */

  pMutex = (PthreadMutex *)lsmMallocZero(sizeof(PthreadMutex));
  if( !pMutex ) return LSM_NOMEM_BKPT;

  pthread_mutexattr_init(&attr);
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutex_init(&pMutex->mutex, &attr);
  pthread_mutexattr_destroy(&attr);

  *ppNew = (lsm_mutex *)pMutex;
  return LSM_OK;
}

static void pthreads_mutex_del(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
  pthread_mutex_destroy(&pMutex->mutex);
  lsmFree(pMutex);
}

static void pthreads_mutex_enter(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
  pthread_mutex_lock(&pMutex->mutex);

#ifdef LSM_DEBUG
  assert( !pthread_equal(pMutex->owner, pthread_self()) );
  pMutex->owner = pthread_self();
  assert( pthread_equal(pMutex->owner, pthread_self()) );
#endif
}

static int pthreads_mutex_try(lsm_mutex *p){
  int ret;
  PthreadMutex *pMutex = (PthreadMutex *)p;
  ret = pthread_mutex_trylock(&pMutex->mutex);
#ifdef LSM_DEBUG
  if( ret==0 ){
    assert( !pthread_equal(pMutex->owner, pthread_self()) );
    pMutex->owner = pthread_self();
    assert( pthread_equal(pMutex->owner, pthread_self()) );
  }
#endif
  return ret;
}

static void pthreads_mutex_leave(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
#ifdef LSM_DEBUG
  assert( pthread_equal(pMutex->owner, pthread_self()) );
  pMutex->owner = 0;
  assert( !pthread_equal(pMutex->owner, pthread_self()) );
#endif
  pthread_mutex_unlock(&pMutex->mutex);
}

#ifdef LSM_DEBUG
static int pthreads_mutex_held(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
  return pthread_equal(pMutex->owner, pthread_self());
}
#endif
/*
** End of pthreads mutex implementation.
*************************************************************************/
#else
/*************************************************************************
** The following mutex methods are no-ops. For single threaded 
** applications only. 
*/
#define dflt_mutex_method(x)  concat(noop, x)

#ifdef LSM_DEBUG

typedef struct NoopMutex NoopMutex;
struct NoopMutex {
  int bHeld;                      /* True if mutex is held */
  int bStatic;                    /* True for a static mutex */
};
static NoopMutex aStaticNoopMutex[2] = {
  {0, 1},
  {0, 1},
};

static int noop_mutex_static(int iMutex, lsm_mutex **ppStatic){
  assert( iMutex>=1 && iMutex<=(int)array_size(aStaticNoopMutex) );
  *ppStatic = (lsm_mutex *)&aStaticNoopMutex[iMutex-1];
  return LSM_OK;
}
static int noop_mutex_new(lsm_mutex **ppNew){
  int rc = LSM_OK;
  *ppNew = (lsm_mutex *)lsmMallocZeroRc(sizeof(NoopMutex), &rc);
  return rc;
}
static void noop_mutex_del(lsm_mutex *pMutex)  { 
  NoopMutex *p = (NoopMutex *)pMutex;
  assert( p->bStatic==0 );
  lsmFree(p);
}
static void noop_mutex_enter(lsm_mutex *pMutex){ 
  NoopMutex *p = (NoopMutex *)pMutex;
  assert( p->bHeld==0 );
  p->bHeld = 1;
}
static int noop_mutex_try(lsm_mutex *pMutex){
  NoopMutex *p = (NoopMutex *)pMutex;
  assert( p->bHeld==0 );
  p->bHeld = 1;
  return 0;
}
static void noop_mutex_leave(lsm_mutex *pMutex){ 
  NoopMutex *p = (NoopMutex *)pMutex;
  assert( p->bHeld==1 );
  p->bHeld = 0;
}
static int noop_mutex_held(lsm_mutex *pMutex){ 
  NoopMutex *p = (NoopMutex *)pMutex;
  return p->bHeld;
}
#else
static int noop_mutex_static(int iMutex, lsm_mutex **ppStatic){
  unused_parameter(iMutex);
  *ppStatic = 0;
  return LSM_OK;
}
static int noop_mutex_new(lsm_mutex **ppNew){
  *ppNew = 0;
  return LSM_OK;
}
static void noop_mutex_del(lsm_mutex *pMutex)  { unused_parameter(pMutex); }
static void noop_mutex_enter(lsm_mutex *pMutex){ unused_parameter(pMutex); }
static void noop_mutex_leave(lsm_mutex *pMutex){ unused_parameter(pMutex); }
#endif


/*
** End of no-op mutex implementation.
*************************************************************************/
#endif

static lsm_mutex_methods gMutex = {
  dflt_mutex_method(_mutex_static),
  dflt_mutex_method(_mutex_new),
  dflt_mutex_method(_mutex_del),
  dflt_mutex_method(_mutex_enter),
  dflt_mutex_method(_mutex_try),
  dflt_mutex_method(_mutex_leave),
#ifdef LSM_DEBUG
  dflt_mutex_method(_mutex_held)
#else
  0
#endif
};

void lsmConfigSetMutex(lsm_mutex_methods *pMutex){
  gMutex = *pMutex;
}
void lsmConfigGetMutex(lsm_mutex_methods *pMutex){
  *pMutex = gMutex;
}

int lsmMutexNew(lsm_mutex **ppNew){
  return gMutex.xMutexNew(ppNew);
}

int lsmMutexStatic(int iMutex, lsm_mutex **ppStatic){
  assert( iMutex==LSM_MUTEX_GLOBAL );
  return gMutex.xMutexStatic(iMutex, ppStatic);
}

void lsmMutexDel(lsm_mutex *pMutex){
  gMutex.xMutexDel(pMutex);
}

void lsmMutexEnter(lsm_mutex *pMutex){
  gMutex.xMutexEnter(pMutex);
}

int lsmMutexTry(lsm_mutex *pMutex){
  return gMutex.xMutexTry(pMutex);
}

void lsmMutexLeave(lsm_mutex *pMutex){
  gMutex.xMutexLeave(pMutex);
}

int lsmMutexHeld(lsm_mutex *pMutex, int bExpect){
  if( gMutex.xMutexHeld==0 ) return bExpect;
  return gMutex.xMutexHeld(pMutex);
}
