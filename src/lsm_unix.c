/*
** 2011-12-03
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
** Unix-specific run-time environment implementation for LSM.
*/
#include "lsm.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <string.h>

#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include <ctype.h>

#include <unistd.h>

/*
** An open file is an instance of the following object
*/
typedef struct PosixFile PosixFile;
struct PosixFile {
  lsm_env *pEnv;     /* The run-time environment */
  int fd;            /* The open file descriptor */
};

static int lsm_ioerr(void){ return LSM_IOERR; }

static int lsmPosixOsOpen(
  lsm_env *pEnv,
  const char *zFile, 
  lsm_file **ppFile
){
  int rc = LSM_OK;
  PosixFile *p;

  p = lsm_malloc(pEnv, sizeof(PosixFile));
  if( p==0 ){
    rc = LSM_NOMEM;
  }else{
    p->pEnv = pEnv;
    p->fd = open(zFile, O_RDWR|O_CREAT, 0644);
    if( p->fd<0 ){
      lsm_free(pEnv, p);
      p = 0;
      rc = lsm_ioerr();
    }
  }

  *ppFile = (lsm_file *)p;
  return rc;
}

static int lsmPosixOsWrite(
  lsm_file *pFile,                /* File to write to */
  lsm_i64 iOff,                   /* Offset to write to */
  void *pData,                    /* Write data from this buffer */
  int nData                       /* Bytes of data to write */
){
  int rc = LSM_OK;
  PosixFile *p = (PosixFile *)pFile;
  off_t offset;

  offset = lseek(p->fd, (off_t)iOff, SEEK_SET);
  if( offset!=iOff ){
    rc = lsm_ioerr();
  }else{
    ssize_t prc = write(p->fd, pData, (size_t)nData);
    if( prc<0 ) rc = lsm_ioerr();
  }

  return rc;
}

static int lsmPosixOsRead(
  lsm_file *pFile,                /* File to read from */
  lsm_i64 iOff,                   /* Offset to read from */
  void *pData,                    /* Read data into this buffer */
  int nData                       /* Bytes of data to read */
){
  int rc = LSM_OK;
  PosixFile *p = (PosixFile *)pFile;
  off_t offset;

  offset = lseek(p->fd, (off_t)iOff, SEEK_SET);
  if( offset!=iOff ){
    rc = lsm_ioerr();
  }else{
    ssize_t prc = read(p->fd, pData, (size_t)nData);
    if( prc<0 ) rc = lsm_ioerr();
  }

  return rc;
}

static int lsmPosixOsSync(lsm_file *pFile){
  int rc = LSM_OK;

#ifndef LSM_NO_SYNC
  PosixFile *p = (PosixFile *)pFile;
  int prc;

  prc = fdatasync(p->fd);
  if( prc<0 ) rc = lsm_ioerr();
#else
  (void)pFile;
#endif

  return rc;
}

static int lsmPosixOsClose(lsm_file *pFile){
  PosixFile *p = (PosixFile *)pFile;
  close(p->fd);
  lsm_free(p->pEnv, p);
  return LSM_OK;
}

#ifdef LSM_MUTEX_PTHREADS 
/*************************************************************************
** Mutex methods for pthreads based systems.  If LSM_MUTEX_PTHREADS is
** missing then a no-op implementation of mutexes found in lsm_mutex.c
** will be used instead.
*/
#include <pthread.h>

typedef struct PthreadMutex PthreadMutex;
struct PthreadMutex {
  lsm_env *pEnv;
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

static int lsmPosixOsMutexStatic(
  lsm_env *pEnv,
  int iMutex,
  lsm_mutex **ppStatic
){
  static PthreadMutex sMutex[2] = {
    LSM_PTHREAD_STATIC_MUTEX,
    LSM_PTHREAD_STATIC_MUTEX
  };

  assert( iMutex==LSM_MUTEX_GLOBAL || iMutex==LSM_MUTEX_HEAP );
  assert( LSM_MUTEX_GLOBAL==1 && LSM_MUTEX_HEAP==2 );

  *ppStatic = (lsm_mutex *)&sMutex[iMutex-1];
  return LSM_OK;
}

static int lsmPosixOsMutexNew(lsm_env *pEnv, lsm_mutex **ppNew){
  PthreadMutex *pMutex;           /* Pointer to new mutex */
  pthread_mutexattr_t attr;       /* Attributes object */

  pMutex = (PthreadMutex *)lsmMallocZero(pEnv, sizeof(PthreadMutex));
  if( !pMutex ) return LSM_NOMEM_BKPT;

  pMutex->pEnv = pEnv;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutex_init(&pMutex->mutex, &attr);
  pthread_mutexattr_destroy(&attr);

  *ppNew = (lsm_mutex *)pMutex;
  return LSM_OK;
}

static void lsmPosixOsMutexDel(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
  pthread_mutex_destroy(&pMutex->mutex);
  lsmFree(pMutex->pEnv, pMutex);
}

static void lsmPosixOsMutexEnter(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
  pthread_mutex_lock(&pMutex->mutex);

#ifdef LSM_DEBUG
  assert( !pthread_equal(pMutex->owner, pthread_self()) );
  pMutex->owner = pthread_self();
  assert( pthread_equal(pMutex->owner, pthread_self()) );
#endif
}

static int lsmPosixOsMutexTry(lsm_mutex *p){
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

static void lsmPosixOsMutexLeave(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
#ifdef LSM_DEBUG
  assert( pthread_equal(pMutex->owner, pthread_self()) );
  pMutex->owner = 0;
  assert( !pthread_equal(pMutex->owner, pthread_self()) );
#endif
  pthread_mutex_unlock(&pMutex->mutex);
}

#ifdef LSM_DEBUG
static int lsmPosixOsMutexHeld(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
  return pMutex ? pthread_equal(pMutex->owner, pthread_self()) : 1;
}
static int lsmPosixOsMutexNotHeld(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
  return pMutex ? !pthread_equal(pMutex->owner, pthread_self()) : 1;
}
#endif
/*
** End of pthreads mutex implementation.
*************************************************************************/


lsm_env *lsm_default_env(void){
  static lsm_env posix_env = {
    sizeof(lsm_env),         /* nByte */
    1,                       /* iVersion */
    /***** file i/o ******************/
    0,                       /* pVfsCtx */
    lsmPosixOsOpen,          /* xOpen */
    lsmPosixOsRead,          /* xRead */
    lsmPosixOsWrite,         /* xWrite */
    lsmPosixOsSync,          /* xSync */
    lsmPosixOsClose,         /* xClose */
    /***** memory allocation *********/
    0,                       /* pMemCtx */
    lsmPosixOsMalloc,        /* xMalloc */
    lsmPosixOsRealloc,       /* xRealloc */
    lsmPosixOsFree,          /* xFree */
    0,                       /* pMutexCtx */
    /***** mutexes *********************/
    lsmPosixOsMutexStatic,   /* xMutexStatic */
    lsmPosixOsMutexNew,      /* xMutexNew */
    lsmPosixOsMutexDel,      /* xMutexDel */
    lsmPosixOsMutexEnter,    /* xMutexEnter */
    lsmPosixOsMutexTry,      /* xMutexTry */
    lsmPosixOsMutexLeave,    /* xMutexLeave */
    lsmPosixOsMutexHeld,     /* xMutexHeld */
    lsmPosixOsMutexNotHeld,  /* xMutexNotHeld */
  };
  return &posix_env;
}
