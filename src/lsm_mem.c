/*
** 2011-08-18
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
** Helper routines for memory allocation.
*/
#include "lsmInt.h"

/* Default allocation size. */
#define CHUNKSIZE 16*1024

typedef struct Chunk Chunk;

struct Chunk {
  int iOff;                       /* Offset of free space within pSpace */
  u8 *aData;                      /* Pointer to space for user allocations */
  int nData;                      /* Size of buffer aData, in bytes */
  Chunk *pNext;
};

struct Mempool {
  Chunk *pFirst;                  /* First in list of chunks */
  Chunk *pLast;                   /* Last in list of chunks */
  int nUsed;                      /* Total number of bytes allocated */
};

static void *dflt_malloc(lsm_env *pEnv, int N){ return malloc(N); }
static void dflt_free(lsm_env *pEnv, void *p){ free(p); }
static void *dflt_realloc(lsm_env *pEnv, void *p, int N){
  return realloc(p, N);
}

/*
** Core memory allocation routines for LSM.
*/
void *lsm_malloc(lsm_env *pEnv, size_t N){
  if( pEnv==0 ) pEnv = lsm_default_env();
  return pEnv->xMalloc(pEnv, N);
}
void lsm_free(lsm_env *pEnv, void *p){
  if( pEnv==0 ) pEnv = lsm_default_env();
  pEnv->xFree(pEnv, p);
}
void *lsm_realloc(lsm_env *pEnv, void *p, size_t N){
  if( pEnv==0 ) pEnv = lsm_default_env();
  pEnv->xRealloc(pEnv, p, N);
}

/*
** Memory allocation routines which guarantee that pEnv!=0
*/
void *lsmMalloc(lsm_env *pEnv, size_t N){
  if( pEnv==0 ) pEnv = lsm_default_env();
  return pEnv->xMalloc(pEnv, N);
}
void lsmFree(lsm_env *pEnv, void *p){
  if( pEnv==0 ) pEnv = lsm_default_env();
  pEnv->xFree(pEnv, p);
}
void *lsmRealloc(lsm_env *pEnv, void *p, size_t N){
  if( pEnv==0 ) pEnv = lsm_default_env();
  pEnv->xRealloc(pEnv, p, N);
}

void *lsmMallocZero(lsm_env *pEnv, size_t N){
  void *pRet;
  pRet = lsm_malloc(pEnv, N);
  if( pRet ) memset(pRet, 0, N);
  return pRet;
}

void *lsmMallocRc(lsm_env *pEnv, size_t N, int *pRc){
  void *pRet = 0;
  if( *pRc==LSM_OK ){
    pRet = lsmMalloc(pEnv, N);
    if( pRet==0 ){
      *pRc = LSM_NOMEM_BKPT;
    }
  }
  return pRet;
}

void *lsmMallocZeroRc(lsm_env *pEnv, size_t N, int *pRc){
  void *pRet = 0;
  if( *pRc==LSM_OK ){
    pRet = lsmMallocZero(pEnv, N);
    if( pRet==0 ){
      *pRc = LSM_NOMEM_BKPT;
    }
  }
  return pRet;
}

void *lsmReallocOrFree(lsm_env *pEnv, void *p, size_t N){
  void *pNew;
  pNew = lsm_realloc(pEnv, p, N);
  if( !pNew ) lsm_free(pEnv, p);
  return pNew;
}


char *lsmMallocStrdup(const char *zIn){
  int nByte;
  char *zRet;
  nByte = strlen(zIn);
  zRet = lsmMalloc(0, nByte+1);
  if( zRet ){
    memcpy(zRet, zIn, nByte+1);
  }
  return zRet;
}


/*
** Allocate a new Chunk structure (using lsmMalloc()).
*/
static Chunk * poolChunkNew(int nMin){
  Chunk *pChunk;
  int nAlloc = MAX(CHUNKSIZE, nMin + sizeof(Chunk));

  pChunk = (Chunk *)lsmMalloc(0, nAlloc);
  if( pChunk ){
    pChunk->pNext = 0;
    pChunk->iOff = 0;
    pChunk->aData = (u8 *)&pChunk[1];
    pChunk->nData = nAlloc - sizeof(Chunk);
  }

  return pChunk;
}

/*
** Allocate sz bytes from chunk pChunk.
*/
static u8 *poolChunkAlloc(Chunk *pChunk, int sz){
  u8 *pRet;                       /* Pointer value to return */
  assert( sz<=(pChunk->nData - pChunk->iOff) );
  pRet = &pChunk->aData[pChunk->iOff];
  pChunk->iOff += sz;
  return pRet;
}


int lsmPoolNew(Mempool **ppPool){
  int rc = LSM_NOMEM;
  Mempool *pPool = 0;
  Chunk *pChunk;

  pChunk = poolChunkNew(0);
  if( pChunk ){
    pPool = (Mempool *)poolChunkAlloc(pChunk, sizeof(Mempool));
    pPool->pFirst = pChunk;
    pPool->pLast = pChunk;
    pPool->nUsed = 0;
    rc = LSM_OK;
  }

  *ppPool = pPool;
  return rc;
}

void lsmPoolDestroy(Mempool *pPool){
  Chunk *pChunk = pPool->pFirst;
  while( pChunk ){
    Chunk *pFree = pChunk;
    pChunk = pChunk->pNext;
    lsmFree(0, pFree);
  }
}

void *lsmPoolMalloc(Mempool *pPool, int nByte){
  u8 *pRet = 0;
  Chunk *pLast = pPool->pLast;

  nByte = ROUND8(nByte);
  if( nByte > (pLast->nData - pLast->iOff) ){
    Chunk *pNew = poolChunkNew(nByte);
    if( pNew ){
      pLast->pNext = pNew;
      pPool->pLast = pNew;
      pLast = pNew;
    }
  }

  if( pLast ){
    pRet = poolChunkAlloc(pLast, nByte);
    pPool->nUsed += nByte;
  }
  return (void *)pRet;
}

void *lsmPoolMallocZero(Mempool *pPool, int nByte){
  void *pRet = lsmPoolMalloc(pPool, nByte);
  if( pRet ) memset(pRet, 0, nByte);
  return pRet;
}

/*
** Return the amount of memory currently allocated from this pool.
*/
int lsmPoolUsed(Mempool *pPool){
  return pPool->nUsed;
}

void lsmPoolMark(Mempool *pPool, void **ppChunk, int *piOff){
  *ppChunk = (void *)pPool->pLast;
  *piOff = pPool->pLast->iOff;
}

void lsmPoolRollback(Mempool *pPool, void *pChunk, int iOff){
  Chunk *pLast = (Chunk *)pChunk;
  Chunk *p;
  Chunk *pNext;

#ifdef LSM_EXPENSIVE_DEBUG
  /* Check that pLast is actually in the list of chunks */
  for(p=pPool->pFirst; p!=pLast; p=p->pNext);
#endif

  pPool->nUsed -= (pLast->iOff - iOff);
  for(p=pLast->pNext; p; p=pNext){
    pPool->nUsed -= p->iOff;
    pNext = p->pNext;
    lsmFree(0, p);
  }

  pLast->pNext = 0;
  pLast->iOff = iOff;
  pPool->pLast = pLast;
}
