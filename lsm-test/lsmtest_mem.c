
#include <stdio.h>
#include <assert.h>
#include <string.h>

#define ArraySize(x) ((int)(sizeof(x) / sizeof((x)[0])))

#define MIN(x,y) ((x)<(y) ? (x) : (y))

typedef unsigned int  u32;
typedef unsigned char u8;
typedef long long int i64;
typedef unsigned long long int u64;

#if defined(__GLIBC__) && defined(LSM_DEBUG_MEM)
  extern int backtrace(void**,int);
  extern void backtrace_symbols_fd(void*const*,int,int);
# define TM_BACKTRACE 12
#else
# define backtrace(A,B) 1
# define backtrace_symbols_fd(A,B,C)
#endif


typedef struct TmBlockHdr TmBlockHdr;
typedef struct TmAgg TmAgg;

static struct TmGlobal {
  TmBlockHdr *pFirst;
  void *(*xMalloc)(int);          /* underlying malloc(3) function */
  void *(*xRealloc)(void *, int); /* underlying realloc(3) function */
  void (*xFree)(void *);          /* underlying free(3) function */

  /* Mutex to protect pFirst and aHash */
  void (*xEnterMutex)(void *);    /* Call this to enter the mutex */
  void (*xLeaveMutex)(void *);    /* Call this to leave mutex */
  void (*xDelMutex)(void *);      /* Call this to delete mutex */
  void *pMutex;                   /* Mutex handle */

#ifdef TM_BACKTRACE
  TmAgg *aHash[10000];
#endif
} tmglobal;
static int tmglobal_isinit = 0;

struct TmBlockHdr {
  TmBlockHdr *pNext;
  TmBlockHdr *pPrev;
  int nByte;
#ifdef TM_BACKTRACE
  TmAgg *pAgg;
#endif
  u32 iForeGuard;
};

#ifdef TM_BACKTRACE
struct TmAgg {
  int nAlloc;                     /* Number of allocations at this path */
  int nByte;                      /* Total number of bytes allocated */
  int nOutAlloc;                  /* Number of outstanding allocations */
  int nOutByte;                   /* Number of outstanding bytes */
  void *aFrame[TM_BACKTRACE];     /* backtrace() output */
  TmAgg *pNext;                   /* Next object in hash-table collision */
};
#endif

#define FOREGUARD 0x80F5E153
#define REARGUARD 0xE4676B53
static const u32 rearguard = REARGUARD;

#define ROUND8(x) (((x)+7)&~7)

#define BLOCK_HDR_SIZE (ROUND8( sizeof(TmBlockHdr) ))

static void tmEnterMutex(void){
  tmglobal.xEnterMutex(tmglobal.pMutex);
}
static void tmLeaveMutex(void){
  tmglobal.xLeaveMutex(tmglobal.pMutex);
}


static void *tmMalloc(int nByte){
  TmBlockHdr *pNew;
  u8 *pUser;                      /* Return value */
  int nReq;

  assert( sizeof(rearguard)==4 );
  nReq = BLOCK_HDR_SIZE + nByte + 4;
  pNew = (TmBlockHdr *)tmglobal.xMalloc(nReq);
  memset(pNew, 0, sizeof(TmBlockHdr));

  tmEnterMutex();
  pNew->iForeGuard = FOREGUARD;
  pNew->nByte = nByte;
  pNew->pNext = tmglobal.pFirst;

  if( tmglobal.pFirst ){
    tmglobal.pFirst->pPrev = pNew;
  }
  tmglobal.pFirst = pNew;

  pUser = &((u8 *)pNew)[BLOCK_HDR_SIZE];
  memset(pUser, 0x56, nByte);
  memcpy(&pUser[nByte], &rearguard, 4);

#ifdef TM_BACKTRACE
  {
    TmAgg *pAgg;
    int i;
    u32 iHash = 0;
    void *aFrame[TM_BACKTRACE];
    memset(aFrame, 0, sizeof(aFrame));
    backtrace(aFrame, TM_BACKTRACE);

    for(i=0; i<ArraySize(aFrame); i++){
      iHash += (u64)(aFrame[i]) + (iHash<<3);
    }
    iHash = iHash % ArraySize(tmglobal.aHash);

    for(pAgg=tmglobal.aHash[iHash]; pAgg; pAgg=pAgg->pNext){
      if( memcmp(pAgg->aFrame, aFrame, sizeof(aFrame))==0 ) break;
    }
    if( !pAgg ){
      pAgg = (TmAgg *)tmglobal.xMalloc(sizeof(TmAgg));
      memset(pAgg, 0, sizeof(TmAgg));
      memcpy(pAgg->aFrame, aFrame, sizeof(aFrame));
      pAgg->pNext = tmglobal.aHash[iHash];
      tmglobal.aHash[iHash] = pAgg;
    }
    pAgg->nAlloc++;
    pAgg->nByte += nByte;
    pAgg->nOutAlloc++;
    pAgg->nOutByte += nByte;
    pNew->pAgg = pAgg;
  }
#endif

  tmLeaveMutex();
  return pUser;
}

static void tmFree(void *p){
  if( p ){
    TmBlockHdr *pHdr;
    u8 *pUser = (u8 *)p;

    tmEnterMutex();
    pHdr = (TmBlockHdr *)&pUser[BLOCK_HDR_SIZE * -1];
    assert( pHdr->iForeGuard==FOREGUARD );
    assert( 0==memcmp(&pUser[pHdr->nByte], &rearguard, 4) );

    if( pHdr->pPrev ){
      assert( pHdr->pPrev->pNext==pHdr );
      pHdr->pPrev->pNext = pHdr->pNext;
    }else{
      assert( pHdr==tmglobal.pFirst );
      tmglobal.pFirst = pHdr->pNext;
    }
    if( pHdr->pNext ){
      assert( pHdr->pNext->pPrev==pHdr );
      pHdr->pNext->pPrev = pHdr->pPrev;
    }

#ifdef TM_BACKTRACE
    pHdr->pAgg->nOutAlloc--;
    pHdr->pAgg->nOutByte -= pHdr->nByte;
#endif

    tmLeaveMutex();
    memset(pUser, 0x58, pHdr->nByte);
    memset(pHdr, 0x57, sizeof(TmBlockHdr));
    tmglobal.xFree(pHdr);
  }
}

static void *tmRealloc(void *p, int nByte){
  void *pNew;

  pNew = tmMalloc(nByte);
  if( p ){
    TmBlockHdr *pHdr;
    u8 *pUser = (u8 *)p;
    pHdr = (TmBlockHdr *)&pUser[BLOCK_HDR_SIZE * -1];
    memcpy(pNew, p, MIN(nByte, pHdr->nByte));
    tmFree(p);
  }
  return pNew;
}

void testMallocCheck(
  int *pnLeakAlloc,
  int *pnLeakByte,
  FILE *pFile
){

  TmBlockHdr *pHdr;
  int nLeak = 0;
  int nByte = 0;

  for(pHdr=tmglobal.pFirst; pHdr; pHdr=pHdr->pNext){
    nLeak++; 
    nByte += pHdr->nByte;
  }
  if( pnLeakAlloc ) *pnLeakAlloc = nLeak;
  if( pnLeakByte ) *pnLeakByte = nByte;

#ifdef TM_BACKTRACE
  if( pFile ){
    int i;
    fprintf(pFile, "LEAKS\n");
    for(i=0; i<ArraySize(tmglobal.aHash); i++){
      TmAgg *pAgg;
      for(pAgg=tmglobal.aHash[i]; pAgg; pAgg=pAgg->pNext){
        if( pAgg->nOutAlloc ){
          int j;
          fprintf(pFile, "%d %d ", pAgg->nOutByte, pAgg->nOutAlloc);
          for(j=0; j<TM_BACKTRACE; j++){
            fprintf(pFile, "%p ", pAgg->aFrame[j]);
          }
          fprintf(pFile, "\n");
        }
      }
    }
    fprintf(pFile, "\nALLOCATIONS\n");
    for(i=0; i<ArraySize(tmglobal.aHash); i++){
      TmAgg *pAgg;
      for(pAgg=tmglobal.aHash[i]; pAgg; pAgg=pAgg->pNext){
        int j;
        fprintf(pFile, "%d %d ", pAgg->nByte, pAgg->nAlloc);
        for(j=0; j<TM_BACKTRACE; j++) fprintf(pFile, "%p ", pAgg->aFrame[j]);
        fprintf(pFile, "\n");
      }
    }
  }
#else
  (void)pFile;
#endif
}


#include "lsm.h"

typedef struct LsmMutex LsmMutex;
struct LsmMutex {
  lsm_env *pEnv;
  lsm_mutex *pMutex;
};

static void tmLsmMutexEnter(void *pCtx){
  LsmMutex *p = (LsmMutex *)pCtx;
  p->pEnv->xMutexEnter(p->pMutex);
}
static void tmLsmMutexLeave(void *pCtx){
  LsmMutex *p = (LsmMutex *)pCtx;
  p->pEnv->xMutexLeave(p->pMutex);
}
static void tmLsmMutexDel(void *pCtx){
  LsmMutex *p = (LsmMutex *)pCtx;
  p->pEnv->xMutexDel(p->pMutex);
  tmglobal.xFree(pCtx);
}
static void *tmLsmMalloc(int n){ return malloc(n); }
static void tmLsmFree(void *ptr){ free(ptr); }
static void *tmLsmRealloc(void *ptr, int n){ return realloc(ptr, n); }

static void *tmLsmEnvMalloc(lsm_env *p, int n){ return tmMalloc(n); }
static void tmLsmEnvFree(lsm_env *p, void *ptr){ tmFree(ptr); }
static void *tmLsmEnvRealloc(lsm_env *p, void *ptr, int n){ 
  return tmRealloc(ptr, n);
}

void testMallocInstall(lsm_env *pEnv){
  pEnv->pMemCtx = 0;
  pEnv->xMalloc = tmLsmEnvMalloc;
  pEnv->xRealloc = tmLsmEnvRealloc;
  pEnv->xFree = tmLsmEnvFree;

  if( tmglobal_isinit==0 ){
    LsmMutex *p;

    memset(&tmglobal, 0, sizeof(tmglobal));
    tmglobal.xMalloc = tmLsmMalloc;
    tmglobal.xRealloc = tmLsmRealloc;
    tmglobal.xFree = tmLsmFree;
    p = (LsmMutex *)tmglobal.xMalloc(sizeof(LsmMutex));
    p->pEnv = pEnv;
    pEnv->xMutexStatic(pEnv, LSM_MUTEX_HEAP, &p->pMutex);
    tmglobal.xEnterMutex = tmLsmMutexEnter;
    tmglobal.xLeaveMutex = tmLsmMutexLeave;
    tmglobal.xDelMutex = tmLsmMutexDel;
    tmglobal.pMutex = (void *)p;
    tmglobal_isinit = 1;
  }
}

