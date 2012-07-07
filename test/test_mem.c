/*
** 2012 July 7
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*/
#include <stdio.h>
#include <assert.h>
#include <string.h>

#include "sqlite4.h"

#define ArraySize(x) ((int)(sizeof(x) / sizeof((x)[0])))

#define MIN(x,y) ((x)<(y) ? (x) : (y))

typedef unsigned int  u32;
typedef unsigned char u8;
typedef long long int i64;
typedef unsigned long long int u64;

#if defined(__GLIBC__)
  extern int backtrace(void**,int);
  extern void backtrace_symbols_fd(void*const*,int,int);
# define TM_BACKTRACE 12
#else
# define backtrace(A,B) 1
# define backtrace_symbols_fd(A,B,C)
#endif


typedef struct TmBlockHdr TmBlockHdr;
typedef struct TmAgg TmAgg;
typedef struct TmGlobal TmGlobal;

struct TmGlobal {
  /* Linked list of all currently outstanding allocations. And a table of
  ** all allocations, past and present, indexed by backtrace() info.  */
  TmBlockHdr *pFirst;
#ifdef TM_BACKTRACE
  TmAgg *aHash[10000];
#endif

  /* Underlying malloc/realloc/free functions */
  sqlite4_mem_methods mem;
};

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

static void tmEnterMutex(TmGlobal *pTm){
  /*pTm->xEnterMutex(pTm);*/
}
static void tmLeaveMutex(TmGlobal *pTm){
  /* pTm->xLeaveMutex(pTm); */
}

static void *tmMalloc(TmGlobal *pTm, int nByte){
  TmBlockHdr *pNew;               /* New allocation header block */
  u8 *pUser;                      /* Return value */
  int nReq;                       /* Total number of bytes requested */

  assert( sizeof(rearguard)==4 );
  nReq = BLOCK_HDR_SIZE + nByte + 4;
  pNew = (TmBlockHdr *)pTm->mem.xMalloc(pTm->mem.pMemEnv, nReq);
  memset(pNew, 0, sizeof(TmBlockHdr));

  tmEnterMutex(pTm);

  pNew->iForeGuard = FOREGUARD;
  pNew->nByte = nByte;
  pNew->pNext = pTm->pFirst;

  if( pTm->pFirst ){
    pTm->pFirst->pPrev = pNew;
  }
  pTm->pFirst = pNew;

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
    iHash = iHash % ArraySize(pTm->aHash);

    for(pAgg=pTm->aHash[iHash]; pAgg; pAgg=pAgg->pNext){
      if( memcmp(pAgg->aFrame, aFrame, sizeof(aFrame))==0 ) break;
    }
    if( !pAgg ){
      pAgg = (TmAgg *)pTm->mem.xMalloc(pTm->mem.pMemEnv, sizeof(TmAgg));
      memset(pAgg, 0, sizeof(TmAgg));
      memcpy(pAgg->aFrame, aFrame, sizeof(aFrame));
      pAgg->pNext = pTm->aHash[iHash];
      pTm->aHash[iHash] = pAgg;
    }
    pAgg->nAlloc++;
    pAgg->nByte += nByte;
    pAgg->nOutAlloc++;
    pAgg->nOutByte += nByte;
    pNew->pAgg = pAgg;
  }
#endif

  tmLeaveMutex(pTm);
  return pUser;
}

static void tmFree(TmGlobal *pTm, void *p){
  if( p ){
    TmBlockHdr *pHdr;
    u8 *pUser = (u8 *)p;

    tmEnterMutex(pTm);
    pHdr = (TmBlockHdr *)&pUser[BLOCK_HDR_SIZE * -1];
    assert( pHdr->iForeGuard==FOREGUARD );
    assert( 0==memcmp(&pUser[pHdr->nByte], &rearguard, 4) );

    if( pHdr->pPrev ){
      assert( pHdr->pPrev->pNext==pHdr );
      pHdr->pPrev->pNext = pHdr->pNext;
    }else{
      assert( pHdr==pTm->pFirst );
      pTm->pFirst = pHdr->pNext;
    }
    if( pHdr->pNext ){
      assert( pHdr->pNext->pPrev==pHdr );
      pHdr->pNext->pPrev = pHdr->pPrev;
    }

#ifdef TM_BACKTRACE
    pHdr->pAgg->nOutAlloc--;
    pHdr->pAgg->nOutByte -= pHdr->nByte;
#endif

    tmLeaveMutex(pTm);
    memset(pUser, 0x58, pHdr->nByte);
    memset(pHdr, 0x57, sizeof(TmBlockHdr));
    pTm->mem.xFree(pTm->mem.pMemEnv, pHdr);
  }
}

static void *tmRealloc(TmGlobal *pTm, void *p, int nByte){
  void *pNew;

  pNew = tmMalloc(pTm, nByte);
  if( pNew && p ){
    TmBlockHdr *pHdr;
    u8 *pUser = (u8 *)p;
    pHdr = (TmBlockHdr *)&pUser[BLOCK_HDR_SIZE * -1];
    memcpy(pNew, p, MIN(nByte, pHdr->nByte));
    tmFree(pTm, p);
  }
  return pNew;
}

static void tmMallocCheck(
  TmGlobal *pTm,
  int *pnLeakAlloc,
  int *pnLeakByte,
  FILE *pFile
){
  TmBlockHdr *pHdr;
  int nLeak = 0;
  int nByte = 0;

  if( pTm==0 ) return;

  for(pHdr=pTm->pFirst; pHdr; pHdr=pHdr->pNext){
    nLeak++; 
    nByte += pHdr->nByte;
  }
  if( pnLeakAlloc ) *pnLeakAlloc = nLeak;
  if( pnLeakByte ) *pnLeakByte = nByte;

#ifdef TM_BACKTRACE
  if( pFile ){
    int i;
    fprintf(pFile, "LEAKS\n");
    for(i=0; i<ArraySize(pTm->aHash); i++){
      TmAgg *pAgg;
      for(pAgg=pTm->aHash[i]; pAgg; pAgg=pAgg->pNext){
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
    for(i=0; i<ArraySize(pTm->aHash); i++){
      TmAgg *pAgg;
      for(pAgg=pTm->aHash[i]; pAgg; pAgg=pAgg->pNext){
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


static void *tmLsmEnvXMalloc(void *p, sqlite4_size_t n){
  return tmMalloc( (TmGlobal*) p, (int)n );
}

static void tmLsmEnvXFree(void *p, void *ptr){ 
  tmFree( (TmGlobal *)p, ptr );
}

static void *tmLsmEnvXRealloc(void *ptr, void * mem, int n){
  return tmRealloc((TmGlobal*)ptr, mem, n);
}


static sqlite4_size_t tmLsmXSize(void *p, void *ptr){
  if(NULL==ptr){
    return 0;
  }else{
    unsigned char * pUc = (unsigned char *)ptr;
    TmBlockHdr * pBlock = (TmBlockHdr*)(pUc-BLOCK_HDR_SIZE);
    assert( pBlock->nByte > 0 );
    return (sqlite4_size_t) pBlock->nByte;
  }
}

static int tmInitStub(void* ignored){
  assert("Set breakpoint here.");
  return 0;
}
static void tmVoidStub(void* ignored){}


int testMallocInstall(sqlite4_env *pEnv){
  TmGlobal *pGlobal;              /* Object containing allocation hash */
  sqlite4_mem_methods allocator;  /* This malloc system */
  sqlite4_mem_methods orig;       /* Underlying malloc system */

  /* Allocate and populate a TmGlobal structure. sqlite4_malloc cannot be
  ** used to allocate the TmGlobal struct as this would cause the environment
  ** to move to "initialized" state and the SQLITE4_ENVCONFIG_MALLOC 
  ** to fail. */
  sqlite4_env_config(pEnv, SQLITE4_ENVCONFIG_GETMALLOC, &orig);
  pGlobal = (TmGlobal *)orig.xMalloc(orig.pMemEnv, sizeof(TmGlobal));
  memset(pGlobal, 0, sizeof(TmGlobal));
  memcpy(&pGlobal->mem, &orig, sizeof(orig));

  /* Set up pEnv to the use the new TmGlobal */
  allocator.xRealloc = tmLsmEnvXRealloc;
  allocator.xMalloc = tmLsmEnvXMalloc;
  allocator.xFree = tmLsmEnvXFree;
  allocator.xSize = tmLsmXSize;
  allocator.xInit = tmInitStub;
  allocator.xShutdown = tmVoidStub;
  allocator.xBeginBenign = tmVoidStub;
  allocator.xEndBenign = tmVoidStub;
  allocator.pMemEnv = pGlobal;
  return sqlite4_env_config(pEnv, SQLITE4_ENVCONFIG_MALLOC, &allocator);
}

int testMallocUninstall(sqlite4_env *pEnv){
  TmGlobal *pGlobal;              /* Object containing allocation hash */
  sqlite4_mem_methods allocator;  /* This malloc system */
  int rc;

  sqlite4_env_config(pEnv, SQLITE4_ENVCONFIG_GETMALLOC, &allocator);
  assert( allocator.xMalloc==tmLsmEnvXMalloc );
  pGlobal = (TmGlobal *)allocator.pMemEnv;

  rc = sqlite4_env_config(pEnv, SQLITE4_ENVCONFIG_MALLOC, &pGlobal->mem);
  if( rc==SQLITE4_OK ){
    sqlite4_free(pEnv, pGlobal);
  }
  return rc;
}

void testMallocCheck(
  sqlite4_env *pEnv,
  int *pnLeakAlloc,
  int *pnLeakByte,
  FILE *pFile
){
  TmGlobal *pGlobal;
  sqlite4_mem_methods allocator;  /* This malloc system */

  sqlite4_env_config(pEnv, SQLITE4_ENVCONFIG_GETMALLOC, &allocator);
  assert( allocator.xMalloc==tmLsmEnvXMalloc );
  pGlobal = (TmGlobal *)allocator.pMemEnv;

  tmMallocCheck(pGlobal, pnLeakAlloc, pnLeakByte, pFile);
}

#include <tcl.h>

/*
** testmem install
** testmem uninstall
** testmem report ?FILENAME?
*/
static int testmem_cmd(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  sqlite4_env *pEnv;              /* SQLite 4 environment to work with */
  int iOpt;
  const char *azSub[] = {"install", "uninstall", "report", 0};

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "sub-command");
    return TCL_ERROR;
  }
  if( Tcl_GetIndexFromObj(interp, objv[1], azSub, "sub-command", 0, &iOpt) ){
    return TCL_ERROR;
  }

  pEnv = sqlite4_env_default();
  switch( iOpt ){
    case 0: {
      int rc;
      if( objc!=2 ){
        Tcl_WrongNumArgs(interp, 2, objv, "");
        return TCL_ERROR;
      }
      rc = testMallocInstall(pEnv);
      if( rc!=SQLITE4_OK ){
        Tcl_AppendResult(interp, "Failed to install testmem wrapper", 0);
        return TCL_ERROR;
      }
      break;
    }

    case 1:
      if( objc!=2 ){
        Tcl_WrongNumArgs(interp, 2, objv, "");
        return TCL_ERROR;
      }
      testMallocUninstall(pEnv);
      break;

    case 2: {
      int nLeakAlloc = 0;
      int nLeakByte = 0;
      FILE *pReport = 0;
      Tcl_Obj *pRes;

      if( objc!=2 && objc!=3 ){
        Tcl_WrongNumArgs(interp, 2, objv, "?filename?");
        return TCL_ERROR;
      }
      if( objc==3 ){
        const char *zFile = Tcl_GetString(objv[2]);
        pReport = fopen(zFile, "w");
        if( !pReport ){
          Tcl_AppendResult(interp, "Failed to open file: ", zFile, 0);
          return TCL_ERROR;
        }
      }

      testMallocCheck(pEnv, &nLeakAlloc, &nLeakByte, pReport);
      if( pReport ) fclose(pReport);

      pRes = Tcl_NewObj();
      Tcl_ListObjAppendElement(interp, pRes, Tcl_NewIntObj(nLeakAlloc));
      Tcl_ListObjAppendElement(interp, pRes, Tcl_NewIntObj(nLeakByte));
      Tcl_SetObjResult(interp, pRes);
      break;
    }
  }

  return TCL_OK;
}

int Sqlitetest_mem_Init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp, "testmem", testmem_cmd, 0, 0);
  return TCL_OK;
}


