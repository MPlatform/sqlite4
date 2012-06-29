/*
** 2007 August 15
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
** This file contains low-level memory allocation drivers for when
** SQLite will use the standard C-library malloc/realloc/free interface
** to obtain the memory it needs while adding lots of additional debugging
** information to each allocation in order to help detect and fix memory
** leaks and memory usage errors.
**
** This file contains implementations of the low-level memory allocation
** routines specified in the sqlite4_mem_methods object.
*/
#include "sqliteInt.h"

/*
** This version of the memory allocator is used only if the
** SQLITE4_MEMDEBUG macro is defined
*/
#ifdef SQLITE4_MEMDEBUG

/*
** The backtrace functionality is only available with GLIBC
*/
#ifdef __GLIBC__
  extern int backtrace(void**,int);
  extern void backtrace_symbols_fd(void*const*,int,int);
#else
# define backtrace(A,B) 1
# define backtrace_symbols_fd(A,B,C)
#endif
#include <stdio.h>

/*
** Each memory allocation looks like this:
**
**  ------------------------------------------------------------------------
**  | Title |  backtrace pointers |  MemBlockHdr |  allocation |  EndGuard |
**  ------------------------------------------------------------------------
**
** The application code sees only a pointer to the allocation.  We have
** to back up from the allocation pointer to find the MemBlockHdr.  The
** MemBlockHdr tells us the size of the allocation and the number of
** backtrace pointers.  There is also a guard word at the end of the
** MemBlockHdr.
*/
struct MemBlockHdr {
  i64 iSize;                          /* Size of this allocation */
  struct MemBlockHdr *pNext, *pPrev;  /* Linked list of all unfreed memory */
  char nBacktrace;                    /* Number of backtraces on this alloc */
  char nBacktraceSlots;               /* Available backtrace slots */
  u8 nTitle;                          /* Bytes of title; includes '\0' */
  u8 eType;                           /* Allocation type code */
  int iForeGuard;                     /* Guard word for sanity */
};

/*
** Guard words
*/
#define FOREGUARD 0x80F5E153
#define REARGUARD 0xE4676B53

/*
** Number of malloc size increments to track.
*/
#define NCSIZE  1000

/*
** All of the static variables used by this module are collected
** into a single structure named "mem".  This is to keep the
** static variables organized and to reduce namespace pollution
** when this module is combined with other in the amalgamation.
*/
static struct {
  
  /*
  ** Mutex to control access to the memory allocation subsystem.
  */
  sqlite4_mutex *mutex;

  /*
  ** Head and tail of a linked list of all outstanding allocations
  */
  struct MemBlockHdr *pFirst;
  struct MemBlockHdr *pLast;
  
  /*
  ** The number of levels of backtrace to save in new allocations.
  */
  int nBacktrace;
  void (*xBacktrace)(int, int, void **);

  /*
  ** Title text to insert in front of each block
  */
  int nTitle;        /* Bytes of zTitle to save.  Includes '\0' and padding */
  char zTitle[100];  /* The title text */

  /* 
  ** sqlite4MallocDisallow() increments the following counter.
  ** sqlite4MallocAllow() decrements it.
  */
  int disallow; /* Do not allow memory allocation */

  /*
  ** Gather statistics on the sizes of memory allocations.
  ** nAlloc[i] is the number of allocation attempts of i*8
  ** bytes.  i==NCSIZE is the number of allocation attempts for
  ** sizes more than NCSIZE*8 bytes.
  */
  int nAlloc[NCSIZE];      /* Total number of allocations */
  int nCurrent[NCSIZE];    /* Current number of allocations */
  int mxCurrent[NCSIZE];   /* Highwater mark for nCurrent */

} mem2;


/*
** Adjust memory usage statistics
*/
static void adjustStats(int iSize, int increment){
  int i = ROUND8(iSize)/8;
  if( i>NCSIZE-1 ){
    i = NCSIZE - 1;
  }
  if( increment>0 ){
    mem2.nAlloc[i]++;
    mem2.nCurrent[i]++;
    if( mem2.nCurrent[i]>mem2.mxCurrent[i] ){
      mem2.mxCurrent[i] = mem2.nCurrent[i];
    }
  }else{
    mem2.nCurrent[i]--;
    assert( mem2.nCurrent[i]>=0 );
  }
}

/*
** Given an allocation, find the MemBlockHdr for that allocation.
**
** This routine checks the guards at either end of the allocation and
** if they are incorrect it asserts.
*/
static struct MemBlockHdr *sqlite4MemsysGetHeader(void *pAllocation){
  struct MemBlockHdr *p;
  int *pInt;
  u8 *pU8;
  int nReserve;

  p = (struct MemBlockHdr*)pAllocation;
  p--;
  assert( p->iForeGuard==(int)FOREGUARD );
  nReserve = ROUND8(p->iSize);
  pInt = (int*)pAllocation;
  pU8 = (u8*)pAllocation;
  assert( pInt[nReserve/sizeof(int)]==(int)REARGUARD );
  /* This checks any of the "extra" bytes allocated due
  ** to rounding up to an 8 byte boundary to ensure 
  ** they haven't been overwritten.
  */
  while( nReserve-- > p->iSize ) assert( pU8[nReserve]==0x65 );
  return p;
}

/*
** Return the number of bytes currently allocated at address p.
*/
static int sqlite4MemSize(void *pMem, void *p){
  struct MemBlockHdr *pHdr;
  assert( pMem==(void*)&mem2 );
  if( !p ){
    return 0;
  }
  pHdr = sqlite4MemsysGetHeader(p);
  return pHdr->iSize;
}

/*
** Initialize the memory allocation subsystem.
*/
static int sqlite4MemInit(void *pMallocEnv){
  sqlite4_env *pEnv = (sqlite4_env*)pMallocEnv;
  int rc = SQLITE4_OK;
  assert( (sizeof(struct MemBlockHdr)&7) == 0 );
  if( !pEnv->bMemstat ){
    mem2.mutex = sqlite4MutexAlloc(pEnv, SQLITE4_MUTEX_FAST);
    if( mem2.mutex==0 && pEnv->bCoreMutex ) rc = SQLITE4_NOMEM;
  }
  return rc;
}

/*
** Deinitialize the memory allocation subsystem.
*/
static void sqlite4MemShutdown(void *NotUsed){
  UNUSED_PARAMETER(NotUsed);
  sqlite4_mutex_free(mem2.mutex);
  mem2.mutex = 0;
}

/*
** Fill a buffer with pseudo-random bytes.  This is used to preset
** the content of a new memory allocation to unpredictable values and
** to clear the content of a freed allocation to unpredictable values.
*/
static void randomFill(char *pBuf, int nByte){
  unsigned int x, y, r;
  x = SQLITE4_PTR_TO_INT(pBuf);
  y = nByte | 1;
  while( nByte >= 4 ){
    x = (x>>1) ^ (-(x&1) & 0xd0000001);
    y = y*1103515245 + 12345;
    r = x ^ y;
    *(int*)pBuf = r;
    pBuf += 4;
    nByte -= 4;
  }
  while( nByte-- > 0 ){
    x = (x>>1) ^ (-(x&1) & 0xd0000001);
    y = y*1103515245 + 12345;
    r = x ^ y;
    *(pBuf++) = r & 0xff;
  }
}

/*
** Allocate nByte bytes of memory.
*/
static void *sqlite4MemMalloc(void *pMem, sqlite4_size_t nByte){
  struct MemBlockHdr *pHdr;
  void **pBt;
  char *z;
  int *pInt;
  void *p = 0;
  int totalSize;
  int nReserve;
  sqlite4_mutex_enter(mem2.mutex);
  assert( mem2.disallow==0 );
  nReserve = ROUND8(nByte);
  totalSize = nReserve + sizeof(*pHdr) + sizeof(int) +
               mem2.nBacktrace*sizeof(void*) + mem2.nTitle;
  assert( pMem==(void*)&mem2 );
  p = malloc(totalSize);
  if( p ){
    z = p;
    pBt = (void**)&z[mem2.nTitle];
    pHdr = (struct MemBlockHdr*)&pBt[mem2.nBacktrace];
    pHdr->pNext = 0;
    pHdr->pPrev = mem2.pLast;
    if( mem2.pLast ){
      mem2.pLast->pNext = pHdr;
    }else{
      mem2.pFirst = pHdr;
    }
    mem2.pLast = pHdr;
    pHdr->iForeGuard = FOREGUARD;
    pHdr->eType = MEMTYPE_HEAP;
    pHdr->nBacktraceSlots = mem2.nBacktrace;
    pHdr->nTitle = mem2.nTitle;
    if( mem2.nBacktrace ){
      void *aAddr[40];
      pHdr->nBacktrace = backtrace(aAddr, mem2.nBacktrace+1)-1;
      memcpy(pBt, &aAddr[1], pHdr->nBacktrace*sizeof(void*));
      assert(pBt[0]);
      if( mem2.xBacktrace ){
        mem2.xBacktrace(nByte, pHdr->nBacktrace-1, &aAddr[1]);
      }
    }else{
      pHdr->nBacktrace = 0;
    }
    if( mem2.nTitle ){
      memcpy(z, mem2.zTitle, mem2.nTitle);
    }
    pHdr->iSize = nByte;
    adjustStats(nByte, +1);
    pInt = (int*)&pHdr[1];
    pInt[nReserve/sizeof(int)] = REARGUARD;
    randomFill((char*)pInt, nByte);
    memset(((char*)pInt)+nByte, 0x65, nReserve-nByte);
    p = (void*)pInt;
  }
  sqlite4_mutex_leave(mem2.mutex);
  return p; 
}

/*
** Free memory.
*/
static void sqlite4MemFree(void *pMem, void *pPrior){
  struct MemBlockHdr *pHdr;
  void **pBt;
  char *z;
  assert( pMem==(void*)&mem2 );
  assert( sqlite4DefaultEnv.bMemstat || sqlite4DefaultEnv.bCoreMutex==0 
       || mem2.mutex!=0 );
  pHdr = sqlite4MemsysGetHeader(pPrior);
  pBt = (void**)pHdr;
  pBt -= pHdr->nBacktraceSlots;
  sqlite4_mutex_enter(mem2.mutex);
  if( pHdr->pPrev ){
    assert( pHdr->pPrev->pNext==pHdr );
    pHdr->pPrev->pNext = pHdr->pNext;
  }else{
    assert( mem2.pFirst==pHdr );
    mem2.pFirst = pHdr->pNext;
  }
  if( pHdr->pNext ){
    assert( pHdr->pNext->pPrev==pHdr );
    pHdr->pNext->pPrev = pHdr->pPrev;
  }else{
    assert( mem2.pLast==pHdr );
    mem2.pLast = pHdr->pPrev;
  }
  z = (char*)pBt;
  z -= pHdr->nTitle;
  adjustStats(pHdr->iSize, -1);
  randomFill(z, sizeof(void*)*pHdr->nBacktraceSlots + sizeof(*pHdr) +
                pHdr->iSize + sizeof(int) + pHdr->nTitle);
  free(z);
  sqlite4_mutex_leave(mem2.mutex);  
}

/*
** Change the size of an existing memory allocation.
**
** For this debugging implementation, we *always* make a copy of the
** allocation into a new place in memory.  In this way, if the 
** higher level code is using pointer to the old allocation, it is 
** much more likely to break and we are much more liking to find
** the error.
*/
static void *sqlite4MemRealloc(void *p, void *pPrior, sqlite4_size_t nByte){
  struct MemBlockHdr *pOldHdr;
  void *pNew;
  assert( p==(void*)&mem2 );
  assert( mem2.disallow==0 );
  assert( (nByte & 7)==0 );     /* EV: R-46199-30249 */
  pOldHdr = sqlite4MemsysGetHeader(pPrior);
  pNew = sqlite4MemMalloc(p, nByte);
  if( pNew ){
    memcpy(pNew, pPrior, nByte<pOldHdr->iSize ? nByte : pOldHdr->iSize);
    if( nByte>pOldHdr->iSize ){
      randomFill(&((char*)pNew)[pOldHdr->iSize], nByte - pOldHdr->iSize);
    }
    sqlite4MemFree(p, pPrior);
  }
  return pNew;
}

/*
** Populate the low-level memory allocation function pointers in
** sqlite4DefaultEnv.m with pointers to the routines in this file.
*/
void sqlite4MemSetDefault(sqlite4_env *pEnv){
  static const sqlite4_mem_methods defaultMethods = {
     sqlite4MemMalloc,
     sqlite4MemFree,
     sqlite4MemRealloc,
     sqlite4MemSize,
     sqlite4MemInit,
     sqlite4MemShutdown,
     0,
     0,
     &mem2
  };
  pEnv->m = defaultMethods;
}

/*
** Set the "type" of an allocation.
*/
void sqlite4MemdebugSetType(void *p, u8 eType){
  if( p && sqlite4DefaultEnv.m.xMalloc==sqlite4MemMalloc ){
    struct MemBlockHdr *pHdr;
    pHdr = sqlite4MemsysGetHeader(p);
    assert( pHdr->iForeGuard==FOREGUARD );
    pHdr->eType = eType;
  }
}

/*
** Return TRUE if the mask of type in eType matches the type of the
** allocation p.  Also return true if p==NULL.
**
** This routine is designed for use within an assert() statement, to
** verify the type of an allocation.  For example:
**
**     assert( sqlite4MemdebugHasType(p, MEMTYPE_DB) );
*/
int sqlite4MemdebugHasType(const void *p, u8 eType){
  int rc = 1;
  if( p && sqlite4DefaultEnv.m.xMalloc==sqlite4MemMalloc ){
    struct MemBlockHdr *pHdr;
    pHdr = sqlite4MemsysGetHeader((void*)p);
    assert( pHdr->iForeGuard==FOREGUARD );         /* Allocation is valid */
    if( (pHdr->eType&eType)==0 ){
      rc = 0;
    }
  }
  return rc;
}

/*
** Return TRUE if the mask of type in eType matches no bits of the type of the
** allocation p.  Also return true if p==NULL.
**
** This routine is designed for use within an assert() statement, to
** verify the type of an allocation.  For example:
**
**     assert( sqlite4MemdebugNoType(p, MEMTYPE_DB) );
*/
int sqlite4MemdebugNoType(const void *p, u8 eType){
  int rc = 1;
  if( p && sqlite4DefaultEnv.m.xMalloc==sqlite4MemMalloc ){
    struct MemBlockHdr *pHdr;
    pHdr = sqlite4MemsysGetHeader((void*)p);
    assert( pHdr->iForeGuard==FOREGUARD );         /* Allocation is valid */
    if( (pHdr->eType&eType)!=0 ){
      rc = 0;
    }
  }
  return rc;
}

/*
** Set the number of backtrace levels kept for each allocation.
** A value of zero turns off backtracing.  The number is always rounded
** up to a multiple of 2.
*/
void sqlite4MemdebugBacktrace(int depth){
  if( depth<0 ){ depth = 0; }
  if( depth>20 ){ depth = 20; }
  depth = (depth+1)&0xfe;
  mem2.nBacktrace = depth;
}

void sqlite4MemdebugBacktraceCallback(void (*xBacktrace)(int, int, void **)){
  mem2.xBacktrace = xBacktrace;
}

/*
** Set the title string for subsequent allocations.
*/
void sqlite4MemdebugSettitle(const char *zTitle){
  unsigned int n = sqlite4Strlen30(zTitle) + 1;
  sqlite4_mutex_enter(mem2.mutex);
  if( n>=sizeof(mem2.zTitle) ) n = sizeof(mem2.zTitle)-1;
  memcpy(mem2.zTitle, zTitle, n);
  mem2.zTitle[n] = 0;
  mem2.nTitle = ROUND8(n);
  sqlite4_mutex_leave(mem2.mutex);
}

void sqlite4MemdebugSync(){
  struct MemBlockHdr *pHdr;
  for(pHdr=mem2.pFirst; pHdr; pHdr=pHdr->pNext){
    void **pBt = (void**)pHdr;
    pBt -= pHdr->nBacktraceSlots;
    mem2.xBacktrace(pHdr->iSize, pHdr->nBacktrace-1, &pBt[1]);
  }
}

/*
** Open the file indicated and write a log of all unfreed memory 
** allocations into that log.
*/
void sqlite4MemdebugDump(const char *zFilename){
  FILE *out;
  struct MemBlockHdr *pHdr;
  void **pBt;
  int i;
  out = fopen(zFilename, "w");
  if( out==0 ){
    fprintf(stderr, "** Unable to output memory debug output log: %s **\n",
                    zFilename);
    return;
  }
  for(pHdr=mem2.pFirst; pHdr; pHdr=pHdr->pNext){
    char *z = (char*)pHdr;
    z -= pHdr->nBacktraceSlots*sizeof(void*) + pHdr->nTitle;
    fprintf(out, "**** %lld bytes at %p from %s ****\n", 
            pHdr->iSize, &pHdr[1], pHdr->nTitle ? z : "???");
    if( pHdr->nBacktrace ){
      fflush(out);
      pBt = (void**)pHdr;
      pBt -= pHdr->nBacktraceSlots;
      backtrace_symbols_fd(pBt, pHdr->nBacktrace, fileno(out));
      fprintf(out, "\n");
    }
  }
  fprintf(out, "COUNTS:\n");
  for(i=0; i<NCSIZE-1; i++){
    if( mem2.nAlloc[i] ){
      fprintf(out, "   %5d: %10d %10d %10d\n", 
            i*8, mem2.nAlloc[i], mem2.nCurrent[i], mem2.mxCurrent[i]);
    }
  }
  if( mem2.nAlloc[NCSIZE-1] ){
    fprintf(out, "   %5d: %10d %10d %10d\n",
             NCSIZE*8-8, mem2.nAlloc[NCSIZE-1],
             mem2.nCurrent[NCSIZE-1], mem2.mxCurrent[NCSIZE-1]);
  }
  fclose(out);
}

/*
** Return the number of times sqlite4MemMalloc() has been called.
*/
int sqlite4MemdebugMallocCount(){
  int i;
  int nTotal = 0;
  for(i=0; i<NCSIZE; i++){
    nTotal += mem2.nAlloc[i];
  }
  return nTotal;
}


#endif /* SQLITE4_MEMDEBUG */
