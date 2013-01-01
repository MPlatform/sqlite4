/*
** 2013-01-01
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
** This file contains the implementation of the "sqlite4_mm" memory
** allocator object.
*/
#include "sqlite4.h"
#include "mem.h"
#include <stdlib.h>

/*************************************************************************
** The SQLITE4_MM_SYSTEM memory allocator.  This allocator uses the
** malloc/realloc/free from the system library.  It also tries to use
** the memory allocation sizer from the system library if such a routine
** exists.  If there is no msize in the system library, then each allocation
** is increased in size by 8 bytes and the size of the allocation is stored
** in those initial 8 bytes.
**
** C-preprocessor macro summary:
**
**    HAVE_MALLOC_USABLE_SIZE     The configure script sets this symbol if
**                                the malloc_usable_size() interface exists
**                                on the target platform.  Or, this symbol
**                                can be set manually, if desired.
**                                If an equivalent interface exists by
**                                a different name, using a separate -D
**                                option to rename it.  This symbol will
**                                be enabled automatically on windows
**                                systems, and malloc_usable_size() will
**                                be redefined to _msize(), unless the
**                                SQLITE4_WITHOUT_MSIZE macro is defined.
**    
**    SQLITE4_WITHOUT_ZONEMALLOC   Some older macs lack support for the zone
**                                memory allocator.  Set this symbol to enable
**                                building on older macs.
**
**    SQLITE4_WITHOUT_MSIZE        Set this symbol to disable the use of
**                                _msize() on windows systems.  This might
**                                be necessary when compiling for Delphi,
**                                for example.
*/

/*
** Windows systems have malloc_usable_size() but it is called _msize().
** The use of _msize() is automatic, but can be disabled by compiling
** with -DSQLITE4_WITHOUT_MSIZE
*/
#if !defined(HAVE_MALLOC_USABLE_SIZE) && SQLITE4_OS_WIN \
      && !defined(SQLITE4_WITHOUT_MSIZE)
# define HAVE_MALLOC_USABLE_SIZE 1
# define SQLITE4_MALLOCSIZE _msize
#endif

#if defined(__APPLE__) && !defined(SQLITE4_WITHOUT_ZONEMALLOC)

/*
** Use the zone allocator available on apple products unless the
** SQLITE4_WITHOUT_ZONEMALLOC symbol is defined.
*/
#include <sys/sysctl.h>
#include <malloc/malloc.h>
#include <libkern/OSAtomic.h>
static malloc_zone_t* _sqliteZone_;
#define SQLITE4_MALLOC(x) malloc_zone_malloc(_sqliteZone_, (x))
#define SQLITE4_FREE(x) malloc_zone_free(_sqliteZone_, (x));
#define SQLITE4_REALLOC(x,y) malloc_zone_realloc(_sqliteZone_, (x), (y))
#define SQLITE4_MALLOCSIZE(x) \
        (_sqliteZone_ ? _sqliteZone_->size(_sqliteZone_,x) : malloc_size(x))

#else /* if not __APPLE__ */

/*
** Use standard C library malloc and free on non-Apple systems.  
** Also used by Apple systems if SQLITE4_WITHOUT_ZONEMALLOC is defined.
*/
#define SQLITE4_MALLOC(x)    malloc(x)
#define SQLITE4_FREE(x)      free(x)
#define SQLITE4_REALLOC(x,y) realloc((x),(y))

#ifdef HAVE_MALLOC_USABLE_SIZE
# ifndef SQLITE4_MALLOCSIZE
#  include <malloc.h>
#  define SQLITE4_MALLOCSIZE(x) malloc_usable_size(x)
# endif
#else
# undef SQLITE4_MALLOCSIZE
#endif

#endif /* __APPLE__ or not __APPLE__ */

/*
** Implementations of core routines
*/
static void *mmSysMalloc(sqlite4_mm *pMM, sqlite4_int64 iSize){
#ifdef SQLITE4_MALLOCSIZE
  return SQLITE4_MALLOC(iSize);
#else
  unsigned char *pRes = SQLITE4_MALLOC(iSize+8);
  if( pRes ){
    *(sqlite4_int64*)pRes = iSize;
    pRes += 8;
  }
  return pRes;
#endif
}
static void *mmSysRealloc(sqlite4_mm *pMM, void *pOld, sqlite4_int64 iSz){
#ifdef SQLITE4_MALLOCSIZE
  return SQLITE4_REALLOC(pOld, iSz);
#else
  unsigned char *pRes;
  if( pOld==0 ) return mmSysMalloc(pMM, iSz);
  pRes = (unsigned char*)pOld;
  pRes -= 8;
  pRes = SQLITE4_REALLOC(pRes, iSz+8);
  if( pRes ){
    *(sqlite4_int64*)pRes = iSz;
    pRes += 8;
  }
  return pRes;
#endif 
}
static void mmSysFree(sqlite4_mm *pNotUsed, void *pOld){
#ifdef SQLITE4_MALLOCSIZE
  SQLITE4_FREE(pOld);
#else
  unsigned char *pRes;
  if( pOld==0 ) return;
  pRes = (unsigned char *)pOld;
  pRes -= 8;
  SQLITE4_FREE(pRes);
#endif
}
static sqlite4_int64 mmSysMsize(sqlite4_mm *pNotUsed, void *pOld){
#ifdef SQLITE4_MALLOCSIZE
  return SQLITE4_MALLOCSIZE(pOld);
#else
  unsigned char *pX;
  if( pOld==0 ) return 0;
  pX = (unsigned char *)pOld;
  pX -= 8;
  return *(sqlite4_int64*)pX;
#endif
}

static const sqlite4_mm_methods mmSysMethods = {
  /* iVersion */    1,
  /* xMalloc  */    mmSysMalloc,
  /* xRealloc */    mmSysRealloc,
  /* xFree    */    mmSysFree,
  /* xMsize   */    mmSysMsize,
  /* xMember  */    0,
  /* xBenign  */    0,
  /* xFinal   */    0
};
static sqlite4_mm mmSystem =  {
  /* pMethods */    &mmSysMethods
};

/*************************************************************************
** The SQLITE4_MM_OVERFLOW memory allocator.
**
** This memory allocator has two child memory allocators, A and B.  Always
** try to fulfill the request using A first, then overflow to B if the request
** on A fails.  The A allocator must support the xMember method.
*/
struct mmOvfl {
  sqlite4_mm base;    /* Base class - must be first */
  int (*xMemberOfA)(sqlite4_mm*, const void*);
  sqlite4_mm *pA;     /* Primary memory allocator */
  sqlite4_mm *pB;     /* Backup memory allocator in case pA fails */
};

static void *mmOvflMalloc(sqlite4_mm *pMM, sqlite4_int64 iSz){
  const struct mmOvfl *pOvfl = (const struct mmOvfl*)pMM;
  void *pRes;
  pRes = pOvfl->pA->pMethods->xMalloc(pOvfl->pA, iSz);
  if( pRes==0 ){
    pRes = pOvfl->pB->pMethods->xMalloc(pOvfl->pB, iSz);
  }
  return pRes;
}
static void *mmOvflRealloc(sqlite4_mm *pMM, void *pOld, sqlite4_int64 iSz){
  const struct mmOvfl *pOvfl;
  void *pRes;
  if( pOld==0 ) return mmOvflMalloc(pMM, iSz);
  pOvfl = (const struct mmOvfl*)pMM;
  if( pOvfl->xMemberOfA(pOvfl->pA, pOld) ){
    pRes = pOvfl->pA->pMethods->xRealloc(pOvfl->pA, pOld, iSz);
  }else{
    pRes = pOvfl->pB->pMethods->xRealloc(pOvfl->pB, pOld, iSz);
  }
  return pRes;
}
static void mmOvflFree(sqlite4_mm *pMM, void *pOld){
  const struct mmOvfl *pOvfl;
  if( pOld==0 ) return;
  pOvfl = (const struct mmOvfl*)pMM;
  if( pOvfl->xMemberOfA(pOvfl->pA, pOld) ){
    pOvfl->pA->pMethods->xFree(pOvfl->pA, pOld);
  }else{
    pOvfl->pB->pMethods->xFree(pOvfl->pB, pOld);
  }
}
static sqlite4_int64 mmOvflMsize(sqlite4_mm *pMM, void *pOld){
  const struct mmOvfl *pOvfl;
  sqlite4_int64 iSz;
  if( pOld==0 ) return 0;
  pOvfl = (const struct mmOvfl*)pMM;
  if( pOvfl->xMemberOfA(pOvfl->pA, pOld) ){
    iSz = sqlite4_mm_msize(pOvfl->pA, pOld);
  }else{
    iSz = sqlite4_mm_msize(pOvfl->pB, pOld);
  }
  return iSz;
}
static int mmOvflMember(sqlite4_mm *pMM, const void *pOld){
  const struct mmOvfl *pOvfl;
  int iRes;
  if( pOld==0 ) return 0;
  pOvfl = (const struct mmOvfl*)pMM;
  if( pOvfl->xMemberOfA(pOvfl->pA, pOld) ){
    iRes = 1;
  }else{
    iRes = sqlite4_mm_member(pOvfl->pB, pOld);
  }
  return iRes;
}
static void mmOvflBenign(sqlite4_mm *pMM, int bEnable){
  struct mmOvfl *pOvfl = (struct mmOvfl*)pMM;
  sqlite4_mm_benign_failures(pOvfl->pA, bEnable);
  sqlite4_mm_benign_failures(pOvfl->pB, bEnable);
}
static void mmOvflFinal(sqlite4_mm *pMM){
  struct mmOvfl *pOvfl = (struct mmOvfl*)pMM;
  sqlite4_mm *pA = pOvfl->pA;
  sqlite4_mm *pB = pOvfl->pB;
  mmOvflFree(pMM, pMM);
  sqlite4_mm_destroy(pA);
  sqlite4_mm_destroy(pB);
}
static const sqlite4_mm_methods mmOvflMethods = {
  /* iVersion */    1,
  /* xMalloc  */    mmOvflMalloc,
  /* xRealloc */    mmOvflRealloc,
  /* xFree    */    mmOvflFree,
  /* xMsize   */    mmOvflMsize,
  /* xMember  */    mmOvflMember,
  /* xBenign  */    mmOvflBenign,
  /* xFinal   */    mmOvflFinal
};
static sqlite4_mm *mmOvflNew(sqlite4_mm *pA, sqlite4_mm *pB){
  struct mmOvfl *pOvfl;
  if( pA->pMethods->xMember==0 ) return 0;
  pOvfl = sqlite4_mm_malloc(pA, sizeof(*pOvfl));
  if( pOvfl==0 ){
    pOvfl = sqlite4_mm_malloc(pB, sizeof(*pOvfl));
  }
  if( pOvfl ){
    pOvfl->base.pMethods = &mmOvflMethods;
    pOvfl->xMemberOfA = pA->pMethods->xMember;
    pOvfl->pA = pA;
    pOvfl->pB = pB;
  }
  return &pOvfl->base;
}


/*************************************************************************
** The SQLITE4_MM_ONESIZE memory allocator.
**
** All memory allocations are rounded up to a single size, "sz".  A request
** for an allocation larger than sz bytes fails.  All allocations come out
** of a single initial buffer with "cnt" chunks of "sz" bytes each.
**
** Space to hold the sqlite4_mm object comes from the first block in the
** allocation space.
*/
struct mmOnesz {
  sqlite4_mm base;            /* Base class.  Must be first. */
  const void *pSpace;         /* Space to allocate */
  const void *pLast;          /* Last possible allocation */
  struct mmOneszBlock *pFree; /* List of free blocks */
  int sz;                     /* Size of each allocation */
};

/* A free block in the buffer */
struct mmOneszBlock {
  struct mmOneszBlock *pNext;  /* Next on the freelist */
};

static void *mmOneszMalloc(sqlite4_mm *pMM, sqlite4_int64 iSz){
  struct mmOnesz *pOnesz = (struct mmOnesz*)pMM;
  void *pRes;
  if( iSz>pOnesz->sz ) return 0;
  if( pOnesz->pFree==0 ) return 0;
  pRes = pOnesz->pFree;
  pOnesz->pFree = pOnesz->pFree->pNext;
  return pRes;
}
static void mmOneszFree(sqlite4_mm *pMM, void *pOld){
  struct mmOnesz *pOnesz = (struct mmOnesz*)pMM;
  if( pOld ){
    struct mmOneszBlock *pBlock = (struct mmOneszBlock*)pOld;
    pBlock->pNext = pOnesz->pFree;
    pOnesz->pFree = pBlock;
  }
}
static void *mmOneszRealloc(sqlite4_mm *pMM, void *pOld, sqlite4_int64 iSz){
  struct mmOnesz *pOnesz = (struct mmOnesz*)pMM;
  if( pOld==0 ) return mmOneszMalloc(pMM, iSz);
  if( iSz<=0 ){
    mmOneszFree(pMM, pOld);
    return 0;
  }
  if( iSz>pOnesz->sz ) return 0;
  return pOld;
}
static sqlite4_int64 mmOneszMsize(sqlite4_mm *pMM, void *pOld){
  struct mmOnesz *pOnesz = (struct mmOnesz*)pMM;
  return pOld ? pOnesz->sz : 0;  
}
static int mmOneszMember(sqlite4_mm *pMM, const void *pOld){
  struct mmOnesz *pOnesz = (struct mmOnesz*)pMM;
  return pOld && pOld>=pOnesz->pSpace && pOld<=pOnesz->pLast;
}
static const sqlite4_mm_methods mmOneszMethods = {
  /* iVersion */    1,
  /* xMalloc  */    mmOneszMalloc,
  /* xRealloc */    mmOneszRealloc,
  /* xFree    */    mmOneszFree,
  /* xMsize   */    mmOneszMsize,
  /* xMember  */    mmOneszMember,
  /* xBenign  */    0,
  /* xFinal   */    0
};
static sqlite4_mm *mmOneszNew(void *pSpace, int sz, int cnt){
  struct mmOnesz *pOnesz;
  unsigned char *pMem;
  if( sz<sizeof(*pOnesz) ) return 0;
  if( cnt<2 ) return 0;
  pMem = (unsigned char*)pSpace;
  pOnesz = (struct mmOnesz*)pMem;
  pMem += sz;
  pOnesz->base.pMethods = &mmOneszMethods;
  pOnesz->pSpace = (const void*)pMem;
  pOnesz->sz = sz;
  pOnesz->pLast = (const void*)(pMem + sz*(cnt-2));
  pOnesz->pFree = 0;
  while( cnt ){
    struct mmOneszBlock *pBlock = (struct mmOneszBlock*)pMem;
    pBlock->pNext = pOnesz->pFree;
    pOnesz->pFree = pBlock;
    cnt--;
    pMem += sz;
  }
  return &pOnesz->base;
}

/*************************************************************************
** Main interfaces.
*/
void *sqlite4_mm_malloc(sqlite4_mm *pMM, sqlite4_int64 iSize){
  if( pMM==0 ) pMM = &mmSystem;
  return pMM->pMethods->xMalloc(pMM,iSize);
}
void *sqlite4_mm_realloc(sqlite4_mm *pMM, void *pOld, sqlite4_int64 iSize){
  if( pMM==0 ) pMM = &mmSystem;
  return pMM->pMethods->xRealloc(pMM,pOld,iSize);
}
void sqlite4_mm_free(sqlite4_mm *pMM, void *pOld){
  if( pMM==0 ) pMM = &mmSystem;
  pMM->pMethods->xFree(pMM,pOld);
}
sqlite4_int64 sqlite4_mm_msize(sqlite4_mm *pMM, void *pOld){
  if( pMM==0 ) pMM = &mmSystem;
  return pMM->pMethods->xMsize(pMM,pOld);
}
int sqlite4_mm_member(sqlite4_mm *pMM, const void *pOld){
  return (pMM && pMM->pMethods->xMember!=0) ?
            pMM->pMethods->xMember(pMM,pOld) : -1;
}
void sqlite4_mm_benign_failure(sqlite4_mm *pMM, int bEnable){
  if( pMM && pMM->pMethods->xBenign ){
    pMM->pMethods->xBenign(pMM, bEnable);
  }
}
void sqlite4_mm_destroy(sqlite4_mm *pMM){
  if( pMM && pMM->pMethods->xFinal ) pMM->pMethods->xFinal(pMM);
}

/*
** Create a new memory allocation object.  eType determines the type of
** memory allocator and the arguments.
*/
sqlite4_mm *sqlite4_mm_new(sqlite4_mm_type eType, ...){
  va_list ap;
  sqlite4_mm *pMM;

  va_start(ap, eType);
  switch( eType ){
    case SQLITE4_MM_SYSTEM: {
      pMM = &mmSystem;
      break;
    }
    case SQLITE4_MM_OVERFLOW: {
      sqlite4_mm *pA = va_arg(ap, sqlite4_mm*);
      sqlite4_mm *pB = va_arg(ap, sqlite4_mm*);
      pMM = mmOvflNew(pA, pB);
      break;
    }
    case SQLITE4_MM_ONESIZE: {
      void *pSpace = va_arg(ap, void*);
      int sz = va_arg(ap, int);
      int cnt = va_arg(ap, int);
      pMM = mmOneszNew(pSpace, sz, cnt);
      break;
    }
    default: {
      pMM = 0;
      break;
    }
  }
  va_end(ap);
  return pMM;
}
