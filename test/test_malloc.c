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
** This file contains code used to implement test interfaces to the
** memory allocation subsystem.
*/
#include "sqliteInt.h"
#include "tcl.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/*
** This structure is used to encapsulate the global state variables used 
** by malloc() fault simulation.
*/
static struct MemFault {
  int iCountdown;         /* Number of pending successes before a failure */
  int nRepeat;            /* Number of times to repeat the failure */
  int nBenign;            /* Number of benign failures seen since last config */
  int nFail;              /* Number of failures seen since last config */
  u8 enable;              /* True if enabled */
  int isInstalled;        /* True if the fault simulation layer is installed */
  int isBenignMode;       /* True if malloc failures are considered benign */
  sqlite4_mem_methods m;  /* 'Real' malloc implementation */
} memfault;

/*
** This routine exists as a place to set a breakpoint that will
** fire on any simulated malloc() failure.
*/
static void sqlite4Fault(void){
  static int cnt = 0;
  cnt++;
}

/*
** Check to see if a fault should be simulated.  Return true to simulate
** the fault.  Return false if the fault should not be simulated.
*/
static int faultsimStep(void){
  if( !memfault.enable ){
    return 0;
  }
  if( memfault.iCountdown>0 ){
    memfault.iCountdown--;
    return 0;
  }
  sqlite4Fault();
  memfault.nFail++;
  if( memfault.isBenignMode>0 ){
    memfault.nBenign++;
  }
  memfault.nRepeat--;
  if( memfault.nRepeat<=0 ){
    memfault.enable = 0;
  }
  return 1;  
}

/*
** A version of sqlite4_mem_methods.xMalloc() that includes fault simulation
** logic.
*/
static void *faultsimMalloc(void *pMem, sqlite4_size_t n){
  void *p = 0;
  assert( pMem==(void*)&memfault );
  if( !faultsimStep() ){
    p = memfault.m.xMalloc(memfault.m.pMemEnv, n);
  }
  return p;
}


/*
** A version of sqlite4_mem_methods.xRealloc() that includes fault simulation
** logic.
*/
static void *faultsimRealloc(void *pMem, void *pOld, sqlite4_size_t n){
  void *p = 0;
  assert( pMem==(void*)&memfault );
  if( !faultsimStep() ){
    p = memfault.m.xRealloc(memfault.m.pMemEnv, pOld, n);
  }
  return p;
}

/* 
** The following method calls are passed directly through to the underlying
** malloc system:
**
**     xFree
**     xSize
**     xInit
**     xShutdown
*/
static void faultsimFree(void *pMem, void *p){
  assert( pMem==(void*)&memfault );
  memfault.m.xFree(memfault.m.pMemEnv, p);
}
static sqlite4_size_t faultsimSize(void *pMem, void *p){
  assert( pMem==(void*)&memfault );
  return memfault.m.xSize(memfault.m.pMemEnv, p);
}
static int faultsimInit(void *pMem){
  return memfault.m.xInit(memfault.m.pMemEnv);
}
static void faultsimShutdown(void *pMem){
  memfault.m.xShutdown(memfault.m.pMemEnv);
}

/*
** This routine configures the malloc failure simulation.  After
** calling this routine, the next nDelay mallocs will succeed, followed
** by a block of nRepeat failures, after which malloc() calls will begin
** to succeed again.
*/
static void faultsimConfig(int nDelay, int nRepeat){
  memfault.iCountdown = nDelay;
  memfault.nRepeat = nRepeat;
  memfault.nBenign = 0;
  memfault.nFail = 0;
  memfault.enable = nDelay>=0;

  /* Sometimes, when running multi-threaded tests, the isBenignMode 
  ** variable is not properly incremented/decremented so that it is
  ** 0 when not inside a benign malloc block. This doesn't affect
  ** the multi-threaded tests, as they do not use this system. But
  ** it does affect OOM tests run later in the same process. So
  ** zero the variable here, just to be sure.
  */
  memfault.isBenignMode = 0;
}

/*
** Return the number of faults (both hard and benign faults) that have
** occurred since the injector was last configured.
*/
static int faultsimFailures(void){
  return memfault.nFail;
}

/*
** Return the number of benign faults that have occurred since the
** injector was last configured.
*/
static int faultsimBenignFailures(void){
  return memfault.nBenign;
}

/*
** Return the number of successes that will occur before the next failure.
** If no failures are scheduled, return -1.
*/
static int faultsimPending(void){
  if( memfault.enable ){
    return memfault.iCountdown;
  }else{
    return -1;
  }
}


static void faultsimBeginBenign(void *pMem){
  memfault.isBenignMode++;
}
static void faultsimEndBenign(void *pMem){
  memfault.isBenignMode--;
}

/*
** Add or remove the fault-simulation layer using sqlite4_env_config(). If
** the argument is non-zero, the 
*/
static int faultsimInstall(int install){
  static struct sqlite4_mem_methods m = {
    faultsimMalloc,                   /* xMalloc */
    faultsimFree,                     /* xFree */
    faultsimRealloc,                  /* xRealloc */
    faultsimSize,                     /* xSize */
    faultsimInit,                     /* xInit */
    faultsimShutdown,                 /* xShutdown */
    faultsimBeginBenign,              /* xBeginBenign */
    faultsimEndBenign,                /* xEndBenign */
    (void*)&memfault                  /* pMemEnv */
  };
  int rc;

  install = (install ? 1 : 0);
  assert(memfault.isInstalled==1 || memfault.isInstalled==0);

  if( install==memfault.isInstalled ){
    return SQLITE4_ERROR;
  }

  if( install ){
    sqlite4_initialize(0);
    rc = sqlite4_env_config(0, SQLITE4_ENVCONFIG_GETMALLOC, &memfault.m);
    assert(memfault.m.xMalloc);
    if( rc==SQLITE4_OK ){
      rc = sqlite4_env_config(0, SQLITE4_ENVCONFIG_MALLOC, &m);
    }
  }else{
    sqlite4_mem_methods m;
    assert(memfault.m.xMalloc);

    /* One should be able to reset the default memory allocator by storing
    ** a zeroed allocator then calling GETMALLOC. */
    memset(&m, 0, sizeof(m));
    sqlite4_env_config(0, SQLITE4_ENVCONFIG_MALLOC, &m);
    sqlite4_env_config(0, SQLITE4_ENVCONFIG_GETMALLOC, &m);
    assert( memcmp(&m, &memfault.m, sizeof(m))==0 );

    rc = sqlite4_env_config(0, SQLITE4_ENVCONFIG_MALLOC, &memfault.m);
  }

  if( rc==SQLITE4_OK ){
    memfault.isInstalled = 1;
  }
  return rc;
}

#ifdef SQLITE4_TEST

/*
** This function is implemented in test1.c. Returns a pointer to a static
** buffer containing the symbolic SQLite error code that corresponds to
** the least-significant 8-bits of the integer passed as an argument.
** For example:
**
**   sqlite4TestErrorName(1) -> "SQLITE4_ERROR"
*/
const char *sqlite4TestErrorName(int);

/*
** Transform pointers to text and back again
*/
static void pointerToText(void *p, char *z){
  static const char zHex[] = "0123456789abcdef";
  int i, k;
  unsigned int u;
  sqlite4_uint64 n;
  if( p==0 ){
    strcpy(z, "0");
    return;
  }
  if( sizeof(n)==sizeof(p) ){
    memcpy(&n, &p, sizeof(p));
  }else if( sizeof(u)==sizeof(p) ){
    memcpy(&u, &p, sizeof(u));
    n = u;
  }else{
    assert( 0 );
  }
  for(i=0, k=sizeof(p)*2-1; i<sizeof(p)*2; i++, k--){
    z[k] = zHex[n&0xf];
    n >>= 4;
  }
  z[sizeof(p)*2] = 0;
}
static int hexToInt(int h){
  if( h>='0' && h<='9' ){
    return h - '0';
  }else if( h>='a' && h<='f' ){
    return h - 'a' + 10;
  }else{
    return -1;
  }
}
static int textToPointer(const char *z, void **pp){
  sqlite4_uint64 n = 0;
  int i;
  unsigned int u;
  for(i=0; i<sizeof(void*)*2 && z[0]; i++){
    int v;
    v = hexToInt(*z++);
    if( v<0 ) return TCL_ERROR;
    n = n*16 + v;
  }
  if( *z!=0 ) return TCL_ERROR;
  if( sizeof(n)==sizeof(*pp) ){
    memcpy(pp, &n, sizeof(n));
  }else if( sizeof(u)==sizeof(*pp) ){
    u = (unsigned int)n;
    memcpy(pp, &u, sizeof(u));
  }else{
    assert( 0 );
  }
  return TCL_OK;
}

/*
** Usage:    sqlite4_malloc  NBYTES
**
** Raw test interface for sqlite4_malloc().
*/
static int test_malloc(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int nByte;
  void *p;
  char zOut[100];
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "NBYTES");
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[1], &nByte) ) return TCL_ERROR;
  p = sqlite4_malloc(0, (unsigned)nByte);
  pointerToText(p, zOut);
  Tcl_AppendResult(interp, zOut, NULL);
  return TCL_OK;
}

/*
** Usage:    sqlite4_realloc  PRIOR  NBYTES
**
** Raw test interface for sqlite4_realloc().
*/
static int test_realloc(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int nByte;
  void *pPrior, *p;
  char zOut[100];
  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "PRIOR NBYTES");
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[2], &nByte) ) return TCL_ERROR;
  if( textToPointer(Tcl_GetString(objv[1]), &pPrior) ){
    Tcl_AppendResult(interp, "bad pointer: ", Tcl_GetString(objv[1]), (char*)0);
    return TCL_ERROR;
  }
  p = sqlite4_realloc(0, pPrior, (unsigned)nByte);
  pointerToText(p, zOut);
  Tcl_AppendResult(interp, zOut, NULL);
  return TCL_OK;
}

/*
** Usage:    sqlite4_free  PRIOR
**
** Raw test interface for sqlite4_free().
*/
static int test_free(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  void *pPrior;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "PRIOR");
    return TCL_ERROR;
  }
  if( textToPointer(Tcl_GetString(objv[1]), &pPrior) ){
    Tcl_AppendResult(interp, "bad pointer: ", Tcl_GetString(objv[1]), (char*)0);
    return TCL_ERROR;
  }
  sqlite4_free(0, pPrior);
  return TCL_OK;
}

/*
** These routines are in test_hexio.c
*/
int sqlite4TestHexToBin(const char *, int, char *);
int sqlite4TestBinToHex(char*,int);

/*
** Usage:    memset  ADDRESS  SIZE  HEX
**
** Set a chunk of memory (obtained from malloc, probably) to a
** specified hex pattern.
*/
static int test_memset(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  void *p;
  int size, n, i;
  char *zHex;
  char *zOut;
  char zBin[100];

  if( objc!=4 ){
    Tcl_WrongNumArgs(interp, 1, objv, "ADDRESS SIZE HEX");
    return TCL_ERROR;
  }
  if( textToPointer(Tcl_GetString(objv[1]), &p) ){
    Tcl_AppendResult(interp, "bad pointer: ", Tcl_GetString(objv[1]), (char*)0);
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[2], &size) ){
    return TCL_ERROR;
  }
  if( size<=0 ){
    Tcl_AppendResult(interp, "size must be positive", (char*)0);
    return TCL_ERROR;
  }
  zHex = Tcl_GetStringFromObj(objv[3], &n);
  if( n>sizeof(zBin)*2 ) n = sizeof(zBin)*2;
  n = sqlite4TestHexToBin(zHex, n, zBin);
  if( n==0 ){
    Tcl_AppendResult(interp, "no data", (char*)0);
    return TCL_ERROR;
  }
  zOut = p;
  for(i=0; i<size; i++){
    zOut[i] = zBin[i%n];
  }
  return TCL_OK;
}

/*
** Usage:    memget  ADDRESS  SIZE
**
** Return memory as hexadecimal text.
*/
static int test_memget(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  void *p;
  int size, n;
  char *zBin;
  char zHex[100];

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "ADDRESS SIZE");
    return TCL_ERROR;
  }
  if( textToPointer(Tcl_GetString(objv[1]), &p) ){
    Tcl_AppendResult(interp, "bad pointer: ", Tcl_GetString(objv[1]), (char*)0);
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[2], &size) ){
    return TCL_ERROR;
  }
  if( size<=0 ){
    Tcl_AppendResult(interp, "size must be positive", (char*)0);
    return TCL_ERROR;
  }
  zBin = p;
  while( size>0 ){
    if( size>(sizeof(zHex)-1)/2 ){
      n = (sizeof(zHex)-1)/2;
    }else{
      n = size;
    }
    memcpy(zHex, zBin, n);
    zBin += n;
    size -= n;
    sqlite4TestBinToHex(zHex, n);
    Tcl_AppendResult(interp, zHex, (char*)0);
  }
  return TCL_OK;
}

/*
** Usage:    sqlite4_memory_used
**
** Raw test interface for sqlite4_memory_used().
*/
static int test_memory_used(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  Tcl_SetObjResult(interp, Tcl_NewWideIntObj(sqlite4_memory_used(0)));
  return TCL_OK;
}

/*
** Usage:    sqlite4_memory_highwater ?RESETFLAG?
**
** Raw test interface for sqlite4_memory_highwater().
*/
static int test_memory_highwater(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int resetFlag = 0;
  if( objc!=1 && objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "?RESET?");
    return TCL_ERROR;
  }
  if( objc==2 ){
    if( Tcl_GetBooleanFromObj(interp, objv[1], &resetFlag) ) return TCL_ERROR;
  } 
  Tcl_SetObjResult(interp, 
     Tcl_NewWideIntObj(sqlite4_memory_highwater(0, resetFlag)));
  return TCL_OK;
}

/*
** Usage:    sqlite4_memdebug_backtrace DEPTH
**
** Set the depth of backtracing.  If SQLITE4_MEMDEBUG is not defined
** then this routine is a no-op.
*/
static int test_memdebug_backtrace(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int depth;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DEPT");
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[1], &depth) ) return TCL_ERROR;
#ifdef SQLITE4_MEMDEBUG
  {
    extern void sqlite4MemdebugBacktrace(int);
    sqlite4MemdebugBacktrace(depth);
  }
#endif
  return TCL_OK;
}

/*
** Usage:    sqlite4_memdebug_dump  FILENAME
**
** Write a summary of unfreed memory to FILENAME.
*/
static int test_memdebug_dump(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "FILENAME");
    return TCL_ERROR;
  }
#if defined(SQLITE4_MEMDEBUG) || defined(SQLITE4_MEMORY_SIZE) \
     || defined(SQLITE4_POW2_MEMORY_SIZE)
  {
    extern void sqlite4MemdebugDump(const char*);
    sqlite4MemdebugDump(Tcl_GetString(objv[1]));
  }
#endif
  return TCL_OK;
}

/*
** Usage:    sqlite4_memdebug_malloc_count
**
** Return the total number of times malloc() has been called.
*/
static int test_memdebug_malloc_count(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int nMalloc = -1;
  if( objc!=1 ){
    Tcl_WrongNumArgs(interp, 1, objv, "");
    return TCL_ERROR;
  }
#if defined(SQLITE4_MEMDEBUG)
  {
    extern int sqlite4MemdebugMallocCount();
    nMalloc = sqlite4MemdebugMallocCount();
  }
#endif
  Tcl_SetObjResult(interp, Tcl_NewIntObj(nMalloc));
  return TCL_OK;
}


/*
** Usage:    sqlite4_memdebug_fail  COUNTER  ?OPTIONS?
**
** where options are:
**
**     -repeat    <count>
**     -benigncnt <varname>
**
** Arrange for a simulated malloc() failure after COUNTER successes.
** If a repeat count is specified, the fault is repeated that many
** times.
**
** Each call to this routine overrides the prior counter value.
** This routine returns the number of simulated failures that have
** happened since the previous call to this routine.
**
** To disable simulated failures, use a COUNTER of -1.
*/
static int test_memdebug_fail(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int ii;
  int iFail;
  int nRepeat = 1;
  Tcl_Obj *pBenignCnt = 0;
  int nBenign;
  int nFail = 0;

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "COUNTER ?OPTIONS?");
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[1], &iFail) ) return TCL_ERROR;

  for(ii=2; ii<objc; ii+=2){
    int nOption;
    char *zOption = Tcl_GetStringFromObj(objv[ii], &nOption);
    char *zErr = 0;

    if( nOption>1 && strncmp(zOption, "-repeat", nOption)==0 ){
      if( ii==(objc-1) ){
        zErr = "option requires an argument: ";
      }else{
        if( Tcl_GetIntFromObj(interp, objv[ii+1], &nRepeat) ){
          return TCL_ERROR;
        }
      }
    }else if( nOption>1 && strncmp(zOption, "-benigncnt", nOption)==0 ){
      if( ii==(objc-1) ){
        zErr = "option requires an argument: ";
      }else{
        pBenignCnt = objv[ii+1];
      }
    }else{
      zErr = "unknown option: ";
    }

    if( zErr ){
      Tcl_AppendResult(interp, zErr, zOption, 0);
      return TCL_ERROR;
    }
  }
  
  nBenign = faultsimBenignFailures();
  nFail = faultsimFailures();
  faultsimConfig(iFail, nRepeat);

  if( pBenignCnt ){
    Tcl_ObjSetVar2(interp, pBenignCnt, 0, Tcl_NewIntObj(nBenign), 0);
  }
  Tcl_SetObjResult(interp, Tcl_NewIntObj(nFail));
  return TCL_OK;
}

/*
** Usage:    sqlite4_memdebug_pending
**
** Return the number of malloc() calls that will succeed before a 
** simulated failure occurs. A negative return value indicates that
** no malloc() failure is scheduled.
*/
static int test_memdebug_pending(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int nPending;
  if( objc!=1 ){
    Tcl_WrongNumArgs(interp, 1, objv, "");
    return TCL_ERROR;
  }
  nPending = faultsimPending();
  Tcl_SetObjResult(interp, Tcl_NewIntObj(nPending));
  return TCL_OK;
}


/*
** Usage:    sqlite4_memdebug_settitle TITLE
**
** Set a title string stored with each allocation.  The TITLE is
** typically the name of the test that was running when the
** allocation occurred.  The TITLE is stored with the allocation
** and can be used to figure out which tests are leaking memory.
**
** Each title overwrite the previous.
*/
static int test_memdebug_settitle(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  const char *zTitle;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "TITLE");
    return TCL_ERROR;
  }
  zTitle = Tcl_GetString(objv[1]);
#ifdef SQLITE4_MEMDEBUG
  {
    extern int sqlite4MemdebugSettitle(const char*);
    sqlite4MemdebugSettitle(zTitle);
  }
#endif
  return TCL_OK;
}

#define MALLOC_LOG_FRAMES  10 
#define MALLOC_LOG_KEYINTS (                                              \
    10 * ((sizeof(int)>=sizeof(void*)) ? 1 : sizeof(void*)/sizeof(int))   \
)
static Tcl_HashTable aMallocLog;
static int mallocLogEnabled = 0;

typedef struct MallocLog MallocLog;
struct MallocLog {
  int nCall;
  int nByte;
};

#ifdef SQLITE4_MEMDEBUG
static void test_memdebug_callback(int nByte, int nFrame, void **aFrame){
  if( mallocLogEnabled ){
    MallocLog *pLog;
    Tcl_HashEntry *pEntry;
    int isNew;

    int aKey[MALLOC_LOG_KEYINTS];
    int nKey = sizeof(int)*MALLOC_LOG_KEYINTS;

    memset(aKey, 0, nKey);
    if( (sizeof(void*)*nFrame)<nKey ){
      nKey = nFrame*sizeof(void*);
    }
    memcpy(aKey, aFrame, nKey);

    pEntry = Tcl_CreateHashEntry(&aMallocLog, (const char *)aKey, &isNew);
    if( isNew ){
      pLog = (MallocLog *)Tcl_Alloc(sizeof(MallocLog));
      memset(pLog, 0, sizeof(MallocLog));
      Tcl_SetHashValue(pEntry, (ClientData)pLog);
    }else{
      pLog = (MallocLog *)Tcl_GetHashValue(pEntry);
    }

    pLog->nCall++;
    pLog->nByte += nByte;
  }
}
#endif /* SQLITE4_MEMDEBUG */

static void test_memdebug_log_clear(void){
  Tcl_HashSearch search;
  Tcl_HashEntry *pEntry;
  for(
    pEntry=Tcl_FirstHashEntry(&aMallocLog, &search);
    pEntry;
    pEntry=Tcl_NextHashEntry(&search)
  ){
    MallocLog *pLog = (MallocLog *)Tcl_GetHashValue(pEntry);
    Tcl_Free((char *)pLog);
  }
  Tcl_DeleteHashTable(&aMallocLog);
  Tcl_InitHashTable(&aMallocLog, MALLOC_LOG_KEYINTS);
}

static int test_memdebug_log(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  static int isInit = 0;
  int iSub;

  static const char *MB_strs[] = { "start", "stop", "dump", "clear", "sync" };
  enum MB_enum { 
      MB_LOG_START, MB_LOG_STOP, MB_LOG_DUMP, MB_LOG_CLEAR, MB_LOG_SYNC 
  };

  if( !isInit ){
#ifdef SQLITE4_MEMDEBUG
    extern void sqlite4MemdebugBacktraceCallback(
        void (*xBacktrace)(int, int, void **));
    sqlite4MemdebugBacktraceCallback(test_memdebug_callback);
#endif
    Tcl_InitHashTable(&aMallocLog, MALLOC_LOG_KEYINTS);
    isInit = 1;
  }

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUB-COMMAND ...");
  }
  if( Tcl_GetIndexFromObj(interp, objv[1], MB_strs, "sub-command", 0, &iSub) ){
    return TCL_ERROR;
  }

  switch( (enum MB_enum)iSub ){
    case MB_LOG_START:
      mallocLogEnabled = 1;
      break;
    case MB_LOG_STOP:
      mallocLogEnabled = 0;
      break;
    case MB_LOG_DUMP: {
      Tcl_HashSearch search;
      Tcl_HashEntry *pEntry;
      Tcl_Obj *pRet = Tcl_NewObj();

      assert(sizeof(Tcl_WideInt)>=sizeof(void*));

      for(
        pEntry=Tcl_FirstHashEntry(&aMallocLog, &search);
        pEntry;
        pEntry=Tcl_NextHashEntry(&search)
      ){
        Tcl_Obj *apElem[MALLOC_LOG_FRAMES+2];
        MallocLog *pLog = (MallocLog *)Tcl_GetHashValue(pEntry);
        Tcl_WideInt *aKey = (Tcl_WideInt *)Tcl_GetHashKey(&aMallocLog, pEntry);
        int ii;
  
        apElem[0] = Tcl_NewIntObj(pLog->nCall);
        apElem[1] = Tcl_NewIntObj(pLog->nByte);
        for(ii=0; ii<MALLOC_LOG_FRAMES; ii++){
          apElem[ii+2] = Tcl_NewWideIntObj(aKey[ii]);
        }

        Tcl_ListObjAppendElement(interp, pRet,
            Tcl_NewListObj(MALLOC_LOG_FRAMES+2, apElem)
        );
      }

      Tcl_SetObjResult(interp, pRet);
      break;
    }
    case MB_LOG_CLEAR: {
      test_memdebug_log_clear();
      break;
    }

    case MB_LOG_SYNC: {
#ifdef SQLITE4_MEMDEBUG
      extern void sqlite4MemdebugSync();
      test_memdebug_log_clear();
      mallocLogEnabled = 1;
      sqlite4MemdebugSync();
#endif
      break;
    }
  }

  return TCL_OK;
}

/*
** Usage:    sqlite4_env_config_memstatus BOOLEAN
**
** Enable or disable memory status reporting using SQLITE4_CONFIG_MEMSTATUS.
*/
static int test_config_memstatus(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int enable, rc;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "BOOLEAN");
    return TCL_ERROR;
  }
  if( Tcl_GetBooleanFromObj(interp, objv[1], &enable) ) return TCL_ERROR;
  rc = sqlite4_env_config(0, SQLITE4_ENVCONFIG_MEMSTATUS, enable);
  Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
  return TCL_OK;
}

/*
** Usage:    sqlite4_envconfig_lookaside  SIZE  COUNT
**
*/
static int test_envconfig_lookaside(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int rc;
  int sz, cnt;
  Tcl_Obj *pRet;
  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SIZE COUNT");
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[1], &sz) ) return TCL_ERROR;
  if( Tcl_GetIntFromObj(interp, objv[2], &cnt) ) return TCL_ERROR;
  pRet = Tcl_NewObj();
  Tcl_ListObjAppendElement(
      interp, pRet, Tcl_NewIntObj(sqlite4DefaultEnv.szLookaside)
  );
  Tcl_ListObjAppendElement(
      interp, pRet, Tcl_NewIntObj(sqlite4DefaultEnv.nLookaside)
  );
  rc = sqlite4_env_config(0, SQLITE4_ENVCONFIG_LOOKASIDE, sz, cnt);
  Tcl_SetObjResult(interp, pRet);
  return TCL_OK;
}


/*
** Usage:    sqlite4_db_config_lookaside  CONNECTION  BUFID  SIZE  COUNT
**
** There are two static buffers with BUFID 1 and 2.   Each static buffer
** is 10KB in size.  A BUFID of 0 indicates that the buffer should be NULL
** which will cause sqlite4_db_config() to allocate space on its own.
*/
static int test_db_config_lookaside(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int rc;
  int sz, cnt;
  sqlite4 *db;
  int bufid;
  static char azBuf[2][10000];
  int getDbPointer(Tcl_Interp*, const char*, sqlite4**);
  if( objc!=5 ){
    Tcl_WrongNumArgs(interp, 1, objv, "BUFID SIZE COUNT");
    return TCL_ERROR;
  }
  if( getDbPointer(interp, Tcl_GetString(objv[1]), &db) ) return TCL_ERROR;
  if( Tcl_GetIntFromObj(interp, objv[2], &bufid) ) return TCL_ERROR;
  if( Tcl_GetIntFromObj(interp, objv[3], &sz) ) return TCL_ERROR;
  if( Tcl_GetIntFromObj(interp, objv[4], &cnt) ) return TCL_ERROR;
  if( bufid==0 ){
    rc = sqlite4_db_config(db, SQLITE4_DBCONFIG_LOOKASIDE, 0, sz, cnt);
  }else if( bufid>=1 && bufid<=2 && sz*cnt<=sizeof(azBuf[0]) ){
    rc = sqlite4_db_config(db, SQLITE4_DBCONFIG_LOOKASIDE, azBuf[bufid], sz,cnt);
  }else{
    Tcl_AppendResult(interp, "illegal arguments - see documentation", (char*)0);
    return TCL_ERROR;
  }
  Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
  return TCL_OK;
}

/*
** tclcmd:     sqlite4_config_error  [DB]
**
** Invoke sqlite4_env_config() or sqlite4_db_config() with invalid
** opcodes and verify that they return errors.
*/
static int test_config_error(
  void * clientData, 
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  sqlite4 *db;
  int getDbPointer(Tcl_Interp*, const char*, sqlite4**);

  if( objc!=2 && objc!=1 ){
    Tcl_WrongNumArgs(interp, 1, objv, "[DB]");
    return TCL_ERROR;
  }
  if( objc==2 ){
    if( getDbPointer(interp, Tcl_GetString(objv[1]), &db) ) return TCL_ERROR;
    if( sqlite4_db_config(db, 99999)!=SQLITE4_ERROR ){
      Tcl_AppendResult(interp, 
            "sqlite4_db_config(db, 99999) does not return SQLITE4_ERROR",
            (char*)0);
      return TCL_ERROR;
    }
  }else{
    if( sqlite4_env_config(0, 99999)!=SQLITE4_ERROR ){
      Tcl_AppendResult(interp, 
          "sqlite4_env_config(0, 99999) does not return SQLITE4_ERROR",
          (char*)0);
      return TCL_ERROR;
    }
  }
  return TCL_OK;
}


/*
** Usage:    sqlite4_env_status  OPCODE  RESETFLAG
**
** Return a list of three elements which are the sqlite4_env_status() return
** code, the current value, and the high-water mark value.
*/
static int test_status(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int rc;
  sqlite4_uint64 iValue, mxValue;
  int i, op, resetFlag;
  const char *zOpName;
  static const struct {
    const char *zName;
    int op;
  } aOp[] = {
    { "SQLITE4_ENVSTATUS_MEMORY_USED",   SQLITE4_ENVSTATUS_MEMORY_USED         },
    { "SQLITE4_ENVSTATUS_MALLOC_SIZE",   SQLITE4_ENVSTATUS_MALLOC_SIZE         },
    { "SQLITE4_ENVSTATUS_PARSER_STACK",  SQLITE4_ENVSTATUS_PARSER_STACK        },
    { "SQLITE4_ENVSTATUS_MALLOC_COUNT",  SQLITE4_ENVSTATUS_MALLOC_COUNT        },
  };
  Tcl_Obj *pResult;
  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "PARAMETER RESETFLAG");
    return TCL_ERROR;
  }
  zOpName = Tcl_GetString(objv[1]);
  for(i=0; i<ArraySize(aOp); i++){
    if( strcmp(aOp[i].zName, zOpName)==0 ){
      op = aOp[i].op;
      break;
    }
  }
  if( i>=ArraySize(aOp) ){
    if( Tcl_GetIntFromObj(interp, objv[1], &op) ) return TCL_ERROR;
  }
  if( Tcl_GetBooleanFromObj(interp, objv[2], &resetFlag) ) return TCL_ERROR;
  iValue = 0;
  mxValue = 0;
  rc = sqlite4_env_status(0, op, &iValue, &mxValue, resetFlag);
  pResult = Tcl_NewObj();
  Tcl_ListObjAppendElement(0, pResult, Tcl_NewIntObj(rc));
  Tcl_ListObjAppendElement(0, pResult, Tcl_NewWideIntObj(iValue));
  Tcl_ListObjAppendElement(0, pResult, Tcl_NewWideIntObj(mxValue));
  Tcl_SetObjResult(interp, pResult);
  return TCL_OK;
}

/*
** Usage:    sqlite4_db_status  DATABASE  OPCODE  RESETFLAG
**
** Return a list of three elements which are the sqlite4_db_status() return
** code, the current value, and the high-water mark value.
*/
static int test_db_status(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int rc, iValue, mxValue;
  int i, op, resetFlag;
  const char *zOpName;
  sqlite4 *db;
  int getDbPointer(Tcl_Interp*, const char*, sqlite4**);
  static const struct {
    const char *zName;
    int op;
  } aOp[] = {
    { "LOOKASIDE_USED",      SQLITE4_DBSTATUS_LOOKASIDE_USED      },
    { "CACHE_USED",          SQLITE4_DBSTATUS_CACHE_USED          },
    { "SCHEMA_USED",         SQLITE4_DBSTATUS_SCHEMA_USED         },
    { "STMT_USED",           SQLITE4_DBSTATUS_STMT_USED           },
    { "LOOKASIDE_HIT",       SQLITE4_DBSTATUS_LOOKASIDE_HIT       },
    { "LOOKASIDE_MISS_SIZE", SQLITE4_DBSTATUS_LOOKASIDE_MISS_SIZE },
    { "LOOKASIDE_MISS_FULL", SQLITE4_DBSTATUS_LOOKASIDE_MISS_FULL },
    { "CACHE_HIT",           SQLITE4_DBSTATUS_CACHE_HIT           },
    { "CACHE_MISS",          SQLITE4_DBSTATUS_CACHE_MISS          }
  };
  Tcl_Obj *pResult;
  if( objc!=4 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB PARAMETER RESETFLAG");
    return TCL_ERROR;
  }
  if( getDbPointer(interp, Tcl_GetString(objv[1]), &db) ) return TCL_ERROR;
  zOpName = Tcl_GetString(objv[2]);
  if( memcmp(zOpName, "SQLITE4_", 7)==0 ) zOpName += 7;
  if( memcmp(zOpName, "DBSTATUS_", 9)==0 ) zOpName += 9;
  for(i=0; i<ArraySize(aOp); i++){
    if( strcmp(aOp[i].zName, zOpName)==0 ){
      op = aOp[i].op;
      break;
    }
  }
  if( i>=ArraySize(aOp) ){
    if( Tcl_GetIntFromObj(interp, objv[2], &op) ) return TCL_ERROR;
  }
  if( Tcl_GetBooleanFromObj(interp, objv[3], &resetFlag) ) return TCL_ERROR;
  iValue = 0;
  mxValue = 0;
  rc = sqlite4_db_status(db, op, &iValue, &mxValue, resetFlag);
  pResult = Tcl_NewObj();
  Tcl_ListObjAppendElement(0, pResult, Tcl_NewIntObj(rc));
  Tcl_ListObjAppendElement(0, pResult, Tcl_NewIntObj(iValue));
  Tcl_ListObjAppendElement(0, pResult, Tcl_NewIntObj(mxValue));
  Tcl_SetObjResult(interp, pResult);
  return TCL_OK;
}

/*
** install_malloc_faultsim BOOLEAN
*/
static int test_install_malloc_faultsim(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int rc;
  int isInstall;

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "BOOLEAN");
    return TCL_ERROR;
  }
  if( TCL_OK!=Tcl_GetBooleanFromObj(interp, objv[1], &isInstall) ){
    return TCL_ERROR;
  }
  rc = faultsimInstall(isInstall);
  Tcl_SetResult(interp, (char *)sqlite4TestErrorName(rc), TCL_VOLATILE);
  return TCL_OK;
}

/*
** sqlite4_install_memsys3
*/
static int test_install_memsys3(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int rc = SQLITE4_MISUSE;
#ifdef SQLITE4_ENABLE_MEMSYS3
  const sqlite4_mem_methods *sqlite4MemGetMemsys3(void);
  rc = sqlite4_env_config(0, SQLITE4_ENVCONFIG_MALLOC, sqlite4MemGetMemsys3());
#endif
  Tcl_SetResult(interp, (char *)sqlite4TestErrorName(rc), TCL_VOLATILE);
  return TCL_OK;
}

/*
** Register commands with the TCL interpreter.
*/
int Sqlitetest_malloc_Init(Tcl_Interp *interp){
  static struct {
     char *zName;
     Tcl_ObjCmdProc *xProc;
     int clientData;
  } aObjCmd[] = {
     { "sqlite4_malloc",                 test_malloc                   ,0 },
     { "sqlite4_realloc",                test_realloc                  ,0 },
     { "sqlite4_free",                   test_free                     ,0 },
     { "memset",                         test_memset                   ,0 },
     { "memget",                         test_memget                   ,0 },
     { "sqlite4_memory_used",            test_memory_used              ,0 },
     { "sqlite4_memory_highwater",       test_memory_highwater         ,0 },
     { "sqlite4_memdebug_backtrace",     test_memdebug_backtrace       ,0 },
     { "sqlite4_memdebug_dump",          test_memdebug_dump            ,0 },
     { "sqlite4_memdebug_fail",          test_memdebug_fail            ,0 },
     { "sqlite4_memdebug_pending",       test_memdebug_pending         ,0 },
     { "sqlite4_memdebug_settitle",      test_memdebug_settitle        ,0 },
     { "sqlite4_memdebug_malloc_count",  test_memdebug_malloc_count ,0 },
     { "sqlite4_memdebug_log",           test_memdebug_log             ,0 },
     { "sqlite4_env_status",             test_status                   ,0 },
     { "sqlite4_db_status",              test_db_status                ,0 },
     { "install_malloc_faultsim",        test_install_malloc_faultsim  ,0 },
     { "sqlite4_env_config_memstatus",   test_config_memstatus         ,0 },
     { "sqlite4_envconfig_lookaside",    test_envconfig_lookaside      ,0 },
     { "sqlite4_config_error",           test_config_error             ,0 },
     { "sqlite4_db_config_lookaside",    test_db_config_lookaside      ,0 },
     { "sqlite4_install_memsys3",        test_install_memsys3          ,0 },
  };
  int i;
  for(i=0; i<sizeof(aObjCmd)/sizeof(aObjCmd[0]); i++){
    ClientData c = (ClientData)SQLITE4_INT_TO_PTR(aObjCmd[i].clientData);
    Tcl_CreateObjCommand(interp, aObjCmd[i].zName, aObjCmd[i].xProc, c, 0);
  }
  return TCL_OK;
}
#endif
