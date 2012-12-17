/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Main file for the SQLite library.  The routines in this file
** implement the programmer interface to the library.  Routines in
** other files are for internal use by SQLite and should not be
** accessed by users of the library.
*/
#include "sqliteInt.h"

#ifdef SQLITE4_ENABLE_FTS3
# include "fts3.h"
#endif
#ifdef SQLITE4_ENABLE_RTREE
# include "rtree.h"
#endif
#ifdef SQLITE4_ENABLE_ICU
# include "sqliteicu.h"
#endif

/*
** Dummy function used as a unique symbol for SQLITE4_DYNAMIC
*/
void sqlite4_dynamic(void *p){ (void)p; }

#ifndef SQLITE4_AMALGAMATION
/* IMPLEMENTATION-OF: R-46656-45156 The sqlite4_version[] string constant
** contains the text of SQLITE4_VERSION macro. 
*/
const char sqlite4_version[] = SQLITE4_VERSION;
#endif

/* IMPLEMENTATION-OF: R-53536-42575 The sqlite4_libversion() function returns
** a pointer to the to the sqlite4_version[] string constant. 
*/
const char *sqlite4_libversion(void){ return SQLITE4_VERSION; }

/* IMPLEMENTATION-OF: R-63124-39300 The sqlite4_sourceid() function returns a
** pointer to a string constant whose value is the same as the
** SQLITE4_SOURCE_ID C preprocessor macro. 
*/
const char *sqlite4_sourceid(void){ return SQLITE4_SOURCE_ID; }

/* IMPLEMENTATION-OF: R-35210-63508 The sqlite4_libversion_number() function
** returns an integer equal to SQLITE4_VERSION_NUMBER.
*/
int sqlite4_libversion_number(void){ return SQLITE4_VERSION_NUMBER; }

/* Return the thread-safety setting.
*/
int sqlite4_threadsafe(sqlite4_env *pEnv){
  if( pEnv==0 ) pEnv = &sqlite4DefaultEnv;
  return pEnv->bCoreMutex + pEnv->bFullMutex;
}

#if !defined(SQLITE4_OMIT_TRACE) && defined(SQLITE4_ENABLE_IOTRACE)
/*
** If the following function pointer is not NULL and if
** SQLITE4_ENABLE_IOTRACE is enabled, then messages describing
** I/O active are written using this function.  These messages
** are intended for debugging activity only.
*/
void (*sqlite4IoTrace)(const char*, ...) = 0;
#endif

/*
** Initialize SQLite.  
**
** This routine must be called to initialize the run-time environment
** As long as you do not compile with SQLITE4_OMIT_AUTOINIT
** this routine will be called automatically by key routines such as
** sqlite4_open().  
**
** This routine is a no-op except on its very first call for a given
** sqlite4_env object, or for the first call after a call to sqlite4_shutdown.
**
** This routine is not threadsafe.  It should be called from a single
** thread to initialized the library in a multi-threaded system.  Other
** threads should avoid using the sqlite4_env object until after it has
** completely initialized.
*/
int sqlite4_initialize(sqlite4_env *pEnv){
  MUTEX_LOGIC( sqlite4_mutex *pMaster; )       /* The main static mutex */
  int rc;                                      /* Result code */

  if( pEnv==0 ) pEnv = &sqlite4DefaultEnv;

  /* If SQLite is already completely initialized, then this call
  ** to sqlite4_initialize() should be a no-op.  But the initialization
  ** must be complete.  So isInit must not be set until the very end
  ** of this routine.
  */
  if( pEnv->isInit ) return SQLITE4_OK;

  /* Initialize the mutex subsystem
  */
  rc = sqlite4MutexInit(pEnv);
  if( rc ){
    sqlite4MallocEnd(pEnv);
    return rc;
  }

  /* Initialize the memory allocation subsystem
  */
  rc = sqlite4MallocInit(pEnv);
  if( rc ) return rc;

  /* Create required mutexes
  */
  if( pEnv->bCoreMutex ){
    pEnv->pMemMutex = sqlite4MutexAlloc(pEnv, SQLITE4_MUTEX_FAST);
    pEnv->pPrngMutex = sqlite4MutexAlloc(pEnv, SQLITE4_MUTEX_FAST);
    pEnv->pFactoryMutex = sqlite4MutexAlloc(pEnv, SQLITE4_MUTEX_FAST);
    if( pEnv->pMemMutex==0
     || pEnv->pPrngMutex==0
     || pEnv->pFactoryMutex==0
    ){
      rc = SQLITE4_NOMEM;
    }
  }else{
    pEnv->pMemMutex = 0;
    pEnv->pPrngMutex = 0;
  }
  pEnv->isInit = 1;

  sqlite4OsInit(pEnv);

  /* Register global functions */
  if( rc==SQLITE4_OK ){
    sqlite4RegisterGlobalFunctions(pEnv);
  }

  /* The following is just a sanity check to make sure SQLite has
  ** been compiled correctly.  It is important to run this code, but
  ** we don't want to run it too often and soak up CPU cycles for no
  ** reason.  So we run it once during initialization.
  */
#ifndef NDEBUG
#ifndef SQLITE4_OMIT_FLOATING_POINT
  /* This section of code's only "output" is via assert() statements. */
  if ( rc==SQLITE4_OK ){
    u64 x = (((u64)1)<<63)-1;
    double y;
    assert(sizeof(x)==8);
    assert(sizeof(x)==sizeof(y));
    memcpy(&y, &x, 8);
    assert( sqlite4IsNaN(y) );
  }
#endif
#endif

  return rc;
}

/*
** Undo the effects of sqlite4_initialize().  Must not be called while
** there are outstanding database connections or memory allocations or
** while any part of SQLite is otherwise in use in any thread.  This
** routine is not threadsafe.  But it is safe to invoke this routine
** on when SQLite is already shut down.  If SQLite is already shut down
** when this routine is invoked, then this routine is a harmless no-op.
*/
int sqlite4_shutdown(sqlite4_env *pEnv){
  if( pEnv==0 ) pEnv = &sqlite4DefaultEnv;
  if( pEnv->isInit ){
    KVFactory *pMkr;
    sqlite4_mutex_free(pEnv->pFactoryMutex);
    sqlite4_mutex_free(pEnv->pPrngMutex);
    sqlite4_mutex_free(pEnv->pMemMutex);
    pEnv->pMemMutex = 0;
    while( (pMkr = pEnv->pFactory)!=0 && pMkr->isPerm==0 ){
      KVFactory *pNext = pMkr->pNext;
      sqlite4_free(pEnv, pMkr);
      pMkr = pNext;
    }
    sqlite4MutexEnd(pEnv);
    sqlite4MallocEnd(pEnv);
    pEnv->isInit = 0;
  }
  return SQLITE4_OK;
}

/*
** Return the size of an sqlite4_env object
*/
int sqlite4_env_size(void){ return sizeof(sqlite4_env); }

/*
** This API allows applications to modify the configuration described by
** an sqlite4_env object.
*/
int sqlite4_env_config(sqlite4_env *pEnv, int op, ...){
  va_list ap;
  int rc = SQLITE4_OK;

  if( pEnv==0 ) pEnv = sqlite4_env_default();

  va_start(ap, op);
  switch( op ){
    /*
    ** sqlite4_env_config(pEnv, SQLITE4_ENVCONFIG_INIT, template);
    **
    ** Turn bulk memory into a new sqlite4_env object.  The template is
    ** a prior sqlite4_env that is used as a template in initializing the
    ** new sqlite4_env object.  The size of the bulk memory must be at
    ** least as many bytes as returned from sqlite4_env_size().
    */
    case SQLITE4_ENVCONFIG_INIT: {
      /* Disable all mutexing */
      sqlite4_env *pTemplate = va_arg(ap, sqlite4_env*);
      int n = pTemplate->nByte;
      if( n>sizeof(sqlite4_env) ) n = sizeof(sqlite4_env);
      memcpy(pEnv, pTemplate, n);
      pEnv->pFactory = &sqlite4BuiltinFactory;
      pEnv->isInit = 0;
      break;
    }

    /* Mutex configuration options are only available in a threadsafe
    ** compile. 
    */
#if defined(SQLITE4_THREADSAFE) && SQLITE4_THREADSAFE>0
    /*
    ** sqlite4_env_config(pEnv, SQLITE4_ENVCONFIG_SINGLETHREAD);
    **
    ** Configure this environment for a single-threaded application.
    */
    case SQLITE4_ENVCONFIG_SINGLETHREAD: {
      /* Disable all mutexing */
      if( pEnv->isInit ){ rc = SQLITE4_MISUSE; break; }
      pEnv->bCoreMutex = 0;
      pEnv->bFullMutex = 0;
      break;
    }

    /*
    ** sqlite4_env_config(pEnv, SQLITE4_ENVCONFIG_MULTITHREAD);
    **
    ** Configure this environment for a multi-threaded application where
    ** the same database connection is never used by more than a single
    ** thread at a time.
    */
    case SQLITE4_ENVCONFIG_MULTITHREAD: {
      /* Disable mutexing of database connections */
      /* Enable mutexing of core data structures */
      if( pEnv->isInit ){ rc = SQLITE4_MISUSE; break; }
      pEnv->bCoreMutex = 1;
      pEnv->bFullMutex = 0;
      break;
    }

    /*
    ** sqlite4_env_config(pEnv, SQLITE4_ENVCONFIG_MULTITHREAD);
    **
    ** Configure this environment for an unrestricted multi-threaded
    ** application where any thread can do whatever it wants with any
    ** database connection at any time.
    */
    case SQLITE4_ENVCONFIG_SERIALIZED: {
      /* Enable all mutexing */
      if( pEnv->isInit ){ rc = SQLITE4_MISUSE; break; }
      pEnv->bCoreMutex = 1;
      pEnv->bFullMutex = 1;
      break;
    }

    /*
    ** sqlite4_env_config(pEnv, SQLITE4_ENVCONFIG_MUTEXT, sqlite4_mutex_methods*)
    **
    ** Configure this environment to use the mutex routines specified by the
    ** argument.
    */
    case SQLITE4_ENVCONFIG_MUTEX: {
      /* Specify an alternative mutex implementation */
      if( pEnv->isInit ){ rc = SQLITE4_MISUSE; break; }
      pEnv->mutex = *va_arg(ap, sqlite4_mutex_methods*);
      break;
    }

    /*
    ** sqlite4_env_config(p, SQLITE4_ENVCONFIG_GETMUTEX, sqlite4_mutex_methods*)
    **
    ** Copy the mutex routines in use by this environment into the structure
    ** given in the argument.
    */
    case SQLITE4_ENVCONFIG_GETMUTEX: {
      /* Retrieve the current mutex implementation */
      *va_arg(ap, sqlite4_mutex_methods*) = pEnv->mutex;
      break;
    }
#endif


    /*
    ** sqlite4_env_config(p, SQLITE4_ENVCONFIG_MALLOC, sqlite4_mem_methods*)
    **
    ** Set the memory allocation routines to be used by this environment.
    */
    case SQLITE4_ENVCONFIG_MALLOC: {
      /* Specify an alternative malloc implementation */
      if( pEnv->isInit ) return SQLITE4_MISUSE;
      pEnv->m = *va_arg(ap, sqlite4_mem_methods*);
      break;
    }

    /*
    ** sqlite4_env_config(p, SQLITE4_ENVCONFIG_GETMALLOC, sqlite4_mem_methods*)
    **
    ** Copy the memory allocation routines in use by this environment
    ** into the structure given in the argument.
    */
    case SQLITE4_ENVCONFIG_GETMALLOC: {
      /* Retrieve the current malloc() implementation */
      if( pEnv->m.xMalloc==0 ) sqlite4MemSetDefault(pEnv);
      *va_arg(ap, sqlite4_mem_methods*) = pEnv->m;
      break;
    }

    /* sqlite4_env_config(p, SQLITE4_ENVCONFIG_MEMSTAT, int onoff);
    **
    ** Enable or disable collection of memory usage statistics according to
    ** the onoff parameter.  
    */
    case SQLITE4_ENVCONFIG_MEMSTATUS: {
      /* Enable or disable the malloc status collection */
      pEnv->bMemstat = va_arg(ap, int);
      break;
    }

    /*
    ** sqlite4_env_config(p, SQLITE4_ENVCONFIG_LOOKASIDE, size, count);
    **
    ** Set the default lookaside memory settings for all subsequent
    ** database connections constructed in this environment.  The size
    ** parameter is the size of each lookaside memory buffer and the
    ** count parameter is the number of lookaside buffers.  Set both
    ** to zero to disable lookaside memory.
    */
    case SQLITE4_ENVCONFIG_LOOKASIDE: {
      pEnv->szLookaside = va_arg(ap, int);
      pEnv->nLookaside = va_arg(ap, int);
      break;
    }
    
    /*
    ** sqlite4_env_config(p, SQLITE4_ENVCONFIG_LOG, xOutput, pArg);
    **
    ** Set the log function that is called in response to sqlite4_log()
    ** calls.
    */
    case SQLITE4_ENVCONFIG_LOG: {
      /* MSVC is picky about pulling func ptrs from va lists.
      ** http://support.microsoft.com/kb/47961
      ** pEnv->xLog = va_arg(ap, void(*)(void*,int,const char*));
      */
      typedef void(*LOGFUNC_t)(void*,int,const char*);
      pEnv->xLog = va_arg(ap, LOGFUNC_t);
      pEnv->pLogArg = va_arg(ap, void*);
      break;
    }

    /*
    ** sqlite4_env_config(pEnv, SQLITE4_ENVCONFIG_KVSTORE_PUSH, zName, xFactory);
    **
    ** Push a new KVStore factory onto the factory stack.  The new factory
    ** takes priority over prior factories.
    */
    case SQLITE4_ENVCONFIG_KVSTORE_PUSH: {
      typedef int(*PFactory)(sqlite4_env*, KVStore **, const char *, unsigned int);
      const char *zName = va_arg(ap, const char*);
      int nName = sqlite4Strlen30(zName);
      KVFactory *pMkr = sqlite4_malloc(pEnv, sizeof(*pMkr)+nName+1);
      char *z;
      if( pMkr==0 ) return SQLITE4_NOMEM;
      z = (char*)&pMkr[1];
      memcpy(z, zName, nName+1);
      memset(pMkr, 0, sizeof(*pMkr));
      pMkr->zName = z;
      pMkr->xFactory = va_arg(ap, PFactory);
      sqlite4_mutex_enter(pEnv->pFactoryMutex);
      pMkr->pNext = pEnv->pFactory;
      pEnv->pFactory = pMkr;
      sqlite4_mutex_leave(pEnv->pFactoryMutex);
      break;
    }

    /*
    ** sqlite4_env_config(pEnv, SQLITE4_ENVCONFIG_KVSTORE_POP, zName, &pxFact);
    **
    ** Remove a KVStore factory from the stack.
    */
    /*
    ** sqlite4_env_config(pEnv, SQLITE4_ENVCONFIG_KVSTORE_GET, zName, &pxFact);
    **
    ** Get the current factory pointer with the given name but leave the
    ** factory on the stack.
    */
    case SQLITE4_ENVCONFIG_KVSTORE_POP:
    case SQLITE4_ENVCONFIG_KVSTORE_GET: {
      typedef int (**PxFact)(sqlite4_env*,KVStore**,const char*,unsigned);
      const char *zName = va_arg(ap, const char*);
      KVFactory *pMkr, **ppPrev;
      PxFact pxFact;

      pxFact = va_arg(ap,PxFact);
      *pxFact = 0;
      sqlite4_mutex_enter(pEnv->pFactoryMutex);
      ppPrev = &pEnv->pFactory;
      pMkr = *ppPrev;
      while( pMkr && strcmp(zName, pMkr->zName)!=0 ){
        ppPrev = &pMkr->pNext;
        pMkr = *ppPrev;
      }
      if( pMkr ){
        *pxFact = pMkr->xFactory;
        if( op==SQLITE4_ENVCONFIG_KVSTORE_POP && pMkr->isPerm==0 ){
          *ppPrev = pMkr->pNext;
          sqlite4_free(pEnv, pMkr);
        }
      }
      sqlite4_mutex_leave(pEnv->pFactoryMutex);
      break;
    }


    default: {
      rc = SQLITE4_ERROR;
      break;
    }
  }
  va_end(ap);
  return rc;
}

/*
** Set up the lookaside buffers for a database connection.
** Return SQLITE4_OK on success.  
** If lookaside is already active, return SQLITE4_BUSY.
**
** The sz parameter is the number of bytes in each lookaside slot.
** The cnt parameter is the number of slots.  If pStart is NULL the
** space for the lookaside memory is obtained from sqlite4_malloc().
** If pStart is not NULL then it is sz*cnt bytes of memory to use for
** the lookaside memory.
*/
static int setupLookaside(sqlite4 *db, void *pBuf, int sz, int cnt){
  void *pStart;
  if( db->lookaside.nOut ){
    return SQLITE4_BUSY;
  }
  /* Free any existing lookaside buffer for this handle before
  ** allocating a new one so we don't have to have space for 
  ** both at the same time.
  */
  if( db->lookaside.bMalloced ){
    sqlite4_free(db->pEnv, db->lookaside.pStart);
  }
  /* The size of a lookaside slot after ROUNDDOWN8 needs to be larger
  ** than a pointer to be useful.
  */
  sz = ROUNDDOWN8(sz);  /* IMP: R-33038-09382 */
  if( sz<=(int)sizeof(LookasideSlot*) ) sz = 0;
  if( cnt<0 ) cnt = 0;
  if( sz==0 || cnt==0 ){
    sz = 0;
    pStart = 0;
  }else if( pBuf==0 ){
    sqlite4BeginBenignMalloc(db->pEnv);
    pStart = sqlite4Malloc(db->pEnv, sz*cnt );  /* IMP: R-61949-35727 */
    sqlite4EndBenignMalloc(db->pEnv);
    if( pStart ) cnt = sqlite4MallocSize(db->pEnv, pStart)/sz;
  }else{
    pStart = pBuf;
  }
  db->lookaside.pStart = pStart;
  db->lookaside.pFree = 0;
  db->lookaside.sz = (u16)sz;
  if( pStart ){
    int i;
    LookasideSlot *p;
    assert( sz > (int)sizeof(LookasideSlot*) );
    p = (LookasideSlot*)pStart;
    for(i=cnt-1; i>=0; i--){
      p->pNext = db->lookaside.pFree;
      db->lookaside.pFree = p;
      p = (LookasideSlot*)&((u8*)p)[sz];
    }
    db->lookaside.pEnd = p;
    db->lookaside.bEnabled = 1;
    db->lookaside.bMalloced = pBuf==0 ?1:0;
  }else{
    db->lookaside.pEnd = 0;
    db->lookaside.bEnabled = 0;
    db->lookaside.bMalloced = 0;
  }
  return SQLITE4_OK;
}

/*
** Return the mutex associated with a database connection.
*/
sqlite4_mutex *sqlite4_db_mutex(sqlite4 *db){
  return db->mutex;
}

/*
** Free up as much memory as we can from the given database
** connection.
*/
int sqlite4_db_release_memory(sqlite4 *db){
  sqlite4_mutex_enter(db->mutex);
  sqlite4_mutex_leave(db->mutex);
  return SQLITE4_OK;
}

/*
** Configuration settings for an individual database connection
*/
int sqlite4_db_config(sqlite4 *db, int op, ...){
  va_list ap;
  int rc;
  va_start(ap, op);
  switch( op ){
    case SQLITE4_DBCONFIG_LOOKASIDE: {
      void *pBuf = va_arg(ap, void*); /* IMP: R-26835-10964 */
      int sz = va_arg(ap, int);       /* IMP: R-47871-25994 */
      int cnt = va_arg(ap, int);      /* IMP: R-04460-53386 */
      rc = setupLookaside(db, pBuf, sz, cnt);
      break;
    }
    default: {
      static const struct {
        int op;      /* The opcode */
        u32 mask;    /* Mask of the bit in sqlite4.flags to set/clear */
      } aFlagOp[] = {
        { SQLITE4_DBCONFIG_ENABLE_FKEY,    SQLITE4_ForeignKeys    },
        { SQLITE4_DBCONFIG_ENABLE_TRIGGER, SQLITE4_EnableTrigger  },
      };
      unsigned int i;
      rc = SQLITE4_ERROR; /* IMP: R-42790-23372 */
      for(i=0; i<ArraySize(aFlagOp); i++){
        if( aFlagOp[i].op==op ){
          int onoff = va_arg(ap, int);
          int *pRes = va_arg(ap, int*);
          int oldFlags = db->flags;
          if( onoff>0 ){
            db->flags |= aFlagOp[i].mask;
          }else if( onoff==0 ){
            db->flags &= ~aFlagOp[i].mask;
          }
          if( oldFlags!=db->flags ){
            sqlite4ExpirePreparedStatements(db);
          }
          if( pRes ){
            *pRes = (db->flags & aFlagOp[i].mask)!=0;
          }
          rc = SQLITE4_OK;
          break;
        }
      }
      break;
    }
  }
  va_end(ap);
  return rc;
}


/*
** Return true if the buffer z[0..n-1] contains all spaces.
*/
static int allSpaces(const char *z, int n){
  while( n>0 && z[n-1]==' ' ){ n--; }
  return n==0;
}

/*
** This is the default collating function named "BINARY" which is always
** available.
**
** If the padFlag argument is not NULL then space padding at the end
** of strings is ignored.  This implements the RTRIM collation.
*/
static int binCollFunc(
  void *padFlag,
  int nKey1, const void *pKey1,
  int nKey2, const void *pKey2
){
  int rc, n;
  n = nKey1<nKey2 ? nKey1 : nKey2;
  rc = memcmp(pKey1, pKey2, n);
  if( rc==0 ){
    if( padFlag
     && allSpaces(((char*)pKey1)+n, nKey1-n)
     && allSpaces(((char*)pKey2)+n, nKey2-n)
    ){
      /* Leave rc unchanged at 0 */
    }else{
      rc = nKey1 - nKey2;
    }
  }
  return rc;
}

/*
** The xMakeKey callback for the built-in RTRIM collation. The output
** is the same as the input, with any trailing ' ' characters removed.
** (e.g.  " abc   "  ->   " abc").
*/
static int collRtrimMkKey(
  void *NotUsed,                  /* Not used */
  int nIn, const void *pIn,       /* Input text. UTF-8. */
  int nOut, void *pOut            /* Output buffer */
){
  int nCopy = nIn;
  while( nCopy>0 && ((const char *)pIn)[nCopy-1]==' ' ) nCopy--;
  if( nCopy<=nOut ){
    memcpy(pOut, pIn, nCopy);
  }
  return nCopy;
}

/*
** Another built-in collating sequence: NOCASE. 
**
** This collating sequence is intended to be used for "case independant
** comparison". SQLite's knowledge of upper and lower case equivalents
** extends only to the 26 characters used in the English language.
**
** At the moment there is only a UTF-8 implementation.
*/
static int collNocaseCmp(
  void *NotUsed,
  int nKey1, const void *pKey1,
  int nKey2, const void *pKey2
){
  int r = sqlite4StrNICmp(
      (const char *)pKey1, (const char *)pKey2, (nKey1<nKey2)?nKey1:nKey2);
  UNUSED_PARAMETER(NotUsed);
  if( 0==r ){
    r = nKey1-nKey2;
  }
  return r;
}

static int collNocaseMkKey(
  void *NotUsed,
  int nIn, const void *pKey1,
  int nOut, void *pKey2
){
  if( nOut>=nIn ){
    int i;
    u8 *aIn = (u8 *)pKey1;
    u8 *aOut = (u8 *)pKey2;
    for(i=0; i<nIn; i++){
      aOut[i] = sqlite4UpperToLower[aIn[i]];
    }
  }
  return nIn;
}

/*
** Return the ROWID of the most recent insert
*/
sqlite4_int64 sqlite4_last_insert_rowid(sqlite4 *db){
  return db->lastRowid;
}

/*
** Return the number of changes in the most recent call to sqlite4_exec().
*/
int sqlite4_changes(sqlite4 *db){
  return db->nChange;
}

/*
** Return the number of changes since the database handle was opened.
*/
int sqlite4_total_changes(sqlite4 *db){
  return db->nTotalChange;
}

/*
** Close all open savepoints. This function only manipulates fields of the
** database handle object, it does not close any savepoints that may be open
** at the b-tree/pager level.
*/
void sqlite4CloseSavepoints(sqlite4 *db){
  while( db->pSavepoint ){
    Savepoint *pTmp = db->pSavepoint;
    db->pSavepoint = pTmp->pNext;
    sqlite4DbFree(db, pTmp);
  }
  db->nSavepoint = 0;
  db->nStatement = 0;
}

/*
** Invoke the destructor function associated with FuncDef p, if any. Except,
** if this is not the last copy of the function, do not invoke it. Multiple
** copies of a single function are created when create_function() is called
** with SQLITE4_ANY as the encoding.
*/
static void functionDestroy(sqlite4 *db, FuncDef *p){
  FuncDestructor *pDestructor = p->pDestructor;
  if( pDestructor ){
    pDestructor->nRef--;
    if( pDestructor->nRef==0 ){
      pDestructor->xDestroy(pDestructor->pUserData);
      sqlite4DbFree(db, pDestructor);
    }
  }
}

/*
** Close an existing SQLite database
*/
int sqlite4_close(sqlite4 *db){
  HashElem *i;                    /* Hash table iterator */
  int j;

  if( !db ){
    return SQLITE4_OK;
  }
  if( !sqlite4SafetyCheckSickOrOk(db) ){
    return SQLITE4_MISUSE_BKPT;
  }
  sqlite4_mutex_enter(db->mutex);

  /* Force xDestroy calls on all virtual tables */
  sqlite4ResetInternalSchema(db, -1);

  /* If a transaction is open, the ResetInternalSchema() call above
  ** will not have called the xDisconnect() method on any virtual
  ** tables in the db->aVTrans[] array. The following sqlite4VtabRollback()
  ** call will do so. We need to do this before the check for active
  ** SQL statements below, as the v-table implementation may be storing
  ** some prepared statements internally.
  */
  sqlite4VtabRollback(db);

  /* If there are any outstanding VMs, return SQLITE4_BUSY. */
  if( db->pVdbe ){
    sqlite4Error(db, SQLITE4_BUSY, 
        "unable to close due to unfinalised statements");
    sqlite4_mutex_leave(db->mutex);
    return SQLITE4_BUSY;
  }
  assert( sqlite4SafetyCheckSickOrOk(db) );

  /* Free any outstanding Savepoint structures. */
  sqlite4CloseSavepoints(db);

  for(j=0; j<db->nDb; j++){
    struct Db *pDb = &db->aDb[j];
    if( pDb->pKV ){
      sqlite4KVStoreClose(pDb->pKV);
      pDb->pKV = 0;
    }
    sqlite4DbFree(db, pDb->pSchema);
    pDb->pSchema = 0;
  }
  sqlite4ResetInternalSchema(db, -1);

  /* Tell the code in notify.c that the connection no longer holds any
  ** locks and does not require any further unlock-notify callbacks.
  */
  sqlite4ConnectionClosed(db);
  
  /* Delete tokenizers */
  sqlite4ShutdownFts5(db);

  assert( db->nDb<=2 );
  assert( db->aDb==db->aDbStatic );
  {
    FuncDef *pNext, *pSame, *p;
    for(p=db->aFunc.pFirst; p; p=pNext){
      pNext = p->pNextName;
      while( p ){
        functionDestroy(db, p);
        pSame = p->pSameName;
        sqlite4DbFree(db, p);
        p = pSame;
      }
    }
  }
  for(i=sqliteHashFirst(&db->aCollSeq); i; i=sqliteHashNext(i)){
    CollSeq *pColl = (CollSeq *)sqliteHashData(i);
    /* Invoke any destructors registered for collation sequence user data. */
    for(j=0; j<3; j++){
      if( pColl[j].xDel ){
        pColl[j].xDel(pColl[j].pUser);
      }
    }
    sqlite4DbFree(db, pColl);
  }
  sqlite4HashClear(&db->aCollSeq);
#ifndef SQLITE4_OMIT_VIRTUALTABLE
  for(i=sqliteHashFirst(&db->aModule); i; i=sqliteHashNext(i)){
    Module *pMod = (Module *)sqliteHashData(i);
    if( pMod->xDestroy ){
      pMod->xDestroy(pMod->pAux);
    }
    sqlite4DbFree(db, pMod);
  }
  sqlite4HashClear(&db->aModule);
#endif

  sqlite4Error(db, SQLITE4_OK, 0); /* Deallocates any cached error strings. */
  if( db->pErr ){
    sqlite4ValueFree(db->pErr);
  }

  db->magic = SQLITE4_MAGIC_ERROR;
  sqlite4_mutex_leave(db->mutex);
  db->magic = SQLITE4_MAGIC_CLOSED;
  sqlite4_mutex_free(db->mutex);
  assert( db->lookaside.nOut==0 );  /* Fails on a lookaside memory leak */
  if( db->lookaside.bMalloced ){
    sqlite4_free(db->pEnv, db->lookaside.pStart);
  }
  sqlite4_free(db->pEnv, db);
  return SQLITE4_OK;
}

/*
** Return a static string that describes the kind of error specified in the
** argument.
*/
const char *sqlite4ErrStr(int rc){
  static const char* const aMsg[] = {
    /* SQLITE4_OK          */ "not an error",
    /* SQLITE4_ERROR       */ "SQL logic error or missing database",
    /* SQLITE4_INTERNAL    */ 0,
    /* SQLITE4_PERM        */ "access permission denied",
    /* SQLITE4_ABORT       */ "callback requested query abort",
    /* SQLITE4_BUSY        */ "database is locked",
    /* SQLITE4_LOCKED      */ "database table is locked",
    /* SQLITE4_NOMEM       */ "out of memory",
    /* SQLITE4_READONLY    */ "attempt to write a readonly database",
    /* SQLITE4_INTERRUPT   */ "interrupted",
    /* SQLITE4_IOERR       */ "disk I/O error",
    /* SQLITE4_CORRUPT     */ "database disk image is malformed",
    /* SQLITE4_NOTFOUND    */ "unknown operation",
    /* SQLITE4_FULL        */ "database or disk is full",
    /* SQLITE4_CANTOPEN    */ "unable to open database file",
    /* SQLITE4_PROTOCOL    */ "locking protocol",
    /* SQLITE4_EMPTY       */ "table contains no data",
    /* SQLITE4_SCHEMA      */ "database schema has changed",
    /* SQLITE4_TOOBIG      */ "string or blob too big",
    /* SQLITE4_CONSTRAINT  */ "constraint failed",
    /* SQLITE4_MISMATCH    */ "datatype mismatch",
    /* SQLITE4_MISUSE      */ "library routine called out of sequence",
    /* SQLITE4_NOLFS       */ "large file support is disabled",
    /* SQLITE4_AUTH        */ "authorization denied",
    /* SQLITE4_FORMAT      */ "auxiliary database format error",
    /* SQLITE4_RANGE       */ "bind or column index out of range",
    /* SQLITE4_NOTADB      */ "file is encrypted or is not a database",
  };
  rc &= 0xff;
  if( ALWAYS(rc>=0) && rc<(int)(sizeof(aMsg)/sizeof(aMsg[0])) && aMsg[rc]!=0 ){
    return aMsg[rc];
  }else{
    return "unknown error";
  }
}

#ifndef SQLITE4_OMIT_PROGRESS_CALLBACK
/*
** This routine sets the progress callback for an Sqlite database to the
** given callback function with the given argument. The progress callback will
** be invoked every nOps opcodes.
*/
void sqlite4_progress_handler(
  sqlite4 *db, 
  int nOps,
  int (*xProgress)(void*), 
  void *pArg
){
  sqlite4_mutex_enter(db->mutex);
  if( nOps>0 ){
    db->xProgress = xProgress;
    db->nProgressOps = nOps;
    db->pProgressArg = pArg;
  }else{
    db->xProgress = 0;
    db->nProgressOps = 0;
    db->pProgressArg = 0;
  }
  sqlite4_mutex_leave(db->mutex);
}
#endif

/*
** Cause any pending operation to stop at its earliest opportunity.
*/
void sqlite4_interrupt(sqlite4 *db){
  db->u1.isInterrupted = 1;
}


/*
** This function is exactly the same as sqlite4_create_function(), except
** that it is designed to be called by internal code. The difference is
** that if a malloc() fails in sqlite4_create_function(), an error code
** is returned and the mallocFailed flag cleared. 
*/
int sqlite4CreateFunc(
  sqlite4 *db,
  const char *zFunctionName,
  int nArg,
  int enc,
  void *pUserData,
  void (*xFunc)(sqlite4_context*,int,sqlite4_value **),
  void (*xStep)(sqlite4_context*,int,sqlite4_value **),
  void (*xFinal)(sqlite4_context*),
  FuncDestructor *pDestructor
){
  FuncDef *p;
  int nName;

  assert( sqlite4_mutex_held(db->mutex) );
  if( zFunctionName==0 ||
      (xFunc && (xFinal || xStep)) || 
      (!xFunc && (xFinal && !xStep)) ||
      (!xFunc && (!xFinal && xStep)) ||
      (nArg<-1 || nArg>SQLITE4_MAX_FUNCTION_ARG) ||
      (255<(nName = sqlite4Strlen30( zFunctionName))) ){
    return SQLITE4_MISUSE_BKPT;
  }
  
#ifndef SQLITE4_OMIT_UTF16
  /* If SQLITE4_UTF16 is specified as the encoding type, transform this
  ** to one of SQLITE4_UTF16LE or SQLITE4_UTF16BE using the
  ** SQLITE4_UTF16NATIVE macro. SQLITE4_UTF16 is not used internally.
  **
  ** If SQLITE4_ANY is specified, add three versions of the function
  ** to the hash table.
  */
  if( enc==SQLITE4_UTF16 ){
    enc = SQLITE4_UTF16NATIVE;
  }else if( enc==SQLITE4_ANY ){
    int rc;
    rc = sqlite4CreateFunc(db, zFunctionName, nArg, SQLITE4_UTF8,
         pUserData, xFunc, xStep, xFinal, pDestructor);
    if( rc==SQLITE4_OK ){
      rc = sqlite4CreateFunc(db, zFunctionName, nArg, SQLITE4_UTF16LE,
          pUserData, xFunc, xStep, xFinal, pDestructor);
    }
    if( rc!=SQLITE4_OK ){
      return rc;
    }
    enc = SQLITE4_UTF16BE;
  }
#else
  enc = SQLITE4_UTF8;
#endif
  
  /* Check if an existing function is being overridden or deleted. If so,
  ** and there are active VMs, then return SQLITE4_BUSY. If a function
  ** is being overridden/deleted but there are no active VMs, allow the
  ** operation to continue but invalidate all precompiled statements.
  */
  p = sqlite4FindFunction(db, zFunctionName, nName, nArg, (u8)enc, 0);
  if( p && p->iPrefEnc==enc && p->nArg==nArg ){
    if( db->activeVdbeCnt ){
      sqlite4Error(db, SQLITE4_BUSY, 
        "unable to delete/modify user-function due to active statements");
      assert( !db->mallocFailed );
      return SQLITE4_BUSY;
    }else{
      sqlite4ExpirePreparedStatements(db);
    }
  }

  p = sqlite4FindFunction(db, zFunctionName, nName, nArg, (u8)enc, 1);
  assert(p || db->mallocFailed);
  if( !p ){
    return SQLITE4_NOMEM;
  }

  /* If an older version of the function with a configured destructor is
  ** being replaced invoke the destructor function here. */
  functionDestroy(db, p);

  if( pDestructor ){
    pDestructor->nRef++;
  }
  p->pDestructor = pDestructor;
  p->flags = 0;
  p->xFunc = xFunc;
  p->xStep = xStep;
  p->xFinalize = xFinal;
  p->pUserData = pUserData;
  p->nArg = (u16)nArg;
  return SQLITE4_OK;
}

/*
** Create new user functions.
*/
int sqlite4_create_function(
  sqlite4 *db,
  const char *zFunc,
  int nArg,
  int enc,
  void *p,
  void (*xFunc)(sqlite4_context*,int,sqlite4_value **),
  void (*xStep)(sqlite4_context*,int,sqlite4_value **),
  void (*xFinal)(sqlite4_context*)
){
  return sqlite4_create_function_v2(db, zFunc, nArg, enc, p, xFunc, xStep,
                                    xFinal, 0);
}

int sqlite4_create_function_v2(
  sqlite4 *db,
  const char *zFunc,
  int nArg,
  int enc,
  void *p,
  void (*xFunc)(sqlite4_context*,int,sqlite4_value **),
  void (*xStep)(sqlite4_context*,int,sqlite4_value **),
  void (*xFinal)(sqlite4_context*),
  void (*xDestroy)(void *)
){
  int rc = SQLITE4_ERROR;
  FuncDestructor *pArg = 0;
  sqlite4_mutex_enter(db->mutex);
  if( xDestroy ){
    pArg = (FuncDestructor *)sqlite4DbMallocZero(db, sizeof(FuncDestructor));
    if( !pArg ){
      xDestroy(p);
      goto out;
    }
    pArg->xDestroy = xDestroy;
    pArg->pUserData = p;
  }
  rc = sqlite4CreateFunc(db, zFunc, nArg, enc, p, xFunc, xStep, xFinal, pArg);
  if( pArg && pArg->nRef==0 ){
    assert( rc!=SQLITE4_OK );
    xDestroy(p);
    sqlite4DbFree(db, pArg);
  }

 out:
  rc = sqlite4ApiExit(db, rc);
  sqlite4_mutex_leave(db->mutex);
  return rc;
}

#ifndef SQLITE4_OMIT_UTF16
int sqlite4_create_function16(
  sqlite4 *db,
  const void *zFunctionName,
  int nArg,
  int eTextRep,
  void *p,
  void (*xFunc)(sqlite4_context*,int,sqlite4_value**),
  void (*xStep)(sqlite4_context*,int,sqlite4_value**),
  void (*xFinal)(sqlite4_context*)
){
  int rc;
  char *zFunc8;
  sqlite4_mutex_enter(db->mutex);
  assert( !db->mallocFailed );
  zFunc8 = sqlite4Utf16to8(db, zFunctionName, -1, SQLITE4_UTF16NATIVE);
  rc = sqlite4CreateFunc(db, zFunc8, nArg, eTextRep, p, xFunc, xStep, xFinal,0);
  sqlite4DbFree(db, zFunc8);
  rc = sqlite4ApiExit(db, rc);
  sqlite4_mutex_leave(db->mutex);
  return rc;
}
#endif


/*
** Declare that a function has been overloaded by a virtual table.
**
** If the function already exists as a regular global function, then
** this routine is a no-op.  If the function does not exist, then create
** a new one that always throws a run-time error.  
**
** When virtual tables intend to provide an overloaded function, they
** should call this routine to make sure the global function exists.
** A global function must exist in order for name resolution to work
** properly.
*/
int sqlite4_overload_function(
  sqlite4 *db,
  const char *zName,
  int nArg
){
  int nName = sqlite4Strlen30(zName);
  int rc = SQLITE4_OK;
  sqlite4_mutex_enter(db->mutex);
  if( sqlite4FindFunction(db, zName, nName, nArg, SQLITE4_UTF8, 0)==0 ){
    rc = sqlite4CreateFunc(db, zName, nArg, SQLITE4_UTF8,
                           0, sqlite4InvalidFunction, 0, 0, 0);
  }
  rc = sqlite4ApiExit(db, rc);
  sqlite4_mutex_leave(db->mutex);
  return rc;
}

#ifndef SQLITE4_OMIT_TRACE
/*
** Register a trace function.  The pArg from the previously registered trace
** is returned.  
**
** A NULL trace function means that no tracing is executes.  A non-NULL
** trace is a pointer to a function that is invoked at the start of each
** SQL statement.
*/
void *sqlite4_trace(sqlite4 *db, void (*xTrace)(void*,const char*), void *pArg){
  void *pOld;
  sqlite4_mutex_enter(db->mutex);
  pOld = db->pTraceArg;
  db->xTrace = xTrace;
  db->pTraceArg = pArg;
  sqlite4_mutex_leave(db->mutex);
  return pOld;
}
/*
** Register a profile function.  The pArg from the previously registered 
** profile function is returned.  
**
** A NULL profile function means that no profiling is executes.  A non-NULL
** profile is a pointer to a function that is invoked at the conclusion of
** each SQL statement that is run.
*/
void *sqlite4_profile(
  sqlite4 *db,
  void (*xProfile)(void*,const char*,sqlite4_uint64),
  void *pArg
){
  void *pOld;
  sqlite4_mutex_enter(db->mutex);
  pOld = db->pProfileArg;
  db->xProfile = xProfile;
  db->pProfileArg = pArg;
  sqlite4_mutex_leave(db->mutex);
  return pOld;
}
#endif /* SQLITE4_OMIT_TRACE */

/*
** This function returns true if main-memory should be used instead of
** a temporary file for transient pager files and statement journals.
** The value returned depends on the value of db->temp_store (runtime
** parameter) and the compile time value of SQLITE4_TEMP_STORE. The
** following table describes the relationship between these two values
** and this functions return value.
**
**   SQLITE4_TEMP_STORE     db->temp_store     Location of temporary database
**   -----------------     --------------     ------------------------------
**   0                     any                file      (return 0)
**   1                     1                  file      (return 0)
**   1                     2                  memory    (return 1)
**   1                     0                  file      (return 0)
**   2                     1                  file      (return 0)
**   2                     2                  memory    (return 1)
**   2                     0                  memory    (return 1)
**   3                     any                memory    (return 1)
*/
int sqlite4TempInMemory(const sqlite4 *db){
#if SQLITE4_TEMP_STORE==1
  return ( db->temp_store==2 );
#endif
#if SQLITE4_TEMP_STORE==2
  return ( db->temp_store!=1 );
#endif
#if SQLITE4_TEMP_STORE==3
  return 1;
#endif
#if SQLITE4_TEMP_STORE<1 || SQLITE4_TEMP_STORE>3
  return 0;
#endif
}

/*
** Return UTF-8 encoded English language explanation of the most recent
** error.
*/
const char *sqlite4_errmsg(sqlite4 *db){
  const char *z;
  if( !db ){
    return sqlite4ErrStr(SQLITE4_NOMEM);
  }
  if( !sqlite4SafetyCheckSickOrOk(db) ){
    return sqlite4ErrStr(SQLITE4_MISUSE_BKPT);
  }
  sqlite4_mutex_enter(db->mutex);
  if( db->mallocFailed ){
    z = sqlite4ErrStr(SQLITE4_NOMEM);
  }else{
    z = (char*)sqlite4_value_text(db->pErr);
    assert( !db->mallocFailed );
    if( z==0 ){
      z = sqlite4ErrStr(db->errCode);
    }
  }
  sqlite4_mutex_leave(db->mutex);
  return z;
}

#ifndef SQLITE4_OMIT_UTF16
/*
** Return UTF-16 encoded English language explanation of the most recent
** error.
*/
const void *sqlite4_errmsg16(sqlite4 *db){
  static const u16 outOfMem[] = {
    'o', 'u', 't', ' ', 'o', 'f', ' ', 'm', 'e', 'm', 'o', 'r', 'y', 0
  };
  static const u16 misuse[] = {
    'l', 'i', 'b', 'r', 'a', 'r', 'y', ' ', 
    'r', 'o', 'u', 't', 'i', 'n', 'e', ' ', 
    'c', 'a', 'l', 'l', 'e', 'd', ' ', 
    'o', 'u', 't', ' ', 
    'o', 'f', ' ', 
    's', 'e', 'q', 'u', 'e', 'n', 'c', 'e', 0
  };

  const void *z;
  if( !db ){
    return (void *)outOfMem;
  }
  if( !sqlite4SafetyCheckSickOrOk(db) ){
    return (void *)misuse;
  }
  sqlite4_mutex_enter(db->mutex);
  if( db->mallocFailed ){
    z = (void *)outOfMem;
  }else{
    z = sqlite4_value_text16(db->pErr);
    if( z==0 ){
      sqlite4ValueSetStr(db->pErr, -1, sqlite4ErrStr(db->errCode),
           SQLITE4_UTF8, SQLITE4_STATIC);
      z = sqlite4_value_text16(db->pErr);
    }
    /* A malloc() may have failed within the call to sqlite4_value_text16()
    ** above. If this is the case, then the db->mallocFailed flag needs to
    ** be cleared before returning. Do this directly, instead of via
    ** sqlite4ApiExit(), to avoid setting the database handle error message.
    */
    db->mallocFailed = 0;
  }
  sqlite4_mutex_leave(db->mutex);
  return z;
}
#endif /* SQLITE4_OMIT_UTF16 */

/*
** Return the most recent error code generated by an SQLite routine. If NULL is
** passed to this function, we assume a malloc() failed during sqlite4_open().
*/
int sqlite4_errcode(sqlite4 *db){
  if( db && !sqlite4SafetyCheckSickOrOk(db) ){
    return SQLITE4_MISUSE_BKPT;
  }
  if( !db || db->mallocFailed ){
    return SQLITE4_NOMEM;
  }
  return db->errCode;
}

/*
** Create a new collating function for database "db".  The name is zName
** and the encoding is enc.
*/
static int createCollation(
  sqlite4* db,
  const char *zName, 
  u8 enc,
  void* pCtx,
  int(*xCompare)(void*,int,const void*,int,const void*),
  int(*xMakeKey)(void*,int,const void*,int,void*),
  void(*xDel)(void*)
){
  CollSeq *pColl;
  int enc2;
  int nName = sqlite4Strlen30(zName);
  
  assert( sqlite4_mutex_held(db->mutex) );

  /* If SQLITE4_UTF16 is specified as the encoding type, transform this
  ** to one of SQLITE4_UTF16LE or SQLITE4_UTF16BE using the
  ** SQLITE4_UTF16NATIVE macro. SQLITE4_UTF16 is not used internally.
  */
  enc2 = enc;
  testcase( enc2==SQLITE4_UTF16 );
  testcase( enc2==SQLITE4_UTF16_ALIGNED );
  if( enc2==SQLITE4_UTF16 || enc2==SQLITE4_UTF16_ALIGNED ){
    enc2 = SQLITE4_UTF16NATIVE;
  }
  if( enc2<SQLITE4_UTF8 || enc2>SQLITE4_UTF16BE ){
    return SQLITE4_MISUSE_BKPT;
  }

  /* Check if this call is removing or replacing an existing collation 
  ** sequence. If so, and there are active VMs, return busy. If there
  ** are no active VMs, invalidate any pre-compiled statements.
  */
  pColl = sqlite4FindCollSeq(db, (u8)enc2, zName, 0);
  if( pColl && pColl->xCmp ){
    if( db->activeVdbeCnt ){
      sqlite4Error(db, SQLITE4_BUSY, 
        "unable to delete/modify collation sequence due to active statements");
      return SQLITE4_BUSY;
    }
    sqlite4ExpirePreparedStatements(db);

    /* If collation sequence pColl was created directly by a call to
    ** sqlite4_create_collation, and not generated by synthCollSeq(),
    ** then any copies made by synthCollSeq() need to be invalidated.
    ** Also, collation destructor - CollSeq.xDel() - function may need
    ** to be called.
    */ 
    if( (pColl->enc & ~SQLITE4_UTF16_ALIGNED)==enc2 ){
      CollSeq *aColl = sqlite4HashFind(&db->aCollSeq, zName, nName);
      int j;
      for(j=0; j<3; j++){
        CollSeq *p = &aColl[j];
        if( p->enc==pColl->enc ){
          if( p->xDel ){
            p->xDel(p->pUser);
          }
          p->xCmp = 0;
        }
      }
    }
  }

  pColl = sqlite4FindCollSeq(db, (u8)enc2, zName, 1);
  if( pColl==0 ) return SQLITE4_NOMEM;
  pColl->xCmp = xCompare;
  pColl->xMkKey = xMakeKey;
  pColl->pUser = pCtx;
  pColl->xDel = xDel;
  pColl->enc = (u8)(enc2 | (enc & SQLITE4_UTF16_ALIGNED));
  sqlite4Error(db, SQLITE4_OK, 0);
  return SQLITE4_OK;
}


/*
** This array defines hard upper bounds on limit values.  The
** initializer must be kept in sync with the SQLITE4_LIMIT_*
** #defines in sqlite4.h.
*/
static const int aHardLimit[] = {
  SQLITE4_MAX_LENGTH,
  SQLITE4_MAX_SQL_LENGTH,
  SQLITE4_MAX_COLUMN,
  SQLITE4_MAX_EXPR_DEPTH,
  SQLITE4_MAX_COMPOUND_SELECT,
  SQLITE4_MAX_VDBE_OP,
  SQLITE4_MAX_FUNCTION_ARG,
  SQLITE4_MAX_ATTACHED,
  SQLITE4_MAX_LIKE_PATTERN_LENGTH,
  SQLITE4_MAX_VARIABLE_NUMBER,
  SQLITE4_MAX_TRIGGER_DEPTH,
};

/*
** Make sure the hard limits are set to reasonable values
*/
#if SQLITE4_MAX_LENGTH<100
# error SQLITE4_MAX_LENGTH must be at least 100
#endif
#if SQLITE4_MAX_SQL_LENGTH<100
# error SQLITE4_MAX_SQL_LENGTH must be at least 100
#endif
#if SQLITE4_MAX_SQL_LENGTH>SQLITE4_MAX_LENGTH
# error SQLITE4_MAX_SQL_LENGTH must not be greater than SQLITE4_MAX_LENGTH
#endif
#if SQLITE4_MAX_COMPOUND_SELECT<2
# error SQLITE4_MAX_COMPOUND_SELECT must be at least 2
#endif
#if SQLITE4_MAX_VDBE_OP<40
# error SQLITE4_MAX_VDBE_OP must be at least 40
#endif
#if SQLITE4_MAX_FUNCTION_ARG<0 || SQLITE4_MAX_FUNCTION_ARG>1000
# error SQLITE4_MAX_FUNCTION_ARG must be between 0 and 1000
#endif
#if SQLITE4_MAX_ATTACHED<0 || SQLITE4_MAX_ATTACHED>62
# error SQLITE4_MAX_ATTACHED must be between 0 and 62
#endif
#if SQLITE4_MAX_LIKE_PATTERN_LENGTH<1
# error SQLITE4_MAX_LIKE_PATTERN_LENGTH must be at least 1
#endif
#if SQLITE4_MAX_COLUMN>32767
# error SQLITE4_MAX_COLUMN must not exceed 32767
#endif
#if SQLITE4_MAX_TRIGGER_DEPTH<1
# error SQLITE4_MAX_TRIGGER_DEPTH must be at least 1
#endif


/*
** Change the value of a limit.  Report the old value.
** If an invalid limit index is supplied, report -1.
** Make no changes but still report the old value if the
** new limit is negative.
**
** A new lower limit does not shrink existing constructs.
** It merely prevents new constructs that exceed the limit
** from forming.
*/
int sqlite4_limit(sqlite4 *db, int limitId, int newLimit){
  int oldLimit;


  /* EVIDENCE-OF: R-30189-54097 For each limit category SQLITE4_LIMIT_NAME
  ** there is a hard upper bound set at compile-time by a C preprocessor
  ** macro called SQLITE4_MAX_NAME. (The "_LIMIT_" in the name is changed to
  ** "_MAX_".)
  */
  assert( aHardLimit[SQLITE4_LIMIT_LENGTH]==SQLITE4_MAX_LENGTH );
  assert( aHardLimit[SQLITE4_LIMIT_SQL_LENGTH]==SQLITE4_MAX_SQL_LENGTH );
  assert( aHardLimit[SQLITE4_LIMIT_COLUMN]==SQLITE4_MAX_COLUMN );
  assert( aHardLimit[SQLITE4_LIMIT_EXPR_DEPTH]==SQLITE4_MAX_EXPR_DEPTH );
  assert( aHardLimit[SQLITE4_LIMIT_COMPOUND_SELECT]==SQLITE4_MAX_COMPOUND_SELECT);
  assert( aHardLimit[SQLITE4_LIMIT_VDBE_OP]==SQLITE4_MAX_VDBE_OP );
  assert( aHardLimit[SQLITE4_LIMIT_FUNCTION_ARG]==SQLITE4_MAX_FUNCTION_ARG );
  assert( aHardLimit[SQLITE4_LIMIT_ATTACHED]==SQLITE4_MAX_ATTACHED );
  assert( aHardLimit[SQLITE4_LIMIT_LIKE_PATTERN_LENGTH]==
                                               SQLITE4_MAX_LIKE_PATTERN_LENGTH );
  assert( aHardLimit[SQLITE4_LIMIT_VARIABLE_NUMBER]==SQLITE4_MAX_VARIABLE_NUMBER);
  assert( aHardLimit[SQLITE4_LIMIT_TRIGGER_DEPTH]==SQLITE4_MAX_TRIGGER_DEPTH );
  assert( SQLITE4_LIMIT_TRIGGER_DEPTH==(SQLITE4_N_LIMIT-1) );


  if( limitId<0 || limitId>=SQLITE4_N_LIMIT ){
    return -1;
  }
  oldLimit = db->aLimit[limitId];
  if( newLimit>=0 ){                   /* IMP: R-52476-28732 */
    if( newLimit>aHardLimit[limitId] ){
      newLimit = aHardLimit[limitId];  /* IMP: R-51463-25634 */
    }
    db->aLimit[limitId] = newLimit;
  }
  return oldLimit;                     /* IMP: R-53341-35419 */
}

/*
** This function is used to parse both URIs and non-URI filenames passed by the
** user to API functions sqlite4_open() and for database
** URIs specified as part of ATTACH statements.
**
** The first argument to this function is the name of the VFS to use (or
** a NULL to signify the default VFS) if the URI does not contain a "vfs=xxx"
** query parameter. The second argument contains the URI (or non-URI filename)
** itself. When this function is called the *pFlags variable should contain
** the default flags to open the database handle with. The value stored in
** *pFlags may be updated before returning if the URI filename contains 
** "cache=xxx" or "mode=xxx" query parameters.
**
** If successful, SQLITE4_OK is returned. In this case *ppVfs is set to point to
** the VFS that should be used to open the database file. *pzFile is set to
** point to a buffer containing the name of the file to open. It is the 
** responsibility of the caller to eventually call sqlite4_free() to release
** this buffer.
**
** If an error occurs, then an SQLite error code is returned and *pzErrMsg
** may be set to point to a buffer containing an English language error 
** message. It is the responsibility of the caller to eventually release
** this buffer by calling sqlite4_free().
*/
int sqlite4ParseUri(
  sqlite4_env *pEnv,              /* Run-time environment */
  const char *zUri,               /* Nul-terminated URI to parse */
  unsigned int *pFlags,           /* IN/OUT: SQLITE4_OPEN_XXX flags */
  char **pzFile,                  /* OUT: Filename component of URI */
  char **pzErrMsg                 /* OUT: Error message (if rc!=SQLITE4_OK) */
){
  int rc = SQLITE4_OK;
  unsigned int flags = *pFlags;
  char *zFile;
  char c;
  int nUri = sqlite4Strlen30(zUri);

  assert( *pzErrMsg==0 );

  if( nUri>=5 && memcmp(zUri, "file:", 5)==0 ){
    char *zOpt;
    int eState;                   /* Parser state when parsing URI */
    int iIn;                      /* Input character index */
    int iOut = 0;                 /* Output character index */
    int nByte = nUri+2;           /* Bytes of space to allocate */

    for(iIn=0; iIn<nUri; iIn++) nByte += (zUri[iIn]=='&');
    zFile = sqlite4_malloc(pEnv, nByte);
    if( !zFile ) return SQLITE4_NOMEM;

    /* Discard the scheme and authority segments of the URI. */
    if( zUri[5]=='/' && zUri[6]=='/' ){
      iIn = 7;
      while( zUri[iIn] && zUri[iIn]!='/' ) iIn++;

      if( iIn!=7 && (iIn!=16 || memcmp("localhost", &zUri[7], 9)) ){
        *pzErrMsg = sqlite4_mprintf(pEnv,"invalid uri authority: %.*s", 
            iIn-7, &zUri[7]);
        rc = SQLITE4_ERROR;
        goto parse_uri_out;
      }
    }else{
      iIn = 5;
    }

    /* Copy the filename and any query parameters into the zFile buffer. 
    ** Decode %HH escape codes along the way. 
    **
    ** Within this loop, variable eState may be set to 0, 1 or 2, depending
    ** on the parsing context. As follows:
    **
    **   0: Parsing file-name.
    **   1: Parsing name section of a name=value query parameter.
    **   2: Parsing value section of a name=value query parameter.
    */
    eState = 0;
    while( (c = zUri[iIn])!=0 && c!='#' ){
      iIn++;
      if( c=='%' 
       && sqlite4Isxdigit(zUri[iIn]) 
       && sqlite4Isxdigit(zUri[iIn+1]) 
      ){
        int octet = (sqlite4HexToInt(zUri[iIn++]) << 4);
        octet += sqlite4HexToInt(zUri[iIn++]);

        assert( octet>=0 && octet<256 );
        if( octet==0 ){
          /* This branch is taken when "%00" appears within the URI. In this
          ** case we ignore all text in the remainder of the path, name or
          ** value currently being parsed. So ignore the current character
          ** and skip to the next "?", "=" or "&", as appropriate. */
          while( (c = zUri[iIn])!=0 && c!='#' 
              && (eState!=0 || c!='?')
              && (eState!=1 || (c!='=' && c!='&'))
              && (eState!=2 || c!='&')
          ){
            iIn++;
          }
          continue;
        }
        c = octet;
      }else if( eState==1 && (c=='&' || c=='=') ){
        if( zFile[iOut-1]==0 ){
          /* An empty option name. Ignore this option altogether. */
          while( zUri[iIn] && zUri[iIn]!='#' && zUri[iIn-1]!='&' ) iIn++;
          continue;
        }
        if( c=='&' ){
          zFile[iOut++] = '\0';
        }else{
          eState = 2;
        }
        c = 0;
      }else if( (eState==0 && c=='?') || (eState==2 && c=='&') ){
        c = 0;
        eState = 1;
      }
      zFile[iOut++] = c;
    }
    if( eState==1 ) zFile[iOut++] = '\0';
    zFile[iOut++] = '\0';
    zFile[iOut++] = '\0';

    /* Check if there were any options specified that should be interpreted 
    ** here. Options that are interpreted here include "vfs" and those that
    ** correspond to flags that may be passed to the sqlite4_open()
    ** method. */
    zOpt = &zFile[sqlite4Strlen30(zFile)+1];
    while( zOpt[0] ){
      int nOpt = sqlite4Strlen30(zOpt);
      char *zVal = &zOpt[nOpt+1];
      int nVal = sqlite4Strlen30(zVal);
      struct OpenMode {
        const char *z;
        int mode;
      } *aMode = 0;
      char *zModeType = 0;
      int mask = 0;
      int limit = 0;

      if( nOpt==4 && memcmp("mode", zOpt, 4)==0 ){
        static struct OpenMode aOpenMode[] = {
          { "ro",  SQLITE4_OPEN_READONLY },
          { "rw",  SQLITE4_OPEN_READWRITE }, 
          { "rwc", SQLITE4_OPEN_READWRITE | SQLITE4_OPEN_CREATE },
          { 0, 0 }
        };

        mask = SQLITE4_OPEN_READONLY|SQLITE4_OPEN_READWRITE|SQLITE4_OPEN_CREATE;
        aMode = aOpenMode;
        limit = mask & flags;
        zModeType = "access";
      }

      if( aMode ){
        int i;
        int mode = 0;
        for(i=0; aMode[i].z; i++){
          const char *z = aMode[i].z;
          if( nVal==sqlite4Strlen30(z) && 0==memcmp(zVal, z, nVal) ){
            mode = aMode[i].mode;
            break;
          }
        }
        if( mode==0 ){
          *pzErrMsg = sqlite4_mprintf(pEnv, "no such %s mode: %s",
                                      zModeType, zVal);
          rc = SQLITE4_ERROR;
          goto parse_uri_out;
        }
        if( mode>limit ){
          *pzErrMsg = sqlite4_mprintf(pEnv, "%s mode not allowed: %s",
                                      zModeType, zVal);
          rc = SQLITE4_PERM;
          goto parse_uri_out;
        }
        flags = (flags & ~mask) | mode;
      }

      zOpt = &zVal[nVal+1];
    }

  }else{
    zFile = sqlite4_malloc(pEnv, nUri+2);
    if( !zFile ) return SQLITE4_NOMEM;
    memcpy(zFile, zUri, nUri);
    zFile[nUri] = '\0';
    zFile[nUri+1] = '\0';
  }

 parse_uri_out:
  if( rc!=SQLITE4_OK ){
    sqlite4_free(pEnv, zFile);
    zFile = 0;
  }
  *pFlags = flags;
  *pzFile = zFile;
  return rc;
}


/*
** This routine does the work of opening a database on behalf of
** sqlite4_open(). The database filename "zFilename" is UTF-8 encoded.
*/
static int openDatabase(
  sqlite4_env *pEnv,     /* The run-time environment */
  const char *zFilename, /* Database filename UTF-8 encoded */
  unsigned int flags,    /* Flags influencing the open */
  sqlite4 **ppDb,        /* OUT: Returned database handle */
  va_list ap             /* Zero-terminated list of options */
){
  sqlite4 *db;                    /* Store allocated handle here */
  int rc;                         /* Return code */
  int isThreadsafe;               /* True for threadsafe connections */
  char *zOpen = 0;                /* Filename passed to StorageOpen() */
  char *zErrMsg = 0;              /* Error message from sqlite4ParseUri() */

  *ppDb = 0;
#ifndef SQLITE4_OMIT_AUTOINIT
  rc = sqlite4_initialize(pEnv);
  if( rc ) return rc;
#endif

  if( pEnv->bCoreMutex==0 ){
    isThreadsafe = 0;
  }else{
    isThreadsafe = pEnv->bFullMutex;
  }

  /* Allocate the sqlite data structure */
  db = sqlite4MallocZero(pEnv, sizeof(sqlite4) );
  if( db==0 ) goto opendb_out;
  db->pEnv = pEnv;
  if( isThreadsafe ){
    db->mutex = sqlite4MutexAlloc(pEnv, SQLITE4_MUTEX_RECURSIVE);
    if( db->mutex==0 ){
      sqlite4_free(pEnv, db);
      db = 0;
      goto opendb_out;
    }
  }
  sqlite4_mutex_enter(db->mutex);
  db->nDb = 2;
  db->magic = SQLITE4_MAGIC_BUSY;
  db->aDb = db->aDbStatic;

  assert( sizeof(db->aLimit)==sizeof(aHardLimit) );
  memcpy(db->aLimit, aHardLimit, sizeof(db->aLimit));
  db->nextAutovac = -1;
  db->nextPagesize = 0;
  db->flags |=  SQLITE4_AutoIndex
                 | SQLITE4_EnableTrigger
                 | SQLITE4_ForeignKeys
            ;

  sqlite4HashInit(pEnv, &db->aCollSeq);
#ifndef SQLITE4_OMIT_VIRTUALTABLE
  sqlite4HashInit(pEnv, &db->aModule);
#endif

  /* Add the default collation sequence BINARY. BINARY works for both UTF-8
  ** and UTF-16, so add a version for each to avoid any unnecessary
  ** conversions. The only error that can occur here is a malloc() failure.
  */
  createCollation(db, "BINARY", SQLITE4_UTF8, 0, binCollFunc, 0, 0);
  createCollation(db, "BINARY", SQLITE4_UTF16BE, 0, binCollFunc, 0, 0);
  createCollation(db, "BINARY", SQLITE4_UTF16LE, 0, binCollFunc, 0, 0);
  createCollation(
      db, "RTRIM", SQLITE4_UTF8, (void*)1, binCollFunc, collRtrimMkKey, 0
  );
  if( db->mallocFailed ){
    goto opendb_out;
  }
  db->pDfltColl = sqlite4FindCollSeq(db, SQLITE4_UTF8, "BINARY", 0);
  assert( db->pDfltColl!=0 );

  /* Also add a UTF-8 case-insensitive collation sequence. */
  createCollation(db, 
      "NOCASE", SQLITE4_UTF8, 0, collNocaseCmp, collNocaseMkKey, 0
  );

  /* Parse the filename/URI argument. */
  rc = sqlite4ParseUri(pEnv, zFilename, &flags, &zOpen, &zErrMsg);
  if( rc!=SQLITE4_OK ){
    if( rc==SQLITE4_NOMEM ) db->mallocFailed = 1;
    sqlite4Error(db, rc, zErrMsg ? "%s" : 0, zErrMsg);
    sqlite4_free(pEnv, zErrMsg);
    goto opendb_out;
  }
  db->openFlags = flags;

  /* Open the backend database driver */
  rc = sqlite4KVStoreOpen(db, "main", zOpen, &db->aDb[0].pKV, flags);
  if( rc!=SQLITE4_OK ){
    if( rc==SQLITE4_IOERR_NOMEM ){
      rc = SQLITE4_NOMEM;
    }
    sqlite4Error(db, rc, 0);
    goto opendb_out;
  }
  db->aDb[0].pSchema = sqlite4SchemaGet(db);
  db->aDb[1].pSchema = sqlite4SchemaGet(db);

  /* The default safety_level for the main database is 'full'; for the temp
  ** database it is 'NONE'. This matches the pager layer defaults.  
  */
  db->aDb[0].zName = "main";
  db->aDb[1].zName = "temp";

  db->magic = SQLITE4_MAGIC_OPEN;
  if( db->mallocFailed ){
    goto opendb_out;
  }

  /* Register all built-in functions, but do not attempt to read the
  ** database schema yet. This is delayed until the first time the database
  ** is accessed.
  */
  sqlite4Error(db, SQLITE4_OK, 0);
  sqlite4RegisterBuiltinFunctions(db);

  /* Load automatic extensions - extensions that have been registered
  ** using the sqlite4_automatic_extension() API.
  */
  rc = sqlite4_errcode(db);
  if( rc==SQLITE4_OK ){
    /* sqlite4AutoLoadExtensions(db); */
    rc = sqlite4_errcode(db);
    if( rc!=SQLITE4_OK ){
      goto opendb_out;
    }
  }

  if( !db->mallocFailed && rc==SQLITE4_OK ){
    rc = sqlite4InitFts5(db);
  }

#ifdef SQLITE4_ENABLE_ICU
  if( !db->mallocFailed && rc==SQLITE4_OK ){
    rc = sqlite4IcuInit(db);
  }
#endif

#ifdef SQLITE4_ENABLE_RTREE
  if( !db->mallocFailed && rc==SQLITE4_OK){
    rc = sqlite4RtreeInit(db);
  }
#endif

  sqlite4Error(db, rc, 0);

  /* Enable the lookaside-malloc subsystem */
  setupLookaside(db, 0, pEnv->szLookaside,
                        pEnv->nLookaside);

opendb_out:
  sqlite4_free(pEnv, zOpen);
  if( db ){
    assert( db->mutex!=0 || isThreadsafe==0 || pEnv->bFullMutex==0 );
    sqlite4_mutex_leave(db->mutex);
  }
  rc = sqlite4_errcode(db);
  assert( db!=0 || rc==SQLITE4_NOMEM );
  if( rc==SQLITE4_NOMEM ){
    sqlite4_close(db);
    db = 0;
  }else if( rc!=SQLITE4_OK ){
    db->magic = SQLITE4_MAGIC_SICK;
  }
  *ppDb = db;
  return sqlite4ApiExit(0, rc);
}

/*
** Open a new database handle.
*/
int sqlite4_open(
  sqlite4_env *pEnv,
  const char *zFilename, 
  sqlite4 **ppDb,
  ...
){
  va_list ap;
  int rc;
  if( pEnv==0 ) pEnv = sqlite4_env_default();
  va_start(ap, ppDb);
  rc = openDatabase(pEnv, zFilename,
                      SQLITE4_OPEN_READWRITE | SQLITE4_OPEN_CREATE, ppDb, ap);
  va_end(ap);
  return rc;
}

/*
** Return the environment of a database connection
*/
sqlite4_env *sqlite4_db_env(sqlite4 *db){
  return db ? db->pEnv : sqlite4_env_default();
}

/*
** Register a new collation sequence with the database handle db.
*/
int sqlite4_create_collation(
  sqlite4* db, 
  const char *zName, 
  int enc, 
  void* pCtx,
  int(*xCompare)(void*,int,const void*,int,const void*),
  int(*xMakeKey)(void*,int,const void*,int,void*),
  void(*xDel)(void*)
){
  int rc;
  sqlite4_mutex_enter(db->mutex);
  assert( !db->mallocFailed );
  rc = createCollation(db, zName, (u8)enc, pCtx, xCompare, xMakeKey, xDel);
  rc = sqlite4ApiExit(db, rc);
  sqlite4_mutex_leave(db->mutex);
  return rc;
}

/*
** Register a collation sequence factory callback with the database handle
** db. Replace any previously installed collation sequence factory.
*/
int sqlite4_collation_needed(
  sqlite4 *db, 
  void *pCollNeededArg, 
  void(*xCollNeeded)(void*,sqlite4*,int eTextRep,const char*)
){
  sqlite4_mutex_enter(db->mutex);
  db->xCollNeeded = xCollNeeded;
  db->xCollNeeded16 = 0;
  db->pCollNeededArg = pCollNeededArg;
  sqlite4_mutex_leave(db->mutex);
  return SQLITE4_OK;
}

#ifndef SQLITE4_OMIT_UTF16
/*
** Register a collation sequence factory callback with the database handle
** db. Replace any previously installed collation sequence factory.
*/
int sqlite4_collation_needed16(
  sqlite4 *db, 
  void *pCollNeededArg, 
  void(*xCollNeeded16)(void*,sqlite4*,int eTextRep,const void*)
){
  sqlite4_mutex_enter(db->mutex);
  db->xCollNeeded = 0;
  db->xCollNeeded16 = xCollNeeded16;
  db->pCollNeededArg = pCollNeededArg;
  sqlite4_mutex_leave(db->mutex);
  return SQLITE4_OK;
}
#endif /* SQLITE4_OMIT_UTF16 */

#ifndef SQLITE4_OMIT_DEPRECATED
/*
** This function is now an anachronism. It used to be used to recover from a
** malloc() failure, but SQLite now does this automatically.
*/
int sqlite4_global_recover(void){
  return SQLITE4_OK;
}
#endif

/*
** Test to see whether or not the database connection is in autocommit
** mode.  Return TRUE if it is and FALSE if not.  Autocommit mode is on
** by default.  Autocommit is disabled by a BEGIN statement and reenabled
** by the next COMMIT or ROLLBACK.
**
******* THIS IS AN EXPERIMENTAL API AND IS SUBJECT TO CHANGE ******
*/
int sqlite4_get_autocommit(sqlite4 *db){
  return (db->pSavepoint==0);
}

/*
** The following routines are subtitutes for constants SQLITE4_CORRUPT,
** SQLITE4_MISUSE, SQLITE4_CANTOPEN, SQLITE4_IOERR and possibly other error
** constants.  They server two purposes:
**
**   1.  Serve as a convenient place to set a breakpoint in a debugger
**       to detect when version error conditions occurs.
**
**   2.  Invoke sqlite4_log() to provide the source code location where
**       a low-level error is first detected.
*/
int sqlite4CorruptError(int lineno){
  testcase( sqlite4DefaultEnv.xLog!=0 );
  sqlite4_log(0, SQLITE4_CORRUPT,
              "database corruption at line %d of [%.10s]",
              lineno, 20+sqlite4_sourceid());
  return SQLITE4_CORRUPT;
}
int sqlite4MisuseError(int lineno){
  testcase( sqlite4DefaultEnv.xLog!=0 );
  sqlite4_log(0, SQLITE4_MISUSE, 
              "misuse at line %d of [%.10s]",
              lineno, 20+sqlite4_sourceid());
  return SQLITE4_MISUSE;
}
int sqlite4CantopenError(int lineno){
  testcase( sqlite4DefaultEnv.xLog!=0 );
  sqlite4_log(0, SQLITE4_CANTOPEN, 
              "cannot open file at line %d of [%.10s]",
              lineno, 20+sqlite4_sourceid());
  return SQLITE4_CANTOPEN;
}


/*
** Sleep for a little while.  Return the amount of time slept.
*/
int sqlite4_sleep(int ms){
  return SQLITE4_MISUSE;
}

/*
** Invoke the xFileControl method on a particular database.
*/
int sqlite4_kvstore_control(
  sqlite4 *db,                    /* Database handle */
  const char *zDbName,            /* Name of database backend ("main" etc.) */
  int op,                         /* First argument to pass to xControl() */
  void *pArg                      /* Second argument to pass to xControl() */
){
  int rc = SQLITE4_ERROR;
  KVStore *pKV = 0;
  int i;

  sqlite4_mutex_enter(db->mutex);

  /* Find the named key-value store */
  for(i=0; i<db->nDb; i++){
    Db *pDb = &db->aDb[i];
    if( pDb->pKV && (0==zDbName || 0==sqlite4StrICmp(zDbName, pDb->zName)) ){
      pKV = pDb->pKV;
      break;
    }
  }

  /* If the named key-value store was located, invoke its xControl() method. */
  if( pKV ){
    rc = pKV->pStoreVfunc->xControl(pKV, op, pArg);
  }

  sqlite4_mutex_leave(db->mutex);
  return rc;   
}


/*
** Interface to the testing logic.
*/
int sqlite4_test_control(int op, ...){
  int rc = 0;
#ifndef SQLITE4_OMIT_BUILTIN_TEST
  va_list ap;
  va_start(ap, op);
  switch( op ){
    /*
    **  sqlite4_test_control(SQLITE4_TESTCTRL_ASSERT, int X)
    **
    ** This action provides a run-time test to see whether or not
    ** assert() was enabled at compile-time.  If X is true and assert()
    ** is enabled, then the return value is true.  If X is true and
    ** assert() is disabled, then the return value is zero.  If X is
    ** false and assert() is enabled, then the assertion fires and the
    ** process aborts.  If X is false and assert() is disabled, then the
    ** return value is zero.
    */
    case SQLITE4_TESTCTRL_ASSERT: {
      volatile int x = 0;
      assert( (x = va_arg(ap,int))!=0 );
      rc = x;
      break;
    }


    /*
    **  sqlite4_test_control(SQLITE4_TESTCTRL_ALWAYS, int X)
    **
    ** This action provides a run-time test to see how the ALWAYS and
    ** NEVER macros were defined at compile-time.
    **
    ** The return value is ALWAYS(X).  
    **
    ** The recommended test is X==2.  If the return value is 2, that means
    ** ALWAYS() and NEVER() are both no-op pass-through macros, which is the
    ** default setting.  If the return value is 1, then ALWAYS() is either
    ** hard-coded to true or else it asserts if its argument is false.
    ** The first behavior (hard-coded to true) is the case if
    ** SQLITE4_TESTCTRL_ASSERT shows that assert() is disabled and the second
    ** behavior (assert if the argument to ALWAYS() is false) is the case if
    ** SQLITE4_TESTCTRL_ASSERT shows that assert() is enabled.
    **
    ** The run-time test procedure might look something like this:
    **
    **    if( sqlite4_test_control(SQLITE4_TESTCTRL_ALWAYS, 2)==2 ){
    **      // ALWAYS() and NEVER() are no-op pass-through macros
    **    }else if( sqlite4_test_control(SQLITE4_TESTCTRL_ASSERT, 1) ){
    **      // ALWAYS(x) asserts that x is true. NEVER(x) asserts x is false.
    **    }else{
    **      // ALWAYS(x) is a constant 1.  NEVER(x) is a constant 0.
    **    }
    */
    case SQLITE4_TESTCTRL_ALWAYS: {
      int x = va_arg(ap,int);
      rc = ALWAYS(x);
      break;
    }

    /*  sqlite4_test_control(SQLITE4_TESTCTRL_OPTIMIZATIONS, sqlite4 *db, int N)
    **
    ** Enable or disable various optimizations for testing purposes.  The 
    ** argument N is a bitmask of optimizations to be disabled.  For normal
    ** operation N should be 0.  The idea is that a test program (like the
    ** SQL Logic Test or SLT test module) can run the same SQL multiple times
    ** with various optimizations disabled to verify that the same answer
    ** is obtained in every case.
    */
    case SQLITE4_TESTCTRL_OPTIMIZATIONS: {
      sqlite4 *db = va_arg(ap, sqlite4*);
      int x = va_arg(ap,int);
      db->flags = (x & SQLITE4_OptMask) | (db->flags & ~SQLITE4_OptMask);
      break;
    }

#ifdef SQLITE4_N_KEYWORD
    /* sqlite4_test_control(SQLITE4_TESTCTRL_ISKEYWORD, const char *zWord)
    **
    ** If zWord is a keyword recognized by the parser, then return the
    ** number of keywords.  Or if zWord is not a keyword, return 0.
    ** 
    ** This test feature is only available in the amalgamation since
    ** the SQLITE4_N_KEYWORD macro is not defined in this file if SQLite
    ** is built using separate source files.
    */
    case SQLITE4_TESTCTRL_ISKEYWORD: {
      const char *zWord = va_arg(ap, const char*);
      int n = sqlite4Strlen30(zWord);
      rc = (sqlite4KeywordCode((u8*)zWord, n)!=TK_ID) ? SQLITE4_N_KEYWORD : 0;
      break;
    }
#endif 

    /*   sqlite4_test_control(SQLITE4_TESTCTRL_LOCALTIME_FAULT, int onoff);
    **
    ** If parameter onoff is non-zero, configure the wrappers so that all
    ** subsequent calls to localtime() and variants fail. If onoff is zero,
    ** undo this setting.
    */
    case SQLITE4_TESTCTRL_LOCALTIME_FAULT: {
      sqlite4DefaultEnv.bLocaltimeFault = va_arg(ap, int);
      break;
    }

#if defined(SQLITE4_ENABLE_TREE_EXPLAIN)
    /*   sqlite4_test_control(SQLITE4_TESTCTRL_EXPLAIN_STMT,
    **                        sqlite4_stmt*,const char**);
    **
    ** If compiled with SQLITE4_ENABLE_TREE_EXPLAIN, each sqlite4_stmt holds
    ** a string that describes the optimized parse tree.  This test-control
    ** returns a pointer to that string.
    */
    case SQLITE4_TESTCTRL_EXPLAIN_STMT: {
      sqlite4_stmt *pStmt = va_arg(ap, sqlite4_stmt*);
      const char **pzRet = va_arg(ap, const char**);
      *pzRet = sqlite4VdbeExplanation((Vdbe*)pStmt);
      break;
    }
#endif

  }
  va_end(ap);
#endif /* SQLITE4_OMIT_BUILTIN_TEST */
  return rc;
}

/*
** This is a utility routine, useful to VFS implementations, that checks
** to see if a database file was a URI that contained a specific query 
** parameter, and if so obtains the value of the query parameter.
**
** The zFilename argument is the filename pointer passed into the xOpen()
** method of a VFS implementation.  The zParam argument is the name of the
** query parameter we seek.  This routine returns the value of the zParam
** parameter if it exists.  If the parameter does not exist, this routine
** returns a NULL pointer.
*/
const char *sqlite4_uri_parameter(const char *zFilename, const char *zParam){
  if( zFilename==0 ) return 0;
  zFilename += sqlite4Strlen30(zFilename) + 1;
  while( zFilename[0] ){
    int x = strcmp(zFilename, zParam);
    zFilename += sqlite4Strlen30(zFilename) + 1;
    if( x==0 ) return zFilename;
    zFilename += sqlite4Strlen30(zFilename) + 1;
  }
  return 0;
}

/*
** Return a boolean value for a query parameter.
*/
int sqlite4_uri_boolean(const char *zFilename, const char *zParam, int bDflt){
  const char *z = sqlite4_uri_parameter(zFilename, zParam);
  return z ? sqlite4GetBoolean(z) : (bDflt!=0);
}

/*
** Return a 64-bit integer value for a query parameter.
*/
sqlite4_int64 sqlite4_uri_int64(
  const char *zFilename,    /* Filename as passed to xOpen */
  const char *zParam,       /* URI parameter sought */
  sqlite4_int64 bDflt       /* return if parameter is missing */
){
  const char *z = sqlite4_uri_parameter(zFilename, zParam);
  sqlite4_int64 v;
  if( z && sqlite4Atoi64(z, &v, sqlite4Strlen30(z), SQLITE4_UTF8)==SQLITE4_OK ){
    bDflt = v;
  }
  return bDflt;
}
