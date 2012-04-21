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

#ifdef SQLITE_ENABLE_FTS3
# include "fts3.h"
#endif
#ifdef SQLITE_ENABLE_RTREE
# include "rtree.h"
#endif
#ifdef SQLITE_ENABLE_ICU
# include "sqliteicu.h"
#endif

#ifndef SQLITE_AMALGAMATION
/* IMPLEMENTATION-OF: R-46656-45156 The sqlite4_version[] string constant
** contains the text of SQLITE_VERSION macro. 
*/
const char sqlite4_version[] = SQLITE_VERSION;
#endif

/* IMPLEMENTATION-OF: R-53536-42575 The sqlite4_libversion() function returns
** a pointer to the to the sqlite4_version[] string constant. 
*/
const char *sqlite4_libversion(void){ return sqlite4_version; }

/* IMPLEMENTATION-OF: R-63124-39300 The sqlite4_sourceid() function returns a
** pointer to a string constant whose value is the same as the
** SQLITE_SOURCE_ID C preprocessor macro. 
*/
const char *sqlite4_sourceid(void){ return SQLITE_SOURCE_ID; }

/* IMPLEMENTATION-OF: R-35210-63508 The sqlite4_libversion_number() function
** returns an integer equal to SQLITE_VERSION_NUMBER.
*/
int sqlite4_libversion_number(void){ return SQLITE_VERSION_NUMBER; }

/* IMPLEMENTATION-OF: R-20790-14025 The sqlite4_threadsafe() function returns
** zero if and only if SQLite was compiled with mutexing code omitted due to
** the SQLITE_THREADSAFE compile-time option being set to 0.
*/
int sqlite4_threadsafe(void){ return SQLITE_THREADSAFE; }

#if !defined(SQLITE_OMIT_TRACE) && defined(SQLITE_ENABLE_IOTRACE)
/*
** If the following function pointer is not NULL and if
** SQLITE_ENABLE_IOTRACE is enabled, then messages describing
** I/O active are written using this function.  These messages
** are intended for debugging activity only.
*/
void (*sqlite4IoTrace)(const char*, ...) = 0;
#endif

/*
** If the following global variable points to a string which is the
** name of a directory, then that directory will be used to store
** temporary files.
**
** See also the "PRAGMA temp_store_directory" SQL command.
*/
char *sqlite4_temp_directory = 0;

/*
** Initialize SQLite.  
**
** This routine must be called to initialize the memory allocation,
** VFS, and mutex subsystems prior to doing any serious work with
** SQLite.  But as long as you do not compile with SQLITE_OMIT_AUTOINIT
** this routine will be called automatically by key routines such as
** sqlite4_open().  
**
** This routine is a no-op except on its very first call for the process,
** or for the first call after a call to sqlite4_shutdown.
**
** The first thread to call this routine runs the initialization to
** completion.  If subsequent threads call this routine before the first
** thread has finished the initialization process, then the subsequent
** threads must block until the first thread finishes with the initialization.
**
** The first thread might call this routine recursively.  Recursive
** calls to this routine should not block, of course.  Otherwise the
** initialization process would never complete.
**
** Let X be the first thread to enter this routine.  Let Y be some other
** thread.  Then while the initial invocation of this routine by X is
** incomplete, it is required that:
**
**    *  Calls to this routine from Y must block until the outer-most
**       call by X completes.
**
**    *  Recursive calls to this routine from thread X return immediately
**       without blocking.
*/
int sqlite4_initialize(void){
  MUTEX_LOGIC( sqlite4_mutex *pMaster; )       /* The main static mutex */
  int rc;                                      /* Result code */

#ifdef SQLITE_OMIT_WSD
  rc = sqlite4_wsd_init(4096, 24);
  if( rc!=SQLITE_OK ){
    return rc;
  }
#endif

  /* If SQLite is already completely initialized, then this call
  ** to sqlite4_initialize() should be a no-op.  But the initialization
  ** must be complete.  So isInit must not be set until the very end
  ** of this routine.
  */
  if( sqlite4GlobalConfig.isInit ) return SQLITE_OK;

  /* Make sure the mutex subsystem is initialized.  If unable to 
  ** initialize the mutex subsystem, return early with the error.
  ** If the system is so sick that we are unable to allocate a mutex,
  ** there is not much SQLite is going to be able to do.
  **
  ** The mutex subsystem must take care of serializing its own
  ** initialization.
  */
  rc = sqlite4MutexInit();
  if( rc ) return rc;

  /* Initialize the malloc() system and the recursive pInitMutex mutex.
  ** This operation is protected by the STATIC_MASTER mutex.  Note that
  ** MutexAlloc() is called for a static mutex prior to initializing the
  ** malloc subsystem - this implies that the allocation of a static
  ** mutex must not require support from the malloc subsystem.
  */
  MUTEX_LOGIC( pMaster = sqlite4MutexAlloc(SQLITE_MUTEX_STATIC_MASTER); )
  sqlite4_mutex_enter(pMaster);
  sqlite4GlobalConfig.isMutexInit = 1;
  if( !sqlite4GlobalConfig.isMallocInit ){
    rc = sqlite4MallocInit();
  }
  if( rc==SQLITE_OK ){
    sqlite4GlobalConfig.isMallocInit = 1;
    if( !sqlite4GlobalConfig.pInitMutex ){
      sqlite4GlobalConfig.pInitMutex =
           sqlite4MutexAlloc(SQLITE_MUTEX_RECURSIVE);
      if( sqlite4GlobalConfig.bCoreMutex && !sqlite4GlobalConfig.pInitMutex ){
        rc = SQLITE_NOMEM;
      }
    }
  }
  if( rc==SQLITE_OK ){
    sqlite4GlobalConfig.nRefInitMutex++;
  }
  sqlite4_mutex_leave(pMaster);

  /* If rc is not SQLITE_OK at this point, then either the malloc
  ** subsystem could not be initialized or the system failed to allocate
  ** the pInitMutex mutex. Return an error in either case.  */
  if( rc!=SQLITE_OK ){
    return rc;
  }

  /* Do the rest of the initialization under the recursive mutex so
  ** that we will be able to handle recursive calls into
  ** sqlite4_initialize().  The recursive calls normally come through
  ** sqlite4_os_init() when it invokes sqlite4_vfs_register(), but other
  ** recursive calls might also be possible.
  */
  sqlite4_mutex_enter(sqlite4GlobalConfig.pInitMutex);
  if( sqlite4GlobalConfig.isInit==0 && sqlite4GlobalConfig.inProgress==0 ){
    FuncDefHash *pHash = &GLOBAL(FuncDefHash, sqlite4GlobalFunctions);
    sqlite4GlobalConfig.inProgress = 1;
    memset(pHash, 0, sizeof(sqlite4GlobalFunctions));
    sqlite4RegisterGlobalFunctions();
    rc = sqlite4OsInit();
    sqlite4GlobalConfig.inProgress = 0;
  }
  sqlite4_mutex_leave(sqlite4GlobalConfig.pInitMutex);

  /* Go back under the static mutex and clean up the recursive
  ** mutex to prevent a resource leak.
  */
  sqlite4_mutex_enter(pMaster);
  sqlite4GlobalConfig.nRefInitMutex--;
  if( sqlite4GlobalConfig.nRefInitMutex<=0 ){
    assert( sqlite4GlobalConfig.nRefInitMutex==0 );
    sqlite4_mutex_free(sqlite4GlobalConfig.pInitMutex);
    sqlite4GlobalConfig.pInitMutex = 0;
  }
  sqlite4_mutex_leave(pMaster);

  /* The following is just a sanity check to make sure SQLite has
  ** been compiled correctly.  It is important to run this code, but
  ** we don't want to run it too often and soak up CPU cycles for no
  ** reason.  So we run it once during initialization.
  */
#ifndef NDEBUG
#ifndef SQLITE_OMIT_FLOATING_POINT
  /* This section of code's only "output" is via assert() statements. */
  if ( rc==SQLITE_OK ){
    u64 x = (((u64)1)<<63)-1;
    double y;
    assert(sizeof(x)==8);
    assert(sizeof(x)==sizeof(y));
    memcpy(&y, &x, 8);
    assert( sqlite4IsNaN(y) );
  }
#endif
#endif

  /* Do extra initialization steps requested by the SQLITE_EXTRA_INIT
  ** compile-time option.
  */
#ifdef SQLITE_EXTRA_INIT
  if( rc==SQLITE_OK && sqlite4GlobalConfig.isInit ){
    int SQLITE_EXTRA_INIT(const char*);
    rc = SQLITE_EXTRA_INIT(0);
  }
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
int sqlite4_shutdown(void){
  if( sqlite4GlobalConfig.isInit ){
#ifdef SQLITE_EXTRA_SHUTDOWN
    void SQLITE_EXTRA_SHUTDOWN(void);
    SQLITE_EXTRA_SHUTDOWN();
#endif
    sqlite4_os_end();
    /* sqlite4_reset_auto_extension(); */
    sqlite4GlobalConfig.isInit = 0;
  }
  if( sqlite4GlobalConfig.isMallocInit ){
    sqlite4MallocEnd();
    sqlite4GlobalConfig.isMallocInit = 0;
  }
  if( sqlite4GlobalConfig.isMutexInit ){
    sqlite4MutexEnd();
    sqlite4GlobalConfig.isMutexInit = 0;
  }

  return SQLITE_OK;
}

/*
** This API allows applications to modify the global configuration of
** the SQLite library at run-time.
**
** This routine should only be called when there are no outstanding
** database connections or memory allocations.  This routine is not
** threadsafe.  Failure to heed these warnings can lead to unpredictable
** behavior.
*/
int sqlite4_config(int op, ...){
  va_list ap;
  int rc = SQLITE_OK;

  /* sqlite4_config() shall return SQLITE_MISUSE if it is invoked while
  ** the SQLite library is in use. */
  if( sqlite4GlobalConfig.isInit ) return SQLITE_MISUSE_BKPT;

  va_start(ap, op);
  switch( op ){
    case SQLITE_CONFIG_SET_KVFACTORY: {
      sqlite4GlobalConfig.xKVFile = *va_arg(ap, 
          int (*)(KVStore **, const char *, unsigned int)
      );
      break;
    }

    case SQLITE_CONFIG_GET_KVFACTORY: {
      *va_arg(ap, int (**)(KVStore **, const char *, unsigned int)) =
          sqlite4GlobalConfig.xKVFile;
      break;
    }

    /* Mutex configuration options are only available in a threadsafe
    ** compile. 
    */
#if defined(SQLITE_THREADSAFE) && SQLITE_THREADSAFE>0
    case SQLITE_CONFIG_SINGLETHREAD: {
      /* Disable all mutexing */
      sqlite4GlobalConfig.bCoreMutex = 0;
      sqlite4GlobalConfig.bFullMutex = 0;
      break;
    }
    case SQLITE_CONFIG_MULTITHREAD: {
      /* Disable mutexing of database connections */
      /* Enable mutexing of core data structures */
      sqlite4GlobalConfig.bCoreMutex = 1;
      sqlite4GlobalConfig.bFullMutex = 0;
      break;
    }
    case SQLITE_CONFIG_SERIALIZED: {
      /* Enable all mutexing */
      sqlite4GlobalConfig.bCoreMutex = 1;
      sqlite4GlobalConfig.bFullMutex = 1;
      break;
    }
    case SQLITE_CONFIG_MUTEX: {
      /* Specify an alternative mutex implementation */
      sqlite4GlobalConfig.mutex = *va_arg(ap, sqlite4_mutex_methods*);
      break;
    }
    case SQLITE_CONFIG_GETMUTEX: {
      /* Retrieve the current mutex implementation */
      *va_arg(ap, sqlite4_mutex_methods*) = sqlite4GlobalConfig.mutex;
      break;
    }
#endif


    case SQLITE_CONFIG_MALLOC: {
      /* Specify an alternative malloc implementation */
      sqlite4GlobalConfig.m = *va_arg(ap, sqlite4_mem_methods*);
      break;
    }
    case SQLITE_CONFIG_GETMALLOC: {
      /* Retrieve the current malloc() implementation */
      if( sqlite4GlobalConfig.m.xMalloc==0 ) sqlite4MemSetDefault();
      *va_arg(ap, sqlite4_mem_methods*) = sqlite4GlobalConfig.m;
      break;
    }
    case SQLITE_CONFIG_MEMSTATUS: {
      /* Enable or disable the malloc status collection */
      sqlite4GlobalConfig.bMemstat = va_arg(ap, int);
      break;
    }
    case SQLITE_CONFIG_SCRATCH: {
      /* Designate a buffer for scratch memory space */
      sqlite4GlobalConfig.pScratch = va_arg(ap, void*);
      sqlite4GlobalConfig.szScratch = va_arg(ap, int);
      sqlite4GlobalConfig.nScratch = va_arg(ap, int);
      break;
    }

#if defined(SQLITE_ENABLE_MEMSYS3) || defined(SQLITE_ENABLE_MEMSYS5)
    case SQLITE_CONFIG_HEAP: {
      /* Designate a buffer for heap memory space */
      sqlite4GlobalConfig.pHeap = va_arg(ap, void*);
      sqlite4GlobalConfig.nHeap = va_arg(ap, int);
      sqlite4GlobalConfig.mnReq = va_arg(ap, int);

      if( sqlite4GlobalConfig.mnReq<1 ){
        sqlite4GlobalConfig.mnReq = 1;
      }else if( sqlite4GlobalConfig.mnReq>(1<<12) ){
        /* cap min request size at 2^12 */
        sqlite4GlobalConfig.mnReq = (1<<12);
      }

      if( sqlite4GlobalConfig.pHeap==0 ){
        /* If the heap pointer is NULL, then restore the malloc implementation
        ** back to NULL pointers too.  This will cause the malloc to go
        ** back to its default implementation when sqlite4_initialize() is
        ** run.
        */
        memset(&sqlite4GlobalConfig.m, 0, sizeof(sqlite4GlobalConfig.m));
      }else{
        /* The heap pointer is not NULL, then install one of the
        ** mem5.c/mem3.c methods. If neither ENABLE_MEMSYS3 nor
        ** ENABLE_MEMSYS5 is defined, return an error.
        */
#ifdef SQLITE_ENABLE_MEMSYS3
        sqlite4GlobalConfig.m = *sqlite4MemGetMemsys3();
#endif
#ifdef SQLITE_ENABLE_MEMSYS5
        sqlite4GlobalConfig.m = *sqlite4MemGetMemsys5();
#endif
      }
      break;
    }
#endif

    case SQLITE_CONFIG_LOOKASIDE: {
      sqlite4GlobalConfig.szLookaside = va_arg(ap, int);
      sqlite4GlobalConfig.nLookaside = va_arg(ap, int);
      break;
    }
    
    /* Record a pointer to the logger funcction and its first argument.
    ** The default is NULL.  Logging is disabled if the function pointer is
    ** NULL.
    */
    case SQLITE_CONFIG_LOG: {
      /* MSVC is picky about pulling func ptrs from va lists.
      ** http://support.microsoft.com/kb/47961
      ** sqlite4GlobalConfig.xLog = va_arg(ap, void(*)(void*,int,const char*));
      */
      typedef void(*LOGFUNC_t)(void*,int,const char*);
      sqlite4GlobalConfig.xLog = va_arg(ap, LOGFUNC_t);
      sqlite4GlobalConfig.pLogArg = va_arg(ap, void*);
      break;
    }

    case SQLITE_CONFIG_URI: {
      sqlite4GlobalConfig.bOpenUri = va_arg(ap, int);
      break;
    }

    default: {
      rc = SQLITE_ERROR;
      break;
    }
  }
  va_end(ap);
  return rc;
}

/*
** Set up the lookaside buffers for a database connection.
** Return SQLITE_OK on success.  
** If lookaside is already active, return SQLITE_BUSY.
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
    return SQLITE_BUSY;
  }
  /* Free any existing lookaside buffer for this handle before
  ** allocating a new one so we don't have to have space for 
  ** both at the same time.
  */
  if( db->lookaside.bMalloced ){
    sqlite4_free(db->lookaside.pStart);
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
    sqlite4BeginBenignMalloc();
    pStart = sqlite4Malloc( sz*cnt );  /* IMP: R-61949-35727 */
    sqlite4EndBenignMalloc();
    if( pStart ) cnt = sqlite4MallocSize(pStart)/sz;
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
  return SQLITE_OK;
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
  return SQLITE_OK;
}

/*
** Configuration settings for an individual database connection
*/
int sqlite4_db_config(sqlite4 *db, int op, ...){
  va_list ap;
  int rc;
  va_start(ap, op);
  switch( op ){
    case SQLITE_DBCONFIG_LOOKASIDE: {
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
        { SQLITE_DBCONFIG_ENABLE_FKEY,    SQLITE_ForeignKeys    },
        { SQLITE_DBCONFIG_ENABLE_TRIGGER, SQLITE_EnableTrigger  },
      };
      unsigned int i;
      rc = SQLITE_ERROR; /* IMP: R-42790-23372 */
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
          rc = SQLITE_OK;
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
** Another built-in collating sequence: NOCASE. 
**
** This collating sequence is intended to be used for "case independant
** comparison". SQLite's knowledge of upper and lower case equivalents
** extends only to the 26 characters used in the English language.
**
** At the moment there is only a UTF-8 implementation.
*/
static int nocaseCollatingFunc(
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

/*
** Return the ROWID of the most recent insert
*/
sqlite_int64 sqlite4_last_insert_rowid(sqlite4 *db){
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
  db->isTransactionSavepoint = 0;
}

/*
** Invoke the destructor function associated with FuncDef p, if any. Except,
** if this is not the last copy of the function, do not invoke it. Multiple
** copies of a single function are created when create_function() is called
** with SQLITE_ANY as the encoding.
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
    return SQLITE_OK;
  }
  if( !sqlite4SafetyCheckSickOrOk(db) ){
    return SQLITE_MISUSE_BKPT;
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

  /* If there are any outstanding VMs, return SQLITE_BUSY. */
  if( db->pVdbe ){
    sqlite4Error(db, SQLITE_BUSY, 
        "unable to close due to unfinalised statements");
    sqlite4_mutex_leave(db->mutex);
    return SQLITE_BUSY;
  }
  assert( sqlite4SafetyCheckSickOrOk(db) );

  /* Free any outstanding Savepoint structures. */
  sqlite4CloseSavepoints(db);

  for(j=0; j<db->nDb; j++){
    struct Db *pDb = &db->aDb[j];
    if( pDb->pKV ){
      sqlite4KVStoreClose(pDb->pKV);
      pDb->pKV = 0;
      if( j!=1 ){
        pDb->pSchema = 0;
      }
    }
  }
  sqlite4ResetInternalSchema(db, -1);

  /* Tell the code in notify.c that the connection no longer holds any
  ** locks and does not require any further unlock-notify callbacks.
  */
  sqlite4ConnectionClosed(db);

  assert( db->nDb<=2 );
  assert( db->aDb==db->aDbStatic );
  for(j=0; j<ArraySize(db->aFunc.a); j++){
    FuncDef *pNext, *pHash, *p;
    for(p=db->aFunc.a[j]; p; p=pHash){
      pHash = p->pHash;
      while( p ){
        functionDestroy(db, p);
        pNext = p->pNext;
        sqlite4DbFree(db, p);
        p = pNext;
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
#ifndef SQLITE_OMIT_VIRTUALTABLE
  for(i=sqliteHashFirst(&db->aModule); i; i=sqliteHashNext(i)){
    Module *pMod = (Module *)sqliteHashData(i);
    if( pMod->xDestroy ){
      pMod->xDestroy(pMod->pAux);
    }
    sqlite4DbFree(db, pMod);
  }
  sqlite4HashClear(&db->aModule);
#endif

  sqlite4Error(db, SQLITE_OK, 0); /* Deallocates any cached error strings. */
  if( db->pErr ){
    sqlite4ValueFree(db->pErr);
  }

  db->magic = SQLITE_MAGIC_ERROR;

  /* The temp-database schema is allocated differently from the other schema
  ** objects (using sqliteMalloc() directly, instead of sqlite4BTreeSchema()).
  ** So it needs to be freed here. Todo: Why not roll the temp schema into
  ** the same sqliteMalloc() as the one that allocates the database 
  ** structure?
  */
  sqlite4DbFree(db, db->aDb[1].pSchema);
  sqlite4_mutex_leave(db->mutex);
  db->magic = SQLITE_MAGIC_CLOSED;
  sqlite4_mutex_free(db->mutex);
  assert( db->lookaside.nOut==0 );  /* Fails on a lookaside memory leak */
  if( db->lookaside.bMalloced ){
    sqlite4_free(db->lookaside.pStart);
  }
  sqlite4_free(db);
  return SQLITE_OK;
}

/*
** Rollback all database files.
*/
void sqlite4RollbackAll(sqlite4 *db){
  int i;
  int inTrans = 0;
  assert( sqlite4_mutex_held(db->mutex) );
  sqlite4BeginBenignMalloc();
  for(i=0; i<db->nDb; i++){
    if( db->aDb[i].pKV ){
      if( db->aDb[i].pKV->iTransLevel ){
        inTrans = 1;
      }
      sqlite4KVStoreRollback(db->aDb[i].pKV, 0);
      db->aDb[i].inTrans = 0;
    }
  }
  sqlite4VtabRollback(db);
  sqlite4EndBenignMalloc();

  if( db->flags&SQLITE_InternChanges ){
    sqlite4ExpirePreparedStatements(db);
    sqlite4ResetInternalSchema(db, -1);
  }

  /* Any deferred constraint violations have now been resolved. */
  db->nDeferredCons = 0;

  /* If one has been configured, invoke the rollback-hook callback */
  if( db->xRollbackCallback && (inTrans || !db->autoCommit) ){
    db->xRollbackCallback(db->pRollbackArg);
  }
}

/*
** Return a static string that describes the kind of error specified in the
** argument.
*/
const char *sqlite4ErrStr(int rc){
  static const char* const aMsg[] = {
    /* SQLITE_OK          */ "not an error",
    /* SQLITE_ERROR       */ "SQL logic error or missing database",
    /* SQLITE_INTERNAL    */ 0,
    /* SQLITE_PERM        */ "access permission denied",
    /* SQLITE_ABORT       */ "callback requested query abort",
    /* SQLITE_BUSY        */ "database is locked",
    /* SQLITE_LOCKED      */ "database table is locked",
    /* SQLITE_NOMEM       */ "out of memory",
    /* SQLITE_READONLY    */ "attempt to write a readonly database",
    /* SQLITE_INTERRUPT   */ "interrupted",
    /* SQLITE_IOERR       */ "disk I/O error",
    /* SQLITE_CORRUPT     */ "database disk image is malformed",
    /* SQLITE_NOTFOUND    */ "unknown operation",
    /* SQLITE_FULL        */ "database or disk is full",
    /* SQLITE_CANTOPEN    */ "unable to open database file",
    /* SQLITE_PROTOCOL    */ "locking protocol",
    /* SQLITE_EMPTY       */ "table contains no data",
    /* SQLITE_SCHEMA      */ "database schema has changed",
    /* SQLITE_TOOBIG      */ "string or blob too big",
    /* SQLITE_CONSTRAINT  */ "constraint failed",
    /* SQLITE_MISMATCH    */ "datatype mismatch",
    /* SQLITE_MISUSE      */ "library routine called out of sequence",
    /* SQLITE_NOLFS       */ "large file support is disabled",
    /* SQLITE_AUTH        */ "authorization denied",
    /* SQLITE_FORMAT      */ "auxiliary database format error",
    /* SQLITE_RANGE       */ "bind or column index out of range",
    /* SQLITE_NOTADB      */ "file is encrypted or is not a database",
  };
  rc &= 0xff;
  if( ALWAYS(rc>=0) && rc<(int)(sizeof(aMsg)/sizeof(aMsg[0])) && aMsg[rc]!=0 ){
    return aMsg[rc];
  }else{
    return "unknown error";
  }
}

#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
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
      (nArg<-1 || nArg>SQLITE_MAX_FUNCTION_ARG) ||
      (255<(nName = sqlite4Strlen30( zFunctionName))) ){
    return SQLITE_MISUSE_BKPT;
  }
  
#ifndef SQLITE_OMIT_UTF16
  /* If SQLITE_UTF16 is specified as the encoding type, transform this
  ** to one of SQLITE_UTF16LE or SQLITE_UTF16BE using the
  ** SQLITE_UTF16NATIVE macro. SQLITE_UTF16 is not used internally.
  **
  ** If SQLITE_ANY is specified, add three versions of the function
  ** to the hash table.
  */
  if( enc==SQLITE_UTF16 ){
    enc = SQLITE_UTF16NATIVE;
  }else if( enc==SQLITE_ANY ){
    int rc;
    rc = sqlite4CreateFunc(db, zFunctionName, nArg, SQLITE_UTF8,
         pUserData, xFunc, xStep, xFinal, pDestructor);
    if( rc==SQLITE_OK ){
      rc = sqlite4CreateFunc(db, zFunctionName, nArg, SQLITE_UTF16LE,
          pUserData, xFunc, xStep, xFinal, pDestructor);
    }
    if( rc!=SQLITE_OK ){
      return rc;
    }
    enc = SQLITE_UTF16BE;
  }
#else
  enc = SQLITE_UTF8;
#endif
  
  /* Check if an existing function is being overridden or deleted. If so,
  ** and there are active VMs, then return SQLITE_BUSY. If a function
  ** is being overridden/deleted but there are no active VMs, allow the
  ** operation to continue but invalidate all precompiled statements.
  */
  p = sqlite4FindFunction(db, zFunctionName, nName, nArg, (u8)enc, 0);
  if( p && p->iPrefEnc==enc && p->nArg==nArg ){
    if( db->activeVdbeCnt ){
      sqlite4Error(db, SQLITE_BUSY, 
        "unable to delete/modify user-function due to active statements");
      assert( !db->mallocFailed );
      return SQLITE_BUSY;
    }else{
      sqlite4ExpirePreparedStatements(db);
    }
  }

  p = sqlite4FindFunction(db, zFunctionName, nName, nArg, (u8)enc, 1);
  assert(p || db->mallocFailed);
  if( !p ){
    return SQLITE_NOMEM;
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
  return SQLITE_OK;
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
  int rc = SQLITE_ERROR;
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
    assert( rc!=SQLITE_OK );
    xDestroy(p);
    sqlite4DbFree(db, pArg);
  }

 out:
  rc = sqlite4ApiExit(db, rc);
  sqlite4_mutex_leave(db->mutex);
  return rc;
}

#ifndef SQLITE_OMIT_UTF16
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
  zFunc8 = sqlite4Utf16to8(db, zFunctionName, -1, SQLITE_UTF16NATIVE);
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
  int rc = SQLITE_OK;
  sqlite4_mutex_enter(db->mutex);
  if( sqlite4FindFunction(db, zName, nName, nArg, SQLITE_UTF8, 0)==0 ){
    rc = sqlite4CreateFunc(db, zName, nArg, SQLITE_UTF8,
                           0, sqlite4InvalidFunction, 0, 0, 0);
  }
  rc = sqlite4ApiExit(db, rc);
  sqlite4_mutex_leave(db->mutex);
  return rc;
}

#ifndef SQLITE_OMIT_TRACE
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
  void (*xProfile)(void*,const char*,sqlite_uint64),
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
#endif /* SQLITE_OMIT_TRACE */

/*** EXPERIMENTAL ***
**
** Register a function to be invoked when a transaction comments.
** If the invoked function returns non-zero, then the commit becomes a
** rollback.
*/
void *sqlite4_commit_hook(
  sqlite4 *db,              /* Attach the hook to this database */
  int (*xCallback)(void*),  /* Function to invoke on each commit */
  void *pArg                /* Argument to the function */
){
  void *pOld;
  sqlite4_mutex_enter(db->mutex);
  pOld = db->pCommitArg;
  db->xCommitCallback = xCallback;
  db->pCommitArg = pArg;
  sqlite4_mutex_leave(db->mutex);
  return pOld;
}

/*
** Register a callback to be invoked each time a row is updated,
** inserted or deleted using this database connection.
*/
void *sqlite4_update_hook(
  sqlite4 *db,              /* Attach the hook to this database */
  void (*xCallback)(void*,int,char const *,char const *,sqlite_int64),
  void *pArg                /* Argument to the function */
){
  void *pRet;
  sqlite4_mutex_enter(db->mutex);
  pRet = db->pUpdateArg;
  db->xUpdateCallback = xCallback;
  db->pUpdateArg = pArg;
  sqlite4_mutex_leave(db->mutex);
  return pRet;
}

/*
** Register a callback to be invoked each time a transaction is rolled
** back by this database connection.
*/
void *sqlite4_rollback_hook(
  sqlite4 *db,              /* Attach the hook to this database */
  void (*xCallback)(void*), /* Callback function */
  void *pArg                /* Argument to the function */
){
  void *pRet;
  sqlite4_mutex_enter(db->mutex);
  pRet = db->pRollbackArg;
  db->xRollbackCallback = xCallback;
  db->pRollbackArg = pArg;
  sqlite4_mutex_leave(db->mutex);
  return pRet;
}

#ifndef SQLITE_OMIT_WAL
/*
** The sqlite4_wal_hook() callback registered by sqlite4_wal_autocheckpoint().
** Invoke sqlite4_wal_checkpoint if the number of frames in the log file
** is greater than sqlite4.pWalArg cast to an integer (the value configured by
** wal_autocheckpoint()).
*/ 
int sqlite4WalDefaultHook(
  void *pClientData,     /* Argument */
  sqlite4 *db,           /* Connection */
  const char *zDb,       /* Database */
  int nFrame             /* Size of WAL */
){
  if( nFrame>=SQLITE_PTR_TO_INT(pClientData) ){
    sqlite4BeginBenignMalloc();
    sqlite4_wal_checkpoint(db, zDb);
    sqlite4EndBenignMalloc();
  }
  return SQLITE_OK;
}
#endif /* SQLITE_OMIT_WAL */

/*
** Configure an sqlite4_wal_hook() callback to automatically checkpoint
** a database after committing a transaction if there are nFrame or
** more frames in the log file. Passing zero or a negative value as the
** nFrame parameter disables automatic checkpoints entirely.
**
** The callback registered by this function replaces any existing callback
** registered using sqlite4_wal_hook(). Likewise, registering a callback
** using sqlite4_wal_hook() disables the automatic checkpoint mechanism
** configured by this function.
*/
int sqlite4_wal_autocheckpoint(sqlite4 *db, int nFrame){
#ifdef SQLITE_OMIT_WAL
  UNUSED_PARAMETER(db);
  UNUSED_PARAMETER(nFrame);
#else
  if( nFrame>0 ){
    sqlite4_wal_hook(db, sqlite4WalDefaultHook, SQLITE_INT_TO_PTR(nFrame));
  }else{
    sqlite4_wal_hook(db, 0, 0);
  }
#endif
  return SQLITE_OK;
}

/*
** Register a callback to be invoked each time a transaction is written
** into the write-ahead-log by this database connection.
*/
void *sqlite4_wal_hook(
  sqlite4 *db,                    /* Attach the hook to this db handle */
  int(*xCallback)(void *, sqlite4*, const char*, int),
  void *pArg                      /* First argument passed to xCallback() */
){
#ifndef SQLITE_OMIT_WAL
  void *pRet;
  sqlite4_mutex_enter(db->mutex);
  pRet = db->pWalArg;
  db->xWalCallback = xCallback;
  db->pWalArg = pArg;
  sqlite4_mutex_leave(db->mutex);
  return pRet;
#else
  return 0;
#endif
}

/*
** Checkpoint database zDb.
*/
int sqlite4_wal_checkpoint_v2(
  sqlite4 *db,                    /* Database handle */
  const char *zDb,                /* Name of attached database (or NULL) */
  int eMode,                      /* SQLITE_CHECKPOINT_* value */
  int *pnLog,                     /* OUT: Size of WAL log in frames */
  int *pnCkpt                     /* OUT: Total number of frames checkpointed */
){
#ifdef SQLITE_OMIT_WAL
  return SQLITE_OK;
#else
  int rc;                         /* Return code */
  int iDb = SQLITE_MAX_ATTACHED;  /* sqlite4.aDb[] index of db to checkpoint */

  /* Initialize the output variables to -1 in case an error occurs. */
  if( pnLog ) *pnLog = -1;
  if( pnCkpt ) *pnCkpt = -1;

  assert( SQLITE_CHECKPOINT_FULL>SQLITE_CHECKPOINT_PASSIVE );
  assert( SQLITE_CHECKPOINT_FULL<SQLITE_CHECKPOINT_RESTART );
  assert( SQLITE_CHECKPOINT_PASSIVE+2==SQLITE_CHECKPOINT_RESTART );
  if( eMode<SQLITE_CHECKPOINT_PASSIVE || eMode>SQLITE_CHECKPOINT_RESTART ){
    return SQLITE_MISUSE;
  }

  sqlite4_mutex_enter(db->mutex);
  if( zDb && zDb[0] ){
    iDb = sqlite4FindDbName(db, zDb);
  }
  if( iDb<0 ){
    rc = SQLITE_ERROR;
    sqlite4Error(db, SQLITE_ERROR, "unknown database: %s", zDb);
  }else{
    rc = sqlite4Checkpoint(db, iDb, eMode, pnLog, pnCkpt);
    sqlite4Error(db, rc, 0);
  }
  rc = sqlite4ApiExit(db, rc);
  sqlite4_mutex_leave(db->mutex);
  return rc;
#endif
}


/*
** Checkpoint database zDb. If zDb is NULL, or if the buffer zDb points
** to contains a zero-length string, all attached databases are 
** checkpointed.
*/
int sqlite4_wal_checkpoint(sqlite4 *db, const char *zDb){
  return sqlite4_wal_checkpoint_v2(db, zDb, SQLITE_CHECKPOINT_PASSIVE, 0, 0);
}

/*
** This function returns true if main-memory should be used instead of
** a temporary file for transient pager files and statement journals.
** The value returned depends on the value of db->temp_store (runtime
** parameter) and the compile time value of SQLITE_TEMP_STORE. The
** following table describes the relationship between these two values
** and this functions return value.
**
**   SQLITE_TEMP_STORE     db->temp_store     Location of temporary database
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
#if SQLITE_TEMP_STORE==1
  return ( db->temp_store==2 );
#endif
#if SQLITE_TEMP_STORE==2
  return ( db->temp_store!=1 );
#endif
#if SQLITE_TEMP_STORE==3
  return 1;
#endif
#if SQLITE_TEMP_STORE<1 || SQLITE_TEMP_STORE>3
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
    return sqlite4ErrStr(SQLITE_NOMEM);
  }
  if( !sqlite4SafetyCheckSickOrOk(db) ){
    return sqlite4ErrStr(SQLITE_MISUSE_BKPT);
  }
  sqlite4_mutex_enter(db->mutex);
  if( db->mallocFailed ){
    z = sqlite4ErrStr(SQLITE_NOMEM);
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

#ifndef SQLITE_OMIT_UTF16
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
           SQLITE_UTF8, SQLITE_STATIC);
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
#endif /* SQLITE_OMIT_UTF16 */

/*
** Return the most recent error code generated by an SQLite routine. If NULL is
** passed to this function, we assume a malloc() failed during sqlite4_open().
*/
int sqlite4_errcode(sqlite4 *db){
  if( db && !sqlite4SafetyCheckSickOrOk(db) ){
    return SQLITE_MISUSE_BKPT;
  }
  if( !db || db->mallocFailed ){
    return SQLITE_NOMEM;
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
  int(*xMakeKey)(void*,const void*,int,const void*,int),
  void(*xDel)(void*)
){
  CollSeq *pColl;
  int enc2;
  int nName = sqlite4Strlen30(zName);
  
  assert( sqlite4_mutex_held(db->mutex) );

  /* If SQLITE_UTF16 is specified as the encoding type, transform this
  ** to one of SQLITE_UTF16LE or SQLITE_UTF16BE using the
  ** SQLITE_UTF16NATIVE macro. SQLITE_UTF16 is not used internally.
  */
  enc2 = enc;
  testcase( enc2==SQLITE_UTF16 );
  testcase( enc2==SQLITE_UTF16_ALIGNED );
  if( enc2==SQLITE_UTF16 || enc2==SQLITE_UTF16_ALIGNED ){
    enc2 = SQLITE_UTF16NATIVE;
  }
  if( enc2<SQLITE_UTF8 || enc2>SQLITE_UTF16BE ){
    return SQLITE_MISUSE_BKPT;
  }

  /* Check if this call is removing or replacing an existing collation 
  ** sequence. If so, and there are active VMs, return busy. If there
  ** are no active VMs, invalidate any pre-compiled statements.
  */
  pColl = sqlite4FindCollSeq(db, (u8)enc2, zName, 0);
  if( pColl && pColl->xCmp ){
    if( db->activeVdbeCnt ){
      sqlite4Error(db, SQLITE_BUSY, 
        "unable to delete/modify collation sequence due to active statements");
      return SQLITE_BUSY;
    }
    sqlite4ExpirePreparedStatements(db);

    /* If collation sequence pColl was created directly by a call to
    ** sqlite4_create_collation, and not generated by synthCollSeq(),
    ** then any copies made by synthCollSeq() need to be invalidated.
    ** Also, collation destructor - CollSeq.xDel() - function may need
    ** to be called.
    */ 
    if( (pColl->enc & ~SQLITE_UTF16_ALIGNED)==enc2 ){
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
  if( pColl==0 ) return SQLITE_NOMEM;
  pColl->xCmp = xCompare;
  pColl->xMkKey = xMakeKey;
  pColl->pUser = pCtx;
  pColl->xDel = xDel;
  pColl->enc = (u8)(enc2 | (enc & SQLITE_UTF16_ALIGNED));
  sqlite4Error(db, SQLITE_OK, 0);
  return SQLITE_OK;
}


/*
** This array defines hard upper bounds on limit values.  The
** initializer must be kept in sync with the SQLITE_LIMIT_*
** #defines in sqlite4.h.
*/
static const int aHardLimit[] = {
  SQLITE_MAX_LENGTH,
  SQLITE_MAX_SQL_LENGTH,
  SQLITE_MAX_COLUMN,
  SQLITE_MAX_EXPR_DEPTH,
  SQLITE_MAX_COMPOUND_SELECT,
  SQLITE_MAX_VDBE_OP,
  SQLITE_MAX_FUNCTION_ARG,
  SQLITE_MAX_ATTACHED,
  SQLITE_MAX_LIKE_PATTERN_LENGTH,
  SQLITE_MAX_VARIABLE_NUMBER,
  SQLITE_MAX_TRIGGER_DEPTH,
};

/*
** Make sure the hard limits are set to reasonable values
*/
#if SQLITE_MAX_LENGTH<100
# error SQLITE_MAX_LENGTH must be at least 100
#endif
#if SQLITE_MAX_SQL_LENGTH<100
# error SQLITE_MAX_SQL_LENGTH must be at least 100
#endif
#if SQLITE_MAX_SQL_LENGTH>SQLITE_MAX_LENGTH
# error SQLITE_MAX_SQL_LENGTH must not be greater than SQLITE_MAX_LENGTH
#endif
#if SQLITE_MAX_COMPOUND_SELECT<2
# error SQLITE_MAX_COMPOUND_SELECT must be at least 2
#endif
#if SQLITE_MAX_VDBE_OP<40
# error SQLITE_MAX_VDBE_OP must be at least 40
#endif
#if SQLITE_MAX_FUNCTION_ARG<0 || SQLITE_MAX_FUNCTION_ARG>1000
# error SQLITE_MAX_FUNCTION_ARG must be between 0 and 1000
#endif
#if SQLITE_MAX_ATTACHED<0 || SQLITE_MAX_ATTACHED>62
# error SQLITE_MAX_ATTACHED must be between 0 and 62
#endif
#if SQLITE_MAX_LIKE_PATTERN_LENGTH<1
# error SQLITE_MAX_LIKE_PATTERN_LENGTH must be at least 1
#endif
#if SQLITE_MAX_COLUMN>32767
# error SQLITE_MAX_COLUMN must not exceed 32767
#endif
#if SQLITE_MAX_TRIGGER_DEPTH<1
# error SQLITE_MAX_TRIGGER_DEPTH must be at least 1
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


  /* EVIDENCE-OF: R-30189-54097 For each limit category SQLITE_LIMIT_NAME
  ** there is a hard upper bound set at compile-time by a C preprocessor
  ** macro called SQLITE_MAX_NAME. (The "_LIMIT_" in the name is changed to
  ** "_MAX_".)
  */
  assert( aHardLimit[SQLITE_LIMIT_LENGTH]==SQLITE_MAX_LENGTH );
  assert( aHardLimit[SQLITE_LIMIT_SQL_LENGTH]==SQLITE_MAX_SQL_LENGTH );
  assert( aHardLimit[SQLITE_LIMIT_COLUMN]==SQLITE_MAX_COLUMN );
  assert( aHardLimit[SQLITE_LIMIT_EXPR_DEPTH]==SQLITE_MAX_EXPR_DEPTH );
  assert( aHardLimit[SQLITE_LIMIT_COMPOUND_SELECT]==SQLITE_MAX_COMPOUND_SELECT);
  assert( aHardLimit[SQLITE_LIMIT_VDBE_OP]==SQLITE_MAX_VDBE_OP );
  assert( aHardLimit[SQLITE_LIMIT_FUNCTION_ARG]==SQLITE_MAX_FUNCTION_ARG );
  assert( aHardLimit[SQLITE_LIMIT_ATTACHED]==SQLITE_MAX_ATTACHED );
  assert( aHardLimit[SQLITE_LIMIT_LIKE_PATTERN_LENGTH]==
                                               SQLITE_MAX_LIKE_PATTERN_LENGTH );
  assert( aHardLimit[SQLITE_LIMIT_VARIABLE_NUMBER]==SQLITE_MAX_VARIABLE_NUMBER);
  assert( aHardLimit[SQLITE_LIMIT_TRIGGER_DEPTH]==SQLITE_MAX_TRIGGER_DEPTH );
  assert( SQLITE_LIMIT_TRIGGER_DEPTH==(SQLITE_N_LIMIT-1) );


  if( limitId<0 || limitId>=SQLITE_N_LIMIT ){
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
** user to API functions sqlite4_open() or sqlite4_open_v2(), and for database
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
** If successful, SQLITE_OK is returned. In this case *ppVfs is set to point to
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
  const char *zDefaultVfs,        /* VFS to use if no "vfs=xxx" query option */
  const char *zUri,               /* Nul-terminated URI to parse */
  unsigned int *pFlags,           /* IN/OUT: SQLITE_OPEN_XXX flags */
  sqlite4_vfs **ppVfs,            /* OUT: VFS to use */ 
  char **pzFile,                  /* OUT: Filename component of URI */
  char **pzErrMsg                 /* OUT: Error message (if rc!=SQLITE_OK) */
){
  int rc = SQLITE_OK;
  unsigned int flags = *pFlags;
  const char *zVfs = zDefaultVfs;
  char *zFile;
  char c;
  int nUri = sqlite4Strlen30(zUri);

  assert( *pzErrMsg==0 );

  if( ((flags & SQLITE_OPEN_URI) || sqlite4GlobalConfig.bOpenUri) 
   && nUri>=5 && memcmp(zUri, "file:", 5)==0 
  ){
    char *zOpt;
    int eState;                   /* Parser state when parsing URI */
    int iIn;                      /* Input character index */
    int iOut = 0;                 /* Output character index */
    int nByte = nUri+2;           /* Bytes of space to allocate */

    /* Make sure the SQLITE_OPEN_URI flag is set to indicate to the VFS xOpen 
    ** method that there may be extra parameters following the file-name.  */
    flags |= SQLITE_OPEN_URI;

    for(iIn=0; iIn<nUri; iIn++) nByte += (zUri[iIn]=='&');
    zFile = sqlite4_malloc(nByte);
    if( !zFile ) return SQLITE_NOMEM;

    /* Discard the scheme and authority segments of the URI. */
    if( zUri[5]=='/' && zUri[6]=='/' ){
      iIn = 7;
      while( zUri[iIn] && zUri[iIn]!='/' ) iIn++;

      if( iIn!=7 && (iIn!=16 || memcmp("localhost", &zUri[7], 9)) ){
        *pzErrMsg = sqlite4_mprintf("invalid uri authority: %.*s", 
            iIn-7, &zUri[7]);
        rc = SQLITE_ERROR;
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
    ** correspond to flags that may be passed to the sqlite4_open_v2()
    ** method. */
    zOpt = &zFile[sqlite4Strlen30(zFile)+1];
    while( zOpt[0] ){
      int nOpt = sqlite4Strlen30(zOpt);
      char *zVal = &zOpt[nOpt+1];
      int nVal = sqlite4Strlen30(zVal);

      if( nOpt==3 && memcmp("vfs", zOpt, 3)==0 ){
        zVfs = zVal;
      }else{
        struct OpenMode {
          const char *z;
          int mode;
        } *aMode = 0;
        char *zModeType = 0;
        int mask = 0;
        int limit = 0;

        if( nOpt==4 && memcmp("mode", zOpt, 4)==0 ){
          static struct OpenMode aOpenMode[] = {
            { "ro",  SQLITE_OPEN_READONLY },
            { "rw",  SQLITE_OPEN_READWRITE }, 
            { "rwc", SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE },
            { 0, 0 }
          };

          mask = SQLITE_OPEN_READONLY|SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE;
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
            *pzErrMsg = sqlite4_mprintf("no such %s mode: %s", zModeType, zVal);
            rc = SQLITE_ERROR;
            goto parse_uri_out;
          }
          if( mode>limit ){
            *pzErrMsg = sqlite4_mprintf("%s mode not allowed: %s",
                                        zModeType, zVal);
            rc = SQLITE_PERM;
            goto parse_uri_out;
          }
          flags = (flags & ~mask) | mode;
        }
      }

      zOpt = &zVal[nVal+1];
    }

  }else{
    zFile = sqlite4_malloc(nUri+2);
    if( !zFile ) return SQLITE_NOMEM;
    memcpy(zFile, zUri, nUri);
    zFile[nUri] = '\0';
    zFile[nUri+1] = '\0';
  }

  *ppVfs = sqlite4_vfs_find(zVfs);
  if( *ppVfs==0 ){
    *pzErrMsg = sqlite4_mprintf("no such vfs: %s", zVfs);
    rc = SQLITE_ERROR;
  }
 parse_uri_out:
  if( rc!=SQLITE_OK ){
    sqlite4_free(zFile);
    zFile = 0;
  }
  *pFlags = flags;
  *pzFile = zFile;
  return rc;
}


/*
** This routine does the work of opening a database on behalf of
** sqlite4_open() and sqlite4_open16(). The database filename "zFilename"  
** is UTF-8 encoded.
*/
static int openDatabase(
  const char *zFilename, /* Database filename UTF-8 encoded */
  sqlite4 **ppDb,        /* OUT: Returned database handle */
  unsigned int flags,    /* Operational flags */
  const char *zVfs       /* Name of the VFS to use */
){
  sqlite4 *db;                    /* Store allocated handle here */
  int rc;                         /* Return code */
  int isThreadsafe;               /* True for threadsafe connections */
  char *zOpen = 0;                /* Filename passed to StorageOpen() */
  char *zErrMsg = 0;              /* Error message from sqlite4ParseUri() */

  *ppDb = 0;
#ifndef SQLITE_OMIT_AUTOINIT
  rc = sqlite4_initialize();
  if( rc ) return rc;
#endif

  /* Only allow sensible combinations of bits in the flags argument.  
  ** Throw an error if any non-sense combination is used.  If we
  ** do not block illegal combinations here, it could trigger
  ** assert() statements in deeper layers.  Sensible combinations
  ** are:
  **
  **  1:  SQLITE_OPEN_READONLY
  **  2:  SQLITE_OPEN_READWRITE
  **  6:  SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
  */
  assert( SQLITE_OPEN_READONLY  == 0x01 );
  assert( SQLITE_OPEN_READWRITE == 0x02 );
  assert( SQLITE_OPEN_CREATE    == 0x04 );
  testcase( (1<<(flags&7))==0x02 ); /* READONLY */
  testcase( (1<<(flags&7))==0x04 ); /* READWRITE */
  testcase( (1<<(flags&7))==0x40 ); /* READWRITE | CREATE */
  if( ((1<<(flags&7)) & 0x46)==0 ) return SQLITE_MISUSE_BKPT;

  if( sqlite4GlobalConfig.bCoreMutex==0 ){
    isThreadsafe = 0;
  }else if( flags & SQLITE_OPEN_NOMUTEX ){
    isThreadsafe = 0;
  }else if( flags & SQLITE_OPEN_FULLMUTEX ){
    isThreadsafe = 1;
  }else{
    isThreadsafe = sqlite4GlobalConfig.bFullMutex;
  }

  /* Remove harmful bits from the flags parameter
  **
  ** The SQLITE_OPEN_NOMUTEX and SQLITE_OPEN_FULLMUTEX flags were
  ** dealt with in the previous code block.  Besides these, the only
  ** valid input flags for sqlite4_open_v2() are SQLITE_OPEN_READONLY,
  ** SQLITE_OPEN_READWRITE, SQLITE_OPEN_CREATE and some
  ** reserved bits.  Silently mask off all other flags.
  */
  flags &=  ~( SQLITE_OPEN_DELETEONCLOSE |
               SQLITE_OPEN_EXCLUSIVE |
               SQLITE_OPEN_MAIN_DB |
               SQLITE_OPEN_TEMP_DB | 
               SQLITE_OPEN_TRANSIENT_DB | 
               SQLITE_OPEN_MAIN_JOURNAL | 
               SQLITE_OPEN_TEMP_JOURNAL | 
               SQLITE_OPEN_SUBJOURNAL | 
               SQLITE_OPEN_MASTER_JOURNAL |
               SQLITE_OPEN_NOMUTEX |
               SQLITE_OPEN_FULLMUTEX |
               SQLITE_OPEN_WAL
             );

  /* Allocate the sqlite data structure */
  db = sqlite4MallocZero( sizeof(sqlite4) );
  if( db==0 ) goto opendb_out;
  if( isThreadsafe ){
    db->mutex = sqlite4MutexAlloc(SQLITE_MUTEX_RECURSIVE);
    if( db->mutex==0 ){
      sqlite4_free(db);
      db = 0;
      goto opendb_out;
    }
  }
  sqlite4_mutex_enter(db->mutex);
  db->nDb = 2;
  db->magic = SQLITE_MAGIC_BUSY;
  db->aDb = db->aDbStatic;

  assert( sizeof(db->aLimit)==sizeof(aHardLimit) );
  memcpy(db->aLimit, aHardLimit, sizeof(db->aLimit));
  db->autoCommit = 1;
  db->nextAutovac = -1;
  db->nextPagesize = 0;
  db->flags |=  SQLITE_AutoIndex | SQLITE_EnableTrigger
#if SQLITE_DEFAULT_FILE_FORMAT<4
                 | SQLITE_LegacyFileFmt
#endif
#ifdef SQLITE_ENABLE_LOAD_EXTENSION
                 | SQLITE_LoadExtension
#endif
#if SQLITE_DEFAULT_RECURSIVE_TRIGGERS
                 | SQLITE_RecTriggers
#endif
#if defined(SQLITE_DEFAULT_FOREIGN_KEYS) && SQLITE_DEFAULT_FOREIGN_KEYS
                 | SQLITE_ForeignKeys
#endif
      ;
  sqlite4HashInit(&db->aCollSeq);
#ifndef SQLITE_OMIT_VIRTUALTABLE
  sqlite4HashInit(&db->aModule);
#endif

  /* Add the default collation sequence BINARY. BINARY works for both UTF-8
  ** and UTF-16, so add a version for each to avoid any unnecessary
  ** conversions. The only error that can occur here is a malloc() failure.
  */
  createCollation(db, "BINARY", SQLITE_UTF8, 0, binCollFunc, 0, 0);
  createCollation(db, "BINARY", SQLITE_UTF16BE, 0, binCollFunc, 0, 0);
  createCollation(db, "BINARY", SQLITE_UTF16LE, 0, binCollFunc, 0, 0);
  createCollation(db, "RTRIM", SQLITE_UTF8, (void*)1, binCollFunc, 0, 0);
  if( db->mallocFailed ){
    goto opendb_out;
  }
  db->pDfltColl = sqlite4FindCollSeq(db, SQLITE_UTF8, "BINARY", 0);
  assert( db->pDfltColl!=0 );

  /* Also add a UTF-8 case-insensitive collation sequence. */
  createCollation(db, "NOCASE", SQLITE_UTF8, 0, nocaseCollatingFunc, 0, 0);

  /* Parse the filename/URI argument. */
  db->openFlags = flags;
  rc = sqlite4ParseUri(zVfs, zFilename, &flags, &db->pVfs, &zOpen, &zErrMsg);
  if( rc!=SQLITE_OK ){
    if( rc==SQLITE_NOMEM ) db->mallocFailed = 1;
    sqlite4Error(db, rc, zErrMsg ? "%s" : 0, zErrMsg);
    sqlite4_free(zErrMsg);
    goto opendb_out;
  }

  /* Open the backend database driver */
  rc = sqlite4KVStoreOpen(db, "main", zOpen, &db->aDb[0].pKV, 0);
  if( rc!=SQLITE_OK ){
    if( rc==SQLITE_IOERR_NOMEM ){
      rc = SQLITE_NOMEM;
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

  db->magic = SQLITE_MAGIC_OPEN;
  if( db->mallocFailed ){
    goto opendb_out;
  }

  /* Register all built-in functions, but do not attempt to read the
  ** database schema yet. This is delayed until the first time the database
  ** is accessed.
  */
  sqlite4Error(db, SQLITE_OK, 0);
  sqlite4RegisterBuiltinFunctions(db);

  /* Load automatic extensions - extensions that have been registered
  ** using the sqlite4_automatic_extension() API.
  */
  rc = sqlite4_errcode(db);
  if( rc==SQLITE_OK ){
    /* sqlite4AutoLoadExtensions(db); */
    rc = sqlite4_errcode(db);
    if( rc!=SQLITE_OK ){
      goto opendb_out;
    }
  }

#ifdef SQLITE_ENABLE_FTS1
  if( !db->mallocFailed ){
    extern int sqlite4Fts1Init(sqlite4*);
    rc = sqlite4Fts1Init(db);
  }
#endif

#ifdef SQLITE_ENABLE_FTS2
  if( !db->mallocFailed && rc==SQLITE_OK ){
    extern int sqlite4Fts2Init(sqlite4*);
    rc = sqlite4Fts2Init(db);
  }
#endif

#ifdef SQLITE_ENABLE_FTS3
  if( !db->mallocFailed && rc==SQLITE_OK ){
    rc = sqlite4Fts3Init(db);
  }
#endif

#ifdef SQLITE_ENABLE_ICU
  if( !db->mallocFailed && rc==SQLITE_OK ){
    rc = sqlite4IcuInit(db);
  }
#endif

#ifdef SQLITE_ENABLE_RTREE
  if( !db->mallocFailed && rc==SQLITE_OK){
    rc = sqlite4RtreeInit(db);
  }
#endif

  sqlite4Error(db, rc, 0);

  /* Enable the lookaside-malloc subsystem */
  setupLookaside(db, 0, sqlite4GlobalConfig.szLookaside,
                        sqlite4GlobalConfig.nLookaside);

  sqlite4_wal_autocheckpoint(db, SQLITE_DEFAULT_WAL_AUTOCHECKPOINT);

opendb_out:
  sqlite4_free(zOpen);
  if( db ){
    assert( db->mutex!=0 || isThreadsafe==0 || sqlite4GlobalConfig.bFullMutex==0 );
    sqlite4_mutex_leave(db->mutex);
  }
  rc = sqlite4_errcode(db);
  assert( db!=0 || rc==SQLITE_NOMEM );
  if( rc==SQLITE_NOMEM ){
    sqlite4_close(db);
    db = 0;
  }else if( rc!=SQLITE_OK ){
    db->magic = SQLITE_MAGIC_SICK;
  }
  *ppDb = db;
  return sqlite4ApiExit(0, rc);
}

/*
** Open a new database handle.
*/
int sqlite4_open(
  const char *zFilename, 
  sqlite4 **ppDb 
){
  return openDatabase(zFilename, ppDb,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 0);
}
int sqlite4_open_v2(
  const char *filename,   /* Database filename (UTF-8) */
  sqlite4 **ppDb,         /* OUT: SQLite db handle */
  int flags,              /* Flags */
  const char *zVfs        /* Name of VFS module to use */
){
  return openDatabase(filename, ppDb, (unsigned int)flags, zVfs);
}

#ifndef SQLITE_OMIT_UTF16
/*
** Open a new database handle.
*/
int sqlite4_open16(
  const void *zFilename, 
  sqlite4 **ppDb
){
  char const *zFilename8;   /* zFilename encoded in UTF-8 instead of UTF-16 */
  sqlite4_value *pVal;
  int rc;

  assert( zFilename );
  assert( ppDb );
  *ppDb = 0;
#ifndef SQLITE_OMIT_AUTOINIT
  rc = sqlite4_initialize();
  if( rc ) return rc;
#endif
  pVal = sqlite4ValueNew(0);
  sqlite4ValueSetStr(pVal, -1, zFilename, SQLITE_UTF16NATIVE, SQLITE_STATIC);
  zFilename8 = sqlite4ValueText(pVal, SQLITE_UTF8);
  if( zFilename8 ){
    rc = openDatabase(zFilename8, ppDb,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 0);
    assert( *ppDb || rc==SQLITE_NOMEM );
    if( rc==SQLITE_OK && !DbHasProperty(*ppDb, 0, DB_SchemaLoaded) ){
      ENC(*ppDb) = SQLITE_UTF16NATIVE;
    }
  }else{
    rc = SQLITE_NOMEM;
  }
  sqlite4ValueFree(pVal);

  return sqlite4ApiExit(0, rc);
}
#endif /* SQLITE_OMIT_UTF16 */

/*
** Register a new collation sequence with the database handle db.
*/
int sqlite4_create_collation(
  sqlite4* db, 
  const char *zName, 
  int enc, 
  void* pCtx,
  int(*xCompare)(void*,int,const void*,int,const void*),
  int(*xMakeKey)(void*,int,const void*,int,const void*),
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
  return SQLITE_OK;
}

#ifndef SQLITE_OMIT_UTF16
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
  return SQLITE_OK;
}
#endif /* SQLITE_OMIT_UTF16 */

#ifndef SQLITE_OMIT_DEPRECATED
/*
** This function is now an anachronism. It used to be used to recover from a
** malloc() failure, but SQLite now does this automatically.
*/
int sqlite4_global_recover(void){
  return SQLITE_OK;
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
  return db->autoCommit;
}

/*
** The following routines are subtitutes for constants SQLITE_CORRUPT,
** SQLITE_MISUSE, SQLITE_CANTOPEN, SQLITE_IOERR and possibly other error
** constants.  They server two purposes:
**
**   1.  Serve as a convenient place to set a breakpoint in a debugger
**       to detect when version error conditions occurs.
**
**   2.  Invoke sqlite4_log() to provide the source code location where
**       a low-level error is first detected.
*/
int sqlite4CorruptError(int lineno){
  testcase( sqlite4GlobalConfig.xLog!=0 );
  sqlite4_log(SQLITE_CORRUPT,
              "database corruption at line %d of [%.10s]",
              lineno, 20+sqlite4_sourceid());
  return SQLITE_CORRUPT;
}
int sqlite4MisuseError(int lineno){
  testcase( sqlite4GlobalConfig.xLog!=0 );
  sqlite4_log(SQLITE_MISUSE, 
              "misuse at line %d of [%.10s]",
              lineno, 20+sqlite4_sourceid());
  return SQLITE_MISUSE;
}
int sqlite4CantopenError(int lineno){
  testcase( sqlite4GlobalConfig.xLog!=0 );
  sqlite4_log(SQLITE_CANTOPEN, 
              "cannot open file at line %d of [%.10s]",
              lineno, 20+sqlite4_sourceid());
  return SQLITE_CANTOPEN;
}


/*
** Sleep for a little while.  Return the amount of time slept.
*/
int sqlite4_sleep(int ms){
  sqlite4_vfs *pVfs;
  int rc;
  pVfs = sqlite4_vfs_find(0);
  if( pVfs==0 ) return 0;

  /* This function works in milliseconds, but the underlying OsSleep() 
  ** API uses microseconds. Hence the 1000's.
  */
  rc = (sqlite4OsSleep(pVfs, 1000*ms)/1000);
  return rc;
}


/*
** Interface to the testing logic.
*/
int sqlite4_test_control(int op, ...){
  int rc = 0;
#ifndef SQLITE_OMIT_BUILTIN_TEST
  va_list ap;
  va_start(ap, op);
  switch( op ){

    /*
    ** Save the current state of the PRNG.
    */
    case SQLITE_TESTCTRL_PRNG_SAVE: {
      sqlite4PrngSaveState();
      break;
    }

    /*
    ** Restore the state of the PRNG to the last state saved using
    ** PRNG_SAVE.  If PRNG_SAVE has never before been called, then
    ** this verb acts like PRNG_RESET.
    */
    case SQLITE_TESTCTRL_PRNG_RESTORE: {
      sqlite4PrngRestoreState();
      break;
    }

    /*
    ** Reset the PRNG back to its uninitialized state.  The next call
    ** to sqlite4_randomness() will reseed the PRNG using a single call
    ** to the xRandomness method of the default VFS.
    */
    case SQLITE_TESTCTRL_PRNG_RESET: {
      sqlite4PrngResetState();
      break;
    }

    /*
    **  sqlite4_test_control(BITVEC_TEST, size, program)
    **
    ** Run a test against a Bitvec object of size.  The program argument
    ** is an array of integers that defines the test.  Return -1 on a
    ** memory allocation error, 0 on success, or non-zero for an error.
    ** See the sqlite4BitvecBuiltinTest() for additional information.
    */
    case SQLITE_TESTCTRL_BITVEC_TEST: {
      int sz = va_arg(ap, int);
      int *aProg = va_arg(ap, int*);
      rc = sqlite4BitvecBuiltinTest(sz, aProg);
      break;
    }

    /*
    **  sqlite4_test_control(BENIGN_MALLOC_HOOKS, xBegin, xEnd)
    **
    ** Register hooks to call to indicate which malloc() failures 
    ** are benign.
    */
    case SQLITE_TESTCTRL_BENIGN_MALLOC_HOOKS: {
      typedef void (*void_function)(void);
      void_function xBenignBegin;
      void_function xBenignEnd;
      xBenignBegin = va_arg(ap, void_function);
      xBenignEnd = va_arg(ap, void_function);
      sqlite4BenignMallocHooks(xBenignBegin, xBenignEnd);
      break;
    }

    /*
    **  sqlite4_test_control(SQLITE_TESTCTRL_PENDING_BYTE, unsigned int X)
    **
    ** Set the PENDING byte to the value in the argument, if X>0.
    ** Make no changes if X==0.  Return the value of the pending byte
    ** as it existing before this routine was called.
    **
    ** IMPORTANT:  Changing the PENDING byte from 0x40000000 results in
    ** an incompatible database file format.  Changing the PENDING byte
    ** while any database connection is open results in undefined and
    ** dileterious behavior.
    */
    case SQLITE_TESTCTRL_PENDING_BYTE: {
      rc = PENDING_BYTE;
#ifndef SQLITE_OMIT_WSD
      {
        unsigned int newVal = va_arg(ap, unsigned int);
        if( newVal ) sqlite4PendingByte = newVal;
      }
#endif
      break;
    }

    /*
    **  sqlite4_test_control(SQLITE_TESTCTRL_ASSERT, int X)
    **
    ** This action provides a run-time test to see whether or not
    ** assert() was enabled at compile-time.  If X is true and assert()
    ** is enabled, then the return value is true.  If X is true and
    ** assert() is disabled, then the return value is zero.  If X is
    ** false and assert() is enabled, then the assertion fires and the
    ** process aborts.  If X is false and assert() is disabled, then the
    ** return value is zero.
    */
    case SQLITE_TESTCTRL_ASSERT: {
      volatile int x = 0;
      assert( (x = va_arg(ap,int))!=0 );
      rc = x;
      break;
    }


    /*
    **  sqlite4_test_control(SQLITE_TESTCTRL_ALWAYS, int X)
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
    ** SQLITE_TESTCTRL_ASSERT shows that assert() is disabled and the second
    ** behavior (assert if the argument to ALWAYS() is false) is the case if
    ** SQLITE_TESTCTRL_ASSERT shows that assert() is enabled.
    **
    ** The run-time test procedure might look something like this:
    **
    **    if( sqlite4_test_control(SQLITE_TESTCTRL_ALWAYS, 2)==2 ){
    **      // ALWAYS() and NEVER() are no-op pass-through macros
    **    }else if( sqlite4_test_control(SQLITE_TESTCTRL_ASSERT, 1) ){
    **      // ALWAYS(x) asserts that x is true. NEVER(x) asserts x is false.
    **    }else{
    **      // ALWAYS(x) is a constant 1.  NEVER(x) is a constant 0.
    **    }
    */
    case SQLITE_TESTCTRL_ALWAYS: {
      int x = va_arg(ap,int);
      rc = ALWAYS(x);
      break;
    }

    /*  sqlite4_test_control(SQLITE_TESTCTRL_OPTIMIZATIONS, sqlite4 *db, int N)
    **
    ** Enable or disable various optimizations for testing purposes.  The 
    ** argument N is a bitmask of optimizations to be disabled.  For normal
    ** operation N should be 0.  The idea is that a test program (like the
    ** SQL Logic Test or SLT test module) can run the same SQL multiple times
    ** with various optimizations disabled to verify that the same answer
    ** is obtained in every case.
    */
    case SQLITE_TESTCTRL_OPTIMIZATIONS: {
      sqlite4 *db = va_arg(ap, sqlite4*);
      int x = va_arg(ap,int);
      db->flags = (x & SQLITE_OptMask) | (db->flags & ~SQLITE_OptMask);
      break;
    }

#ifdef SQLITE_N_KEYWORD
    /* sqlite4_test_control(SQLITE_TESTCTRL_ISKEYWORD, const char *zWord)
    **
    ** If zWord is a keyword recognized by the parser, then return the
    ** number of keywords.  Or if zWord is not a keyword, return 0.
    ** 
    ** This test feature is only available in the amalgamation since
    ** the SQLITE_N_KEYWORD macro is not defined in this file if SQLite
    ** is built using separate source files.
    */
    case SQLITE_TESTCTRL_ISKEYWORD: {
      const char *zWord = va_arg(ap, const char*);
      int n = sqlite4Strlen30(zWord);
      rc = (sqlite4KeywordCode((u8*)zWord, n)!=TK_ID) ? SQLITE_N_KEYWORD : 0;
      break;
    }
#endif 

    /* sqlite4_test_control(SQLITE_TESTCTRL_SCRATCHMALLOC, sz, &pNew, pFree);
    **
    ** Pass pFree into sqlite4ScratchFree(). 
    ** If sz>0 then allocate a scratch buffer into pNew.  
    */
    case SQLITE_TESTCTRL_SCRATCHMALLOC: {
      void *pFree, **ppNew;
      int sz;
      sz = va_arg(ap, int);
      ppNew = va_arg(ap, void**);
      pFree = va_arg(ap, void*);
      if( sz ) *ppNew = sqlite4ScratchMalloc(sz);
      sqlite4ScratchFree(pFree);
      break;
    }

    /*   sqlite4_test_control(SQLITE_TESTCTRL_LOCALTIME_FAULT, int onoff);
    **
    ** If parameter onoff is non-zero, configure the wrappers so that all
    ** subsequent calls to localtime() and variants fail. If onoff is zero,
    ** undo this setting.
    */
    case SQLITE_TESTCTRL_LOCALTIME_FAULT: {
      sqlite4GlobalConfig.bLocaltimeFault = va_arg(ap, int);
      break;
    }

#if defined(SQLITE_ENABLE_TREE_EXPLAIN)
    /*   sqlite4_test_control(SQLITE_TESTCTRL_EXPLAIN_STMT,
    **                        sqlite4_stmt*,const char**);
    **
    ** If compiled with SQLITE_ENABLE_TREE_EXPLAIN, each sqlite4_stmt holds
    ** a string that describes the optimized parse tree.  This test-control
    ** returns a pointer to that string.
    */
    case SQLITE_TESTCTRL_EXPLAIN_STMT: {
      sqlite4_stmt *pStmt = va_arg(ap, sqlite4_stmt*);
      const char **pzRet = va_arg(ap, const char**);
      *pzRet = sqlite4VdbeExplanation((Vdbe*)pStmt);
      break;
    }
#endif

  }
  va_end(ap);
#endif /* SQLITE_OMIT_BUILTIN_TEST */
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
  if( z && sqlite4Atoi64(z, &v, sqlite4Strlen30(z), SQLITE_UTF8)==SQLITE_OK ){
    bDflt = v;
  }
  return bDflt;
}
