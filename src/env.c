/*
** 2013 January 7
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
** This file contains code used to help implement the sqlite4_env object.
*/
#include "sqliteInt.h"


/*
** Default factory objects
*/
static KVFactory memFactory = {
   0,
   "temp",
   sqlite4KVStoreOpenMem,
   1
};
KVFactory sqlite4BuiltinFactory = {
   &memFactory,
   "main",
   sqlite4KVStoreOpenLsm,
   1
};

/*
** The following singleton contains the global configuration for
** the SQLite library.
*/
struct sqlite4_env sqlite4DefaultEnv = {
   sizeof(sqlite4_env),       /* nByte */
   1,                         /* iVersion */
   SQLITE4_DEFAULT_MEMSTATUS, /* bMemstat */
   1,                         /* bCoreMutex */
   SQLITE4_THREADSAFE==1,     /* bFullMutex */
   0x7ffffffe,                /* mxStrlen */
   128,                       /* szLookaside */
   500,                       /* nLookaside */
   &sqlite4MMSystem,          /* pMM */
   {0,0,0,0,0,0,0,0,0},       /* m */
   {0,0,0,0,0,0,0,0,0,0},     /* mutex */
   (void*)0,                  /* pHeap */
   0,                         /* nHeap */
   0, 0,                      /* mnHeap, mxHeap */
   0,                         /* mxParserStack */
   &sqlite4BuiltinFactory,    /* pFactory */
   sqlite4OsRandomness,       /* xRandomness */
   sqlite4OsCurrentTime,      /* xCurrentTime */
   /* All the rest should always be initialized to zero */
   0,                         /* isInit */
   0,                         /* pFactoryMutex */
   0,                         /* pPrngMutex */
   0, 0,                      /* prngX, prngY */
   0,                         /* xLog */
   0,                         /* pLogArg */
   0,                         /* bLocaltimeFault */
   0,                         /* pMemMutex */
   {0,0,0,0},                 /* nowValue[] */
   {0,0,0,0},                 /* mxValue[] */
   {0,}                       /* hashGlobalFunc */
};

/*
** Return the default environment
*/
sqlite4_env *sqlite4_env_default(void){ return &sqlite4DefaultEnv; }

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
    ** sqlite4_env_config(pEnv, SQLITE4_ENVCONFIG_SERIALIZED);
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
    ** sqlite4_env_config(pEnv, SQLITE4_ENVCONFIG_KVSTORE_PUSH, zName,xFactory);
    **
    ** Push a new KVStore factory onto the factory stack.  The new factory
    ** takes priority over prior factories.
    */
    case SQLITE4_ENVCONFIG_KVSTORE_PUSH: {
      const char *zName = va_arg(ap, const char*);
      int nName = sqlite4Strlen30(zName);
      KVFactory *pMkr = sqlite4_malloc(pEnv, sizeof(*pMkr)+nName+1);
      char *z;
      if( pMkr==0 ) return SQLITE4_NOMEM;
      z = (char*)&pMkr[1];
      memcpy(z, zName, nName+1);
      memset(pMkr, 0, sizeof(*pMkr));
      pMkr->zName = z;
      pMkr->xFactory = va_arg(ap, sqlite4_kvfactory);
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
