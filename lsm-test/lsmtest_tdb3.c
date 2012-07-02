
#include "lsmtest_tdb.h"
#include "lsm.h"

#include "lsmtest.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <stdio.h>

#include <sys/time.h>

typedef struct LsmDb LsmDb;
typedef struct LsmWorker LsmWorker;
typedef struct LsmFile LsmFile;

#ifdef LSM_MUTEX_PTHREADS
#include <pthread.h>
struct LsmWorker {
  LsmDb *pDb;                     /* Main database structure */
  lsm_db *pWorker;                /* Worker database handle */
  pthread_t worker_thread;        /* Worker thread */
  pthread_cond_t worker_cond;     /* Condition var the worker waits on */
  pthread_mutex_t worker_mutex;   /* Mutex used with worker_cond */
  int bDoWork;                    /* Set to true by client when there is work */
  int worker_rc;                  /* Store error code here */

  int lsm_work_flags;             /* Flags to pass to lsm_work() */
  int lsm_work_npage;             /* nPage parameter to pass to lsm_work() */
};
#else
struct LsmWorker { int worker_rc; };
#endif

static void mt_shutdown(LsmDb *);

lsm_env *tdb_lsm_env(void){
  static int bInit = 0;
  static lsm_env env;
  if( bInit==0 ){
    memcpy(&env, lsm_default_env(), sizeof(env));
    bInit = 1;
  }
  return &env;
}

typedef struct FileSector FileSector;
typedef struct FileData FileData;

struct FileSector {
  u8 *aOld;                       /* Old data for this sector */
};

struct FileData {
  int nSector;                    /* Allocated size of apSector[] array */
  FileSector *aSector;            /* Array of file sectors */
};

/*
** bPrepareCrash:
**   If non-zero, the file wrappers maintain enough in-memory data to
**   simulate the effect of a power-failure on the file-system (i.e. that
**   unsynced sectors may be written, not written, or overwritten with
**   arbitrary data when the crash occurs).
**
** bCrashed:
**   Set to true after a crash is simulated. Once this variable is true, all
**   VFS methods other than xClose() return LSM_IOERR as soon as they are
**   called (without affecting the contents of the file-system).
**
** env:
**   The environment object used by all lsm_db* handles opened by this
**   object (i.e. LsmDb.db plus any worker connections). Variable env.pVfsCtx
**   always points to the containing LsmDb structure.
*/
struct LsmDb {
  TestDb base;                    /* Base class - methods table */
  lsm_env env;                    /* Environment used by connection db */
  char *zName;                    /* Database file name */
  lsm_db *db;                     /* LSM database handle */

  lsm_cursor *pCsr;               /* Cursor held open during read transaction */
  void *pBuf;                     /* Buffer for tdb_fetch() output */
  int nBuf;                       /* Allocated (not used) size of pBuf */

  /* Crash testing related state */
  int bCrashed;                   /* True once a crash has occurred */
  int nAutoCrash;                 /* Number of syncs until a crash */
  int bPrepareCrash;              /* True to store writes in memory */

  /* Unsynced data (while crash testing) */
  int szSector;                   /* Assumed size of disk sectors (512B) */
  FileData aFile[2];              /* Database and log file data */

  /* Work hook redirection */
  void (*xWork)(lsm_db *, void *);
  void *pWorkCtx;

  /* IO logging hook */
  void (*xWriteHook)(void *, int, lsm_i64, int, int);
  void *pWriteCtx;
  
  /* Worker threads (for lsm_mt) */
  int nWorker;
  LsmWorker *aWorker;
};

/*************************************************************************
**************************************************************************
** Begin test VFS code.
*/

struct LsmFile {
  lsm_file *pReal;                /* Real underlying file */
  int bLog;                       /* True for log file. False for db file */
  LsmDb *pDb;                     /* Database handle that uses this file */
};

static int testEnvFullpath(
  lsm_env *pEnv,                  /* Environment for current LsmDb */
  const char *zFile,              /* Relative path name */
  char *zOut,                     /* Output buffer */
  int *pnOut                      /* IN/OUT: Size of output buffer */
){
  lsm_env *pRealEnv = tdb_lsm_env();
  return pRealEnv->xFullpath(pRealEnv, zFile, zOut, pnOut);
}

static int testEnvOpen(
  lsm_env *pEnv,                  /* Environment for current LsmDb */
  const char *zFile,              /* Name of file to open */
  lsm_file **ppFile               /* OUT: New file handle object */
){
  lsm_env *pRealEnv = tdb_lsm_env();
  LsmDb *pDb = (LsmDb *)pEnv->pVfsCtx;
  int rc;                         /* Return Code */
  LsmFile *pRet;                  /* The new file handle */
  int nFile;                      /* Length of string zFile in bytes */

  nFile = strlen(zFile);
  pRet = (LsmFile *)testMalloc(sizeof(LsmFile));
  pRet->pDb = pDb;
  pRet->bLog = (nFile > 4 && 0==memcmp("-log", &zFile[nFile-4], 4));

  rc = pRealEnv->xOpen(pRealEnv, zFile, &pRet->pReal);
  if( rc!=LSM_OK ){
    testFree(pRet);
    pRet = 0;
  }

  *ppFile = (lsm_file *)pRet;
  return rc;
}

static int testEnvRead(lsm_file *pFile, lsm_i64 iOff, void *pData, int nData){
  lsm_env *pRealEnv = tdb_lsm_env();
  LsmFile *p = (LsmFile *)pFile;
  if( p->pDb->bCrashed ) return LSM_IOERR;
  return pRealEnv->xRead(p->pReal, iOff, pData, nData);
}

static int testEnvWrite(lsm_file *pFile, lsm_i64 iOff, void *pData, int nData){
  lsm_env *pRealEnv = tdb_lsm_env();
  LsmFile *p = (LsmFile *)pFile;
  LsmDb *pDb = p->pDb;

  if( pDb->bCrashed ) return LSM_IOERR;

  if( pDb->bPrepareCrash ){
    FileData *pData = &pDb->aFile[p->bLog];
    int iFirst;                 
    int iLast;
    int iSector;

    iFirst = (iOff / pDb->szSector);
    iLast =  ((iOff + nData - 1) / pDb->szSector);

    if( pData->nSector<(iLast+1) ){
      int nNew = ( ((iLast + 1) + 63) / 64 ) * 64;
      assert( nNew>iLast );
      pData->aSector = (FileSector *)testRealloc(
          pData->aSector, nNew*sizeof(FileSector)
      );
      memset(&pData->aSector[pData->nSector], 
          0, (nNew - pData->nSector) * sizeof(FileSector)
      );
      pData->nSector = nNew;
    }

    for(iSector=iFirst; iSector<=iLast; iSector++){
      if( pData->aSector[iSector].aOld==0 ){
        u8 *aOld = (u8 *)testMalloc(pDb->szSector);
        pRealEnv->xRead(
            p->pReal, (lsm_i64)iSector*pDb->szSector, aOld, pDb->szSector
        );
        pData->aSector[iSector].aOld = aOld;
      }
    }
  }

  if( pDb->xWriteHook ){
    int rc;
    int nUs;
    struct timeval t1;
    struct timeval t2;

    gettimeofday(&t1, 0);
    assert( nData>0 );
    rc = pRealEnv->xWrite(p->pReal, iOff, pData, nData);
    gettimeofday(&t2, 0);

    nUs = (t2.tv_sec - t1.tv_sec) * 1000000 + (t2.tv_usec - t1.tv_usec);
    pDb->xWriteHook(pDb->pWriteCtx, p->bLog, iOff, nData, nUs);
    return rc;
  }

  return pRealEnv->xWrite(p->pReal, iOff, pData, nData);
}

static void doSystemCrash(LsmDb *pDb);

static int testEnvSync(lsm_file *pFile){
  lsm_env *pRealEnv = tdb_lsm_env();
  LsmFile *p = (LsmFile *)pFile;
  LsmDb *pDb = p->pDb;
  FileData *pData = &pDb->aFile[p->bLog];
  int i;

  if( pDb->bCrashed ) return LSM_IOERR;

  if( pDb->nAutoCrash ){
    pDb->nAutoCrash--;
    if( pDb->nAutoCrash==0 ){
      doSystemCrash(pDb);
      pDb->bCrashed = 1;
      return LSM_IOERR;
    }
  }

  if( pDb->bPrepareCrash ){
    for(i=0; i<pData->nSector; i++){
      testFree(pData->aSector[i].aOld);
      pData->aSector[i].aOld = 0;
    }
  }

  if( pDb->xWriteHook ){
    int rc;
    int nUs;
    struct timeval t1;
    struct timeval t2;

    gettimeofday(&t1, 0);
    rc = pRealEnv->xSync(p->pReal);
    gettimeofday(&t2, 0);

    nUs = (t2.tv_sec - t1.tv_sec) * 1000000 + (t2.tv_usec - t1.tv_usec);
    pDb->xWriteHook(pDb->pWriteCtx, p->bLog, 0, 0, nUs);
    return rc;
  }

  return pRealEnv->xSync(p->pReal);
}

static int testEnvTruncate(lsm_file *pFile, lsm_i64 iOff){
  lsm_env *pRealEnv = tdb_lsm_env();
  LsmFile *p = (LsmFile *)pFile;
  if( p->pDb->bCrashed ) return LSM_IOERR;
  return pRealEnv->xTruncate(p->pReal, iOff);
}

static int testEnvSectorSize(lsm_file *pFile){
  lsm_env *pRealEnv = tdb_lsm_env();
  LsmFile *p = (LsmFile *)pFile;
  return pRealEnv->xSectorSize(p->pReal);
}

static int testEnvRemap(
  lsm_file *pFile, 
  lsm_i64 iMin, 
  void **ppOut,
  lsm_i64 *pnOut
){
  lsm_env *pRealEnv = tdb_lsm_env();
  LsmFile *p = (LsmFile *)pFile;
  return pRealEnv->xRemap(p->pReal, iMin, ppOut, pnOut);
}

static int testEnvFileid(
  lsm_file *pFile, 
  void *ppOut,
  int *pnOut
){
  lsm_env *pRealEnv = tdb_lsm_env();
  LsmFile *p = (LsmFile *)pFile;
  return pRealEnv->xFileid(p->pReal, ppOut, pnOut);
}

static int testEnvClose(lsm_file *pFile){
  lsm_env *pRealEnv = tdb_lsm_env();
  LsmFile *p = (LsmFile *)pFile;

  pRealEnv->xClose(p->pReal);
  testFree(p);
  return LSM_OK;
}

static int testEnvUnlink(lsm_env *pEnv, const char *zFile){
  lsm_env *pRealEnv = tdb_lsm_env();
  unused_parameter(pEnv);
  return pRealEnv->xUnlink(pRealEnv, zFile);
}

static void doSystemCrash(LsmDb *pDb){
  lsm_env *pEnv = tdb_lsm_env();
  int iFile;
  int iSeed = pDb->aFile[0].nSector + pDb->aFile[1].nSector;

  char *zFile = pDb->zName;
  char *zFree = 0;

  for(iFile=0; iFile<2; iFile++){
    lsm_file *pFile = 0;
    int i;

    pEnv->xOpen(pEnv, zFile, &pFile);
    for(i=0; i<pDb->aFile[iFile].nSector; i++){
      u8 *aOld = pDb->aFile[iFile].aSector[i].aOld;
      if( aOld ){
        int iOpt = testPrngValue(iSeed++) % 3;
        switch( iOpt ){
          case 0:
            break;

          case 1:
            testPrngArray(iSeed++, (u32 *)aOld, pDb->szSector/4);

          case 2:
            pEnv->xWrite(
                pFile, (lsm_i64)i * pDb->szSector, aOld, pDb->szSector
            );
            break;
        }
        testFree(aOld);
        pDb->aFile[iFile].aSector[i].aOld = 0;
      }
    }
    pEnv->xClose(pFile);
    zFree = zFile = sqlite3_mprintf("%s-log", pDb->zName);
  }

  sqlite3_free(zFree);
}
/*
** End test VFS code.
**************************************************************************
*************************************************************************/

static int test_lsm_close(TestDb *pTestDb){
  int i;
  int rc = LSM_OK;
  LsmDb *pDb = (LsmDb *)pTestDb;

  lsm_csr_close(pDb->pCsr);
  lsm_close(pDb->db);

  /* If this is a multi-threaded database, wait on the worker threads. */
  mt_shutdown(pDb);
  for(i=0; i<pDb->nWorker && rc==LSM_OK; i++){
    rc = pDb->aWorker[i].worker_rc;
  }

  for(i=0; i<pDb->aFile[0].nSector; i++){
    testFree(pDb->aFile[0].aSector[i].aOld);
  }
  testFree(pDb->aFile[0].aSector);
  for(i=0; i<pDb->aFile[1].nSector; i++){
    testFree(pDb->aFile[1].aSector[i].aOld);
  }
  testFree(pDb->aFile[1].aSector);

  memset(pDb, sizeof(LsmDb), 0x11);
  testFree((char *)pDb->pBuf);
  testFree((char *)pDb);
  return rc;
}

static int test_lsm_write(
  TestDb *pTestDb, 
  void *pKey, 
  int nKey, 
  void *pVal, 
  int nVal
){
  LsmDb *pDb = (LsmDb *)pTestDb;
  return lsm_write(pDb->db, pKey, nKey, pVal, nVal);
}

static int test_lsm_delete(TestDb *pTestDb, void *pKey, int nKey){
  LsmDb *pDb = (LsmDb *)pTestDb;
  return lsm_delete(pDb->db, pKey, nKey);
}

static int test_lsm_fetch(
  TestDb *pTestDb, 
  void *pKey, 
  int nKey, 
  void **ppVal, 
  int *pnVal
){
  int rc;
  LsmDb *pDb = (LsmDb *)pTestDb;
  lsm_cursor *csr;

  if( pKey==0 ) return LSM_OK;

  rc = lsm_csr_open(pDb->db, &csr);
  if( rc!=LSM_OK ) return rc;

  rc = lsm_csr_seek(csr, pKey, nKey, LSM_SEEK_EQ);
  if( rc==LSM_OK ){
    if( lsm_csr_valid(csr) ){
      const void *pVal; int nVal;
      rc = lsm_csr_value(csr, &pVal, &nVal);
      if( nVal>pDb->nBuf ){
        testFree(pDb->pBuf);
        pDb->pBuf = testMalloc(nVal*2);
        pDb->nBuf = nVal*2;
      }
      memcpy(pDb->pBuf, pVal, nVal);
      *ppVal = pDb->pBuf;
      *pnVal = nVal;
    }else{
      *ppVal = 0;
      *pnVal = -1;
    }
  }
  lsm_csr_close(csr);
  return rc;
}

static int test_lsm_scan(
  TestDb *pTestDb,
  void *pCtx,
  int bReverse,
  void *pFirst, int nFirst,
  void *pLast, int nLast,
  void (*xCallback)(void *, void *, int , void *, int)
){
  LsmDb *pDb = (LsmDb *)pTestDb;
  lsm_cursor *csr;
  int rc;

  rc = lsm_csr_open(pDb->db, &csr);
  if( rc!=LSM_OK ) return rc;

  if( bReverse ){
    if( pLast ){
      rc = lsm_csr_seek(csr, pLast, nLast, LSM_SEEK_LE);
    }else{
      rc = lsm_csr_last(csr);
    }
  }else{
    if( pFirst ){
      rc = lsm_csr_seek(csr, pFirst, nFirst, LSM_SEEK_GE);
    }else{
      rc = lsm_csr_first(csr);
    }
  }

  while( rc==LSM_OK && lsm_csr_valid(csr) ){
    const void *pKey; int nKey;
    const void *pVal; int nVal;
    int cmp;

    lsm_csr_key(csr, &pKey, &nKey);
    lsm_csr_value(csr, &pVal, &nVal);

    if( bReverse && pFirst ){
      cmp = memcmp(pFirst, pKey, MIN(nKey, nFirst));
      if( cmp>0 || (cmp==0 && nFirst>nKey) ) break;
    }else if( bReverse==0 && pLast ){
      cmp = memcmp(pLast, pKey, MIN(nKey, nLast));
      if( cmp<0 || (cmp==0 && nLast<nKey) ) break;
    }

    xCallback(pCtx, (void *)pKey, nKey, (void *)pVal, nVal);

    if( bReverse ){
      rc = lsm_csr_prev(csr);
    }else{
      rc = lsm_csr_next(csr);
    }
  }

  lsm_csr_close(csr);
  return rc;
}

static int test_lsm_begin(TestDb *pTestDb, int iLevel){
  int rc = LSM_OK;
  LsmDb *pDb = (LsmDb *)pTestDb;

  /* iLevel==0 is a no-op. */
  if( iLevel==0 ) return 0;

  if( pDb->pCsr==0 ) rc = lsm_csr_open(pDb->db, &pDb->pCsr);
  if( rc==LSM_OK && iLevel>1 ){
    rc = lsm_begin(pDb->db, iLevel-1);
  }

  return rc;
}
static int test_lsm_commit(TestDb *pTestDb, int iLevel){
  LsmDb *pDb = (LsmDb *)pTestDb;

  /* If iLevel==0, close any open read transaction */
  if( iLevel==0 && pDb->pCsr ){
    lsm_csr_close(pDb->pCsr);
    pDb->pCsr = 0;
  }

  /* If iLevel==0, close any open read transaction */
  return lsm_commit(pDb->db, MAX(0, iLevel-1));
}
static int test_lsm_rollback(TestDb *pTestDb, int iLevel){
  LsmDb *pDb = (LsmDb *)pTestDb;

  /* If iLevel==0, close any open read transaction */
  if( iLevel==0 && pDb->pCsr ){
    lsm_csr_close(pDb->pCsr);
    pDb->pCsr = 0;
  }

  return lsm_rollback(pDb->db, MAX(0, iLevel-1));
}

/*
** A log message callback registered with lsm connections. Prints all 
** messages to stderr.
*/
static void xLog(void *pCtx, int rc, const char *z){
  unused_parameter(pCtx);
  unused_parameter(rc);
  /* fprintf(stderr, "lsm: rc=%d \"%s\"\n", rc, z); */
  fprintf(stderr, "%s\n", z);
  fflush(stderr);

}

static void xWorkHook(lsm_db *db, void *pArg){
  LsmDb *p = (LsmDb *)pArg;
  if( p->xWork ) p->xWork(db, p->pWorkCtx);
}


int test_lsm_config_str(
  lsm_db *pDb, 
  int bWorker,
  const char *zStr
){
  
  struct CfgParam {
    const char *zParam;
    int bWorker;
    int eParam;
  } aParam[] = {
    { "write_buffer",   0, LSM_CONFIG_WRITE_BUFFER },
    { "page_size",      0, LSM_CONFIG_PAGE_SIZE },
    { "block_size",     0, LSM_CONFIG_BLOCK_SIZE },
    { "safety",         0, LSM_CONFIG_SAFETY },
    { "autowork",       0, LSM_CONFIG_AUTOWORK },
    { "log_size",       0, LSM_CONFIG_LOG_SIZE },
    { "mmap",           0, LSM_CONFIG_MMAP },
    { "use_log",        0, LSM_CONFIG_USE_LOG },
    { "nmerge",         0, LSM_CONFIG_NMERGE },
    { "worker_nmerge",  1, LSM_CONFIG_NMERGE },
    { 0, 0 }
  };
  const char *z = zStr;

  while( z[0] && pDb ){
    const char *zStart;

    /* Skip whitespace */
    while( *z==' ' ) z++;
    zStart = z;

    while( *z && *z!='=' ) z++;
    if( *z ){
      int eParam;
      int i;
      int iVal;
      int rc;
      char zParam[32];
      int nParam = z-zStart;
      if( nParam==0 || nParam>sizeof(zParam)-1 ) goto syntax_error;

      memcpy(zParam, zStart, nParam);
      zParam[nParam] = '\0';
      rc = testArgSelect(aParam, "param", zParam, &i);
      if( rc!=0 ) return rc;
      eParam = aParam[i].eParam;

      z++;
      zStart = z;
      while( *z>='0' && *z<='9' ) z++;
      nParam = z-zStart;
      if( nParam==0 || nParam>sizeof(zParam)-1 ) goto syntax_error;
      memcpy(zParam, zStart, nParam);
      zParam[nParam] = '\0';
      iVal = atoi(zParam);

      if( bWorker || aParam[i].bWorker==0 ){
        lsm_config(pDb, eParam, &iVal);
      }
    }else if( z!=zStart ){
      goto syntax_error;
    }
  }

  return 0;
 syntax_error:
  testPrintError("syntax error at: \"%s\"\n", z);
  return 1;
}

int tdb_lsm_config_str(TestDb *pDb, const char *zStr){
  int rc = 0;
  if( tdb_lsm(pDb) ){
    int i;
    LsmDb *pLsm = (LsmDb *)pDb;

    rc = test_lsm_config_str(pLsm->db, 0, zStr);
#ifdef LSM_MUTEX_PTHREADS
    for(i=0; rc==0 && i<pLsm->nWorker; i++){
      rc = test_lsm_config_str(pLsm->aWorker[i].pWorker, 1, zStr);
    }
#endif
  }
  return rc;
}

static int testLsmOpen(
  const char *zCfg,
  const char *zFilename, 
  int bClear, 
  TestDb **ppDb
){
  static const DatabaseMethods LsmMethods = {
    test_lsm_close,
    test_lsm_write,
    test_lsm_delete,
    test_lsm_fetch,
    test_lsm_scan,
    test_lsm_begin,
    test_lsm_commit,
    test_lsm_rollback
  };

  int rc;
  int nFilename;
  LsmDb *pDb;

  /* If the bClear flag is set, delete any existing database. */
  assert( zFilename);
  if( bClear ) testDeleteLsmdb(zFilename);
  nFilename = strlen(zFilename);

  pDb = (LsmDb *)testMalloc(sizeof(LsmDb) + nFilename + 1);
  memset(pDb, 0, sizeof(LsmDb));
  pDb->base.pMethods = &LsmMethods;
  pDb->zName = (char *)&pDb[1];
  memcpy(pDb->zName, zFilename, nFilename + 1);

  /* Default the sector size used for crash simulation to 512 bytes. 
  ** Todo: There should be an OS method to obtain this value - just as
  ** there is in SQLite. For now, LSM assumes that it is smaller than
  ** the page size (default 4KB).
  */
  pDb->szSector = 256;

  memcpy(&pDb->env, tdb_lsm_env(), sizeof(lsm_env));
  pDb->env.pVfsCtx = (void *)pDb;
  pDb->env.xFullpath = testEnvFullpath;
  pDb->env.xOpen = testEnvOpen;
  pDb->env.xRead = testEnvRead;
  pDb->env.xWrite = testEnvWrite;
  pDb->env.xTruncate = testEnvTruncate;
  pDb->env.xSync = testEnvSync;
  pDb->env.xSectorSize = testEnvSectorSize;
  pDb->env.xRemap = testEnvRemap;
  pDb->env.xFileid = testEnvFileid;
  pDb->env.xClose = testEnvClose;
  pDb->env.xUnlink = testEnvUnlink;

  rc = lsm_new(&pDb->env, &pDb->db);
  if( rc==LSM_OK ){
    lsm_config_log(pDb->db, xLog, 0);
    lsm_config_work_hook(pDb->db, xWorkHook, (void *)pDb);
    tdb_lsm_config_str((TestDb *)pDb, zCfg);
    rc = lsm_open(pDb->db, zFilename);
    if( rc!=LSM_OK ){
      test_lsm_close((TestDb *)pDb);
      pDb = 0;
    }
  }

  *ppDb = (TestDb *)pDb;
  return rc;
}

int test_lsm_open(const char *zFilename, int bClear, TestDb **ppDb){
  return testLsmOpen("", zFilename, bClear, ppDb);
}

int test_lsm_small_open(
  const char *zFile, 
  int bClear, 
  TestDb **ppDb
){
  const char *zCfg = "page_size=256 block_size=65536";
  return testLsmOpen(zCfg, zFile, bClear, ppDb);
}

int test_lsm_lomem_open(
  const char *zFilename, 
  int bClear, 
  TestDb **ppDb
){
  const char *zCfg = "page_size=256 block_size=65536 write_buffer=16384";
  return testLsmOpen(zCfg, zFilename, bClear, ppDb);
}

lsm_db *tdb_lsm(TestDb *pDb){
  if( pDb->pMethods->xClose==test_lsm_close ){
    return ((LsmDb *)pDb)->db;
  }
  return 0;
}

void tdb_lsm_enable_log(TestDb *pDb, int bEnable){
  lsm_db *db = tdb_lsm(pDb);
  if( db ){
    lsm_config_log(db, (bEnable ? xLog : 0), 0);
  }
}

void tdb_lsm_application_crash(TestDb *pDb){
  if( tdb_lsm(pDb) ){
    LsmDb *p = (LsmDb *)pDb;
    p->bCrashed = 1;
  }
}

void tdb_lsm_prepare_system_crash(TestDb *pDb){
  if( tdb_lsm(pDb) ){
    LsmDb *p = (LsmDb *)pDb;
    p->bPrepareCrash = 1;
  }
}

void tdb_lsm_system_crash(TestDb *pDb){
  if( tdb_lsm(pDb) ){
    LsmDb *p = (LsmDb *)pDb;
    p->bCrashed = 1;
    doSystemCrash(p);
  }
}

void tdb_lsm_safety(TestDb *pDb, int eMode){
  assert( eMode==LSM_SAFETY_OFF 
       || eMode==LSM_SAFETY_NORMAL 
       || eMode==LSM_SAFETY_FULL 
  );
  if( tdb_lsm(pDb) ){
    int iParam = eMode;
    LsmDb *p = (LsmDb *)pDb;
    lsm_config(p->db, LSM_CONFIG_SAFETY, &iParam);
  }
}

void tdb_lsm_prepare_sync_crash(TestDb *pDb, int iSync){
  assert( iSync>0 );
  if( tdb_lsm(pDb) ){
    LsmDb *p = (LsmDb *)pDb;
    p->nAutoCrash = iSync;
    p->bPrepareCrash = 1;
  }
}

void tdb_lsm_config_work_hook(
  TestDb *pDb, 
  void (*xWork)(lsm_db *, void *), 
  void *pWorkCtx
){
  if( tdb_lsm(pDb) ){
    LsmDb *p = (LsmDb *)pDb;
    p->xWork = xWork;
    p->pWorkCtx = pWorkCtx;
  }
}

void tdb_lsm_write_hook(
  TestDb *pDb, 
  void (*xWrite)(void *, int, lsm_i64, int, int),
  void *pWriteCtx
){
  if( tdb_lsm(pDb) ){
    LsmDb *p = (LsmDb *)pDb;
    p->xWriteHook = xWrite;
    p->pWriteCtx = pWriteCtx;
  }
}

int tdb_lsm_open(const char *zCfg, const char *zDb, int bClear, TestDb **ppDb){
  return testLsmOpen(zCfg, zDb, bClear, ppDb);
}

#ifdef LSM_MUTEX_PTHREADS

static void *worker_main(void *pArg){
  LsmWorker *p = (LsmWorker *)pArg;
  lsm_db *pWorker;                /* Connection to access db through */

  pthread_mutex_lock(&p->worker_mutex);
  while( (pWorker = p->pWorker) ){
    int nWrite = 0;
    int rc;

    /* Do some work. If an error occurs, exit. */
    pthread_mutex_unlock(&p->worker_mutex);
    rc = lsm_work(pWorker, p->lsm_work_flags, p->lsm_work_npage, &nWrite);
    pthread_mutex_lock(&p->worker_mutex);
    if( rc!=LSM_OK ){
      p->worker_rc = rc;
      break;
    }

    /* If the call to lsm_work() indicates that there is nothing more
    ** to do at this point, wait on the condition variable. The thread will
    ** wake up when it is signaled either because the client thread has
    ** flushed an in-memory tree into the db file or when the connection
    ** is being closed.  */
    if( nWrite==0 ){
      if( p->pWorker && p->bDoWork==0 ){
        pthread_cond_wait(&p->worker_cond, &p->worker_mutex);
      }
      p->bDoWork = 0;
    }
  }
  pthread_mutex_unlock(&p->worker_mutex);
  
  return 0;
}


/*
** Signal worker thread iWorker that there may be work to do.
*/
static void mt_signal_worker(LsmDb *pDb, int iWorker){
  LsmWorker *p = &pDb->aWorker[iWorker];
  pthread_mutex_lock(&p->worker_mutex);
  p->bDoWork = 1;
  pthread_cond_signal(&p->worker_cond);
  pthread_mutex_unlock(&p->worker_mutex);
}


static void mt_stop_worker(LsmDb *pDb, int iWorker){
  LsmWorker *p = &pDb->aWorker[iWorker];
  if( p->pWorker ){
    void *pDummy;
    lsm_db *pWorker;

    /* Signal the worker to stop */
    pthread_mutex_lock(&p->worker_mutex);
    pWorker = p->pWorker;
    p->pWorker = 0;
    pthread_cond_signal(&p->worker_cond);
    pthread_mutex_unlock(&p->worker_mutex);

    /* Join the worker thread. */
    pthread_join(p->worker_thread, &pDummy);

    /* Free resources allocated in mt_start_worker() */
    pthread_cond_destroy(&p->worker_cond);
    pthread_mutex_destroy(&p->worker_mutex);
    lsm_close(pWorker);
  }
}

static void mt_shutdown(LsmDb *pDb){
  int i;
  for(i=0; i<pDb->nWorker; i++){
    mt_stop_worker(pDb, i);
  }
}

/*
** This callback is invoked by LSM when the client database writes to
** the database file (i.e. to flush the contents of the in-memory tree).
** This implies there may be work to do on the database, so signal
** the worker threads.
*/
static void mt_client_work_hook(lsm_db *db, void *pArg){
  LsmDb *pDb = (LsmDb *)pArg;     /* LsmDb database handle */
  int i;                          /* Iterator variable */

  /* Invoke the user level work-hook, if any. */
  if( pDb->xWork ) pDb->xWork(db, pDb->pWorkCtx);

  /* Signal each worker thread */
  for(i=0; i<pDb->nWorker; i++){
    mt_signal_worker(pDb, i);
  }
}

static void mt_worker_work_hook(lsm_db *db, void *pArg){
  LsmDb *pDb = (LsmDb *)pArg;     /* LsmDb database handle */

  /* Invoke the user level work-hook, if any. */
  if( pDb->xWork ) pDb->xWork(db, pDb->pWorkCtx);
}

/*
** Launch worker thread iWorker for database connection pDb.
*/
static int mt_start_worker(
  LsmDb *pDb,                     /* Main database structure */
  int iWorker,                    /* Worker number to start */
  const char *zFilename,          /* File name of database to open */
  int flags,                      /* flags parameter to lsm_work() */
  int nPage                       /* nPage parameter to lsm_work() */
){
  int rc = 0;                     /* Return code */
  LsmWorker *p;                   /* Object to initialize */

  assert( iWorker<pDb->nWorker );

  p = &pDb->aWorker[iWorker];
  p->lsm_work_flags = flags;
  p->lsm_work_npage = nPage;

  /* Open the worker connection */
  if( rc==0 ) rc = lsm_new(&pDb->env, &p->pWorker);
  if( rc==0 ) rc = lsm_open(p->pWorker, zFilename);

  /* Configure the work-hook */
  if( rc==0 ){
    lsm_config_work_hook(p->pWorker, mt_worker_work_hook, (void *)pDb);
  }

  /* Kick off the worker thread. */
  if( rc==0 ) rc = pthread_cond_init(&p->worker_cond, 0);
  if( rc==0 ) rc = pthread_mutex_init(&p->worker_mutex, 0);
  if( rc==0 ) rc = pthread_create(&p->worker_thread, 0, worker_main, (void *)p);

  return rc;
}

static int test_lsm_mt(
  const char *zFilename,          /* File to open */
  int nWorker,                    /* Either 1 or 2, for worker threads */
  int bClear,                     /* True to delete any existing db */
  TestDb **ppDb                   /* OUT: TestDb database handle */
){
  LsmDb *pDb;
  int rc;

  rc = test_lsm_open(zFilename, bClear, ppDb);
  pDb = (LsmDb *)*ppDb;

  assert( nWorker==1 || nWorker==2 );

  /* Turn off auto-work and configure a work-hook on the client connection. */
  if( rc==0 ){
    int bAutowork = 0;
    lsm_config(pDb->db, LSM_CONFIG_AUTOWORK, &bAutowork);
    lsm_config_work_hook(pDb->db, mt_client_work_hook, (void *)pDb);
  }

  if( rc==0 ){
    pDb->aWorker = (LsmWorker *)testMalloc(sizeof(LsmWorker) * nWorker);
    memset(pDb->aWorker, 0, sizeof(LsmWorker) * nWorker);
    pDb->nWorker = nWorker;

    rc = mt_start_worker(pDb, 0, zFilename, LSM_WORK_CHECKPOINT, 
        nWorker==1 ? 512 : 0
    );
  }

  if( rc==0 && nWorker==2 ){
    rc = mt_start_worker(pDb, 1, zFilename, 0, 512);
  }

  return rc;
}

int test_lsm_mt2(const char *zFilename, int bClear, TestDb **ppDb){
  return test_lsm_mt(zFilename, 1, bClear, ppDb);
}

int test_lsm_mt3(const char *zFilename, int bClear, TestDb **ppDb){
  return test_lsm_mt(zFilename, 2, bClear, ppDb);
}


#else
static void mt_shutdown(LsmDb *pDb) { 
  unused_parameter(pDb); 
}
int test_lsm_mt(const char *zFilename, int bClear, TestDb **ppDb){
  unused_parameter(zFilename);
  unused_parameter(bClear);
  unused_parameter(ppDb);
  testPrintError("threads unavailable - recompile with LSM_MUTEX_PTHREADS\n");
  return 1;
}
#endif
