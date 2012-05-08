
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
typedef struct TestFile TestFile;
typedef struct IVector IVector;
typedef struct Sector Sector;

/*
** This structure is used to implement an array of type int. It supports
** the following methods:
**
**     testIvecSet()
**     testIvecGet()
**     testIvecSize()
**     testIvecClear()
*/
struct IVector {
  int nEntry;                     /* Size of array */
  int nAlloc;                     /* Size of allocated array */
  int *aEntry;                    /* Array of integers */
};

struct Sector {
  u8 *aData;                      /* A sectors worth of data */
  int iSector;                    /* Sector number. */
  Sector *pNext;                  /* Next sector with same hash */
};

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
*/
struct LsmDb {
  TestDb base;                    /* Base class - methods table */
  lsm_db *db;                     /* LSM database handle */
  lsm_cursor *pCsr;               /* Cursor held open during read transaction */
  void *pBuf;                     /* Buffer for tdb_fetch() output */
  int nBuf;                       /* Allocated (not used) size of pBuf */

  /* Crash testing related state */
  int nAutoCrash;                 /* Number of syncs until a crash */
  int bPrepareCrash;              /* True to store writes in memory */
  int bCrashed;                   /* True once a crash has occurred */
  int nSectorsize;                /* Assumed size of disk sectors (512B) */

  /* xSync timing related */
  int bSyncDebug;                 /* True to print out debugging at xSync() */

  /* Work hook redirection */
  void (*xWork)(lsm_db *, void *);
  void *pWorkCtx;
  
  /* Worker threads (for lsm_mt) */
  int nWorker;
  LsmWorker *aWorker;
};

/*************************************************************************
**************************************************************************
** Begin code for IVector
*/
static void testIvecSet(IVector *p, int iIdx, int iVal){
  if( p->nAlloc<=iIdx ){
    int nNew = (iIdx+1)*2;
    int *aNew;
    aNew = (int *)realloc(p->aEntry, nNew*sizeof(int));
    memset(&aNew[p->nAlloc], 0, (nNew-p->nAlloc)*sizeof(int));
    p->nAlloc = nNew;
    p->aEntry = aNew;
  }

  p->aEntry[iIdx] = iVal;
  p->nEntry = MAX(iIdx+1, p->nEntry);
}

#if 0
static int testIvecGet(IVector *p, int iIdx){
  return ((p->nAlloc>iIdx) ? p->aEntry[iIdx] : 0);
}
#endif

static int testIvecSize(IVector *p){
  return p->nEntry;
}

static void testIvecClear(IVector *p){
  free(p->aEntry);
  memset(p, 0, sizeof(IVector));
}

static void testIvecBitSet(IVector *p, int iBit){
  int iIdx = (iBit >> 5);
  int mask = (1 << (iBit & 0x1F));

  if( p->nAlloc<=iIdx ) testIvecSet(p, iIdx, 0);
  p->aEntry[iIdx] |= mask;
}

static int testIvecBitGet(IVector *p, int iBit){
  int iIdx = (iBit >> 5);
  int mask = (1 << (iBit & 0x1F));
  if( p->nAlloc<=iIdx ) testIvecSet(p, iIdx, 0);
  return ((p->aEntry[iIdx] & mask)!=0);
}
/*
** End of IVector code
**************************************************************************
*************************************************************************/


/*************************************************************************
**************************************************************************
** Begin test VFS code.
*/
#if 0
struct TestFile {
  lsm_file *pReal;
  LsmDb *pDb;                     /* Database handle that uses this file */

  /* Used by system crash simulation */
  int nHash;
  Sector **aHash;
  
  /* Used by sync-logging. */
  IVector aiUnsynced;             /* Map of synced and unsynced blocks */
  i64 iWriteStart;                /* Start of current contiguous write() */
  int nWrite;                     /* Number of bytes in the same */
  FILE *iolog;                    /* Write IO log here */
  int nSynced;                    /* Running count of sectors synced */
};

static void createHashTable(TestFile *p){
  assert( p->pDb->bPrepareCrash );
  if( p->aHash==0 ){
    p->nHash = 10000;
    p->aHash = (Sector **)malloc(p->nHash * sizeof(Sector *));
    memset(p->aHash, 0, p->nHash*sizeof(Sector *));
  }
}

int sectorHash(TestFile *p, int iSector){
  createHashTable(p);
  return (iSector % p->nHash);
}



static int testVfsOpen(void *pCtx, const char *zFile, lsm_file **ppFile){
  int rc;                         /* Return Code */
  TestFile *pRet;                 /* The new file handle */
  lsm_vfs *pRealVfs;              /* The actual interface to the OS */
  void *pRealCtx;

  pRet = malloc(sizeof(TestFile));
  memset(pRet, 0, sizeof(TestFile));
  pRet->pDb = (LsmDb *)pCtx;
  assert( pRet->pDb->bCrashed==0 );

  pRealVfs = lsm_default_vfs();
  rc = pRealVfs->xOpen(pRealCtx, zFile, &pRet->pReal);
  if( rc!=LSM_OK ){
    free(pRet);
    pRet = 0;
  }

#if 0 
  if( zFile[strlen(zFile)-1]=='b' ) pRet->iolog = stderr;
#endif

  *ppFile = (lsm_file *)pRet;
  return rc;
}

static int testVfsRead(lsm_file *pFile, lsm_i64 iOff, void *pData, int nData){
  int rc;
  TestFile *p = (TestFile *)pFile;
  LsmDb *pDb = p->pDb;

  if( pDb->bCrashed ){
    return LSM_IOERR;
  }else if( pDb->bPrepareCrash ){
    int i;
    int nSectorsize = pDb->nSectorsize;
    int iFirst = (iOff / nSectorsize);
    int iLast = iFirst + (nData / nSectorsize);
    u8 *aData = (u8 *)pData;

    assert( (iOff % nSectorsize)==0 );
    assert( (nData % nSectorsize)==0 );

    for(i=iFirst; i<iLast; i++){
      Sector *pSector;
      int iHash;

      iHash = sectorHash(p, i);
      pSector = p->aHash[iHash];
      while( pSector && pSector->iSector!=i ){
        pSector = pSector->pNext;
      }

      if( pSector ){
        memcpy(aData, pSector->aData, nSectorsize);
      }else{
        rc = lsm_default_vfs()->xRead(
            p->pReal, ((i64)nSectorsize * i), aData, nSectorsize
        );
      }
      aData += nSectorsize;
    }
    rc = LSM_OK;
  }else{
    rc = lsm_default_vfs()->xRead(p->pReal, iOff, pData, nData);
  }
  return rc;
}

static int testVfsWrite(lsm_file *pFile, lsm_i64 iOff, void *pData, int nData){
  TestFile *p = (TestFile *)pFile;
  LsmDb *pDb = p->pDb;
  int rc;

  if( pDb->bSyncDebug ){
    int iFirst;                   /* First sector being written */
    int iLast;                    /* Last sector being written */
    int i;                        /* Iterator variable */

    iFirst = iOff / pDb->nSectorsize;
    iLast = (iOff+nData-1) / pDb->nSectorsize;

    for(i=iFirst; i<=iLast; i++){
      testIvecBitSet(&p->aiUnsynced, i);
    }
  }

  if( p->iolog ){
    if( p->nWrite ){
      if( iOff!=(p->iWriteStart + p->nWrite) ){
        fprintf(p->iolog, "%dK@%dK\n", 
            p->nWrite/1024, (int)(p->iWriteStart/1024)
            );
        p->iWriteStart = iOff;
        p->nWrite = 0;
      }
      p->nWrite += nData;
    }else{
      p->iWriteStart = iOff;
      p->nWrite = nData;
    }
  }

  /* If this database has already crashed, return LSM_IOERR immediately. 
  ** Otherwise, if a crash is scheduled, then store the modified sector
  ** data in the Testfile.aHash[] hash table. Or, if no crash is scheduled
  ** and none has occurred, write to the underlying file.
  */
  if( pDb->bCrashed ){
    rc = LSM_IOERR;
  }else if( pDb->bPrepareCrash ){
    int i;
    int nSectorsize = pDb->nSectorsize;
    int iFirst = (iOff / nSectorsize);
    int iLast = iFirst + (nData / nSectorsize);
    u8 *aData = (u8 *)pData;

    assert( (iOff % nSectorsize)==0 );
    assert( (nData % nSectorsize)==0 );

    for(i=iFirst; i<iLast; i++){
      Sector *pSector;
      int iHash;

      iHash = sectorHash(p, i);
      pSector = p->aHash[iHash];
      while( pSector && pSector->iSector!=i ){
        pSector = pSector->pNext;
      }
      if( pSector==0 ){
        pSector = malloc(sizeof(Sector) + nSectorsize);
        memset(pSector, 0, sizeof(Sector));
        pSector->aData = (u8 *)&pSector[1];
        pSector->iSector = i;
        pSector->pNext = p->aHash[iHash];
        p->aHash[iHash] = pSector;
      }
      memcpy(pSector->aData, aData, nSectorsize);
      aData += nSectorsize;
    }
    rc = LSM_OK;
  }else{
    rc = lsm_default_vfs()->xWrite(p->pReal, iOff, pData, nData);
  }

  return rc;
}

static void printDirty(TestFile *p){
  int i;
  int nSector;

  int iStart;
  int bOn = 0;
  int nTotal = 0;

  nSector = testIvecSize(&p->aiUnsynced) * 32;

  for(i=0; i<=nSector; i++){
    int bit = (i==nSector ? 0 : testIvecBitGet(&p->aiUnsynced, i));
    if( bit && !bOn ){
      iStart = i;
      bOn = 1;
    }

    if( !bit && bOn ){
      fprintf(stderr, "%dK@%dK ", i-iStart, iStart); 
      bOn = 0;
      nTotal += i-iStart;
    }
  }

  p->nSynced += nTotal;
}

static void flushFileBuffers(TestFile *p, int iCrash){
  if( p->aHash ){
    LsmDb *pDb = p->pDb;
    int nSectorsize = pDb->nSectorsize;
    int i;
    for(i=0; i<p->nHash; i++){
      Sector *pSector;
      Sector *pNext;

      for(pSector=p->aHash[i]; pSector; pSector=pNext){
        i64 iOff = pSector->iSector * (i64)nSectorsize;

        /* Set variable iOpt to indicate the action taken for this sector:
        **
        **     iOpt==0    ->    Do nothing. Do not write the sector to disk.
        **     iOpt==1    ->    Write garbage to disk in place of sector data.
        **     iOpt==2    ->    Write sector data to disk.
        */
        int iOpt = 2;
        if( iCrash ){
          iOpt = (testPrngValue(iCrash + pSector->iSector) % 3);
#if 0
          fprintf(stderr, "file=%p sector=%d iOpt=%d\n", 
              p, pSector->iSector, iOpt
          );
#endif
        }

        if( iOpt ){
          if( iOpt==1 ){
            memset(pSector->aData, 0x01, nSectorsize);
          }
          lsm_default_vfs()->xWrite(
              p->pReal, iOff, pSector->aData, nSectorsize
          );
        }

        pNext = pSector->pNext;
        free(pSector);
      }
    }
    memset(p->aHash, 0, sizeof(Sector *)*p->nHash);
  }
}

static int testVfsSync(lsm_file *pFile){
  TestFile *p = (TestFile *)pFile;
  LsmDb *pDb = p->pDb;
  int rc = LSM_OK;

  if( pDb->bCrashed ){
    rc = LSM_IOERR;
  }else if( pDb->bPrepareCrash ){
    if( pDb->nAutoCrash>0 ){
      pDb->nAutoCrash--;
      if( pDb->nAutoCrash==0 ){
        pDb->bCrashed = 1;
        rc = LSM_IOERR;
      }else{
        flushFileBuffers(p, 0);
      }
    }else{
      flushFileBuffers(p, 0);
    }
  }

  if( p->iolog ){
    if( p->nWrite ){
      fprintf(p->iolog, "%dK@%dK\n", p->nWrite/1024,(int)(p->iWriteStart/1024));
      p->nWrite = 0;
    }
    fprintf(p->iolog, "S\n");
  }

  if( rc==LSM_OK ){
    if( p->pDb->bSyncDebug ){
      struct timeval one;
      struct timeval two;
      int ms;

      gettimeofday(&one, 0);
      rc = lsm_default_vfs()->xSync(p->pReal);
      gettimeofday(&two, 0);
      ms = (((int)two.tv_sec - (int)one.tv_sec)*1000) +
           (((int)two.tv_usec - (int)one.tv_usec)/1000);

#if 0
      fprintf(stderr, "SYNC: (%d ms) ", ms);
#endif
      printDirty(p);
      fprintf(stderr, "S\n");
      testIvecClear(&p->aiUnsynced);
    }else{
      rc = lsm_default_vfs()->xSync(p->pReal);
    }
  }

  return rc;
}

static int testVfsClose(lsm_file *pFile){
  TestFile *p = (TestFile *)pFile;
  int rc;

  flushFileBuffers(p, p->pDb->bCrashed);
  rc = lsm_default_vfs()->xClose(p->pReal);
  free(p->aHash);
  free(p);

  return rc;
}
#endif

static int installTestVFS(LsmDb *pDb){
#if 0
  static lsm_vfs test_vfs = {
    testVfsOpen,
    testVfsRead,
    testVfsWrite,
    testVfsSync,
    testVfsClose
  };
  return lsm_config_vfs(pDb->db, (void *)pDb, &test_vfs);
#endif
  return 0;
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

  free((char *)pDb->pBuf);
  free((char *)pDb);
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
      void *pVal;
      int nVal;
      rc = lsm_csr_value(csr, &pVal, &nVal);
      if( nVal>pDb->nBuf ){
        free(pDb->pBuf);
        pDb->pBuf = malloc(nVal*2);
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
    void *pKey; int nKey;
    void *pVal; int nVal;
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

    xCallback(pCtx, pKey, nKey, pVal, nVal);

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

int test_lsm_open(const char *zFilename, int bClear, TestDb **ppDb){
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
  LsmDb *pDb;

  if( bClear && zFilename && zFilename[0] ){
    char *zCmd = sqlite3_mprintf("rm -rf %s\n", zFilename);
    system(zCmd);
    sqlite3_free(zCmd);
    zCmd = sqlite3_mprintf("rm -rf %s-log\n", zFilename);
    system(zCmd);
    sqlite3_free(zCmd);
  }

  pDb = (LsmDb *)malloc(sizeof(LsmDb));
  memset(pDb, 0, sizeof(LsmDb));
  pDb->base.pMethods = &LsmMethods;

  /* Default the sector size used for crash simulation to 512 bytes. 
  ** Todo: There should be an OS method to obtain this value - just as
  ** there is in SQLite. For now, LSM assumes that it is smaller than
  ** the page size (default 4KB).
  */
  pDb->nSectorsize = 256;
  pDb->bSyncDebug = 0;

  rc = lsm_new(tdb_lsm_env(), &pDb->db);
  if( rc==LSM_OK ){
    lsm_config_log(pDb->db, xLog, 0);
    lsm_config_work_hook(pDb->db, xWorkHook, (void *)pDb);
    installTestVFS(pDb);
    rc = lsm_open(pDb->db, zFilename);
    if( rc!=LSM_OK ){
      test_lsm_close((TestDb *)pDb);
      pDb = 0;
    }
  }

  *ppDb = (TestDb *)pDb;
  return rc;
}

int test_lsm_small_open(
  const char *zFilename, 
  int bClear, 
  TestDb **ppDb
){
  int rc;
  rc = test_lsm_open(zFilename, bClear, ppDb);
  if( rc==0 ){
    LsmDb *pDb = (LsmDb *)*ppDb;
    int nPgsz = 256;
    int nBlocksize = 64 * 1024;
    int nSeg = 2;

    lsm_config(pDb->db, LSM_CONFIG_PAGE_SIZE, &nPgsz);
    lsm_config(pDb->db, LSM_CONFIG_BLOCK_SIZE, &nBlocksize);
    lsm_config(pDb->db, LSM_CONFIG_SEGMENT_RATIO, &nSeg);
    assert( nPgsz==256 || bClear==0 );
    assert( nBlocksize==64*1024 || bClear==0 );
    assert( nSeg==2 );
  }
  return rc;
}

int test_lsm_lomem_open(
  const char *zFilename, 
  int bClear, 
  TestDb **ppDb
){
  int rc;
  rc = test_lsm_small_open(zFilename, bClear, ppDb);
  if( rc==0 ){
    LsmDb *pDb = (LsmDb *)*ppDb;
    int nMem = 1024 * 16;
    lsm_config(pDb->db, LSM_CONFIG_WRITE_BUFFER, &nMem);
    assert( nMem==(1024 * 16) );
  }
  return rc;
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

#ifdef LSM_MUTEX_PTHREADS

static void *worker_main(void *pArg){
  LsmWorker *p = (LsmWorker *)pArg;
  lsm_db *pWorker;                /* Connection to access db through */

  pthread_mutex_lock(&p->worker_mutex);
  pWorker = p->pWorker;
  pthread_mutex_unlock(&p->worker_mutex);

  while( pWorker ){
    int nWrite = 0;
    int rc;

    /* Do some work. If an error occurs, exit. */
    rc = lsm_work(pWorker, p->lsm_work_flags, p->lsm_work_npage, &nWrite);
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
      pthread_mutex_lock(&p->worker_mutex);
      if( p->pWorker && p->bDoWork==0 ){
        pthread_cond_wait(&p->worker_cond, &p->worker_mutex);
      }
      p->bDoWork = 0;
      pWorker = p->pWorker;
      pthread_mutex_unlock(&p->worker_mutex);
    }
  }
  
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
  if( rc==0 ) rc = lsm_new(tdb_lsm_env(), &p->pWorker);
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
    pDb->aWorker = (LsmWorker *)malloc(sizeof(LsmWorker) * nWorker);
    memset(pDb->aWorker, 0, sizeof(LsmWorker) * nWorker);
    pDb->nWorker = nWorker;

    rc = mt_start_worker(pDb, 0, zFilename, LSM_WORK_CHECKPOINT, 
        nWorker==1 ? 32 : 0
    );
  }

  if( rc==0 && nWorker==2 ){
    rc = mt_start_worker(pDb, 1, zFilename, 0, 32);
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

