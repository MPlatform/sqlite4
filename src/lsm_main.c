/*
** 2011-08-18
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
** The main interface to the LSM module.
*/
#include "lsmInt.h"

#ifdef LSM_DEBUG
/*
** This function returns a copy of its only argument.
**
** When the library is built with LSM_DEBUG defined, this function is called
** whenever an error code is generated (not propagated - generated). So
** if the library is mysteriously returning (say) LSM_IOERR, a breakpoint
** may be set in this function to determine why.
*/
int lsmErrorBkpt(int rc){
  /* Set breakpoint here! */
  return rc;
}

/*
** This function contains various assert() statements that test that the
** lsm_db structure passed as an argument is internally consistent.
*/
static void assert_db_state(lsm_db *pDb){

  /* If there is at least one cursor or a write transaction open, the database
  ** handle must be holding a pointer to a client snapshot. And the reverse 
  ** - if there are no open cursors and no write transactions then there must 
  ** not be a client snapshot.  */
  assert( (pDb->pCsr!=0 || pDb->nTransOpen>0)==(pDb->pClient!=0) );

  /* If there is a write transaction open according to pDb->nTransOpen, then
  ** the connection must be holding the read/write TreeVersion.  */
  assert( pDb->nTransOpen>=0 );
  assert( pDb->nTransOpen==0 || lsmTreeIsWriteVersion(pDb->pTV) );
}
#else
# define assert_db_state(x) 
#endif

/*
** The default key-compare function.
*/
static int xCmp(void *p1, int n1, void *p2, int n2){
  int res;
  res = memcmp(p1, p2, LSM_MIN(n1, n2));
  if( res==0 ) res = (n1-n2);
  return res;
}

/*
** Allocate a new db handle.
*/
int lsm_new(lsm_env *pEnv, lsm_db **ppDb){
  lsm_db *pDb;

  /* If the user did not provide an environment, use the default. */
  if( pEnv==0 ) pEnv = lsm_default_env();
  assert( pEnv );

  /* Allocate the new database handle */
  *ppDb = pDb = (lsm_db *)lsmMallocZero(pEnv, sizeof(lsm_db));
  if( pDb==0 ) return LSM_NOMEM_BKPT;

  /* Initialize the new object */
  pDb->pEnv = pEnv;
  pDb->nTreeLimit = LSM_TREE_BYTES;
  pDb->bAutowork = 1;
  pDb->eSafety = LSM_SAFETY_NORMAL;
  pDb->xCmp = xCmp;
  pDb->nLogSz = LSM_DEFAULT_LOG_SIZE;
  pDb->nDfltPgsz = LSM_PAGE_SIZE;
  pDb->nDfltBlksz = LSM_BLOCK_SIZE;
  pDb->nMerge = LSM_DEFAULT_NMERGE;
  pDb->bUseLog = 1;

  return LSM_OK;
}

lsm_env *lsm_get_env(lsm_db *pDb){
  assert( pDb->pEnv );
  return pDb->pEnv;
}

/*
** Release snapshot handle *ppSnap. Then set *ppSnap to zero. This
** is useful for doing (say):
**
**   dbReleaseSnapshot(pDb->pEnv, &pDb->pWorker);
*/
static void dbReleaseSnapshot(lsm_env *pEnv, Snapshot **ppSnap){
  lsmDbSnapshotRelease(pEnv, *ppSnap);
  *ppSnap = 0;
}

/*
** If database handle pDb is currently holding a client snapshot, but does
** not have any open cursors or write transactions, release it.
*/
static void dbReleaseClientSnapshot(lsm_db *pDb){
  if( pDb->nTransOpen==0 && pDb->pCsr==0 ){
    lsmFinishReadTrans(pDb);
  }
}

static void dbWorkerStart(lsm_db *pDb){
  assert( pDb->pWorker==0 );
  pDb->pWorker = lsmDbSnapshotWorker(pDb);
}

static void dbWorkerDone(lsm_db *pDb){
  assert( pDb->pWorker );
  dbReleaseSnapshot(pDb->pEnv, &pDb->pWorker);
}

static int dbAutoWork(lsm_db *pDb, int nUnit){
  int rc = LSM_OK;                /* Return code */

  assert( pDb->pWorker==0 );
  assert( pDb->bAutowork );
  assert( nUnit>0 );

  /* If one is required, run a checkpoint. */
  rc = lsmCheckpointWrite(pDb);

  dbWorkerStart(pDb);
  rc = lsmSortedAutoWork(pDb, nUnit);
  dbWorkerDone(pDb);

  return rc;
}

/*
** If required, run the recovery procedure to initialize the database.
** Return LSM_OK if successful or an error code otherwise.
*/
static int dbRecoverIfRequired(lsm_db *pDb){
  int rc = LSM_OK;

  assert( pDb->pWorker==0 && pDb->pClient==0 );

  /* The following call returns NULL if recovery is not required. */
  pDb->pWorker = lsmDbSnapshotRecover(pDb);
  if( pDb->pWorker ){
    int bOvfl;

    /* Read the database structure */
    rc = lsmCheckpointRead(pDb, &bOvfl);

    /* Read the free block list and any level records stored in the LSM. */
    if( rc==LSM_OK && bOvfl ){
      rc = lsmSortedLoadSystem(pDb);
    }

    /* Set up the initial append list */
    if( rc==LSM_OK ){
      rc = lsmFsSetupAppendList(pDb);
    }

    /* Populate the in-memory tree by reading the log file. */
    if( rc==LSM_OK ){
      rc = lsmLogRecover(pDb);
    }

    /* Set the "recovery done" flag */
    if( rc==LSM_OK ){
      lsmDbRecoveryComplete(pDb, 1);
    }

    /* Set up the initial client snapshot. */
    if( rc==LSM_OK ){
      rc = lsmDbUpdateClient(pDb, 0, 0);
    }

    dbReleaseSnapshot(pDb->pEnv, &pDb->pWorker);
  }

  return rc;
}

static int getFullpathname(
  lsm_env *pEnv, 
  const char *zRel,
  char **pzAbs
){
  int nAlloc = 0;
  char *zAlloc = 0;
  int nReq = 0;
  int rc;

  do{
    nAlloc = nReq;
    rc = pEnv->xFullpath(pEnv, zRel, zAlloc, &nReq);
    if( nReq>nAlloc ){
      zAlloc = lsmReallocOrFreeRc(pEnv, zAlloc, nReq, &rc);
    }
  }while( nReq>nAlloc && rc==LSM_OK );

  if( rc!=LSM_OK ){
    lsmFree(pEnv, zAlloc);
    zAlloc = 0;
  }
  *pzAbs = zAlloc;
  return rc;
}

/*
** Open a new connection to database zFilename.
*/
int lsm_open(lsm_db *pDb, const char *zFilename){
  int rc;

  if( pDb->pDatabase ){
    rc = LSM_MISUSE;
  }else{
    char *zFull;

    /* Translate the possibly relative pathname supplied by the user into
    ** an absolute pathname. This is required because the supplied path
    ** is used (either directly or with "-log" appended to it) for more 
    ** than one purpose - to open both the database and log files, and 
    ** perhaps to unlink the log file during disconnection. An absolute
    ** path is required to ensure that the correct files are operated
    ** on even if the application changes the cwd.  */
    rc = getFullpathname(pDb->pEnv, zFilename, &zFull);
    assert( rc==LSM_OK || zFull==0 );

    /* Open the database file */
    if( rc==LSM_OK ){
      rc = lsmFsOpen(pDb, zFull);
    }

    /* Open the shared data handle. */
    if( rc==LSM_OK ){
      rc = lsmDbDatabaseFind(pDb, zFilename);
    }

    if( rc==LSM_OK ){
      rc = dbRecoverIfRequired(pDb);
    }

    lsmFree(pDb->pEnv, zFull);
  }

  return rc;
}

/*
** This function flushes the contents of the in-memory tree to disk. It
** returns LSM_OK if successful, or an error code otherwise.
*/
int lsmFlushToDisk(lsm_db *pDb){
  int rc = LSM_OK;                /* Return code */
  int nLsmLevel;
  int bOvfl;

  /* Must not hold the worker snapshot when this is called. */
  assert( pDb->pWorker==0 );
  dbWorkerStart(pDb);

  /* Save the position of each open cursor belonging to pDb. */
  rc = lsmSaveCursors(pDb);

  bOvfl = lsmCheckpointOverflow(pDb, &nLsmLevel);

  /* Write the contents of the in-memory tree into the database file and 
  ** update the worker snapshot accordingly. Then flush the contents of 
  ** the db file to disk too. No calls to fsync() are made here - just 
  ** write().  */
  if( rc==LSM_OK ) rc = lsmSortedFlushTree(pDb, nLsmLevel, bOvfl);
  if( rc==LSM_OK ) rc = lsmSortedFlushDb(pDb);

  /* Create a new client snapshot - one that uses the new runs created above. */
  if( rc==LSM_OK ) rc = lsmDbUpdateClient(pDb, nLsmLevel, bOvfl);

  /* Restore the position of any open cursors */
  rc = lsmRestoreCursors(pDb);

#if 0
  if( rc==LSM_OK ) lsmSortedDumpStructure(pDb, pDb->pWorker, 0, 0, "flush");
#endif

  dbWorkerDone(pDb);
  return rc;
}

int lsm_close(lsm_db *pDb){
  int rc = LSM_OK;
  if( pDb ){
    assert_db_state(pDb);
    if( pDb->pCsr || pDb->nTransOpen ){
      rc = LSM_MISUSE_BKPT;
    }else{
      assert( pDb->pWorker==0 && pDb->pTV==0 );
      lsmDbDatabaseRelease(pDb);
      lsmFsClose(pDb->pFS);
      lsmFree(pDb->pEnv, pDb->aTrans);
      lsmFree(pDb->pEnv, pDb);
    }
  }
  return rc;
}

int lsm_config(lsm_db *pDb, int eParam, ...){
  int rc = LSM_OK;
  va_list ap;
  va_start(ap, eParam);

  switch( eParam ){
    case LSM_CONFIG_WRITE_BUFFER: {
      int *piVal = va_arg(ap, int *);
      if( *piVal>0 ){
        pDb->nTreeLimit = *piVal;
      }
      *piVal = pDb->nTreeLimit;
      break;
    }

    case LSM_CONFIG_AUTOWORK: {
      int *piVal = va_arg(ap, int *);
      if( *piVal>=0 ){
        pDb->bAutowork = *piVal;
      }
      *piVal = pDb->bAutowork;
      break;
    }

    case LSM_CONFIG_LOG_SIZE: {
      int *piVal = va_arg(ap, int *);
      if( *piVal>0 ){
        pDb->nLogSz = *piVal;
      }
      *piVal = pDb->nLogSz;
      break;
    }

    case LSM_CONFIG_PAGE_SIZE: {
      int *piVal = va_arg(ap, int *);
      if( pDb->pDatabase ){
        /* If lsm_open() has been called, this is a read-only parameter. 
        ** Set the output variable to the page-size according to the 
        ** FileSystem object.  */
        *piVal = lsmFsPageSize(pDb->pFS);
      }else{
        if( *piVal>=256 && *piVal<=65536 && ((*piVal-1) & *piVal)==0 ){
          pDb->nDfltPgsz = *piVal;
        }else{
          *piVal = pDb->nDfltPgsz;
        }
      }
      break;
    }

    case LSM_CONFIG_BLOCK_SIZE: {
      int *piVal = va_arg(ap, int *);
      if( pDb->pDatabase ){
        /* If lsm_open() has been called, this is a read-only parameter. 
        ** Set the output variable to the page-size according to the 
        ** FileSystem object.  */
        *piVal = lsmFsBlockSize(pDb->pFS);
      }else{
        if( *piVal>=65536 && ((*piVal-1) & *piVal)==0 ){
          pDb->nDfltBlksz = *piVal;
        }else{
          *piVal = pDb->nDfltBlksz;
        }
      }
      break;
    }

    case LSM_CONFIG_SAFETY: {
      int *piVal = va_arg(ap, int *);
      if( *piVal>=0 && *piVal<=2 ){
        pDb->eSafety = *piVal;
      }
      *piVal = pDb->eSafety;
      break;
    }

    case LSM_CONFIG_MMAP: {
      int *piVal = va_arg(ap, int *);
      rc = lsmConfigMmap(pDb, piVal);
      break;
    }

    case LSM_CONFIG_USE_LOG: {
      int *piVal = va_arg(ap, int *);
      if( pDb->nTransOpen==0 && (*piVal==0 || *piVal==1) ){
        pDb->bUseLog = *piVal;
      }
      *piVal = pDb->bUseLog;
      break;
    }

    case LSM_CONFIG_NMERGE: {
      int *piVal = va_arg(ap, int *);
      if( *piVal>1 ) pDb->nMerge = *piVal;
      *piVal = pDb->nMerge;
      break;
    }

    default:
      rc = LSM_MISUSE;
      break;
  }

  va_end(ap);
  return rc;
}

void lsmAppendSegmentList(LsmString *pStr, char *zPre, Segment *pSeg){
  lsmStringAppendf(pStr, "%s{%d %d %d %d}", zPre, 
        pSeg->iFirst, pSeg->iLast, pSeg->iRoot, pSeg->nSize
  );
}

int lsmStructList(
  lsm_db *pDb,                    /* Database handle */
  char **pzOut                    /* OUT: Nul-terminated string (tcl list) */
){
  Level *pTopLevel = 0;           /* Top level of snapshot to report on */
  int rc = LSM_OK;
  Level *p;
  LsmString s;
  Snapshot *pWorker;              /* Worker snapshot */
  Snapshot *pRelease = 0;         /* Snapshot to release */

  /* Obtain the worker snapshot */
  pWorker = pDb->pWorker;
  if( !pWorker ){
    pRelease = pWorker = lsmDbSnapshotWorker(pDb);
  }

  /* Format the contents of the snapshot as text */
  pTopLevel = lsmDbSnapshotLevel(pWorker);
  lsmStringInit(&s, pDb->pEnv);
  for(p=pTopLevel; rc==LSM_OK && p; p=p->pNext){
    int i;
    lsmStringAppendf(&s, "%s{", (s.n ? " " : ""));
    lsmAppendSegmentList(&s, "", &p->lhs);
    for(i=0; rc==LSM_OK && i<p->nRight; i++){
      lsmAppendSegmentList(&s, " ", &p->aRhs[i]);
    }
    lsmStringAppend(&s, "}", 1);
  }
  rc = s.n>=0 ? LSM_OK : LSM_NOMEM;

  /* Release the snapshot and return */
  lsmDbSnapshotRelease(pDb->pEnv, pRelease);
  *pzOut = s.z;
  return rc;
}

int lsm_info(lsm_db *pDb, int eParam, ...){
  int rc = LSM_OK;
  va_list ap;
  va_start(ap, eParam);

  switch( eParam ){
    case LSM_INFO_NWRITE: {
      int *piVal = va_arg(ap, int *);
      *piVal = lsmFsNWrite(pDb->pFS);
      break;
    }

    case LSM_INFO_NREAD: {
      int *piVal = va_arg(ap, int *);
      *piVal = lsmFsNRead(pDb->pFS);
      break;
    }

    case LSM_INFO_DB_STRUCTURE: {
      char **pzVal = va_arg(ap, char **);
      rc = lsmStructList(pDb, pzVal);
      break;
    }

    case LSM_INFO_ARRAY_STRUCTURE: {
      Pgno pgno = va_arg(ap, Pgno);
      char **pzVal = va_arg(ap, char **);
      rc = lsmInfoArrayStructure(pDb, pgno, pzVal);
      break;
    }

    case LSM_INFO_PAGE_HEX_DUMP:
    case LSM_INFO_PAGE_ASCII_DUMP: {
      Pgno pgno = va_arg(ap, Pgno);
      char **pzVal = va_arg(ap, char **);
      rc = lsmInfoPageDump(pDb, pgno, (eParam==LSM_INFO_PAGE_HEX_DUMP), pzVal);
      break;
    }

    case LSM_INFO_LOG_STRUCTURE: {
      char **pzVal = va_arg(ap, char **);
      rc = lsmLogStructure(pDb, pzVal);
      break;
    }

    default:
      rc = LSM_MISUSE;
      break;
  }

  va_end(ap);
  return rc;
}

/* 
** Write a new value into the database.
*/
int lsm_write(lsm_db *pDb, void *pKey, int nKey, void *pVal, int nVal){
  int rc = LSM_OK;                /* Return code */
  int bCommit = 0;                /* True to commit before returning */

  if( pDb->nTransOpen==0 ){
    bCommit = 1;
    rc = lsm_begin(pDb, 1);
  }

  if( rc==LSM_OK ){
    assert( pDb->pTV && lsmTreeIsWriteVersion(pDb->pTV) );
    rc = lsmLogWrite(pDb, pKey, nKey, pVal, nVal);
  }

  lsmSortedSaveTreeCursors(pDb);

  if( rc==LSM_OK ){
    int pgsz = lsmFsPageSize(pDb->pFS);
    int nQuant = 32 * pgsz;
    int nBefore;
    int nAfter;
    int nDiff;

    if( nQuant>pDb->nTreeLimit ){
      nQuant = pDb->nTreeLimit;
    }

    nBefore = lsmTreeSize(pDb->pTV);
    rc = lsmTreeInsert(pDb, pKey, nKey, pVal, nVal);
    nAfter = lsmTreeSize(pDb->pTV);

    nDiff = (nAfter/nQuant) - (nBefore/nQuant);
    if( rc==LSM_OK && pDb->bAutowork && nDiff!=0 ){
      rc = dbAutoWork(pDb, nDiff*nQuant / pgsz);
    }
  }

  /* If a transaction was opened at the start of this function, commit it. 
  ** Or, if an error has occurred, roll it back.
  */
  if( bCommit ){
    if( rc==LSM_OK ){
      rc = lsm_commit(pDb, 0);
    }else{
      lsm_rollback(pDb, 0);
    }
  }

  return rc;
}

/*
** Delete a value from the database. 
*/
int lsm_delete(lsm_db *pDb, void *pKey, int nKey){
  return lsm_write(pDb, pKey, nKey, 0, -1);
}

/*
** Open a new cursor handle. 
**
** If there are currently no other open cursor handles, and no open write
** transaction, open a read transaction here.
*/
int lsm_csr_open(lsm_db *pDb, lsm_cursor **ppCsr){
  int rc;                         /* Return code */
  MultiCursor *pCsr = 0;          /* New cursor object */

  /* Open a read transaction if one is not already open. */
  assert_db_state(pDb);
  rc = lsmBeginReadTrans(pDb);

  /* Allocate the multi-cursor. */
  if( rc==LSM_OK ) rc = lsmMCursorNew(pDb, &pCsr);

  /* If an error has occured, set the output to NULL and delete any partially
  ** allocated cursor. If this means there are no open cursors, release the
  ** client snapshot.  */
  if( rc!=LSM_OK ){
    lsmMCursorClose(pCsr);
    dbReleaseClientSnapshot(pDb);
  }

  assert_db_state(pDb);
  *ppCsr = (lsm_cursor *)pCsr;
  return rc;
}

/*
** Close a cursor opened using lsm_csr_open().
*/
int lsm_csr_close(lsm_cursor *p){
  if( p ){
    lsm_db *pDb = lsmMCursorDb((MultiCursor *)p);
    assert_db_state(pDb);
    lsmMCursorClose((MultiCursor *)p);
    dbReleaseClientSnapshot(pDb);
    assert_db_state(pDb);
  }
  return LSM_OK;
}

/*
** Attempt to seek the cursor to the database entry specified by pKey/nKey.
** If an error occurs (e.g. an OOM or IO error), return an LSM error code.
** Otherwise, return LSM_OK.
*/
int lsm_csr_seek(lsm_cursor *pCsr, void *pKey, int nKey, int eSeek){
  return lsmMCursorSeek((MultiCursor *)pCsr, pKey, nKey, eSeek);
}

int lsm_csr_next(lsm_cursor *pCsr){
  return lsmMCursorNext((MultiCursor *)pCsr);
}

int lsm_csr_prev(lsm_cursor *pCsr){
  return lsmMCursorPrev((MultiCursor *)pCsr);
}

int lsm_csr_first(lsm_cursor *pCsr){
  return lsmMCursorFirst((MultiCursor *)pCsr);
}

int lsm_csr_last(lsm_cursor *pCsr){
  return lsmMCursorLast((MultiCursor *)pCsr);
}

int lsm_csr_valid(lsm_cursor *pCsr){
  return lsmMCursorValid((MultiCursor *)pCsr);
}

int lsm_csr_key(lsm_cursor *pCsr, void **ppKey, int *pnKey){
  return lsmMCursorKey((MultiCursor *)pCsr, ppKey, pnKey);
}

int lsm_csr_value(lsm_cursor *pCsr, void **ppVal, int *pnVal){
  return lsmMCursorValue((MultiCursor *)pCsr, ppVal, pnVal);
}

void lsm_config_log(
  lsm_db *pDb, 
  void (*xLog)(void *, int, const char *), 
  void *pCtx
){
  pDb->xLog = xLog;
  pDb->pLogCtx = pCtx;
}

void lsm_config_work_hook(
  lsm_db *pDb, 
  void (*xWork)(lsm_db *, void *), 
  void *pCtx
){
  pDb->xWork = xWork;
  pDb->pWorkCtx = pCtx;
}

void lsmLogMessage(lsm_db *pDb, int rc, const char *zFormat, ...){
  if( pDb->xLog ){
    LsmString s;
    va_list ap, ap2;
    lsmStringInit(&s, pDb->pEnv);
    va_start(ap, zFormat);
    va_start(ap2, zFormat);
    lsmStringVAppendf(&s, zFormat, ap, ap2);
    va_end(ap);
    va_end(ap2);
    pDb->xLog(pDb->pLogCtx, rc, s.z);
    lsmStringClear(&s);
  }
}

int lsm_begin(lsm_db *pDb, int iLevel){
  int rc = LSM_OK;

  assert_db_state( pDb );

  /* A value less than zero means open one more transaction. */
  if( iLevel<0 ) iLevel = pDb->nTransOpen + 1;

  if( iLevel>pDb->nTransOpen ){
    int i;

    /* Extend the pDb->aTrans[] array if required. */
    if( rc==LSM_OK && pDb->nTransAlloc<iLevel ){
      TransMark *aNew;            /* New allocation */
      int nByte = sizeof(TransMark) * (iLevel+1);
      aNew = (TransMark *)lsmRealloc(pDb->pEnv, pDb->aTrans, nByte);
      if( !aNew ){
        rc = LSM_NOMEM;
      }else{
        nByte = sizeof(TransMark) * (iLevel+1 - pDb->nTransAlloc);
        memset(&aNew[pDb->nTransAlloc], 0, nByte);
        pDb->nTransAlloc = iLevel+1;
        pDb->aTrans = aNew;
      }
    }

    if( rc==LSM_OK && pDb->nTransOpen==0 ){
      rc = lsmBeginWriteTrans(pDb);
    }

    if( rc==LSM_OK ){
      for(i=pDb->nTransOpen; i<iLevel; i++){
        lsmTreeMark(pDb->pTV, &pDb->aTrans[i].tree);
        lsmLogTell(pDb, &pDb->aTrans[i].log);
      }
      pDb->nTransOpen = iLevel;
    }
  }

  return rc;
}

int lsm_commit(lsm_db *pDb, int iLevel){
  int rc = LSM_OK;

  assert_db_state( pDb );

  /* A value less than zero means close the innermost nested transaction. */
  if( iLevel<0 ) iLevel = LSM_MAX(0, pDb->nTransOpen - 1);

  if( iLevel<pDb->nTransOpen ){
    if( iLevel==0 ){
      /* Commit the transaction to disk. */
      if( pDb->pTV && lsmTreeSize(pDb->pTV)>pDb->nTreeLimit ){
        rc = lsmFlushToDisk(pDb);
      }
      if( rc==LSM_OK ) rc = lsmLogCommit(pDb);
      if( rc==LSM_OK && pDb->eSafety==LSM_SAFETY_FULL ){
        rc = lsmFsSyncLog(pDb->pFS);
      }

      lsmFinishWriteTrans(pDb, (rc==LSM_OK));
    }
    pDb->nTransOpen = iLevel;
  }
  dbReleaseClientSnapshot(pDb);
  return rc;
}

int lsm_rollback(lsm_db *pDb, int iLevel){
  int rc = LSM_OK;
  assert_db_state( pDb );

  if( pDb->nTransOpen ){
    /* A value less than zero means close the innermost nested transaction. */
    if( iLevel<0 ) iLevel = LSM_MAX(0, pDb->nTransOpen - 1);

    if( iLevel<=pDb->nTransOpen ){
      TransMark *pMark = &pDb->aTrans[(iLevel==0 ? 0 : iLevel-1)];
      lsmTreeRollback(pDb, &pMark->tree);
      if( iLevel ) lsmLogSeek(pDb, &pMark->log);
      pDb->nTransOpen = iLevel;
    }

    if( pDb->nTransOpen==0 ){
      lsmFinishWriteTrans(pDb, 0);
    }
    dbReleaseClientSnapshot(pDb);
  }

  return rc;
}
