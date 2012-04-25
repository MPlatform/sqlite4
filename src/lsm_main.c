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
}
#else
# define assert_db_state(x) 
#endif

/*
** The default key-compare function.
*/
static int xCmp(void *p1, int n1, void *p2, int n2){
  int res;
  res = memcmp(p1, p2, MIN(n1, n2));
  if( res==0 ) res = (n1-n2);
  return res;
}

/*
** Allocate a new db handle.
*/
int lsm_new(lsm_db **ppDb){
  lsm_db *pDb;
  *ppDb = pDb = (lsm_db *)lsmMallocZero(sizeof(lsm_db));
  if( pDb==0 ) return LSM_NOMEM_BKPT;

  pDb->nTreeLimit = LSM_TREE_BYTES;
  pDb->eCola = LSM_ECOLA;
  pDb->bAutowork = 1;
  pDb->eSafety = LSM_SAFETY_NORMAL;
  pDb->xCmp = xCmp;

  return LSM_OK;
}

/*
** Release snapshot handle *ppSnap. Then set *ppSnap to zero. This
** is useful for doing (say):
**
**   dbReleaseSnapshot(&pDb->pWorker);
*/
static void dbReleaseSnapshot(Snapshot **ppSnap){
  lsmDbSnapshotRelease(*ppSnap);
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
  pDb->pWorker = lsmDbSnapshotWorker(pDb->pDatabase);
}

static void dbWorkerDone(lsm_db *pDb){
  assert( pDb->pWorker );
  dbReleaseSnapshot(&pDb->pWorker);
}

static int dbAutoWork(lsm_db *pDb, int nUnit){
  int i;                          /* Iterator variable */
  int rc = LSM_OK;                /* Return code */

  assert( pDb->pWorker==0 );
  assert( pDb->bAutowork );
  assert( nUnit>0 );

  /* If one is required, run a checkpoint. */
  rc = lsmCheckpointWrite(pDb);

  dbWorkerStart(pDb);
  for(i=0; i<nUnit && rc==LSM_OK; i++){
    rc = lsmSortedAutoWork(pDb);
  }
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
  pDb->pWorker = lsmDbSnapshotRecover(pDb->pDatabase);
  if( pDb->pWorker ){

    /* Read the database structure */
    rc = lsmCheckpointRead(pDb);

    /* Read the free block list */
    if( rc==LSM_OK ){
      /* TODO: This (reading the free block list) should be delayed until
      ** the first time it is actually required. Readers never require the
      ** free block list.  */
      rc = lsmSortedLoadFreelist(pDb);
    }

    /* Populate the in-memory tree by reading the log file. */
    if( rc==LSM_OK ){
      rc = lsmLogRecover(pDb);
    }

    /* Set the "recovery done" flag */
    if( rc==LSM_OK ){
      lsmDbRecoveryComplete(pDb->pDatabase, 1);
    }

    /* Set up the initial client snapshot. */
    if( rc==LSM_OK ){
      rc = lsmDbUpdateClient(pDb);
    }

    dbReleaseSnapshot(&pDb->pWorker);
  }

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

    /* Open the shared data handle. */
    rc = lsmDbDatabaseFind(zFilename, &pDb->pDatabase);

    /* Open the database file */
    if( rc==LSM_OK ){
      rc = lsmFsOpen(pDb, zFilename, LSM_PAGE_SIZE);
    }

    if( rc==LSM_OK ){
      rc = dbRecoverIfRequired(pDb);
    }
  }

  return rc;
}

/*
** This function flushes the contents of the in-memory tree to disk. It
** returns LSM_OK if successful, or an error code otherwise.
*/
int lsmFlushToDisk(lsm_db *pDb){
  int rc = LSM_OK;                /* Return code */
  lsm_cursor *pCsr;               /* Used to iterate through open cursors */

  /* Must hold the worker snapshot to do this. */
  assert( pDb->pWorker );

  /* Save the position of each open cursor belonging to pDb. */
  for(pCsr=pDb->pCsr; rc==LSM_OK && pCsr; pCsr=pCsr->pNext){
    rc = lsmMCursorSave(pCsr->pMC);
  }

  /* Flush the log file to disk. Then write the contents of the in-memory
  ** tree into the database file and update the worker snapshot accordingly.
  ** Then flush the contents of the db file to disk too. No calls to fsync()
  ** are made here - just write().  */
  rc = lsmLogFlush(pDb);
  if( rc==LSM_OK ) rc = lsmSortedFlushTree(pDb);
  if( rc==LSM_OK ) rc = lsmSortedFlushDb(pDb);

  /* Update the shared data to reflect the newly checkpointed snapshot */
  if( rc==LSM_OK ) rc = lsmDbUpdateClient(pDb);

  /* Restore the position open cursors */
  for(pCsr=pDb->pCsr; rc==LSM_OK && pCsr; pCsr=pCsr->pNext){
    rc = lsmMCursorRestore(pDb, pCsr->pMC);
  }

#if 0
  if( rc==LSM_OK ) lsmSortedDumpStructure(pDb, pDb->pWorker, 0, "flush");
#endif

  return rc;
}

int lsm_close(lsm_db *pDb){
  int rc = LSM_OK;
  if( pDb ){
    lsmDbDatabaseRelease(pDb);
    lsmLogClose(pDb);
    lsmFsClose(pDb->pFS);
    lsmFree(pDb->aTrans);
    lsmFree(pDb);
  }
  return rc;
}

int lsm_config_vfs(lsm_db *pDb, void *pVfsCtx, lsm_vfs *pVfs){
  if( pDb->pFS ){
    return LSM_MISUSE;
  }
  pDb->pVfs = pVfs;
  pDb->pVfsCtx = pVfsCtx;
  return LSM_OK;
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

    case LSM_CONFIG_SEGMENT_RATIO: {
      int *piVal = va_arg(ap, int *);
      if( *piVal>1 ){
        pDb->eCola = *piVal;
      }
      *piVal = pDb->eCola;
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

    case LSM_CONFIG_PAGE_SIZE: {
      int *piVal = va_arg(ap, int *);
      if( *piVal ){
        lsmFsSetPageSize(pDb->pFS, *piVal);
      }
      *piVal = lsmFsPageSize(pDb->pFS);
      break;
    }

    case LSM_CONFIG_BLOCK_SIZE: {
      int *piVal = va_arg(ap, int *);
      if( *piVal ){
        lsmFsSetBlockSize(pDb->pFS, *piVal);
      }
      *piVal = lsmFsBlockSize(pDb->pFS);
      break;
    }

    case LSM_CONFIG_SAFETY: {
      int *piVal = va_arg(ap, int *);
      if( *piVal && *piVal>=1 && *piVal<=3 ){
        pDb->eSafety = *piVal;
      }
      *piVal = pDb->eSafety;
      break;
    }

    default:
      rc = LSM_MISUSE;
      break;
  }

  va_end(ap);
  return rc;
}

int lsm_global_config(int eParam, ...){
  int rc = LSM_OK;
  va_list ap;
  va_start(ap, eParam);

  switch( eParam ){
    case LSM_GLOBAL_CONFIG_SET_HEAP: {
      lsm_heap_methods *pHeap = va_arg(ap, lsm_heap_methods *);
      lsmConfigSetMalloc(pHeap);
      break;
    }

    case LSM_GLOBAL_CONFIG_GET_HEAP: {
      lsm_heap_methods *pHeap = va_arg(ap, lsm_heap_methods *);
      lsmConfigGetMalloc(pHeap);
      break;
    }

    case LSM_GLOBAL_CONFIG_SET_MUTEX: {
      lsm_mutex_methods *pMutex = va_arg(ap, lsm_mutex_methods *);
      lsmConfigSetMutex(pMutex);
      break;
    }

    case LSM_GLOBAL_CONFIG_GET_MUTEX: {
      lsm_mutex_methods *pMutex = va_arg(ap, lsm_mutex_methods *);
      lsmConfigGetMutex(pMutex);
      break;
    }

    default:
      rc = LSM_MISUSE;
      break;
  }

  va_end(ap);
  return rc;
}

static char *appendSegmentList(char *zIn, int *pRc, char *zPre, Segment *pSeg){
  char *zOut;

  zOut = lsmMallocAPrintfRc(zIn, pRc, "%s{%d %d}", zPre,
      pSeg->run.nSize,
      segmentHasSeparators(pSeg)
  );
  return zOut;
}

int lsmStructList(
  lsm_db *pDb,                    /* Database handle */
  char **pzOut                    /* OUT: Nul-terminated string (tcl list) */
){
  Level *pTopLevel = 0;           /* Top level of snapshot to report on */
  int rc = LSM_OK;
  Level *p;
  char *zOut = 0;
  Snapshot *pWorker;              /* Worker snapshot */
  Snapshot *pRelease = 0;         /* Snapshot to release */

  /* Obtain the worker snapshot */
  pWorker = pDb->pWorker;
  if( !pWorker ){
    pRelease = pWorker = lsmDbSnapshotWorker(pDb->pDatabase);
  }

  /* Format the contents of the snapshot as text */
  pTopLevel = lsmDbSnapshotLevel(pWorker);
  for(p=pTopLevel; rc==LSM_OK && p; p=p->pNext){
    int i;
    zOut = lsmMallocAPrintfRc(zOut, &rc, "%s{", (zOut ? " " : ""));
    zOut = appendSegmentList(zOut, &rc, "", &p->lhs);
    for(i=0; rc==LSM_OK && i<p->nRight; i++){
      zOut = appendSegmentList(zOut, &rc, " ", &p->aRhs[i]);
    }
    zOut = lsmMallocAPrintfRc(zOut, &rc, "}");
  }

  /* Release the snapshot and return */
  lsmDbSnapshotRelease(pRelease);
  *pzOut = zOut;
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

    case LSM_INFO_CKPT: {
      int **paVal = va_arg(ap, int **);
      int bRelease = 0;
      if( pDb->pWorker==0 ){
        pDb->pWorker = lsmDbSnapshotWorker(pDb->pDatabase);
        bRelease = 1;
      }
      if( pDb->pWorker==0 ){
        rc = LSM_BUSY;
      }else{
        rc = lsmCheckpointExport(pDb, 0, 0, (void **)paVal, 0);
        if( bRelease ){
          dbReleaseSnapshot(&pDb->pWorker);
        }
      }
      break;
    }

    case LSM_INFO_STRUCTLIST: {
      char **pzVal = va_arg(ap, char **);
      rc = lsmStructList(pDb, pzVal);
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

  if( rc==LSM_OK ){
    int pgsz = lsmFsPageSize(pDb->pFS);
    int nBefore;
    int nAfter;
    nBefore = lsmTreeSize(pDb->pTV)/pgsz;
    rc = lsmTreeInsert(pDb->pTV, pKey, nKey, pVal, nVal);
    nAfter = lsmTreeSize(pDb->pTV)/pgsz;

    if( rc==LSM_OK && pDb->bAutowork && nAfter!=nBefore ){
      rc = dbAutoWork(pDb, nAfter - nBefore);
    }
  }

  /* If a transaction was opened at the start of this function, commit it. 
  ** Or, if an error has occurred, roll it back.
  */
  if( bCommit ){
    if( rc==LSM_OK ){
      rc = lsm_commit(pDb, 1);
    }else{
      lsm_rollback(pDb, 1);
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
  lsm_cursor *pCsr;               /* New cursor object */

  /* Open a read transaction if one is not already open. */
  assert_db_state(pDb);
  rc = lsmBeginReadTrans(pDb);

  /* Allocate the lsm_cursor structure */
  if( rc==LSM_OK ){
    *ppCsr = pCsr = (lsm_cursor *)lsmMallocZero(sizeof(lsm_cursor));
    if( !pCsr ){
      rc = LSM_NOMEM_BKPT;
    }
  }

  /* Allocate the multi-cursor. */
  if( rc==LSM_OK ){
    rc = lsmMCursorNew(pDb, &pCsr->pMC);
  }

  /* If an error has occured, set the output to NULL and delete any partially
  ** allocated cursor. If this means there are no open cursors, release the
  ** client snapshot. Otherwise, link the cursor into the lsm_db.pCsr list.  
  */
  if( rc!=LSM_OK ){
    lsm_csr_close(pCsr);
    pCsr = 0;
    dbReleaseClientSnapshot(pDb);
  }else{
    pCsr->pNext = pDb->pCsr;
    pCsr->pDb = pDb;
    pDb->pCsr = pCsr;
  }
  *ppCsr = pCsr;

  assert_db_state(pDb);
  return rc;
}

/*
** Close a cursor opened using lsm_csr_open().
*/
int lsm_csr_close(lsm_cursor *pCsr){
  if( pCsr ){
    lsm_db *pDb = pCsr->pDb;
    lsm_cursor **pp;

    assert_db_state(pDb);

    /* Remove the cursor from the linked list */
    for(pp=&pDb->pCsr; *pp!=pCsr; pp=&((*pp)->pNext));
    *pp = (*pp)->pNext;

    lsmMCursorClose(pCsr->pMC);
    lsmFree(pCsr);

    dbReleaseClientSnapshot(pDb);
    assert_db_state(pDb);
  }
  return LSM_OK;
}

/*
** Attempt to seek the cursor to the database entry specified by pKey/nKey.
** If an error occurs (e.g. an OOM or IO error), return an LSM error code.
** Otherwise, return LSM_OK.
**
** If no error occurs and the database contains the specified key, the
** cursor is left pointing to it. In this case *pRes is set to 0 before
** returning.
**
** If the database does not contain key (pKey/nKey), then the cursor is 
** left pointing to a key that (pKey/nKey) would be next to if it were in
** the database. If the cursor is left pointing to a key that is smaller
** than (pKey/nKey), then *pRes is set to -1 before returning. If the
** cursor is left pointing to a key that is larger than (pKey/nKey), it is
** set to +1 before returning.
**
** Whether the cursor is left pointing to a smaller or larger key is
** influenced by the value of *pRes when this function is called. If *pRes
** is set to 0, this indicates that the user does not care - LSM selects
** a smaller or larger key based on the internal tree structure. However,
** if *pRes is less than zero, then the cursor is guaranteed to be left 
** pointing to a key smaller than (pKey/nKey), if one exists. And if *pRes
** is greater than zero, it is similarly guaranteed to point to a key
** larger than (pKey/nKey), if one exists.
*/
int lsm_csr_seek(lsm_cursor *pCsr, void *pKey, int nKey, int eSeek){
  int rc;
  rc = lsmMCursorSeek(pCsr->pMC, pKey, nKey, eSeek);
  return rc;
}

int lsm_csr_next(lsm_cursor *pCsr){
  return lsmMCursorNext(pCsr->pMC);
}

int lsm_csr_prev(lsm_cursor *pCsr){
  return lsmMCursorPrev(pCsr->pMC);
}

int lsm_csr_first(lsm_cursor *pCsr){
  return lsmMCursorFirst(pCsr->pMC);
}

int lsm_csr_last(lsm_cursor *pCsr){
  return lsmMCursorLast(pCsr->pMC);
}

int lsm_csr_valid(lsm_cursor *pCsr){
  return lsmMCursorValid(pCsr->pMC);
}

int lsm_csr_key(lsm_cursor *pCsr, void **ppKey, int *pnKey){
  return lsmMCursorKey(pCsr->pMC, ppKey, pnKey);
}

int lsm_csr_value(lsm_cursor *pCsr, void **ppVal, int *pnVal){
  return lsmMCursorValue(pCsr->pMC, ppVal, pnVal);
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
    va_list ap1;
    va_list ap2;
    char *z;                      /* String to pass to log callback */
    va_start(ap1, zFormat);
    va_start(ap2, zFormat);
    z = lsmMallocVPrintf(zFormat, ap1, ap2);
    va_end(ap1);
    va_end(ap2);
    if( z ) pDb->xLog(pDb->pLogCtx, rc, z);
    lsmFree(z);
  }
}

int lsm_begin(lsm_db *pDb, int iLevel){
  int rc = LSM_OK;

  assert_db_state( pDb );

  /* A value less than zero is against the rules. A value of 0 means commit
  ** the innermost transaction.  */
  if( iLevel<0 ) return LSM_MISUSE;
  if( iLevel<1 ) iLevel = pDb->nTransOpen + 1;

  if( iLevel>pDb->nTransOpen ){
    int i;

    if( pDb->nTransOpen==0 ){
      rc = lsmBeginWriteTrans(pDb);
    }

    if( rc==LSM_OK && pDb->nTransAlloc<iLevel ){
      TreeMark *aNew;
      aNew = (TreeMark *)lsmRealloc(pDb->aTrans, sizeof(TreeMark)*(iLevel+1));
      if( !aNew ){
        rc = LSM_NOMEM;
      }else{
        pDb->nTransAlloc = iLevel+1;
        pDb->aTrans = aNew;
      }
    }

    if( rc==LSM_OK ){
      for(i=pDb->nTransOpen; i<iLevel; i++){
        lsmTreeMark(pDb->pTV, &pDb->aTrans[i]);
      }
      pDb->nTransOpen = iLevel;
    }
  }

  return rc;
}

int lsm_commit(lsm_db *pDb, int iLevel){
  int rc = LSM_OK;

  assert_db_state( pDb );

  /* A value less than zero is against the rules. A value of 0 means open
  ** one more nested transaction.  */
  if( iLevel<0 ) return LSM_MISUSE;
  if( iLevel<1 ) iLevel = MIN(1, pDb->nTransOpen - 1);

  if( iLevel<=pDb->nTransOpen ){
    if( iLevel==1 ){
      rc = lsmLogCommit(pDb);
      if( rc==LSM_OK && pDb->eSafety==LSM_SAFETY_FULL ){
        rc = lsmFsLogSync(pDb->pFS);
      }
      if( rc==LSM_OK && pDb->pTV && lsmTreeSize(pDb->pTV)>pDb->nTreeLimit ){
        dbWorkerStart(pDb);
        rc = lsmFlushToDisk(pDb);
        dbWorkerDone(pDb);
      }
      lsmFinishWriteTrans(pDb);
    }
    pDb->nTransOpen = iLevel-1;
  }
  dbReleaseClientSnapshot(pDb);
  return rc;
}

int lsm_rollback(lsm_db *pDb, int iLevel){
  int rc = LSM_OK;

  assert_db_state( pDb );

  /* A value less than zero is against the rules. A value of 0 means roll
  ** back the innermost transaction.  */
  if( iLevel<0 ) return LSM_MISUSE;
  if( iLevel<1 ) iLevel = MIN(1, pDb->nTransOpen - 1);

  if( iLevel<=pDb->nTransOpen ){
    lsmTreeRollback(pDb->pTV, &pDb->aTrans[iLevel-1]);
    pDb->nTransOpen = iLevel-1;
  }

  if( pDb->nTransOpen==0 ){
    lsmFinishWriteTrans(pDb);
  }
  dbReleaseClientSnapshot(pDb);
  return rc;
}
