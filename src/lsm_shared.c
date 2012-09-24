/*
** 2012-01-23
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
** Utilities used to help multiple LSM clients to coexist within the
** same process space.
*/
#include "lsmInt.h"

/*
** Global data. All global variables used by code in this file are grouped
** into the following structure instance.
**
** pDatabase:
**   Linked list of all Database objects allocated within this process.
**   This list may not be traversed without holding the global mutex (see
**   functions enterGlobalMutex() and leaveGlobalMutex()).
*/
static struct SharedData {
  Database *pDatabase;            /* Linked list of all Database objects */
} gShared;

/*
** Database structure. There is one such structure for each distinct 
** database accessed by this process. They are stored in the singly linked 
** list starting at global variable gShared.pDatabase. Database objects are 
** reference counted. Once the number of connections to the associated
** database drops to zero, they are removed from the linked list and deleted.
*/
struct Database {
  /* Protected by the global mutex (enterGlobalMutex/leaveGlobalMutex): */
  char *zName;                    /* Canonical path to database file */
  int nName;                      /* strlen(zName) */
  int nDbRef;                     /* Number of associated lsm_db handles */
  Database *pDbNext;              /* Next Database structure in global list */

  /* Protected by the local mutex (pClientMutex) */
  lsm_file *pFile;                /* Used for locks/shm in multi-proc mode */
  LsmFile *pLsmFile;              /* List of deferred closes */
  lsm_mutex *pClientMutex;        /* Protects the apShmChunk[] and pConn */
  int nShmChunk;                  /* Number of entries in apShmChunk[] array */
  void **apShmChunk;              /* Array of "shared" memory regions */
  lsm_db *pConn;                  /* List of connections to this db. */
};

/*
** Functions to enter and leave the global mutex. This mutex is used
** to protect the global linked-list headed at gShared.pDatabase.
*/
static int enterGlobalMutex(lsm_env *pEnv){
  lsm_mutex *p;
  int rc = lsmMutexStatic(pEnv, LSM_MUTEX_GLOBAL, &p);
  if( rc==LSM_OK ) lsmMutexEnter(pEnv, p);
  return rc;
}
static void leaveGlobalMutex(lsm_env *pEnv){
  lsm_mutex *p;
  lsmMutexStatic(pEnv, LSM_MUTEX_GLOBAL, &p);
  lsmMutexLeave(pEnv, p);
}

#ifdef LSM_DEBUG
static int holdingGlobalMutex(lsm_env *pEnv){
  lsm_mutex *p;
  lsmMutexStatic(pEnv, LSM_MUTEX_GLOBAL, &p);
  return lsmMutexHeld(pEnv, p);
}
static void assertNotInFreelist(Freelist *p, int iBlk){
  int i; 
  for(i=0; i<p->nEntry; i++){
    assert( p->aEntry[i].iBlk!=iBlk );
  }
}
#else
# define assertNotInFreelist(x,y)
#endif

/*
** Append an entry to the free-list.
*/
int lsmFreelistAppend(lsm_env *pEnv, Freelist *p, int iBlk, i64 iId){

  /* Assert that this is not an attempt to insert a duplicate block number */
  assertNotInFreelist(p, iBlk);

  /* Extend the space allocated for the freelist, if required */
  assert( p->nAlloc>=p->nEntry );
  if( p->nAlloc==p->nEntry ){
    int nNew; 
    FreelistEntry *aNew;

    nNew = (p->nAlloc==0 ? 4 : p->nAlloc*2);
    aNew = (FreelistEntry *)lsmRealloc(pEnv, p->aEntry,
                                       sizeof(FreelistEntry)*nNew);
    if( !aNew ) return LSM_NOMEM_BKPT;
    p->nAlloc = nNew;
    p->aEntry = aNew;
  }

  /* Append the new entry to the freelist */
  p->aEntry[p->nEntry].iBlk = iBlk;
  p->aEntry[p->nEntry].iId = iId;
  p->nEntry++;

  return LSM_OK;
}

static int flInsertEntry(lsm_env *pEnv, Freelist *p, int iBlk){
  int rc;

  rc = lsmFreelistAppend(pEnv, p, iBlk, 1);
  if( rc==LSM_OK ){
    memmove(&p->aEntry[1], &p->aEntry[0], sizeof(FreelistEntry)*(p->nEntry-1));
    p->aEntry[0].iBlk = iBlk;
    p->aEntry[0].iId = 1;
  }
  return rc;
}

/*
** Remove the first entry of the free-list.
*/
static void flRemoveEntry0(Freelist *p){
  int nNew = p->nEntry - 1;
  assert( nNew>=0 );
  memmove(&p->aEntry[0], &p->aEntry[1], sizeof(FreelistEntry) * nNew);
  p->nEntry = nNew;
}

/*
** tHIS Function frees all resources held by the Database structure passed
** as the only argument.
*/
static void freeDatabase(lsm_env *pEnv, Database *p){
  assert( holdingGlobalMutex(pEnv) );
  if( p ){
    /* Free the mutexes */
    lsmMutexDel(pEnv, p->pClientMutex);

    if( p->pFile ){
      lsmEnvClose(pEnv, p->pFile);
    }

    /* Free the array of shm pointers */
    lsmFree(pEnv, p->apShmChunk);

    /* Free the memory allocated for the Database struct itself */
    lsmFree(pEnv, p);
  }
}

static void doDbDisconnect(lsm_db *pDb){
  int rc;

  /* Block for an exclusive lock on DMS1. This lock serializes all calls
  ** to doDbConnect() and doDbDisconnect() across all processes.  */
  rc = lsmShmLock(pDb, LSM_LOCK_DMS1, LSM_LOCK_EXCL, 1);
  if( rc==LSM_OK ){

    /* Try an exclusive lock on DMS2. If successful, this is the last
    ** connection to the database. In this case flush the contents of the
    ** in-memory tree to disk and write a checkpoint.  */
    rc = lsmShmLock(pDb, LSM_LOCK_DMS2, LSM_LOCK_EXCL, 0);
    if( rc==LSM_OK ){
      /* Flush the in-memory tree, if required. If there is data to flush,
      ** this will create a new client snapshot in Database.pClient. The
      ** checkpoint (serialization) of this snapshot may be written to disk
      ** by the following block.  */
      rc = lsmTreeLoadHeader(pDb, 0);
      if( rc==LSM_OK && (lsmTreeHasOld(pDb) || lsmTreeSize(pDb)>0) ){
        assert( pDb->nTransOpen==0 );
        pDb->nTransOpen = 1;
        lsmTreeMakeOld(pDb);
        if( pDb->treehdr.iOldShmid ){
          rc = lsmFlushTreeToDisk(pDb);
        }
        pDb->nTransOpen = 0;
      }

      /* Write a checkpoint to disk. */
      if( rc==LSM_OK ){
        rc = lsmCheckpointWrite(pDb, 0);
      }

      /* If the checkpoint was written successfully, delete the log file */
      if( rc==LSM_OK && pDb->pFS 
       && pDb->treehdr.iOldShmid==0 && pDb->treehdr.nByte==0 
      ){
        Database *p = pDb->pDatabase;
        lsmFsCloseAndDeleteLog(pDb->pFS);
        if( p->pFile ) lsmEnvShmUnmap(pDb->pEnv, p->pFile, 1);
      }
    }
  }

  lsmShmLock(pDb, LSM_LOCK_DMS2, LSM_LOCK_UNLOCK, 0);
  lsmShmLock(pDb, LSM_LOCK_DMS1, LSM_LOCK_UNLOCK, 0);
  pDb->pShmhdr = 0;
}

static int doDbConnect(lsm_db *pDb){
  const int nUsMax = 100000;      /* Max value for nUs */
  int nUs = 1000;                 /* us to wait between DMS1 attempts */
  int rc;

  /* Obtain a pointer to the shared-memory header */
  assert( pDb->pShmhdr==0 );
  rc = lsmShmChunk(pDb, 0, (void **)&pDb->pShmhdr);
  if( rc!=LSM_OK ) return rc;

  /* Block for an exclusive lock on DMS1. This lock serializes all calls
  ** to doDbConnect() and doDbDisconnect() across all processes.  */
  while( 1 ){
    rc = lsmShmLock(pDb, LSM_LOCK_DMS1, LSM_LOCK_EXCL, 1);
    if( rc!=LSM_BUSY ) break;
    lsmEnvSleep(pDb->pEnv, nUs);
    nUs = nUs * 2;
    if( nUs>nUsMax ) nUs = nUsMax;
  }
  if( rc!=LSM_OK ){
    pDb->pShmhdr = 0;
    return rc;
  }

  /* Try an exclusive lock on DMS2. If successful, this is the first and 
  ** only connection to the database. In this case initialize the 
  ** shared-memory and run log file recovery.  */
  rc = lsmShmLock(pDb, LSM_LOCK_DMS2, LSM_LOCK_EXCL, 0);
  if( rc==LSM_OK ){
    memset(pDb->pShmhdr, 0, sizeof(ShmHeader));
    rc = lsmCheckpointRecover(pDb);
    if( rc==LSM_OK ){
      rc = lsmLogRecover(pDb);
    }
  }else if( rc==LSM_BUSY ){
    rc = LSM_OK;
  }

  /* Take a shared lock on DMS2. This lock "cannot" fail, as connections 
  ** may only hold an exclusive lock on DMS2 if they first hold an exclusive
  ** lock on DMS1. And this connection is currently holding the exclusive
  ** lock on DSM1.  */
  if( rc==LSM_OK ){
    rc = lsmShmLock(pDb, LSM_LOCK_DMS2, LSM_LOCK_SHARED, 0);
    assert( rc!=LSM_BUSY );
  }

  /* If anything went wrong, unlock DMS2. Unlock DMS1 in any case. */
  if( rc!=LSM_OK ){
    lsmShmLock(pDb, LSM_LOCK_DMS2, LSM_LOCK_UNLOCK, 0);
    pDb->pShmhdr = 0;
  }
  lsmShmLock(pDb, LSM_LOCK_DMS1, LSM_LOCK_UNLOCK, 0);
  return rc;
}

/*
** Return a reference to the shared Database handle for the database 
** identified by canonical path zName. If this is the first connection to
** the named database, a new Database object is allocated. Otherwise, a
** pointer to an existing object is returned.
**
** If successful, *ppDatabase is set to point to the shared Database 
** structure and LSM_OK returned. Otherwise, *ppDatabase is set to NULL
** and and LSM error code returned.
**
** Each successful call to this function should be (eventually) matched
** by a call to lsmDbDatabaseRelease().
*/
int lsmDbDatabaseConnect(
  lsm_db *pDb,                    /* Database handle */
  const char *zName               /* Full-path to db file */
){
  lsm_env *pEnv = pDb->pEnv;
  int rc;                         /* Return code */
  Database *p = 0;                /* Pointer returned via *ppDatabase */
  int nName = lsmStrlen(zName);

  assert( pDb->pDatabase==0 );
  rc = enterGlobalMutex(pEnv);
  if( rc==LSM_OK ){

    /* Search the global list for an existing object. TODO: Need something
    ** better than the strcmp() below to figure out if a given Database
    ** object represents the requested file.  */
    for(p=gShared.pDatabase; p; p=p->pDbNext){
      if( nName==p->nName && 0==memcmp(zName, p->zName, nName) ) break;
    }

    /* If no suitable Database object was found, allocate a new one. */
    if( p==0 ){
      p = (Database *)lsmMallocZeroRc(pEnv, sizeof(Database)+nName+1, &rc);

      /* If the allocation was successful, fill in other fields and
      ** allocate the client mutex. */ 
      if( rc==LSM_OK ){
        p->zName = (char *)&p[1];
        p->nName = nName;
        memcpy((void *)p->zName, zName, nName+1);
        rc = lsmMutexNew(pEnv, &p->pClientMutex);
      }

      /* If running in multi-process mode and nothing has gone wrong so far,
      ** open the shared fd */
      if( rc==LSM_OK && pDb->bMultiProc ){
        rc = lsmEnvOpen(pDb->pEnv, p->zName, &p->pFile);
      }

      if( rc==LSM_OK ){
        p->pDbNext = gShared.pDatabase;
        gShared.pDatabase = p;
      }else{
        freeDatabase(pEnv, p);
        p = 0;
      }
    }

    if( p ){
      p->nDbRef++;
    }
    leaveGlobalMutex(pEnv);

    if( p ){
      lsmMutexEnter(pDb->pEnv, p->pClientMutex);
      pDb->pNext = p->pConn;
      p->pConn = pDb;
      lsmMutexLeave(pDb->pEnv, p->pClientMutex);
    }
  }

  pDb->pDatabase = p;
  if( rc==LSM_OK ){
    assert( p );
    rc = lsmFsOpen(pDb, zName);
  }
  if( rc==LSM_OK ){
    rc = doDbConnect(pDb);
  }

  return rc;
}

static void dbDeferClose(lsm_db *pDb){
  if( pDb->pFS ){
    LsmFile *pLsmFile = 0;
    Database *p = pDb->pDatabase;
    lsmFsDeferClose(pDb->pFS, &pLsmFile);
    pLsmFile->pNext = p->pLsmFile;
    p->pLsmFile = pLsmFile;
  }
}

LsmFile *lsmDbRecycleFd(lsm_db *db){
  LsmFile *pRet;
  Database *p = db->pDatabase;
  lsmMutexEnter(db->pEnv, p->pClientMutex);
  if( (pRet = p->pLsmFile)!=0 ){
    p->pLsmFile = pRet->pNext;
  }
  lsmMutexLeave(db->pEnv, p->pClientMutex);
  return pRet;
}

/*
** Release a reference to a Database object obtained from 
** lsmDbDatabaseConnect(). There should be exactly one call to this function 
** for each successful call to Find().
*/
void lsmDbDatabaseRelease(lsm_db *pDb){
  Database *p = pDb->pDatabase;
  if( p ){
    lsm_db **ppDb;

    if( pDb->pShmhdr ){
      doDbDisconnect(pDb);
    }

    lsmMutexEnter(pDb->pEnv, p->pClientMutex);
    for(ppDb=&p->pConn; *ppDb!=pDb; ppDb=&((*ppDb)->pNext));
    *ppDb = pDb->pNext;
    if( lsmDbMultiProc(pDb) ){
      dbDeferClose(pDb);
    }
    lsmMutexLeave(pDb->pEnv, p->pClientMutex);

    enterGlobalMutex(pDb->pEnv);
    p->nDbRef--;
    if( p->nDbRef==0 ){
      Database **pp;

      /* Remove the Database structure from the linked list. */
      for(pp=&gShared.pDatabase; *pp!=p; pp=&((*pp)->pDbNext));
      *pp = p->pDbNext;

      /* Free the Database object and shared memory buffers. */
      if( p->pFile==0 ){
        int i;
        for(i=0; i<p->nShmChunk; i++){
          lsmFree(pDb->pEnv, p->apShmChunk[i]);
        }
      }else{
        LsmFile *pIter;
        LsmFile *pNext;
        for(pIter=p->pLsmFile; pIter; pIter=pNext){
          pNext = pIter->pNext;
          lsmEnvClose(pDb->pEnv, pIter->pFile);
          lsmFree(pDb->pEnv, pIter);
        }
      }
      freeDatabase(pDb->pEnv, p);
    }
    leaveGlobalMutex(pDb->pEnv);
  }
}

Level *lsmDbSnapshotLevel(Snapshot *pSnapshot){
  return pSnapshot->pLevel;
}

void lsmDbSnapshotSetLevel(Snapshot *pSnap, Level *pLevel){
  pSnap->pLevel = pLevel;
}


/*
** Allocate a new database file block to write data to, either by extending
** the database file or by recycling a free-list entry. The worker snapshot 
** must be held in order to call this function.
**
** If successful, *piBlk is set to the block number allocated and LSM_OK is
** returned. Otherwise, *piBlk is zeroed and an lsm error code returned.
*/
int lsmBlockAllocate(lsm_db *pDb, int *piBlk){
  Snapshot *p = pDb->pWorker;
  Freelist *pFree;                /* Database free list */
  int iRet = 0;                   /* Block number of allocated block */
  int rc = LSM_OK;

  assert( pDb->pWorker );
 
  pFree = &p->freelist;
  if( pFree->nEntry>0 ){
    /* The first block on the free list was freed as part of the work done
    ** to create the snapshot with id iFree. So, we can reuse this block if
    ** snapshot iFree or later has been checkpointed and all currently 
    ** active clients are reading from snapshot iFree or later.  */
    i64 iFree = pFree->aEntry[0].iId;
    int bInUse = 0;

    /* The "is in use" bit */
    rc = lsmLsmInUse(pDb, iFree, &bInUse);

    /* The "has been checkpointed" bit */
    if( rc==LSM_OK && bInUse==0 ){
      i64 iId = 0;
      rc = lsmCheckpointSynced(pDb, &iId, 0, 0);
      if( rc!=LSM_OK || iId<iFree ) bInUse = 2;
    }

    if( rc==LSM_OK && bInUse==0 ){
      iRet = pFree->aEntry[0].iBlk;
      flRemoveEntry0(pFree);
      assert( iRet!=0 );
    }
#if 0
    lsmLogMessage(
        pDb, 0, "%d reusing block %d%s", (iRet==0 ? "not " : "")
        pFree->aEntry[0].iBlk, 
        bInUse==0 ? "" : bInUse==1 ? " (client)" : " (unsynced)"
    );
#endif
  }

  /* If no block was allocated from the free-list, allocate one at the
  ** end of the file. */
  if( rc==LSM_OK && iRet==0 ){
    iRet = ++pDb->pWorker->nBlock;
  }

  *piBlk = iRet;
  return LSM_OK;
}

/*
** Free a database block. The worker snapshot must be held in order to call 
** this function.
**
** If successful, LSM_OK is returned. Otherwise, an lsm error code (e.g. 
** LSM_NOMEM).
*/
int lsmBlockFree(lsm_db *pDb, int iBlk){
  Snapshot *p = pDb->pWorker;

  assert( lsmShmAssertWorker(pDb) );
  /* TODO: Should assert() that lsmCheckpointOverflow() has not been called */

  return lsmFreelistAppend(pDb->pEnv, &p->freelist, iBlk, p->iId);
}

/*
** Refree a database block. The worker snapshot must be held in order to call 
** this function.
**
** Refreeing is required when a block is allocated using lsmBlockAllocate()
** but then not used. This function is used to push the block back onto
** the freelist. Refreeing a block is different from freeing is, as a refreed
** block may be reused immediately. Whereas a freed block can not be reused 
** until (at least) after the next checkpoint.
*/
int lsmBlockRefree(lsm_db *pDb, int iBlk){
  int rc = LSM_OK;                /* Return code */
  Snapshot *p = pDb->pWorker;

  if( iBlk==p->nBlock ){
    p->nBlock--;
  }else{
    rc = flInsertEntry(pDb->pEnv, &p->freelist, iBlk);
  }

  return rc;
}

/*
** If required, copy a database checkpoint from shared memory into the
** database itself.
**
** The WORKER lock must not be held when this is called. This is because
** this function may indirectly call fsync(). And the WORKER lock should
** not be held that long (in case it is required by a client flushing an
** in-memory tree to disk).
*/
int lsmCheckpointWrite(lsm_db *pDb, u32 *pnWrite){
  int rc;                         /* Return Code */
  u32 nWrite = 0;

  assert( pDb->pWorker==0 );
  assert( 1 || pDb->pClient==0 );
  assert( lsmShmAssertLock(pDb, LSM_LOCK_WORKER, LSM_LOCK_UNLOCK) );

  rc = lsmShmLock(pDb, LSM_LOCK_CHECKPOINTER, LSM_LOCK_EXCL, 0);
  if( rc!=LSM_OK ) return rc;

  rc = lsmCheckpointLoad(pDb, 0);
  if( rc==LSM_OK ){
    ShmHeader *pShm = pDb->pShmhdr;
    int bDone = 0;                /* True if checkpoint is already stored */

    /* Check if this checkpoint has already been written to the database
    ** file. If so, set variable bDone to true.  */
    if( pShm->iMetaPage ){
      MetaPage *pPg;              /* Meta page */
      u8 *aData;                  /* Meta-page data buffer */
      int nData;                  /* Size of aData[] in bytes */
      i64 iCkpt;                  /* Id of checkpoint just loaded */
      i64 iDisk;                  /* Id of checkpoint already stored in db */
      iCkpt = lsmCheckpointId(pDb->aSnapshot, 0);
      rc = lsmFsMetaPageGet(pDb->pFS, 0, pShm->iMetaPage, &pPg);
      if( rc==LSM_OK ){
        aData = lsmFsMetaPageData(pPg, &nData);
        iDisk = lsmCheckpointId((u32 *)aData, 1);
        nWrite = lsmCheckpointNWrite((u32 *)aData, 1);
        lsmFsMetaPageRelease(pPg);
      }
      bDone = (iDisk>=iCkpt);
    }

    if( rc==LSM_OK && bDone==0 ){
      int iMeta = (pShm->iMetaPage % 2) + 1;
#if 0
  lsmLogMessage(pDb, 0, "starting checkpoint");
#endif
      if( pDb->eSafety!=LSM_SAFETY_OFF ){
        rc = lsmFsSyncDb(pDb->pFS);
      }
      if( rc==LSM_OK ) rc = lsmCheckpointStore(pDb, iMeta);
      if( rc==LSM_OK && pDb->eSafety!=LSM_SAFETY_OFF){
        rc = lsmFsSyncDb(pDb->pFS);
      }
      if( rc==LSM_OK ){
        pShm->iMetaPage = iMeta;
        nWrite = lsmCheckpointNWrite(pDb->aSnapshot, 0) - nWrite;
      }
#ifdef LSM_LOG_WORK
  lsmLogMessage(pDb, 0, "finish checkpoint %d", 
      (int)lsmCheckpointId(pDb->aSnapshot, 0)
  );
#endif
    }
  }

  lsmShmLock(pDb, LSM_LOCK_CHECKPOINTER, LSM_LOCK_UNLOCK, 0);
  if( pnWrite && rc==LSM_OK ) *pnWrite = nWrite;
  return rc;
}

int lsmBeginWork(lsm_db *pDb){
  int rc;

  /* Attempt to take the WORKER lock */
  rc = lsmShmLock(pDb, LSM_LOCK_WORKER, LSM_LOCK_EXCL, 0);

  /* Deserialize the current worker snapshot */
  if( rc==LSM_OK ){
    rc = lsmCheckpointLoadWorker(pDb);
    if( pDb->pWorker ) pDb->pWorker->pDatabase = pDb->pDatabase;
  }
  return rc;
}

void lsmFreeSnapshot(lsm_env *pEnv, Snapshot *p){
  if( p ){
    lsmSortedFreeLevel(pEnv, p->pLevel);
    lsmFree(pEnv, p->freelist.aEntry);
    lsmFree(pEnv, p);
  }
}

/*
** Argument bFlush is true if the contents of the in-memory tree has just
** been flushed to disk. The significance of this is that once the snapshot
** created to hold the updated state of the database is synced to disk, log
** file space can be recycled.
*/
void lsmFinishWork(lsm_db *pDb, int bFlush, int nOvfl, int *pRc){
  /* If no error has occurred, serialize the worker snapshot and write
  ** it to shared memory.  */
  if( *pRc==LSM_OK ){
    *pRc = lsmCheckpointSaveWorker(pDb, bFlush, nOvfl);
  }

  if( pDb->pWorker ){
    lsmFreeSnapshot(pDb->pEnv, pDb->pWorker);
    pDb->pWorker = 0;
  }

  lsmShmLock(pDb, LSM_LOCK_WORKER, LSM_LOCK_UNLOCK, 0);
}


/*
** Called when recovery is finished.
*/
int lsmFinishRecovery(lsm_db *pDb){
  lsmTreeEndTransaction(pDb, 1);
  return LSM_OK;
}

/*
** Begin a read transaction. This function is a no-op if the connection
** passed as the only argument already has an open read transaction.
*/
int lsmBeginReadTrans(lsm_db *pDb){
  const int MAX_READLOCK_ATTEMPTS = 5;
  int rc = LSM_OK;                /* Return code */
  int iAttempt = 0;

  assert( pDb->pWorker==0 );
  assert( (pDb->pClient!=0)==(pDb->iReader>=0) );

  while( rc==LSM_OK && pDb->pClient==0 && (iAttempt++)<MAX_READLOCK_ATTEMPTS ){
    int iTreehdr = 0;
    int iSnap = 0;
    assert( pDb->pCsr==0 && pDb->nTransOpen==0 );

    /* Load the in-memory tree header. */
    rc = lsmTreeLoadHeader(pDb, &iTreehdr);

    /* Load the database snapshot */
    if( rc==LSM_OK ){
      rc = lsmCheckpointLoad(pDb, &iSnap);
    }

    /* Take a read-lock on the tree and snapshot just loaded. Then check
    ** that the shared-memory still contains the same values. If so, proceed.
    ** Otherwise, relinquish the read-lock and retry the whole procedure
    ** (starting with loading the in-memory tree header).  */
    if( rc==LSM_OK ){
      ShmHeader *pShm = pDb->pShmhdr;
      u32 iShmMax = pDb->treehdr.iUsedShmid;
      u32 iShmMin = pDb->treehdr.iNextShmid+1-pDb->treehdr.nChunk;
      rc = lsmReadlock(
          pDb, lsmCheckpointId(pDb->aSnapshot, 0), iShmMin, iShmMax
      );
      if( rc==LSM_OK ){
        if( lsmTreeLoadHeaderOk(pDb, iTreehdr)
         && lsmCheckpointLoadOk(pDb, iSnap)
        ){
          /* Read lock has been successfully obtained. Deserialize the 
          ** checkpoint just loaded. TODO: This will be removed after 
          ** lsm_sorted.c is changed to work directly from the serialized
          ** version of the snapshot.  */
          rc = lsmCheckpointDeserialize(pDb, 0, pDb->aSnapshot, &pDb->pClient);
          assert( (rc==LSM_OK)==(pDb->pClient!=0) );
          assert( pDb->iReader>=0 );
        }else{
          rc = lsmReleaseReadlock(pDb);
        }
      }
      if( rc==LSM_BUSY ) rc = LSM_OK;
    }
  }
  if( pDb->pClient==0 && rc==LSM_OK ) rc = LSM_BUSY;

  return rc;
}

/*
** Close the currently open read transaction.
*/
void lsmFinishReadTrans(lsm_db *pDb){
  Snapshot *pClient = pDb->pClient;

  /* Worker connections should not be closing read transactions. And
  ** read transactions should only be closed after all cursors and write
  ** transactions have been closed. Finally pClient should be non-NULL
  ** only iff pDb->iReader>=0.  */
  assert( pDb->pWorker==0 );
  assert( pDb->pCsr==0 && pDb->nTransOpen==0 );

  if( pClient ){
    lsmFreeSnapshot(pDb->pEnv, pDb->pClient);
    pDb->pClient = 0;
  }
  if( pDb->iReader>=0 ) lsmReleaseReadlock(pDb);
  assert( (pDb->pClient!=0)==(pDb->iReader>=0) );
}

/*
** Open a write transaction.
*/
int lsmBeginWriteTrans(lsm_db *pDb){
  int rc;                         /* Return code */
  ShmHeader *pShm = pDb->pShmhdr; /* Shared memory header */

  assert( pDb->nTransOpen==0 );

  /* If there is no read-transaction open, open one now. */
  rc = lsmBeginReadTrans(pDb);

  /* Attempt to take the WRITER lock */
  if( rc==LSM_OK ){
    rc = lsmShmLock(pDb, LSM_LOCK_WRITER, LSM_LOCK_EXCL, 0);
  }

  /* If the previous writer failed mid-transaction, run emergency rollback. */
  if( rc==LSM_OK && pShm->bWriter ){
    rc = lsmTreeRepair(pDb);
    if( rc==LSM_OK ) pShm->bWriter = 0;
  }

  /* Check that this connection is currently reading from the most recent
  ** version of the database. If not, return LSM_BUSY.  */
  if( rc==LSM_OK && memcmp(&pShm->hdr1, &pDb->treehdr, sizeof(TreeHeader)) ){
    rc = LSM_BUSY;
  }

  if( rc==LSM_OK ){
    rc = lsmLogBegin(pDb);
  }

  /* If everything was successful, set the "transaction-in-progress" flag
  ** and return LSM_OK. Otherwise, if some error occurred, relinquish the 
  ** WRITER lock and return an error code.  */
  if( rc==LSM_OK ){
    TreeHeader *p = &pDb->treehdr;
    pShm->bWriter = 1;
    p->root.iTransId++;
    if( lsmTreeHasOld(pDb) && p->iOldLog==pDb->pClient->iLogOff ){
      lsmTreeDiscardOld(pDb);
    }
  }else{
    lsmShmLock(pDb, LSM_LOCK_WRITER, LSM_LOCK_UNLOCK, 0);
    if( pDb->pCsr==0 ) lsmFinishReadTrans(pDb);
  }
  return rc;
}

/*
** End the current write transaction. The connection is left with an open
** read transaction. It is an error to call this if there is no open write 
** transaction.
**
** If the transaction was committed, then a commit record has already been
** written into the log file when this function is called. Or, if the
** transaction was rolled back, both the log file and in-memory tree 
** structure have already been restored. In either case, this function 
** merely releases locks and other resources held by the write-transaction.
**
** LSM_OK is returned if successful, or an LSM error code otherwise.
*/
int lsmFinishWriteTrans(lsm_db *pDb, int bCommit){
  lsmLogEnd(pDb, bCommit);
  lsmTreeEndTransaction(pDb, bCommit);
  lsmShmLock(pDb, LSM_LOCK_WRITER, LSM_LOCK_UNLOCK, 0);
  return LSM_OK;
}


/*
** Return non-zero if the caller is holding the client mutex.
*/
#ifdef LSM_DEBUG
int lsmHoldingClientMutex(lsm_db *pDb){
  return lsmMutexHeld(pDb->pEnv, pDb->pDatabase->pClientMutex);
}
#endif

static int slotIsUsable(ShmReader *p, i64 iLsm, u32 iShmMin, u32 iShmMax){
  return( 
      p->iLsmId && p->iLsmId<=iLsm 
      && shm_sequence_ge(iShmMax, p->iTreeId)
      && shm_sequence_ge(p->iTreeId, iShmMin)
  );
}

/*
** Obtain a read-lock on database version identified by the combination
** of snapshot iLsm and tree iTree. Return LSM_OK if successful, or
** an LSM error code otherwise.
*/
int lsmReadlock(lsm_db *db, i64 iLsm, u32 iShmMin, u32 iShmMax){
  ShmHeader *pShm = db->pShmhdr;
  int i;
  int rc = LSM_OK;

  assert( db->iReader<0 );
  assert( shm_sequence_ge(iShmMax, iShmMin) );

  /* Search for an exact match. */
  for(i=0; db->iReader<0 && rc==LSM_OK && i<LSM_LOCK_NREADER; i++){
    ShmReader *p = &pShm->aReader[i];
    if( p->iLsmId==iLsm && p->iTreeId==iShmMax ){
      rc = lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_SHARED, 0);
      if( rc==LSM_OK && p->iLsmId==iLsm && p->iTreeId==iShmMax ){
        db->iReader = i;
      }else if( rc==LSM_BUSY ){
        rc = LSM_OK;
      }
    }
  }

  /* Try to obtain a write-lock on each slot, in order. If successful, set
  ** the slot values to iLsm/iTree.  */
  for(i=0; db->iReader<0 && rc==LSM_OK && i<LSM_LOCK_NREADER; i++){
    rc = lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_EXCL, 0);
    if( rc==LSM_BUSY ){
      rc = LSM_OK;
    }else{
      ShmReader *p = &pShm->aReader[i];
      p->iLsmId = iLsm;
      p->iTreeId = iShmMax;
      rc = lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_SHARED, 0);
      if( rc==LSM_OK ) db->iReader = i;
    }
  }

  /* Search for any usable slot */
  for(i=0; db->iReader<0 && rc==LSM_OK && i<LSM_LOCK_NREADER; i++){
    ShmReader *p = &pShm->aReader[i];
    if( slotIsUsable(p, iLsm, iShmMax, iShmMax) ){
      rc = lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_SHARED, 0);
      if( rc==LSM_OK && slotIsUsable(p, iLsm, iShmMax, iShmMax) ){
        db->iReader = i;
      }else if( rc==LSM_BUSY ){
        rc = LSM_OK;
      }
    }
  }

  return rc;
}

/*
** This is used to check if there exists a read-lock locking a particular
** version of either the in-memory tree or database file. 
**
** If iLsmId is non-zero, then it is a snapshot id. If there exists a 
** read-lock using this snapshot or newer, set *pbInUse to true. Or,
** if there is no such read-lock, set it to false.
**
** Or, if iLsmId is zero, then iShmid is a shared-memory sequence id.
** Search for a read-lock using this sequence id or newer. etc.
*/
static int isInUse(lsm_db *db, i64 iLsmId, u32 iShmid, int *pbInUse){
  ShmHeader *pShm = db->pShmhdr;
  int i;
  int rc = LSM_OK;

  for(i=0; rc==LSM_OK && i<LSM_LOCK_NREADER; i++){
    ShmReader *p = &pShm->aReader[i];
    if( p->iLsmId ){
      if( (iLsmId!=0 && p->iLsmId!=0 && iLsmId>=p->iLsmId) 
       || (iLsmId==0 && shm_sequence_ge(p->iTreeId, iShmid))
      ){
        rc = lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_EXCL, 0);
        if( rc==LSM_OK ){
          p->iLsmId = 0;
          lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_UNLOCK, 0);
        }
      }
    }
  }

  if( rc==LSM_BUSY ){
    *pbInUse = 1;
    return LSM_OK;
  }
  *pbInUse = 0;
  return rc;
}

int lsmTreeInUse(lsm_db *db, u32 iShmid, int *pbInUse){
  if( db->treehdr.iUsedShmid==iShmid ){
    *pbInUse = 1;
    return LSM_OK;
  }
  return isInUse(db, 0, iShmid, pbInUse);
}

int lsmLsmInUse(lsm_db *db, i64 iLsmId, int *pbInUse){
  if( db->pClient && db->pClient->iId<=iLsmId ){
    *pbInUse = 1;
    return LSM_OK;
  }
  return isInUse(db, iLsmId, 0, pbInUse);
}

/*
** Release the read-lock currently held by connection db.
*/
int lsmReleaseReadlock(lsm_db *db){
  int rc = LSM_OK;
  if( db->iReader>=0 ){
    assert( db->pClient==0 );
    rc = lsmShmLock(db, LSM_LOCK_READER(db->iReader), LSM_LOCK_UNLOCK, 0);
    db->iReader = -1;
  }
  return rc;
}

/*
** This function may only be called after a successful call to
** lsmDbDatabaseConnect(). It returns true if the connection is in
** multi-process mode, or false otherwise.
*/
int lsmDbMultiProc(lsm_db *pDb){
  return pDb->pDatabase && (pDb->pDatabase->pFile!=0);
}


/*************************************************************************
**************************************************************************
**************************************************************************
**************************************************************************
**************************************************************************
*************************************************************************/

/*
** Retrieve a pointer to shared-memory chunk iChunk. Chunks are numbered
** starting from 0 (i.e. the header chunk is chunk 0).
*/
int lsmShmChunk(lsm_db *db, int iChunk, void **ppData){
  int rc = LSM_OK;
  void *pRet = 0;
  Database *p = db->pDatabase;
  lsm_env *pEnv = db->pEnv;

  while( iChunk>=db->nShm ){
    void **apShm;
    apShm = lsmRealloc(pEnv, db->apShm, sizeof(void*)*(db->nShm+16));
    if( !apShm ) return LSM_NOMEM_BKPT;
    memset(&apShm[db->nShm], 0, sizeof(void*)*16);
    db->apShm = apShm;
    db->nShm += 16;
  }
  if( db->apShm[iChunk] ){
    *ppData = db->apShm[iChunk];
    return rc;
  }

  /* Enter the client mutex */
  assert( iChunk>=0 );
  lsmMutexEnter(pEnv, p->pClientMutex);

  if( iChunk>=p->nShmChunk ){
    int nNew = iChunk+1;
    void **apNew;
    apNew = (void **)lsmRealloc(pEnv, p->apShmChunk, sizeof(void*) * nNew);
    if( apNew==0 ){
      rc = LSM_NOMEM_BKPT;
    }else{
      memset(&apNew[p->nShmChunk], 0, sizeof(void*) * (nNew-p->nShmChunk));
      p->apShmChunk = apNew;
      p->nShmChunk = nNew;
    }
  }

  if( rc==LSM_OK && p->apShmChunk[iChunk]==0 ){
    void *pChunk = 0;
    if( p->pFile==0 ){
      /* Single process mode */
      pChunk = lsmMallocZeroRc(pEnv, LSM_SHM_CHUNK_SIZE, &rc);
    }else{
      /* Multi-process mode */
      rc = lsmEnvShmMap(pEnv, p->pFile, iChunk, LSM_SHM_CHUNK_SIZE, &pChunk);
    }
    p->apShmChunk[iChunk] = pChunk;
  }

  if( rc==LSM_OK ){
    pRet = p->apShmChunk[iChunk];
  }

  /* Release the client mutex */
  lsmMutexLeave(pEnv, p->pClientMutex);

  *ppData = db->apShm[iChunk] = pRet; 
  return rc;
}

/*
** Attempt to obtain the lock identified by the iLock and bExcl parameters.
** If successful, return LSM_OK. If the lock cannot be obtained because 
** there exists some other conflicting lock, return LSM_BUSY. If some other
** error occurs, return an LSM error code.
**
** Parameter iLock must be one of LSM_LOCK_WRITER, WORKER or CHECKPOINTER,
** or else a value returned by the LSM_LOCK_READER macro.
*/
int lsmShmLock(
  lsm_db *db, 
  int iLock,
  int eOp,                        /* One of LSM_LOCK_UNLOCK, SHARED or EXCL */
  int bBlock                      /* True for a blocking lock */
){
  lsm_db *pIter;
  const u32 me = (1 << (iLock-1));
  const u32 ms = (1 << (iLock+16-1));
  int rc = LSM_OK;
  Database *p = db->pDatabase;

  assert( iLock>=1 && iLock<=LSM_LOCK_READER(LSM_LOCK_NREADER-1) );
  assert( iLock<=16 );
  assert( eOp==LSM_LOCK_UNLOCK || eOp==LSM_LOCK_SHARED || eOp==LSM_LOCK_EXCL );

  /* Check for a no-op. Proceed only if this is not one of those. */
  if( (eOp==LSM_LOCK_UNLOCK && (db->mLock & (me|ms))!=0)
   || (eOp==LSM_LOCK_SHARED && (db->mLock & (me|ms))!=ms)
   || (eOp==LSM_LOCK_EXCL   && (db->mLock & me)==0)
  ){
    int nExcl = 0;                /* Number of connections holding EXCLUSIVE */
    int nShared = 0;              /* Number of connections holding SHARED */
    lsmMutexEnter(db->pEnv, p->pClientMutex);

    /* Figure out the locks currently held by this process on iLock, not
    ** including any held by connection db.  */
    for(pIter=p->pConn; pIter; pIter=pIter->pNext){
      assert( (pIter->mLock & me)==0 || (pIter->mLock & ms)!=0 );
      if( pIter!=db ){
        if( pIter->mLock & me ){
          nExcl++;
        }else if( pIter->mLock & ms ){
          nShared++;
        }
      }
    }
    assert( nExcl==0 || nExcl==1 );
    assert( nExcl==0 || nShared==0 );
    assert( nExcl==0 || (db->mLock & (me|ms))==0 );

    switch( eOp ){
      case LSM_LOCK_UNLOCK:
        if( nShared==0 ){
          lsmEnvLock(db->pEnv, p->pFile, iLock, LSM_LOCK_UNLOCK);
        }
        db->mLock &= ~(me|ms);
        break;

      case LSM_LOCK_SHARED:
        if( nExcl ){
          rc = LSM_BUSY;
        }else{
          if( nShared==0 ){
            rc = lsmEnvLock(db->pEnv, p->pFile, iLock, LSM_LOCK_SHARED);
          }
          db->mLock |= ms;
          db->mLock &= ~me;
        }
        break;

      default:
        assert( eOp==LSM_LOCK_EXCL );
        if( nExcl || nShared ){
          rc = LSM_BUSY;
        }else{
          rc = lsmEnvLock(db->pEnv, p->pFile, iLock, LSM_LOCK_EXCL);
          db->mLock |= (me|ms);
        }
        break;
    }

    lsmMutexLeave(db->pEnv, p->pClientMutex);
  }

  return rc;
}

#ifdef LSM_DEBUG

int shmLockType(lsm_db *db, int iLock){
  const u32 me = (1 << (iLock-1));
  const u32 ms = (1 << (iLock+16-1));

  if( db->mLock & me ) return LSM_LOCK_EXCL;
  if( db->mLock & ms ) return LSM_LOCK_SHARED;
  return LSM_LOCK_UNLOCK;
}

/*
** The arguments passed to this function are similar to those passed to
** the lsmShmLock() function. However, instead of obtaining a new lock 
** this function returns true if the specified connection already holds 
** (or does not hold) such a lock, depending on the value of eOp. As
** follows:
**
**   (eOp==LSM_LOCK_UNLOCK) -> true if db has no lock on iLock
**   (eOp==LSM_LOCK_SHARED) -> true if db has at least a SHARED lock on iLock.
**   (eOp==LSM_LOCK_EXCL)   -> true if db has an EXCLUSIVE lock on iLock.
*/
int lsmShmAssertLock(lsm_db *db, int iLock, int eOp){
  int ret;
  int eHave;

  assert( iLock>=1 && iLock<=LSM_LOCK_READER(LSM_LOCK_NREADER-1) );
  assert( iLock<=16 );
  assert( eOp==LSM_LOCK_UNLOCK || eOp==LSM_LOCK_SHARED || eOp==LSM_LOCK_EXCL );

  eHave = shmLockType(db, iLock);

  switch( eOp ){
    case LSM_LOCK_UNLOCK:
      ret = (eHave==LSM_LOCK_UNLOCK);
      break;
    case LSM_LOCK_SHARED:
      ret = (eHave!=LSM_LOCK_UNLOCK);
      break;
    case LSM_LOCK_EXCL:
      ret = (eHave==LSM_LOCK_EXCL);
      break;
    default:
      assert( !"bad eOp value passed to lsmShmAssertLock()" );
      break;
  }

  return ret;
}

int lsmShmAssertWorker(lsm_db *db){
  return lsmShmAssertLock(db, LSM_LOCK_WORKER, LSM_LOCK_EXCL) && db->pWorker;
}

/*
** This function does not contribute to library functionality, and is not
** included in release builds. It is intended to be called from within
** an interactive debugger.
**
** When called, this function prints a single line of human readable output
** to stdout describing the locks currently held by the connection. For 
** example:
**
**     (gdb) call print_db_locks(pDb)
**     (shared on dms2) (exclusive on writer) 
*/
void print_db_locks(lsm_db *db){
  int iLock;
  for(iLock=0; iLock<16; iLock++){
    int bOne = 0;
    const char *azLock[] = {0, "shared", "exclusive"};
    const char *azName[] = {
      0, "dms1", "dms2", "writer", "worker", "checkpointer",
      "reader0", "reader1", "reader2", "reader3", "reader4", "reader5"
    };
    int eHave = shmLockType(db, iLock);
    if( azLock[eHave] ){
      printf("%s(%s on %s)", (bOne?" ":""), azLock[eHave], azName[iLock]);
      bOne = 1;
    }
  }
  printf("\n");
}
void print_all_db_locks(lsm_db *db){
  lsm_db *p;
  for(p=db->pDatabase->pConn; p; p=p->pNext){
    printf("%s connection %p ", ((p==db)?"*":""), p);
    print_db_locks(p);
  }
}
#endif

void lsmShmBarrier(lsm_db *db){
  lsmEnvShmBarrier(db->pEnv);
}

int lsm_checkpoint(lsm_db *pDb, int *pnByte){
  int rc;                         /* Return code */
  u32 nWrite = 0;                 /* Number of pages checkpointed */

  /* Attempt the checkpoint. If successful, nWrite is set to the number of
  ** pages written between this and the previous checkpoint.  */
  rc = lsmCheckpointWrite(pDb, &nWrite);

  /* If required, calculate the output variable (bytes of data checkpointed). 
  ** Set it to zero if an error occured.  */
  if( pnByte ){
    int nByte = 0;
    if( rc==LSM_OK && nWrite ){
      nByte = (int)nWrite * lsmFsPageSize(pDb->pFS);
    }
    *pnByte = nByte;
  }

  return rc;
}

