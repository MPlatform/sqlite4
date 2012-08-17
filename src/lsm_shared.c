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
  void *pId;                      /* Database id (file inode) */
  int nId;                        /* Size of pId in bytes */
  int nDbRef;                     /* Number of associated lsm_db handles */
  Database *pDbNext;              /* Next Database structure in global list */

  /* Protected by the local mutex (pClientMutex) */
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
static void assertMustbeWorker(lsm_db *pDb){
  assert( pDb->pWorker );
}
#else
# define assertNotInFreelist(x,y)
# define assertMustbeWorker(x)
#endif

/*
** Append an entry to the free-list.
*/
static int flAppendEntry(lsm_env *pEnv, Freelist *p, int iBlk, i64 iId){

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

  rc = flAppendEntry(pEnv, p, iBlk, 1);
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
** This function frees all resources held by the Database structure passed
** as the only argument.
*/
static void freeDatabase(lsm_env *pEnv, Database *p){
  assert( holdingGlobalMutex(pEnv) );
  if( p ){
    /* Free the mutexes */
    lsmMutexDel(pEnv, p->pClientMutex);

    /* Free the memory allocated for the Database struct itself */
    lsmFree(pEnv, p);
  }
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
int lsmDbDatabaseFind(
  lsm_db *pDb,                    /* Database handle */
  const char *zName               /* Path to db file */
){
  lsm_env *pEnv = pDb->pEnv;
  int rc;                         /* Return code */
  Database *p = 0;                /* Pointer returned via *ppDatabase */
  int nId = 0;
  void *pId = 0;

  assert( pDb->pDatabase==0 );
  rc = lsmFsFileid(pDb, &pId, &nId);
  if( rc!=LSM_OK ) return rc;

  rc = enterGlobalMutex(pEnv);
  if( rc==LSM_OK ){

    /* Search the global list for an existing object. TODO: Need something
    ** better than the strcmp() below to figure out if a given Database
    ** object represents the requested file.  */
    for(p=gShared.pDatabase; p; p=p->pDbNext){
      if( nId==p->nId && 0==memcmp(pId, p->pId, nId) ) break;
    }

    /* If no suitable Database object was found, allocate a new one. */
    if( p==0 ){
      int nName = strlen(zName);
      p = (Database *)lsmMallocZeroRc(pEnv, sizeof(Database)+nId+nName+1, &rc);

      /* Allocate the mutex */
      if( rc==LSM_OK ) rc = lsmMutexNew(pEnv, &p->pClientMutex);

      /* If no error has occurred, fill in other fields and link the new 
      ** Database structure into the global list starting at 
      ** gShared.pDatabase. Otherwise, if an error has occurred, free any
      ** resources allocated and return without linking anything new into
      ** the gShared.pDatabase list.  */
      if( rc==LSM_OK ){
        p->zName = (char *)&p[1];
        memcpy((void *)p->zName, zName, nName+1);
        p->pId = (void *)&p->zName[nName+1];
        memcpy(p->pId, pId, nId);
        p->nId = nId;
        p->pDbNext = gShared.pDatabase;
        gShared.pDatabase = p;
      }else{
        freeDatabase(pEnv, p);
        p = 0;
      }
    }

    if( p ) p->nDbRef++;
    leaveGlobalMutex(pEnv);

    if( p ){
      lsmMutexEnter(pDb->pEnv, p->pClientMutex);
      pDb->pNext = p->pConn;
      p->pConn = pDb;
      lsmMutexLeave(pDb->pEnv, p->pClientMutex);
    }
  }

  lsmFree(pEnv, pId);
  pDb->pDatabase = p;
  return rc;
}

/*
** Release a reference to a Database object obtained from lsmDbDatabaseFind().
** There should be exactly one call to this function for each successful
** call to Find().
*/
void lsmDbDatabaseRelease(lsm_db *pDb){
  Database *p = pDb->pDatabase;
  if( p ){
    lsm_db **ppDb;

    lsmMutexEnter(pDb->pEnv, p->pClientMutex);
    for(ppDb=&p->pConn; *ppDb!=pDb; ppDb=&((*ppDb)->pNext));
    *ppDb = pDb->pNext;
    lsmMutexLeave(pDb->pEnv, p->pClientMutex);

    enterGlobalMutex(pDb->pEnv);
    p->nDbRef--;
    if( p->nDbRef==0 ){
      int rc = LSM_OK;
      int i;
      Database **pp;

      /* Remove the Database structure from the linked list. */
      for(pp=&gShared.pDatabase; *pp!=p; pp=&((*pp)->pDbNext));
      *pp = p->pDbNext;

      if( pDb->pShmhdr && pDb->pShmhdr->bInit ){
        /* Flush the in-memory tree, if required. If there is data to flush,
        ** this will create a new client snapshot in Database.pClient. The
        ** checkpoint (serialization) of this snapshot may be written to disk
        ** by the following block.  */
        if( 0==lsmTreeIsEmpty(pDb) ){
          rc = lsmFlushToDisk(pDb);
        }

        /* Write a checkpoint, also if required */
        if( rc==LSM_OK ){
          rc = lsmCheckpointWrite(pDb);
        }

        /* If the checkpoint was written successfully, delete the log file */
        if( rc==LSM_OK && pDb->pFS ){
          lsmFsCloseAndDeleteLog(pDb->pFS);
        }
      }

      for(i=0; i<p->nShmChunk; i++){
        lsmFree(pDb->pEnv, p->apShmChunk[i]);
      }
      lsmFree(pDb->pEnv, p->apShmChunk);
      
      /* Free the Database object */
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
** Get/set methods for the snapshot block-count. These should only be
** used with worker snapshots.
*/
void lsmSnapshotSetNBlock(Snapshot *pSnap, int nNew){
}

static void snapshotDecrRefcnt(lsm_env *pEnv, Snapshot *pSnap){
#if 0
  Database *p = pSnap->pDatabase;

  pSnap->nRef--;
  assert( pSnap->nRef>=0 );
  if( pSnap->nRef==0 ){
    Snapshot *pIter = p->pClient;
    assert( pSnap!=pIter );
    while( pIter->pSnapshotNext!=pSnap ) pIter = pIter->pSnapshotNext;
    pIter->pSnapshotNext = pSnap->pSnapshotNext;
    freeClientSnapshot(pEnv, pSnap);
  }
#endif
}

/*
** Release a snapshot reference obtained by calling lsmDbSnapshotWorker()
** or lsmDbSnapshotClient().
*/
void lsmDbSnapshotRelease(lsm_env *pEnv, Snapshot *pSnap){
  if( pSnap ){
    Database *p = pSnap->pDatabase;

    /* If this call is to release a pointer to the worker snapshot, relinquish
    ** the worker mutex.  
    **
    ** If pSnap is a client snapshot, decrement the reference count. When the
    ** reference count reaches zero, free the snapshot object. The decrement
    ** and (nRef==0) test are protected by the database client mutex.
    */
    lsmMutexEnter(pEnv, p->pClientMutex);
    snapshotDecrRefcnt(pEnv, pSnap);
    lsmMutexLeave(pEnv, p->pClientMutex);
  }
}

/*
** Create a new client snapshot based on the current contents of the worker 
** snapshot. The connection must be the worker to call this function.
*/
int lsmDbUpdateClient(lsm_db *pDb, int nLsmLevel, int bOvfl){
  assert( 0 );
  return 0;
#if 0
  Database *p = pDb->pDatabase;   /* Database handle */
  Snapshot *pOld;                 /* Old client snapshot object */
  Snapshot *pNew;                 /* New client snapshot object */
  int nByte;                      /* Memory required for new client snapshot */
  int rc = LSM_OK;                /* Memory required for new client snapshot */
  int nLevel = 0;                 /* Number of levels in worker snapshot */
  int nRight = 0;                 /* Total number of rhs in worker */
  int nKeySpace = 0;              /* Total size of split keys */
  Level *pLevel;                  /* Used to iterate through worker levels */
  Level **ppLink;                 /* Used to link levels together */
  u8 *pAvail;                     /* Used to divide up allocation */

  /* Must be the worker to call this. */
  assertMustbeWorker(pDb);

  /* Allocate space for the client snapshot and all levels. */
  for(pLevel=p->worker.pLevel; pLevel; pLevel=pLevel->pNext){
    nLevel++;
    nRight += pLevel->nRight;
  }
  nByte = sizeof(Snapshot) 
        + nLevel * sizeof(Level)
        + nRight * sizeof(Segment)
        + nKeySpace;
  pNew = (Snapshot *)lsmMallocZero(pDb->pEnv, nByte);
  if( !pNew ) return LSM_NOMEM_BKPT;
  pNew->pDatabase = p;
  pNew->iId = p->worker.iId;

  /* Copy the linked-list of Level structures */
  pAvail = (u8 *)&pNew[1];
  ppLink = &pNew->pLevel;
  for(pLevel=p->worker.pLevel; pLevel && rc==LSM_OK; pLevel=pLevel->pNext){
    Level *pNew;

    pNew = (Level *)pAvail;
    memcpy(pNew, pLevel, sizeof(Level));
    pAvail += sizeof(Level);

    if( pNew->nRight ){
      pNew->aRhs = (Segment *)pAvail;
      memcpy(pNew->aRhs, pLevel->aRhs, sizeof(Segment) * pNew->nRight);
      pAvail += (sizeof(Segment) * pNew->nRight);
      lsmSortedSplitkey(pDb, pNew, &rc);
    }

    /* This needs to come after any call to lsmSortedSplitkey(). Splitkey()
    ** uses data within the Merge object to set pNew->pSplitKey and co.  */
    pNew->pMerge = 0;

    *ppLink = pNew;
    ppLink = &pNew->pNext;
  }

  /* Create the serialized version of the new client snapshot. */
  if( p->bDirty && rc==LSM_OK ){
    assert( nLevel>nLsmLevel || p->worker.pLevel==0 );
    rc = lsmCheckpointExport(
        pDb, nLsmLevel, bOvfl, pNew->iId, 1, &pNew->pExport, &pNew->nExport
    );
  }

  if( rc==LSM_OK ){
    /* Initialize the new snapshot ref-count to 1 */
    pNew->nRef = 1;

    lsmDbSnapshotRelease(pDb->pEnv, pDb->pClient);

    /* Install the new client snapshot and release the old. */
    lsmMutexEnter(pDb->pEnv, p->pClientMutex);
    pOld = p->pClient;
    pNew->pSnapshotNext = pOld;
    p->pClient = pNew;
    if( pDb->pClient ){
      pDb->pClient = pNew;
      pNew->nRef++;
    }
    lsmMutexLeave(pDb->pEnv, p->pClientMutex);

    lsmDbSnapshotRelease(pDb->pEnv, pOld);
    p->bDirty = 0;

    /* Upgrade the user connection to the new client snapshot */

  }else{
    /* An error has occurred. Delete the allocated object. */
    freeClientSnapshot(pDb->pEnv, pNew);
  }

  return rc;
#endif
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

  assert( pDb->pWorker );
 
  pFree = &p->freelist;
  if( pFree->nEntry>0 ){
    /* The first block on the free list was freed as part of the work done
    ** to create the snapshot with id iFree. So, we can reuse this block if
    ** snapshot iFree or later has been checkpointed and all currently 
    ** active clients are reading from snapshot iFree or later.
    */
    Snapshot *pIter;
    i64 iFree = pFree->aEntry[0].iId;
    i64 iInUse;

    /* Both Database.iCheckpointId and the Database.pClient list are 
    ** protected by the client mutex. So grab it here before determining
    ** the id of the oldest snapshot still potentially in use.  */
#if 0
    lsmMutexEnter(pDb->pEnv, p->pClientMutex);
    for(pIter=p->pClient; pIter->pSnapshotNext; pIter=pIter->pSnapshotNext);
    iInUse = LSM_MIN(pIter->iId, p->iCheckpointId);
    lsmMutexLeave(pDb->pEnv, p->pClientMutex);

    if( 0 ){
      int i;
      printf("choose from freelist: ");
      for(i=0; i<pFree->nEntry && pFree->aEntry[i].iId<=iInUse; i++){
        printf("%d ", pFree->aEntry[i].iBlk);
      }
      printf("\n");
      fflush(stdout);
    }
#endif
    iInUse = p->iId-1;
    /* TODO: Fix the above */

    if( iFree<=iInUse ){
      iRet = pFree->aEntry[0].iBlk;
      flRemoveEntry0(pFree);
      assert( iRet!=0 );
      if( p->bRecordDelta ){
        p->nFreelistDelta++;
      }
    }
  }

  /* If no block was allocated from the free-list, allocate one at the
  ** end of the file. */
  if( iRet==0 ){
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

  assertMustbeWorker(pDb);
  assert( p->bRecordDelta==0 );
  return flAppendEntry(pDb->pEnv, &p->freelist, iBlk, p->iId);
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
    if( p->bRecordDelta ){ p->nFreelistDelta--; }
  }

  return rc;
}

void lsmFreelistDeltaBegin(lsm_db *pDb){
  assertMustbeWorker(pDb);
  assert( pDb->pWorker->bRecordDelta==0 );
  pDb->pWorker->nFreelistDelta = 0;
  pDb->pWorker->bRecordDelta = 1;
}

void lsmFreelistDeltaEnd(lsm_db *pDb){
  assertMustbeWorker(pDb);
  pDb->pWorker->bRecordDelta = 0;
}

int lsmFreelistDelta(lsm_db *pDb){
  return pDb->pWorker->nFreelistDelta;
}

/*
** Return the current contents of the free-list as a list of integers.
*/
int lsmSnapshotFreelist(lsm_db *pDb, int **paFree, int *pnFree){
  int rc = LSM_OK;                /* Return Code */
#if 0
  int *aFree = 0;                 /* Integer array to return via *paFree */
  int nFree;                      /* Value to return via *pnFree */
  Freelist *p;                    /* Database free list object */

  assert( pDb->pWorker );
  p = &pDb->pDatabase->freelist;
  nFree = p->nEntry;
  if( nFree && paFree ){
    aFree = lsmMallocRc(pDb->pEnv, sizeof(int) * nFree, &rc);
    if( aFree ){
      int i;
      for(i=0; i<nFree; i++){
        aFree[i] = p->aEntry[i].iBlk;
      }
    }
  }

  *pnFree = nFree;
  if( paFree ) *paFree = aFree;
#endif
  return rc;
}

int lsmGetFreelist(
  lsm_db *pDb,                    /* Database handle (must be worker) */
  u32 **paFree,                   /* OUT: malloc'd array */
  int *pnFree                     /* OUT: Size of array at *paFree */
){
  int rc = LSM_OK;                /* Return Code */
#if 0
  u32 *aFree = 0;                 /* Integer array to return via *paFree */
  int nFree;                      /* Value to return via *pnFree */
  Freelist *p;                    /* Database free list object */

  assert( pDb->pWorker );
  p = &pDb->pDatabase->freelist;
  nFree = p->nEntry * 3;
  if( nFree && paFree ){
    aFree = lsmMallocRc(pDb->pEnv, sizeof(u32) * nFree, &rc);
    if( aFree ){
      int i;
      for(i=0; i<p->nEntry; i++){
        aFree[i*3] = p->aEntry[i].iBlk;
        aFree[i*3+1] = (u32)((p->aEntry[i].iId >> 32) & 0xFFFFFFFF);
        aFree[i*3+2] = (u32)(p->aEntry[i].iId & 0xFFFFFFFF);
      }
    }
  }

  *pnFree = nFree;
  if( paFree ) *paFree = aFree;
#endif
  return rc;
}

int lsmSetFreelist(lsm_db *pDb, u32 *aElem, int nElem){
#if 0
  Database *p = pDb->pDatabase;
  lsm_env *pEnv = pDb->pEnv;
  int rc = LSM_OK;                /* Return code */
  int i;                          /* Iterator variable */
  Freelist *pFree;                /* Database free-list */

  assert( (nElem%3)==0 );

  pFree = &p->freelist;
  for(i=0; i<nElem; i+=3){
    i64 iId = ((i64)(aElem[i+1]) << 32) + aElem[i+2];
    rc = flAppendEntry(pEnv, pFree, aElem[i], iId);
  }

  return rc;
#endif
  return LSM_OK;
}

/*
** If required, store a new database checkpoint.
**
** The worker mutex must not be held when this is called. This is because
** this function may indirectly call fsync(). And the worker mutex should
** not be held that long (in case it is required by a client flushing an
** in-memory tree to disk).
*/
int lsmCheckpointWrite(lsm_db *pDb){
  int rc;                         /* Return Code */

  assert( pDb->pWorker==0 );
  assert( pDb->pClient==0 );
  rc = lsmShmLock(pDb, LSM_LOCK_CHECKPOINTER, LSM_LOCK_EXCL);
  if( rc!=LSM_OK ) return rc;

  rc = lsmCheckpointLoad(pDb);
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
        lsmFsMetaPageRelease(pPg);
      }
      bDone = (iDisk>=iCkpt);
    }

    if( rc==LSM_OK && bDone==0 ){
      int iMeta = (pShm->iMetaPage % 2) + 1;
      rc = lsmFsSyncDb(pDb->pFS);
      if( rc==LSM_OK ) rc = lsmCheckpointStore(pDb, iMeta);
      if( rc==LSM_OK ) rc = lsmFsSyncDb(pDb->pFS);
      if( rc==LSM_OK ) pShm->iMetaPage = iMeta;
    }
  }

  lsmShmLock(pDb, LSM_LOCK_CHECKPOINTER, LSM_LOCK_UNLOCK);
  return rc;
}

/*
** This function is called when a connection is about to run log file
** recovery (read the contents of the log file from disk and create a new
** in memory tree from it). This happens when the very first connection
** starts up and connects to the database.
**
** This sets the connections tree-version handle to one suitable to insert
** the read data into.
**
** Once recovery is complete (regardless of whether or not it is successful),
** lsmFinishRecovery() must be called to release resources locked by
** this function.
*/
int lsmBeginRecovery(lsm_db *pDb){
  int rc = LSM_OK;                /* Return code */
  Database *p = pDb->pDatabase;   /* Shared data handle */

  assert( 0 );

  assert( p );
  assert( pDb->pWorker );
  assert( pDb->pClient==0 );

#if 0
  if( rc==LSM_OK ){
    assert( pDb->pTV==0 );
    rc = lsmTreeWriteVersion(pDb->pEnv, p->pTree, &pDb->pTV);
  }
#endif
  return rc;
}

int lsmBeginWork(lsm_db *pDb){
  int rc;

  /* Attempt to take the WORKER lock */
  rc = lsmShmLock(pDb, LSM_LOCK_WORKER, LSM_LOCK_EXCL);

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

void lsmFinishWork(lsm_db *pDb, int *pRc){
  /* If no error has occurred, serialize the worker snapshot and write
  ** it to shared memory.  */
  if( *pRc==LSM_OK ){
    *pRc = lsmCheckpointSaveWorker(pDb);
  }

  if( pDb->pWorker ){
    lsmFreeSnapshot(pDb->pEnv, pDb->pWorker);
    pDb->pWorker = 0;
  }

  lsmShmLock(pDb, LSM_LOCK_WORKER, LSM_LOCK_UNLOCK);
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
    assert( pDb->pCsr==0 && pDb->nTransOpen==0 );

    /* Load the in-memory tree header. */
    rc = lsmTreeLoadHeader(pDb);

    /* Load the database snapshot */
    if( rc==LSM_OK ){
      rc = lsmCheckpointLoad(pDb);
    }

    /* Take a read-lock on the tree and snapshot just loaded. Then check
    ** that the shared-memory still contains the same values. If so, proceed.
    ** Otherwise, relinquish the read-lock and retry the whole procedure
    ** (starting with loading the in-memory tree header).  */
    if( rc==LSM_OK ){
      ShmHeader *pShm = pDb->pShmhdr;
      i64 iTree = pDb->treehdr.iTreeId;
      i64 iSnap = lsmCheckpointId(pDb->aSnapshot, 0);
      rc = lsmReadlock(pDb, iSnap, iTree);
      if( rc==LSM_OK ){
        if( (i64)pShm->hdr1.iTreeId==iTree 
         && lsmCheckpointId(pShm->aClient, 0)==iSnap
        ){
          /* Read lock has been successfully obtained. Deserialize the 
          ** checkpoint just loaded. TODO: This will be removed after 
          ** lsm_sorted.c is changed to work directly from the serialized
          ** version of the snapshot.  */
          rc = lsmCheckpointDeserialize(pDb, pDb->aSnapshot, &pDb->pClient);
          assert( (rc==LSM_OK)==(pDb->pClient!=0) );
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
    rc = lsmShmLock(pDb, LSM_LOCK_WRITER, LSM_LOCK_EXCL);
  }

  /* If the previous writer failed mid-transaction, run emergency rollback. */
  if( rc==LSM_OK && pShm->bWriter ){
    /* TODO: This! */
    assert( 0 );
    rc = LSM_CORRUPT_BKPT;
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
    pShm->bWriter = 1;
    pDb->treehdr.iTransId++;
  }else{
    lsmShmLock(pDb, LSM_LOCK_WRITER, LSM_LOCK_UNLOCK);
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
  lsmShmLock(pDb, LSM_LOCK_WRITER, LSM_LOCK_UNLOCK);
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

/*
** Obtain a read-lock on database version identified by the combination
** of snapshot iLsm and tree iTree. Return LSM_OK if successful, or
** an LSM error code otherwise.
*/
int lsmReadlock(lsm_db *db, i64 iLsm, i64 iTree){
  ShmHeader *pShm = db->pShmhdr;
  int i;
  int rc = LSM_OK;

  assert( db->iReader<0 );

  /* Search for an exact match. */
  for(i=0; db->iReader<0 && rc==LSM_OK && i<LSM_LOCK_NREADER; i++){
    ShmReader *p = &pShm->aReader[i];
    if( p->iLsmId==iLsm && p->iTreeId==iTree ){
      rc = lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_SHARED);
      if( rc==LSM_OK && p->iLsmId==iLsm && p->iTreeId==iTree ){
        db->iReader = i;
      }else if( rc==LSM_BUSY ){
        rc = LSM_OK;
      }
    }
  }

  /* Try to obtain a write-lock on each slot, in order. If successful, set
  ** the slot values to iLsm/iTree.  */
  for(i=0; db->iReader<0 && rc==LSM_OK && i<LSM_LOCK_NREADER; i++){
    rc = lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_EXCL);
    if( rc==LSM_BUSY ){
      rc = LSM_OK;
    }else{
      ShmReader *p = &pShm->aReader[i];
      p->iLsmId = iLsm;
      p->iTreeId = iTree;
      rc = lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_SHARED);
      if( rc==LSM_OK ) db->iReader = i;
    }
  }

  /* Search for any usable slot */
  for(i=0; db->iReader<0 && rc==LSM_OK && i<LSM_LOCK_NREADER; i++){
    ShmReader *p = &pShm->aReader[i];
    if( p->iLsmId && p->iTreeId && p->iLsmId<=iLsm && p->iTreeId<=iTree ){
      rc = lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_SHARED);
      if( rc==LSM_OK ){
        if( p->iLsmId && p->iTreeId && p->iLsmId<=iLsm && p->iTreeId<=iTree ){
          db->iReader = i;
        }
      }else if( rc==LSM_BUSY ){
        rc = LSM_OK;
      }
    }
  }

  return rc;
}

/*
**
*/
int lsmTreeInUse(lsm_db *db, u32 iTreeId, int *pbInUse){
  ShmHeader *pShm = db->pShmhdr;
  int i;
  int rc = LSM_OK;

  if( db->treehdr.iTreeId==iTreeId ) rc = LSM_BUSY;

  for(i=0; rc==LSM_OK && i<LSM_LOCK_NREADER; i++){
    ShmReader *p = &pShm->aReader[i];
    if( p->iLsmId && p->iTreeId && p->iTreeId<=iTreeId ){
      rc = lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_EXCL);
      if( rc==LSM_OK ){
        p->iTreeId = p->iLsmId = 0;
        lsmShmLock(db, LSM_LOCK_READER(i), LSM_LOCK_UNLOCK);
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

/*
** Release the read-lock currently held by connection db.
*/
int lsmReleaseReadlock(lsm_db *db){
  int rc = LSM_OK;
  if( db->iReader>=0 ){
    rc = lsmShmLock(db, LSM_LOCK_READER(db->iReader), LSM_LOCK_UNLOCK);
    db->iReader = -1;
  }
  return rc;
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

  /* Enter the client mutex */
  assert( iChunk>=0 );
  lsmMutexEnter(db->pEnv, p->pClientMutex);

  if( iChunk>=p->nShmChunk ){
    int nNew = iChunk+1;
    void **apNew;
    apNew = (void **)lsmRealloc(db->pEnv, p->apShmChunk, sizeof(void*) * nNew);
    if( apNew==0 ){
      rc = LSM_NOMEM_BKPT;
    }else{
      memset(&apNew[p->nShmChunk], 0, sizeof(void*) * (nNew-p->nShmChunk));
      p->apShmChunk = apNew;
      p->nShmChunk = nNew;
    }
  }

  if( rc==LSM_OK && p->apShmChunk[iChunk]==0 ){
    p->apShmChunk[iChunk] = lsmMallocZeroRc(db->pEnv, LSM_SHM_CHUNK_SIZE, &rc);
  }

  if( rc==LSM_OK ){
    pRet = p->apShmChunk[iChunk];
  }

  /* Release the client mutex */
  lsmMutexLeave(db->pEnv, p->pClientMutex);

  *ppData = pRet; 
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
  int eOp                         /* One of LSM_LOCK_UNLOCK, SHARED or EXCL */
){
  int rc = LSM_OK;
  Database *p = db->pDatabase;

  assert( iLock>=1 && iLock<=LSM_LOCK_READER(LSM_LOCK_NREADER-1) );
  assert( iLock<=16 );
  assert( eOp==LSM_LOCK_UNLOCK || eOp==LSM_LOCK_SHARED || eOp==LSM_LOCK_EXCL );

  lsmMutexEnter(db->pEnv, p->pClientMutex);
  if( eOp==LSM_LOCK_UNLOCK ){
    u32 mask = (1 << (iLock-1)) + (1 << (iLock-1+16));
    db->mLock &= ~mask;
  }else{
    lsm_db *pIter;
    u32 mask = (1 << (iLock-1));
    if( eOp==LSM_LOCK_EXCL ) mask |= (1 << (iLock-1+16));

    for(pIter=p->pConn; pIter; pIter=pIter->pNext){
      if( pIter!=db && pIter->mLock & mask ){ 
        rc = LSM_BUSY;
        break;
      }
    }

    if( rc==LSM_OK ){
      if( eOp==LSM_LOCK_EXCL ){
        db->mLock |= (1 << (iLock-1));
      }else{
        db->mLock |= (1 << (iLock-1+16));
        db->mLock &= ~(1 << (iLock-1));
      }
    }
  }
  lsmMutexLeave(db->pEnv, p->pClientMutex);

  return rc;
}

void lsmShmBarrier(lsm_db *db){
  /* TODO */
}




