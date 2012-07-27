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

typedef struct Freelist Freelist;
typedef struct AppendList AppendList;
typedef struct FreelistEntry FreelistEntry;

/*
** TODO: Find homes for these miscellaneous notes. 
**
** SNAPSHOT ID MANIPULATIONS
**
**   When the database is initialized the worker snapshot id is set to the
**   value read from the checkpoint. Or, if there is no valid checkpoint,
**   to a non-zero default value (e.g. 1).
**
**   The client snapshot is then initialized as a copy of the worker. The
**   client snapshot id is a copy of the worker snapshot id (as read from
**   the checkpoint). The worker snapshot id is then incremented.
**
*/

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
** An instance of the following structure stores the current database free
** block list. The free list is a list of blocks that are not currently
** used by the worker snapshot. Assocated with each block in the list is the
** snapshot id of the most recent snapshot that did actually use the block.
*/
struct Freelist {
  FreelistEntry *aEntry;          /* Free list entries */
  int nEntry;                     /* Number of valid slots in aEntry[] */
  int nAlloc;                     /* Allocated size of aEntry[] */
};
struct FreelistEntry {
  int iBlk;                       /* Block number */
  i64 iId;                        /* Largest snapshot id to use this block */
};

struct AppendList {
  Pgno *aPoint;
  int nPoint;
  int nAlloc;
};


/*
** Database structure. There is one such structure for each distinct 
** database accessed by this process. They are stored in the singly linked 
** list starting at global variable gShared.pDatabase. Database objects are 
** reference counted. Once the number of connections to the associated
** database drops to zero, they are removed from the linked list and deleted.
**
** The primary purpose of the Database structure is to manage Snapshots. A
** snapshot contains the information required to read a database - exactly
** where each array is stored, and where new arrays can be written. A 
** database has one worker snapshot and any number of client snapshots.
**
** WORKER SNAPSHOT
**
**   When a connection is first made to a database and the Database object
**   created, the worker snapshot is initialized to the most recently 
**   checkpointed database state (based on the values in the db header).
**   Any time the database file is written to, either to flush the contents
**   of an in-memory tree or to merge existing segments, the worker snapshot
**   is updated to reflect the modifications.
**
**   The worker snapshot is protected by the worker mutex. The worker mutex
**   must be obtained before a connection begins to modify the database
**   file. After the db file is written, the worker snapshot is updated and
**   the worker mutex released.
**
** CLIENT SNAPSHOTS
**
**   Client snapshots are used by database clients (readers). When a 
**   transaction is opened, the client requests a pointer to a read-only 
**   client snapshot. It is relinquished when the transaction ends. Client 
**   snapshots are reference counted objects.
**
**   When a database is first loaded, the client snapshot is a copy of
**   the worker snapshot. Each time the worker snapshot is checkpointed,
**   the client snapshot is updated with the new checkpointed contents.
**
** THE FREE-BLOCK LIST
**
**   Each Database structure maintains a list of free blocks - the "free-list".
**   There is an entry in the free-list for each block in the database file 
**   that is not used in any way by the worker snapshot.
**
**   Associated with each free block in the free-list is a snapshot id.
**   This is the id of the earliest snapshot that does not require the
**   contents of the block. The block may therefore be reused only after:
**
**     (a) a snapshot with an id equal to or greater than the id associated
**         with the block has been checkpointed into the db header, and
**
**     (b) all existing database clients are using a snapshot with an id
**         equal to or greater than the id stored in the free-list entry.
**
** MULTI-THREADING ISSUES
**
**   Each Database structure carries with it two mutexes - the client 
**   mutex and the worker mutex. In a multi-process version of LSM, these 
**   will be replaced by some other robust locking mechanism. 
**
**   TODO - this description.
*/
struct Database {
  char *zName;                    /* Canonical path to database file */
  void *pId;                      /* Database id (file inode) */
  int nId;                        /* Size of pId in bytes */

  DbLog log;                      /* Database log state object */
  int nPgsz;                      /* Nominal database page size */
  int nBlksz;                     /* Database block size */

  Snapshot *pClient;              /* Client (reader) snapshot */
  Snapshot worker;                /* Worker (writer) snapshot */
  AppendList append;              /* List of appendable points */

  int nBlock;                     /* Number of blocks tracked by this ss */
  Freelist freelist;              /* Database free-list */

  int nFreelistDelta;
  int bRecordDelta;               /* True when recording freelist delta */

  lsm_mutex *pWorkerMutex;        /* Protects the worker snapshot */
  lsm_mutex *pClientMutex;        /* Protects pClient */
  int bDirty;                     /* True if worker has been modified */
  int bRecovered;                 /* True if db does not require recovery */

  int bCheckpointer;              /* True if there exists a checkpointer */
  int bWriter;                    /* True if there exists a writer */
  i64 iCheckpointId;              /* Largest snapshot id stored in db file */
  int iSlot;                      /* Meta page containing iCheckpointId */

  int nShmChunk;
  void **apShmChunk;

  /* Protected by the global mutex (enterGlobalMutex/leaveGlobalMutex): */
  int nDbRef;                     /* Number of associated lsm_db handles */
  Database *pDbNext;              /* Next Database structure in global list */

  /* List of connections. Protected by client mutex. */
  lsm_db *pConn;
};

/*
** Macro that evaluates to true if the snapshot passed as the only argument
** is a worker snapshot. 
*/
#define isWorker(pSnap) ((pSnap)==(&(pSnap)->pDatabase->worker))

/*
** Functions to enter and leave the global mutex. This mutex is used
** to protect the global linked-list headed at 
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
static void assertSnapshotListOk(Database *p){
  Snapshot *pIter;
  i64 iPrev = 0;

  for(pIter=p->pClient; pIter; pIter=pIter->pSnapshotNext){
    assert( pIter==p->pClient || pIter->iId<iPrev );
    iPrev = pIter->iId;
  }
}
#else
# define assertNotInFreelist(x,y)
# define assertMustbeWorker(x)
# define assertSnapshotListOk(x)
#endif


Pgno *lsmSharedAppendList(lsm_db *db, int *pnApp){
  Database *p = db->pDatabase;
  assert( db->pWorker );
  *pnApp = p->append.nPoint;
  return p->append.aPoint;
}

int lsmSharedAppendListAdd(lsm_db *db, Pgno iPg){
  AppendList *pList;
  assert( db->pWorker );
  pList = &db->pDatabase->append;

  assert( pList->nAlloc>=pList->nPoint );
  if( pList->nAlloc<=pList->nPoint ){
    int nNew = pList->nAlloc+8;
    Pgno *aNew = (Pgno *)lsmRealloc(db->pEnv, pList->aPoint, sizeof(Pgno)*nNew);
    if( aNew==0 ) return LSM_NOMEM_BKPT;
    pList->aPoint = aNew;
    pList->nAlloc = nNew;
  }

  pList->aPoint[pList->nPoint++] = iPg;
  return LSM_OK;
}

void lsmSharedAppendListRemove(lsm_db *db, int iIdx){
  AppendList *pList;
  int i;
  assert( db->pWorker );
  pList = &db->pDatabase->append;

  assert( pList->nPoint>iIdx );
  for(i=iIdx+1; i<pList->nPoint;i++){
    pList->aPoint[i-1] = pList->aPoint[i];
  }
  pList->nPoint--;
}

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
  if( p ){
    /* Free the mutexes */
    lsmMutexDel(pEnv, p->pClientMutex);
    lsmMutexDel(pEnv, p->pWorkerMutex);

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

      /* Initialize the log handle */
      if( rc==LSM_OK ){
        p->log.cksum0 = LSM_CKSUM0_INIT;
        p->log.cksum1 = LSM_CKSUM1_INIT;
      }

      /* Allocate the two mutexes */
      if( rc==LSM_OK ) rc = lsmMutexNew(pEnv, &p->pWorkerMutex);
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
        p->worker.pDatabase = p;
        p->pDbNext = gShared.pDatabase;
        gShared.pDatabase = p;

        p->worker.iId = LSM_INITIAL_SNAPSHOT_ID;
        p->nPgsz = pDb->nDfltPgsz;
        p->nBlksz = pDb->nDfltBlksz;
      }else{
        freeDatabase(pEnv, p);
        p = 0;
      }
    }

    if( p ) p->nDbRef++;
    leaveGlobalMutex(pEnv);

    lsmMutexEnter(pDb->pEnv, p->pClientMutex);
    pDb->pNext = p->pConn;
    p->pConn = pDb;
    lsmMutexLeave(pDb->pEnv, p->pClientMutex);
  }

  lsmFree(pEnv, pId);
  pDb->pDatabase = p;
  return rc;
}

static void freeClientSnapshot(lsm_env *pEnv, Snapshot *p){
  Level *pLevel;
  
  assert( p->nRef==0 );
  for(pLevel=p->pLevel; pLevel; pLevel=pLevel->pNext){
    lsmFree(pEnv, pLevel->pSplitKey);
  }
  lsmFree(pEnv, p->pExport);
  lsmFree(pEnv, p);
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
      Database **pp;

      /* Remove the Database structure from the linked list. */
      for(pp=&gShared.pDatabase; *pp!=p; pp=&((*pp)->pDbNext));
      *pp = p->pDbNext;

      /* Flush the in-memory tree, if required. If there is data to flush,
      ** this will create a new client snapshot in Database.pClient. The
      ** checkpoint (serialization) of this snapshot may be written to disk
      ** by the following block.  */
      if( p->bDirty || 0==lsmTreeIsEmpty(pDb) ){
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

      /* Free the contents of the worker snapshot */
      lsmFree(pDb->pEnv, p->freelist.aEntry);
      lsmFree(pDb->pEnv, p->append.aPoint);
      
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
  pSnap->pDatabase->nBlock = nNew;
}

void lsmSnapshotSetCkptid(Snapshot *pSnap, i64 iNew){
  assert( isWorker(pSnap) );
  pSnap->iId = iNew;
}

/*
** Return a pointer to the client snapshot object. Each successful call 
** to lsmDbSnapshotClient() must be matched by an lsmDbSnapshotRelease() 
** call.
*/
#if 0
Snapshot *lsmDbSnapshotClient(lsm_db *pDb){
  Database *p = pDb->pDatabase;
  Snapshot *pRet;
  lsmMutexEnter(pDb->pEnv, p->pClientMutex);
  pRet = p->pClient;
  pRet->nRef++;
  lsmMutexLeave(pDb->pEnv, p->pClientMutex);
  return pRet;
}
#endif

/*
** Return a pointer to the worker snapshot. This call grabs the worker 
** mutex. It is released when the pointer to the worker snapshot is passed 
** to lsmDbSnapshotRelease().
*/
Snapshot *lsmDbSnapshotWorker(lsm_db *pDb){
  Database *p = pDb->pDatabase;
  lsmMutexEnter(pDb->pEnv, p->pWorkerMutex);
  return &p->worker;
}

Snapshot *lsmDbSnapshotRecover(lsm_db *pDb){
  Database *p = pDb->pDatabase;
  Snapshot *pRet = 0;
  lsmMutexEnter(pDb->pEnv, p->pWorkerMutex);
  if( p->bRecovered ){
    lsmFsSetPageSize(pDb->pFS, p->nPgsz);
    lsmFsSetBlockSize(pDb->pFS, p->nBlksz);
    lsmMutexLeave(pDb->pEnv, p->pWorkerMutex);
  }else{
    pRet = &p->worker;
  }
  return pRet;
}

/*
** Set (bVal==1) or clear (bVal==0) the "recovery done" flag.
**
** TODO: Should this be combined with BeginRecovery()/FinishRecovery()?
*/
void lsmDbRecoveryComplete(lsm_db *pDb, int iSlot){
  Database *p = pDb->pDatabase;

  assert( iSlot==0 || iSlot==1 || iSlot==2 );
  assert( lsmMutexHeld(pDb->pEnv, p->pWorkerMutex) );

  p->bRecovered = 1;
  p->iCheckpointId = p->worker.iId;
  p->iSlot = iSlot;
  lsmFsSetPageSize(pDb->pFS, p->nPgsz);
  lsmFsSetBlockSize(pDb->pFS, p->nBlksz);
}

void lsmDbSetPagesize(lsm_db *pDb, int nPgsz, int nBlksz){
  Database *p = pDb->pDatabase;
  assert( lsmMutexHeld(pDb->pEnv, p->pWorkerMutex) && p->bRecovered==0 );
  p->nPgsz = nPgsz;
  p->nBlksz = nBlksz;
  lsmFsSetPageSize(pDb->pFS, p->nPgsz);
  lsmFsSetBlockSize(pDb->pFS, p->nBlksz);
}

static void snapshotDecrRefcnt(lsm_env *pEnv, Snapshot *pSnap){
  Database *p = pSnap->pDatabase;

  assertSnapshotListOk(p);
  pSnap->nRef--;
  assert( pSnap->nRef>=0 );
  if( pSnap->nRef==0 ){
    Snapshot *pIter = p->pClient;
    assert( pSnap!=pIter );
    while( pIter->pSnapshotNext!=pSnap ) pIter = pIter->pSnapshotNext;
    pIter->pSnapshotNext = pSnap->pSnapshotNext;
    freeClientSnapshot(pEnv, pSnap);
    assertSnapshotListOk(p);
  }
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
    if( isWorker(pSnap) ){
      lsmMutexLeave(pEnv, p->pWorkerMutex);
    }else{
      lsmMutexEnter(pEnv, p->pClientMutex);
      snapshotDecrRefcnt(pEnv, pSnap);
      lsmMutexLeave(pEnv, p->pClientMutex);
    }
  }
}

/*
** Create a new client snapshot based on the current contents of the worker 
** snapshot. The connection must be the worker to call this function.
*/
int lsmDbUpdateClient(lsm_db *pDb, int nLsmLevel, int bOvfl){
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
    assertSnapshotListOk(p);
    pOld = p->pClient;
    pNew->pSnapshotNext = pOld;
    p->pClient = pNew;
    assertSnapshotListOk(p);
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
  Database *p = pDb->pDatabase;
  Freelist *pFree;                /* Database free list */
  int iRet = 0;                   /* Block number of allocated block */

  assert( pDb->pWorker );
 
#if 0
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
    lsmMutexEnter(pDb->pEnv, p->pClientMutex);
    assertSnapshotListOk(p);
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


    if( iFree<=iInUse ){
      iRet = pFree->aEntry[0].iBlk;
      flRemoveEntry0(pFree);
      assert( iRet!=0 );
      if( p->bRecordDelta ){
        p->nFreelistDelta++;
      }
    }
  }
#endif

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
  Database *p = pDb->pDatabase;
  Snapshot *pWorker = pDb->pWorker;
  int rc = LSM_OK;

  assertMustbeWorker(pDb);
  assert( p->bRecordDelta==0 );

  rc = flAppendEntry(pDb->pEnv, &p->freelist, iBlk, pWorker->iId);
  return rc;
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
  Database *p = pDb->pDatabase;

  if( iBlk==p->nBlock ){
    p->nBlock--;
  }else{
    rc = flInsertEntry(pDb->pEnv, &p->freelist, iBlk);
    if( p->bRecordDelta ){
      p->nFreelistDelta--;
    }
  }

  return rc;
}

void lsmFreelistDeltaBegin(lsm_db *pDb){
  Database *p = pDb->pDatabase;
  assertMustbeWorker(pDb);
  assert( p->bRecordDelta==0 );
  p->nFreelistDelta = 0;
  p->bRecordDelta = 1;
}

void lsmFreelistDeltaEnd(lsm_db *pDb){
  Database *p = pDb->pDatabase;
  assertMustbeWorker(pDb);
  p->bRecordDelta = 0;
}

int lsmFreelistDelta(lsm_db *pDb){
  return pDb->pDatabase->nFreelistDelta;
}

/*
** Return the current contents of the free-list as a list of integers.
*/
int lsmSnapshotFreelist(lsm_db *pDb, int **paFree, int *pnFree){
  int rc = LSM_OK;                /* Return Code */
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
  return rc;
}

int lsmGetFreelist(
  lsm_db *pDb,                    /* Database handle (must be worker) */
  u32 **paFree,                   /* OUT: malloc'd array */
  int *pnFree                     /* OUT: Size of array at *paFree */
){
  int rc = LSM_OK;                /* Return Code */
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
  return rc;
}

int lsmSetFreelist(lsm_db *pDb, u32 *aElem, int nElem){
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
  Snapshot *pSnap;                /* Snapshot to checkpoint */
  Database *p = pDb->pDatabase;
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
  assert( lsmMutexHeld(pDb->pEnv, pDb->pDatabase->pWorkerMutex) );

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

void lsmFinishWork(lsm_db *pDb, int *pRc){
  /* If no error has occurred, serialize the worker snapshot and write
  ** it to shared memory.  */
  if( *pRc==LSM_OK ){
    *pRc = lsmCheckpointSaveWorker(pDb);
  }

  if( pDb->pWorker ){
    lsmSortedFreeLevel(pDb->pEnv, pDb->pWorker->pLevel);
    lsmFree(pDb->pEnv, pDb->pWorker);
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
  int rc = LSM_OK;                /* Return code */

  /* No reason a worker connection should be opening a read-transaction. */
  assert( pDb->pWorker==0 );

  if( pDb->pClient==0 ){
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
      /* TODO */
    }

    /* Deserialize the checkpoint just loaded. TODO: This will be removed
    ** after lsm_sorted.c is changed to work directly from the serialized
    ** version of the snapshot.  */
    if( rc==LSM_OK ){
      rc = lsmCheckpointDeserialize(pDb, pDb->aSnapshot, &pDb->pClient);
    }
    assert( (rc==LSM_OK)==(pDb->pClient!=0) );
  }

  return rc;
}

/*
** Close the currently open read transaction.
*/
void lsmFinishReadTrans(lsm_db *pDb){
  Snapshot *pClient = pDb->pClient;

  /* Worker connections should not be closing read transactions. And
  ** read transactions should only be closed after all cursors and write
  ** transactions have been closed.  */
  assert( pDb->pWorker==0 );
  assert( pDb->pCsr==0 && pDb->nTransOpen==0 );

  if( pClient ){
    freeClientSnapshot(pDb->pEnv, pClient);
    pDb->pClient = 0;
  }
}

/*
** Open a write transaction.
*/
int lsmBeginWriteTrans(lsm_db *pDb){
  int rc;                         /* Return code */
  ShmHeader *pShm = pDb->pShmhdr; /* Shared memory header */
  Database *p = pDb->pDatabase;   /* Shared database object */

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

  /* If everything was successful, set the "transaction-in-progress" flag
  ** and return LSM_OK. Otherwise, if some error occurred, relinquish the 
  ** WRITER lock and return an error code.  */
  if( rc==LSM_OK ){
    pShm->bWriter = 1;
    pDb->treehdr.iTransId++;
  }else{
    lsmShmLock(pDb, LSM_LOCK_WRITER, LSM_LOCK_UNLOCK);
  }
  return rc;

#if 0


  lsmMutexEnter(pDb->pEnv, p->pClientMutex);

  /* There are two reasons the attempt to open a write transaction may fail:
  **
  **   1. There is already a writer.
  **   2. Connection pDb already has an open read transaction, and the read
  **      snapshot is not the most recent version of the database.
  **
  ** If condition 1 is true, then the Database.bWriter flag is set. If the
  ** second is true, then the call to lsmTreeWriteVersion() returns NULL.
  */
  if( p->bWriter ){
    rc = LSM_BUSY;
  }else{
    rc = lsmTreeBeginTransaction(pDb);
  }

  if( rc==LSM_OK ){
    rc = lsmLogBegin(pDb, &p->log);

    if( rc!=LSM_OK ){
      /* If the call to lsmLogBegin() failed, relinquish the read/write
      ** TreeVersion handle obtained above. The attempt to open a transaction
      ** has failed.  */
#if 0
      TreeVersion *pWrite = pDb->pTV;
      TreeVersion **ppRestore = (pDb->pClient ? &pDb->pTV : 0);
      pDb->pTV = 0;
      lsmTreeReleaseWriteVersion(pDb->pEnv, pWrite, 0, ppRestore);
#endif
    }else if( pDb->pClient==0 ){
      /* Otherwise, if the lsmLogBegin() attempt was successful and the 
      ** client did not have a read transaction open when this function
      ** was called, lsm_db.pClient will still be NULL. In this case, grab 
      ** a reference to the lastest checkpointed snapshot now.  */
      p->pClient->nRef++;
      pDb->pClient = p->pClient;
    }
  }

  if( rc==LSM_OK ){
    p->bWriter = 1;
  }
  lsmMutexLeave(pDb->pEnv, p->pClientMutex);
  return rc;
#endif
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

  lsmLogEnd(pDb, &pDb->treehdr.log, bCommit);
  lsmTreeEndTransaction(pDb, bCommit);
  lsmShmLock(pDb, LSM_LOCK_WRITER, LSM_LOCK_UNLOCK);
  return LSM_OK;
}


/*
** This function is called at the beginning of a flush operation (i.e. when
** flushing the contents of the in-memory tree to a segment on disk).
**
** The caller must already be the worker connection.
**
** Also, the caller must have an open write transaction or be in the process
** of shutting down the (shared) database connection. This means we don't
** have to worry about any other connection modifying the in-memory tree
** structure while it is being flushed (although some other clients may be
** reading from it).
*/
int lsmBeginFlush(lsm_db *pDb){

  assert( pDb->pWorker );

  /* TODO */
#if 0
  if( pDb->pTV==0 ){
    pDb->pTV = lsmTreeRecoverVersion(pDb->pDatabase->pTree);
  }
#endif
  return LSM_OK;
}

/*
** This is called to indicate that a "flush-tree" operation has finished.
** If the second argument is true, the contents of the current in-memory
** tree are discarded and a new tree allocated to hold subsequent writes.
*/
int lsmFinishFlush(lsm_db *pDb, int bEmpty){
  Database *p = pDb->pDatabase;
  int rc = LSM_OK;
#if 0

  assert( pDb->pWorker );
  assert( pDb->pTV && (p->nDbRef==0 || lsmTreeIsWriteVersion(pDb->pTV)) );
  lsmMutexEnter(pDb->pEnv, p->pClientMutex);

  if( bEmpty ){
    if( p->bWriter ){
      lsmTreeReleaseWriteVersion(pDb->pEnv, pDb->pTV, 1, 0);
    }
    pDb->pTV = 0;
    lsmTreeRelease(pDb->pEnv, p->pTree);

    if( p->nDbRef>0 ){
      rc = lsmTreeNew(pDb->pEnv, pDb->xCmp, &p->pTree);
    }else{
      /* This is the case if the Database object is being deleted */
      p->pTree = 0;
    }
  }

  if( p->bWriter ){
    assert( pDb->pClient );
    if( 0==pDb->pTV ) rc = lsmTreeWriteVersion(pDb->pEnv, p->pTree, &pDb->pTV);
  }else{
    pDb->pTV = 0;
  }
  lsmMutexLeave(pDb->pEnv, p->pClientMutex);
#endif
  return rc;
}

/*
** Return a pointer to the DbLog object associated with connection pDb.
** Allocate and initialize it if necessary.
*/
DbLog *lsmDatabaseLog(lsm_db *pDb){
  Database *p = pDb->pDatabase;
  return &p->log;
}

/*
** Return non-zero if the caller is holding the client mutex.
*/
#ifdef LSM_DEBUG
int lsmHoldingClientMutex(lsm_db *pDb){
  return lsmMutexHeld(pDb->pEnv, pDb->pDatabase->pClientMutex);
}
#endif


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
      if( pIter->mLock & mask ){ 
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




