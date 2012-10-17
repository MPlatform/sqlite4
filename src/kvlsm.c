/*
** 2012 January 20
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
** An in-memory key/value storage subsystem that presents the interfadce
** defined by storage.h
*/
#include "sqliteInt.h"
#include "lsm.h"

typedef struct KVLsm KVLsm;
typedef struct KVLsmCsr KVLsmCsr;

struct KVLsm {
  KVStore base;                   /* Base class, must be first */
  lsm_db *pDb;                    /* LSM database handle */
  lsm_cursor *pCsr;               /* LSM cursor holding read-trans open */
};

struct KVLsmCsr {
  KVCursor base;                  /* Base class. Must be first */
  lsm_cursor *pCsr;               /* LSM cursor handle */
};
  
/*
** Begin a transaction or subtransaction.
**
** If iLevel==1 then begin an outermost read transaction.
**
** If iLevel==2 then begin an outermost write transaction.
**
** If iLevel>2 then begin a nested write transaction.
**
** iLevel may not be less than 1.  After this routine returns successfully
** the transaction level will be equal to iLevel.  The transaction level
** must be at least 1 to read and at least 2 to write.
*/
static int kvlsmBegin(KVStore *pKVStore, int iLevel){
  int rc = SQLITE4_OK;
  KVLsm *p = (KVLsm *)pKVStore;

  assert( iLevel>0 );
  if( p->pCsr==0 ){
    rc = lsm_csr_open(p->pDb, &p->pCsr);
  }
  if( rc==SQLITE4_OK && iLevel>=2 && iLevel>=pKVStore->iTransLevel ){
    rc = lsm_begin(p->pDb, iLevel-1);
  }

  if( rc==SQLITE4_OK ){
    pKVStore->iTransLevel = SQLITE4_MAX(iLevel, pKVStore->iTransLevel);
  }else if( pKVStore->iTransLevel==0 ){
    lsm_csr_close(p->pCsr);
    p->pCsr = 0;
  }

  return rc;
}

/*
** Commit a transaction or subtransaction.
**
** Make permanent all changes back through the most recent xBegin 
** with the iLevel+1.  If iLevel==0 then make all changes permanent.
** The argument iLevel will always be less than the current transaction
** level when this routine is called.
**
** Commit is divided into two phases.  A rollback is still possible after
** phase one completes.  In this implementation, phase one is a no-op since
** phase two cannot fail.
**
** After this routine returns successfully, the transaction level will be 
** equal to iLevel.
*/
static int kvlsmCommitPhaseOne(KVStore *pKVStore, int iLevel){
  return SQLITE4_OK;
}
static int kvlsmCommitPhaseTwo(KVStore *pKVStore, int iLevel){
  int rc = SQLITE4_OK;
  KVLsm *p = (KVLsm *)pKVStore;

  if( pKVStore->iTransLevel>iLevel ){
    if( pKVStore->iTransLevel>=2 ){
      rc = lsm_commit(p->pDb, SQLITE4_MAX(0, iLevel-1));
    }
    if( iLevel==0 ){
      lsm_csr_close(p->pCsr);
      p->pCsr = 0;
    }
    if( rc==SQLITE4_OK ){
      pKVStore->iTransLevel = iLevel;
    }
  }
  return rc;
}

/*
** Rollback a transaction or subtransaction.
**
** Revert all uncommitted changes back through the most recent xBegin or 
** xCommit with the same iLevel.  If iLevel==0 then back out all uncommited
** changes.
**
** After this routine returns successfully, the transaction level will be
** equal to iLevel.
*/
static int kvlsmRollback(KVStore *pKVStore, int iLevel){
  int rc = SQLITE4_OK;
  KVLsm *p = (KVLsm *)pKVStore;

  if( pKVStore->iTransLevel>=iLevel ){
    if( pKVStore->iTransLevel>=2 ){
      rc = lsm_rollback(p->pDb, SQLITE4_MAX(0, iLevel-1));
    }
    if( iLevel==0 ){
      lsm_csr_close(p->pCsr);
      p->pCsr = 0;
    }
    if( rc==SQLITE4_OK ){
      pKVStore->iTransLevel = iLevel;
    }
  }
  return rc;
}

/*
** Revert a transaction back to what it was when it started.
*/
static int kvlsmRevert(KVStore *pKVStore, int iLevel){
  return SQLITE4_OK;
}

/*
** Implementation of the xReplace(X, aKey, nKey, aData, nData) method.
**
** Insert or replace the entry with the key aKey[0..nKey-1].  The data for
** the new entry is aData[0..nData-1].  Return SQLITE4_OK on success or an
** error code if the insert fails.
**
** The inputs aKey[] and aData[] are only valid until this routine
** returns.  If the storage engine needs to keep that information
** long-term, it will need to make its own copy of these values.
**
** A transaction will always be active when this routine is called.
*/
static int kvlsmReplace(
  KVStore *pKVStore,
  const KVByteArray *aKey, KVSize nKey,
  const KVByteArray *aData, KVSize nData
){
  KVLsm *p = (KVLsm *)pKVStore;
  return lsm_write(p->pDb, (void *)aKey, nKey, (void *)aData, nData);
}

/*
** Create a new cursor object.
*/
static int kvlsmOpenCursor(KVStore *pKVStore, KVCursor **ppKVCursor){
  int rc = SQLITE4_OK;
  KVLsm *p = (KVLsm *)pKVStore;
  KVLsmCsr *pCsr;

  pCsr = (KVLsmCsr *)sqlite4_malloc(pKVStore->pEnv, sizeof(KVLsmCsr));
  if( pCsr==0 ){
    rc = SQLITE4_NOMEM;
  }else{
    memset(pCsr, 0, sizeof(KVLsmCsr));
    rc = lsm_csr_open(p->pDb, &pCsr->pCsr);

    if( rc==SQLITE4_OK ){
      pCsr->base.pStore = pKVStore;
      pCsr->base.pStoreVfunc = pKVStore->pStoreVfunc;
    }else{
      sqlite4_free(pCsr->base.pEnv, pCsr);
      pCsr = 0;
    }
  }

  *ppKVCursor = (KVCursor*)pCsr;
  return rc;
}

/*
** Reset a cursor
*/
static int kvlsmReset(KVCursor *pKVCursor){
  return SQLITE4_OK;
}

/*
** Destroy a cursor object
*/
static int kvlsmCloseCursor(KVCursor *pKVCursor){
  KVLsmCsr *pCsr = (KVLsmCsr *)pKVCursor;
  lsm_csr_close(pCsr->pCsr);
  sqlite4_free(pCsr->base.pEnv, pCsr);
  return SQLITE4_OK;
}

/*
** Move a cursor to the next non-deleted node.
*/
static int kvlsmNextEntry(KVCursor *pKVCursor){
  int rc;
  KVLsmCsr *pCsr = (KVLsmCsr *)pKVCursor;

  if( lsm_csr_valid(pCsr->pCsr)==0 ) return SQLITE4_NOTFOUND;
  rc = lsm_csr_next(pCsr->pCsr);
  if( rc==LSM_OK && lsm_csr_valid(pCsr->pCsr)==0 ){
    rc = SQLITE4_NOTFOUND;
  }
  return rc;
}

/*
** Move a cursor to the previous non-deleted node.
*/
static int kvlsmPrevEntry(KVCursor *pKVCursor){
  int rc;
  KVLsmCsr *pCsr = (KVLsmCsr *)pKVCursor;

  if( lsm_csr_valid(pCsr->pCsr)==0 ) return SQLITE4_NOTFOUND;
  rc = lsm_csr_prev(pCsr->pCsr);
  if( rc==LSM_OK && lsm_csr_valid(pCsr->pCsr)==0 ){
    rc = SQLITE4_NOTFOUND;
  }
  return rc;
}

/*
** Seek a cursor.
*/
static int kvlsmSeek(
  KVCursor *pKVCursor, 
  const KVByteArray *aKey,
  KVSize nKey,
  int dir
){
  int rc;
  KVLsmCsr *pCsr = (KVLsmCsr *)pKVCursor;

  assert( dir==0 || dir==1 || dir==-1 || dir==-2 );
  assert( LSM_SEEK_EQ==0 && LSM_SEEK_GE==1 && LSM_SEEK_LE==-1 );
  assert( LSM_SEEK_LEFAST==-2 );

  rc = lsm_csr_seek(pCsr->pCsr, (void *)aKey, nKey, dir);
  if( rc==SQLITE4_OK ){
    if( lsm_csr_valid(pCsr->pCsr)==0 ){
      rc = SQLITE4_NOTFOUND;
    }else{
      const void *pDbKey;
      int nDbKey;

      rc = lsm_csr_key(pCsr->pCsr, &pDbKey, &nDbKey);
      if( rc==SQLITE4_OK && (nDbKey!=nKey || memcmp(pDbKey, aKey, nKey)) ){
        rc = SQLITE4_INEXACT;
      }
    }
  }

  return rc;
}

/*
** Delete the entry that the cursor is pointing to.
**
** Though the entry is "deleted", it still continues to exist as a
** phantom.  Subsequent xNext or xPrev calls will work, as will
** calls to xKey and xData, thought the result from xKey and xData
** are undefined.
*/
static int kvlsmDelete(KVCursor *pKVCursor){
  int rc;
  const void *pKey;
  int nKey;
  KVLsmCsr *pCsr = (KVLsmCsr *)pKVCursor;

  assert( lsm_csr_valid(pCsr->pCsr) );
  rc = lsm_csr_key(pCsr->pCsr, &pKey, &nKey);
  if( rc==SQLITE4_OK ){
    rc = lsm_delete(((KVLsm *)(pKVCursor->pStore))->pDb, pKey, nKey);
  }

  return SQLITE4_OK;
}

/*
** Return the key of the node the cursor is pointing to.
*/
static int kvlsmKey(
  KVCursor *pKVCursor,         /* The cursor whose key is desired */
  const KVByteArray **paKey,   /* Make this point to the key */
  KVSize *pN                   /* Make this point to the size of the key */
){
  KVLsmCsr *pCsr = (KVLsmCsr *)pKVCursor;

  assert( lsm_csr_valid(pCsr->pCsr) );
  return lsm_csr_key(pCsr->pCsr, (const void **)paKey, (int *)pN);
}

/*
** Return the data of the node the cursor is pointing to.
*/
static int kvlsmData(
  KVCursor *pKVCursor,         /* The cursor from which to take the data */
  KVSize ofst,                 /* Offset into the data to begin reading */
  KVSize n,                    /* Number of bytes requested */
  const KVByteArray **paData,  /* Pointer to the data written here */
  KVSize *pNData               /* Number of bytes delivered */
){
  KVLsmCsr *pCsr = (KVLsmCsr *)pKVCursor;
  int rc;
  void *pData;
  int nData;

  rc = lsm_csr_value(pCsr->pCsr, (const void **)&pData, &nData);
  if( rc==SQLITE4_OK ){
    if( n<0 ){
      *paData = pData;
      *pNData = nData;
    }else{
      int nOut = n;
      if( (ofst+n)>nData ) nOut = nData - ofst;
      if( nOut<0 ) nOut = 0;

      *paData = &((u8 *)pData)[n];
      *pNData = nOut;
    }
  }

  return rc;
}

/*
** Destructor for the entire in-memory storage tree.
*/
static int kvlsmClose(KVStore *pKVStore){
  KVLsm *p = (KVLsm *)pKVStore;

  /* If there is an active transaction, roll it back. The important
  ** part is that the read-transaction cursor is closed. Otherwise, the
  ** call to lsm_close() below will fail.  */
  kvlsmRollback(pKVStore, 0);
  assert( p->pCsr==0 );

  lsm_close(p->pDb);
  sqlite4_free(p->base.pEnv, p);
  return SQLITE4_OK;
}

static int kvlsmControl(KVStore *pKVStore, int op, void *pArg){
  int rc = SQLITE4_OK;
  KVLsm *p = (KVLsm *)pKVStore;

  switch( op ){
    case SQLITE4_KVCTRL_LSM_HANDLE: {
      lsm_db **ppOut = (lsm_db **)pArg;
      *ppOut = p->pDb;
      break;
    }

    case SQLITE4_KVCTRL_SYNCHRONOUS: {
      int *peSafety = (int *)pArg;
      int eParam = *peSafety + 1;
      lsm_config(p->pDb, LSM_CONFIG_SAFETY, &eParam);
      *peSafety = eParam-1;
      break;
    }

    case SQLITE4_KVCTRL_LSM_FLUSH: {
      lsm_work(p->pDb, LSM_WORK_FLUSH, 0, 0);
      break;
    }

    case SQLITE4_KVCTRL_LSM_MERGE: {
      int nPage = *(int*)pArg;
      int nWrite = 0;
      lsm_work(p->pDb, LSM_WORK_OPTIMIZE, nPage, &nWrite);
      *(int*)pArg = nWrite;
      break;
    }

    case SQLITE4_KVCTRL_LSM_CHECKPOINT: {
      lsm_checkpoint(p->pDb, 0);
      break;
    }


    default:
      rc = SQLITE4_NOTFOUND;
      break;
  }

  return rc;
}

/*
** Create a new in-memory storage engine and return a pointer to it.
*/
int sqlite4KVStoreOpenLsm(
  sqlite4_env *pEnv,          /* Run-time environment */
  KVStore **ppKVStore,        /* OUT: write the new KVStore here */
  const char *zName,          /* Name of the file to open */
  unsigned openFlags          /* Flags */
){

  /* Virtual methods for an LSM data store */
  static const KVStoreMethods kvlsmMethods = {
    1,                          /* iVersion */
    sizeof(KVStoreMethods),     /* szSelf */
    kvlsmReplace,               /* xReplace */
    kvlsmOpenCursor,            /* xOpenCursor */
    kvlsmSeek,                  /* xSeek */
    kvlsmNextEntry,             /* xNext */
    kvlsmPrevEntry,             /* xPrev */
    kvlsmDelete,                /* xDelete */
    kvlsmKey,                   /* xKey */
    kvlsmData,                  /* xData */
    kvlsmReset,                 /* xReset */
    kvlsmCloseCursor,           /* xCloseCursor */
    kvlsmBegin,                 /* xBegin */
    kvlsmCommitPhaseOne,        /* xCommitPhaseOne */
    kvlsmCommitPhaseTwo,        /* xCommitPhaseTwo */
    kvlsmRollback,              /* xRollback */
    kvlsmRevert,                /* xRevert */
    kvlsmClose,                 /* xClose */
    kvlsmControl                /* xControl */
  };

  KVLsm *pNew;
  int rc = SQLITE4_OK;

  pNew = (KVLsm *)sqlite4_malloc(pEnv, sizeof(KVLsm));
  if( pNew==0 ){
    rc = SQLITE4_NOMEM;
  }else{
    struct Config {
      const char *zParam;
      int eParam;
    } aConfig[] = {
      { "lsm_block_size", LSM_CONFIG_BLOCK_SIZE }
    };

    memset(pNew, 0, sizeof(KVLsm));
    pNew->base.pStoreVfunc = &kvlsmMethods;
    pNew->base.pEnv = pEnv;
    rc = lsm_new(0, &pNew->pDb);
    if( rc==SQLITE4_OK ){
      int i;
      int bMmap = 0;
      lsm_config(pNew->pDb, LSM_CONFIG_MMAP, &bMmap);
      for(i=0; i<ArraySize(aConfig); i++){
        const char *zVal = sqlite4_uri_parameter(zName, aConfig[i].zParam);
        if( zVal ){
          int nVal = sqlite4Atoi(zVal);
          lsm_config(pNew->pDb, aConfig[i].eParam, &nVal);
        }
      }

      rc = lsm_open(pNew->pDb, zName);
    }

    if( rc!=SQLITE4_OK ){
      lsm_close(pNew->pDb);
      sqlite4_free(pEnv, pNew);
      pNew = 0;
    }
  }

  *ppKVStore = (KVStore*)pNew;
  return rc;
}
