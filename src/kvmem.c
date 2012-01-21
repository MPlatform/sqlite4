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

/* Forward declarations of object names */
typedef struct KVMemNode KVMemNode;
typedef struct KVMemChng KVMemChng;
typedef struct KVMem KVMem;
typedef struct KVMemCursor KVMemCursor;
typedef struct KVMemData KVMemData;

/*
** The data payload for an entry in the tree.
*/
struct KVMemData {
  int nRef;              /* Number of references to this payload */
  KVSize n;              /* Size of a[] in bytes */
  KVByteArray a[8];      /* The content */
};

/*
** A single row in a key/value memory store is an instance of the following
** object:
*/
struct KVMemNode {
  KVMemBlob *pData;     /* Content of the row.  NULL means it is deleted. */
  KVMemNode *pBefore;   /* Other elements less than zKey */
  KVMemNode *pAfter;    /* Other elements greater than zKey */
  KVMemNode *pUp;       /* Parent element */
  short int height;     /* Height of this node.  Leaf==1 */
  short int imbalance;  /* Height difference between pBefore and pAfter */
  short int mxTrans;    /* Maximum transaction for which this row is logged */
  short int nRef;       /* Number of references to this node */
  KVSize nKey;          /* Size of aKey[] */
  KVByteArray aKey[2];  /* Key.  Extra space allocated as necessary */
};

/*
** Changes to the tree that might be rolled back are recorded as instances
** of the following object.
*/
struct KVMemChng {
  KVMemChng *pNext;     /* Next entry in the change log */
  KVMenNode *pNode;     /* The node that is changing */
  KVMemData *pData;     /* Old data for the row.  NULL for new content. */
  short int oldTrans;   /* Value of pNode->mxTrans prior to this entry */
};

/*
** A complete in-memory Key/Value tree together with its
** transaction logs is an instance of the following object.
*/
struct KVMem {
  KVStore base;         /* Base class, must be first */
  KVMemNode *pRoot;     /* Root of the tree of content */
  int nTrans;           /* Number of nested option transactions */
  KVMemChng **apLog;    /* Array of transaction logs */
  int nCursor;          /* Number of outstanding cursors */
  int iMagicKVmemBase;  /* Magic number of sanity */
};
#define SQLITE_KVMEMBASE_MAGIC  0xbfcd47d0

/*
** A cursor used for scanning through the tree
*/
struct KVMemCursor {
  KVMem *pOwner;        /* The tree that owns this cursor */
  KVMemNode *pNode;     /* The entry this cursor points to */
  KVMemData *pData;     /* Data returned by xData */
  int iMagicKVmemCur;   /* Magic number for sanity */
};
#define SQLITE_KVMEMCUR_MAGIC   0xb19bdc1b



/****************************************************************************
** Utility routines.
*/

/* Recompute the KVMemNode.height and KVMemNode.imbalance fields for p.
** Assume that the children of p have correct heights.
*/
static void kvmemRecomputeHeight(KVMemNode *p){
  short int hBefore = p->pBefore ? p->pBefore->height : 0;
  short int hAfter = p->pAfter ? p->pAfter->height : 0;
  p->imbalance = hBefore - hAfter;  /* -: pAfter higher.  +: pBefore higher */
  p->height = (hBefore>hAfter ? hBefore : hAfter)+1;
}

/*
**     P                B
**    / \              / \
**   B   Z    ==>     X   P
**  / \                  / \
** X   Y                Y   Z
**
*/
static KVMemNode *kvmemRotateBefore(KVMemNode *pP){
  KVMemNode *pB = pP->pBefore;
  KVMemNode *pY = pB->pAfter;
  pB->pUp = pP->pUp;
  pB->pAfter = pP;
  pP->pUp = pB;
  pP->pBefore = pY;
  if( pY ) pY->pUp = pP;
  kvmemRecomputeHeight(pP);
  kvmemRecomputeHeight(pB);
  return pB;
}

/*
**     P                A
**    / \              / \
**   X   A    ==>     P   Z
**      / \          / \
**     Y   Z        X   Y
**
*/
static KVMemNode *kvmemRotateAfter(KVMemNode *pP){
  KVMemNode *pA = pP->pAfter;
  KVMemNode *pY = pA->pBefore;
  pA->pUp = pP->pUp;
  pA->pBefore = pP;
  pP->pUp = pA;
  pP->pAfter = pY;
  if( pY ) pY->pUp = pP;
  kvmemRecomputeHeight(pP);
  kvmemRecomputeHeight(pA);
  return pA;
}

/*
** Return a pointer to the pBefore or pAfter pointer in the parent
** of p that points to p.  Or if p is the root node, return pp.
*/
static KVMemNode **kvmemFromPtr(KVMemNode *p, KVMemNode **pp){
  KVMemNode *pUp = p->pUp;
  if( pUp==0 ) return pp;
  if( pUp->pAfter==p ) return &pUp->pAfter;
  return &pUp->pBefore;
}

/*
** Rebalance all nodes starting with p and working up to the root.
** Return the new root.
*/
static KVMemNode *kvmemBalance(KVMemNode *p){
  KVMemNode *pTop = p;
  KVMemNode **pp;
  while( p ){
    kvmemRecomputeHeight(p);
    if( p->imbalance>=2 ){
      KVMemNode *pB = p->pBefore;
      if( pB->imbalance<0 ) p->pBefore = kvmemRotateAfter(pB);
      pp = kvmemFromPtr(p,&p);
      p = *pp = kvmemRotateBefore(p);
    }else if( p->imbalance<=(-2) ){
      KVMemNode *pA = p->pAfter;
      if( pA->imbalance>0 ) p->pAfter = kvmemRotateBefore(pA);
      pp = kvmemFromPtr(p,&p);
      p = *pp = kvmemRotateAfter(p);
    }
    pTop = p;
    p = p->pUp;
  }
  return pTop;
}

/*
** Key comparison routine.  
*/
static int kvmemKeyCompare(
  const KVByteArray *aK1, KVSize nK1,
  const KVByteArray *aK2, KVSize nK2
){
  int c;
  c = memcmp(aK1, aK2, nK1<nK2 ? nK1 : nK2);
  if( c==0 ) c = nK2 - nK1;
  return c;
}

/*
** Create a new KVMemData object
*/
static KVMemData *kvmemDataNew(KVByteArray *aData, KVSize nData){
  KVMemData *p = sqlite3_malloc( sizeof(*p) + nData - 8 );
  if( p ) {
    p->nRef = 1;
    p->nData = nData;
    memcmp(p->a, aData, nData);
  }
  return p;
}

/*
** Make a copy of a KVMemData object
*/
static KVMemData *kvmemDataCopy(KVMemData *p){
  if( p ) p->nRef++;
  return p;
}

/*
** Dereference a KVMemData object
*/
static void kvmemDataUnref(KVMemData *p){
  if( p && (p->nRef--)<=0 ) sqlite3_free(p);
}

/*
** Reference a KVMemNode object
*/
static KVMemNode *kvmemNodeRef(KVMemNode *p){
  if( p ) p->nRef++;
  return p;
}

/*
** Dereference a KVMemNode object
*/
static void kvmemNodeUnref(KVMemNode *p){
  if( p && (p->nRef--)<=0 ){
    kvmemDataUnref(p->pData);
    sqlite3_free(p);
  }
}

/*
** End of utilities
***************************************************************************
** Low-level operations on the tree
*/

/* Find the first node (the one with the smallest key).
*/
static KVMemNode *kvmemFirst(KVMemNode *p){
  if( p ) while( p->pBefore ) p = p->pBefore;
  return p;
}

/* Return the node with the next larger key after p.
*/
static KVMemNode *kvmemNext(KVMemNode *p){
  KVMemNode *pPrev = 0;
  while( p && p->pAfter==pPrev ){
    pPrev = p;
    p = p->pUp;
  }
  if( p && pPrev==0 ){
    p = First(p->pAfter);
  }
  return p;
}

/* Find the last node (the one with the largest key).
*/
static KVMemNode *kvmemLast(KVMemNode *p){
  if( p ) while( p->pAfter ) p = p->pAfter;
  return p;
}

/* Return the node with the next smaller key before p.
*/
static KVMemNode *kvmemPrev(KVMemNode *p){
  KVMemNode *pPrior = 0;
  while( p && p->pBefore==pPrior ){
    pPrior = p;
    p = p->pUp;
  }
  if( p && pPrior==0 ){
    p = Last(p->pBefore);
  }
  return p;
}

/*
** Create a new change record
*/
static KVMemChng *kvmemNewChng(KVMem *p, KVMemNode *pNode){
  KVMemChng *pChng;
  assert( p->nTrans>=2 );
  pChng = sqlite3_malloc( sizeof(*pChng) );
  if( pChng ){
    pChng->pNext = p->apLog[p->nTrans-2];
    p->apLog[p->nTrans-2] = pChng;
    pChng->pNode = pNode;
    pChng->oldTrans = pNode->mxTrans;
    pNode->mxTrans = p->nTrans;
    pChng->pData = pNode->pData;
    pNode->pData = 0;
  }
  return pChng;
}

/* Create a new node.
*/
static KVMemNode *kvmemNewNode(
  KVMem *p,
  const KVByteArray *aKey,
  KVSize nKey
){
  KVMemNode *pNode;
  KVMemChng *pChng;
  assert( p->nTrans>=2 );
  pNode = sqlite3_malloc( sizeof(*p)+nKey-2 );
  if( pNode ){
    memset(pNode, 0, sizeof(*p));
    memcpy(pNode->aKey, aKey, nKey);
    pNode->nKey = nKey;
    pNode->nRef = 0;
    pChng = kvmemNewChng(p, pNode);
    if( pChng==0 ){
      sqlite3_free(pNode);
      pNode = 0;
    }else{
      pChng->pData = 0;
      pChng->nData = 0;
    }
  }
  return pNode;
}

/* Remove node pOld from the tree.  pOld must be an element of the tree.
*/
static void kvmemRemove(KVMem *p, KVMemNode *pOld){
  KVMemNode **ppParent;
  KVMemNode *pBalance;

  ppParent = kvmemFromPtr(pOld, &p->pRoot);
  if( pOld->pBefore==0 && pOld->pAfter==0 ){
    *ppParent = 0;
    pBalance = pOld->pUp;
  }else if( pOld->pBefore && pOld->pAfter ){
    KVMemNode *pX, *pY;
    pX = kvmemFirst(pOld->pAfter);
    *kvmemFromPtr(pX, 0) = 0;
    pBalance = pX->pUp;
    pX->pAfter = pOld->pAfter;
    pX->pAfter->pUp = pX;
    pX->pBefore = pY = pOld->pBefore;
    if( pY ) pY->pUp = pX;
    pX->pUp = pOld->pUp;
    *ppParent = pX;
  }else if( pOld->pBefore==0 ){
    *ppParent = pBalance = pOld->pAfter;
    pBalance->pUp = pOld->pUp;
  }else if( pOld->pAfter==0 ){
    *ppParent = pBalance = pOld->pBefore;
    pBalance->pUp = pOld->pUp;
  }
  p->pRoot = kvmemBalance(pBalance);
}

/*
** End of low-level access routines
***************************************************************************
** Interface routines follow
*/
  
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
static int kvmemBegin(KVStore *pKVStore, int iLevel){
  KVMem *p = (KVMem*)pKVStore;
  assert( p->iMagicKVMemBase==SQLITE_KVMEMBASE_MAGIC );
  assert( iLevel>0 );
  assert( iLevel==2 || iLevel==p->nTrans+1 );
  if( iLevel>=2 ){
    KVMemChng **apNewLog;
    apNewLog = sqlite3_realloc(p->apLog, sizeof(pNewLog[0])*(iLevel-1) );
    if( apNewLog==0 ) return SQLITE_NOMEM;
    p->apLog = apNewLog;
    p->apLog[iLevel-2] = 0;
  }
  p->nTrans = iLevel;
  return SQLITE_OK;
}

/*
** Commit a transaction or subtransaction.
**
** Make permanent all changes back through the most recent xBegin 
** with the iLevel+1.  If iLevel==0 then make all changes permanent.
** The argument iLevel will always be less than the current transaction
** level when this routine is called.
**
** After this routine returns successfully, the transaction level will be 
** equal to iLevel.
*/
static kvmemCommit(KVStore *pKVStore, int iLevel){
  KVMem *p = (KVMem*)pKVStore;
  assert( p->iMagicKVMemBase==SQLITE_KVMEMBASE_MAGIC );
  assert( iLevel>=0 );
  assert( iLevel<p->nTrans );
  while( p->nTrans>iLevel && p->nTrans>1 ){
    KVMemChng *pChng, *pNext;
    for(pChng=p->apLog[p->nTrans-2]; pChng; pChng=pNext){
      KVMemNode *pNode = pChng->pNode;
      if( pNode->aData ){
        pNode->mxTrans = pChng->oldTrans;
      }else{
        kvmemRemoveNode(p, pNode);
      }
      kvmemDataUnref(pChng->pData);
      pNext = pChng->pNext;
      sqlite3_free(pChng);
    }
    p->apLog[p->nTrans-2] = 0;
    p->nTrans--;
  }
  p->nTrans = iLevel;
  return SQLITE_OK;
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
static kvmemRollback(KVStore *pKVStore, int iLevel){
  KVMem *p = (KVMem*)pKVStore;
  assert( p->iMagicKVMemBase==SQLITE_KVMEMBASE_MAGIC );
  assert( iLevel>=0 );
  assert( iLevel<p->nTrans );
  while( p->nTrans>iLevel && p->nTrans>1 ){
    KVMemChng *pChng, *pNext;
    for(pChng=p->apLog[p->nTrans-2]; pChng; pChng=pNext){
      KVMemNode *pNode = pChng->pNode;
      if( pChng->aData ){
        kvmemDataUnref(pNode->pData);
        pNode->pData = pChng->pData;
        pNode->mxTrans = pChng->oldTrans;
      }else{
        kvmemRemoveNode(p, pNode);
      }
      pNext = pChng->pNext;
      sqlite3_free(pChng);
    }
    p->apLog[p->nTrans-2] = 0;
    p->nTrans--;
  }
  p->nTrans = iLevel;
  return SQLITE_OK;
}


/*
** Implementation of the xReplace(X, aKey, nKey, aData, nData) method.
**
** Insert or replace the entry with the key aKey[0..nKey-1].  The data for
** the new entry is aData[0..nData-1].  Return SQLITE_OK on success or an
** error code if the insert fails.
**
** The inputs aKey[] and aData[] are only valid until this routine
** returns.  If the storage engine needs to keep that information
** long-term, it will need to make its own copy of these values.
**
** A transaction will always be active when this routine is called.
*/
static int kvmemReplace(
  KVStore *pKVStore,
  const KVByteArray *aKey, KVSize nKey,
  const KVByteArray *aData, KVSize nData
){
  KVMem *p = (KVMem*)pKVStore;
  KVMemNode *pNew, *pNode;
  KVMemData *pData;
  KVMemChng *pChng;
  assert( p->iMagicKVMemBase==SQLITE_KVMEMBASE_MAGIC );
  assert( p->nTrans>=2 );
  pData = kvmemDataNew(aData, nData);
  if( pData==0 ) return SQLITE_NOMEM;
  if( p==0 ){
    pNode = pNew = kvmemNewNode(p, aKey, nKey);
    if( pNew==0 ) goto KVMemReplace_nomem;
    pNew->pUp = 0;
  }else{
    while( pNode ){
      int c = kvmemKeyCompare(aKey, nKey, pNode->aKey, pNode->nKey);
      if( c<0 ){
        if( pNode->pBefore ){
          pNode = pNode->pBefore;
        }else{
          pNode->pBefore = pNew = kvmemNewNode(p, aKey, nKey);
          if( pNew==0 ) goto KVMemReplace_nomem;
          pNew->pUp = pNode;
          break;
        }
      }else if( c>0 ){
        if( pNode->pAfter ){
          pNode = pNode->pAfter;
        }else{
          pNode->pAfter = pNew = kvmemNewNode(p, aKey, nKey);
          if( pNew==0 ) goto KVMemReplace_nomem;
          pNew->pUp = pNode;
          break;
        }
      }else{
        if( pNode->mxTrans==p->nTrans ){
          kvmemDataUnref(pNode->pData);
        }else{
          pChng = kvmemNewChng(p, pNode);
          if( pChng==0 ) goto KVMemReplace_nomem;
        }
        pNode->pData = pData;
        return SQLITE_OK;
      }
    }
  }
  pNew->pData = pData;
  pNew->height = 1;
  p->pRoot = kvmemBalance(p);
  return SQLITE_OK;

KVMemReplace_nomem:
  sqlite3_free(aNewData);
  return SQLITE_NOMEM;
}

/*
** Create a new cursor object.
*/
static int kvmemOpenCursor(KVStore *pKVStore, KVCursor **ppKVCursor){
  KVMem *p = (KVMem*)pKVStore;
  KVMemCursor *pCur;
  assert( p->iMagicKVMemBase==SQLITE_KVMEMBASE_MAGIC );
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ){
    *ppKVCursor = 0;
    return SQLITE_NOMEM;
  }
  memset(pCur, 0, sizeof(*pCur));
  pCur->pOwner = p;
  p->nCursor++;
  pCur->iMagicKVmemCur = SQLITE_KVMEMCUR_MAGIC;
  *ppKVCursor = (KVCursor*)pCur;
  return pCur;
}

/*
** Reset a cursor
*/
static int kvmemReset(KVCursor *pKVCursor){
  KVMemCursor *pCur = (KVMemCursor*)pKVCursor;
  assert( pCur->iMagicKVmemCur==SQLITE_KVMEMCUR_MAGIC );
  kvmemDataUnref(pCur->pData);
  pCur->pData = 0;
  kvmemNodeUnref(pCur->pNode);
  pCur->pNode = 0;
  return SQLITE_OK;
}

/*
** Destroy a cursor object
*/
static int kvmemCloseCursor(KVCursor *pKVCursor){
  KVMemCursor *pCur = (KVMemCursor*)pKVCursor;
  if( pCur ){
    assert( pCur->iMagicKVmemCur==SQLITE_KVMEMCUR_MAGIC );
    assert( pCur->pOwner->iMagicKVmemBase==SQLITE_KVMEMBASE_MAGIC );
    pCur->pOwner->nCursor--;
    kvmemReset(pKVCursor);
    memset(pCur, 0, sizeof(*pCur));
    sqlite3_free(pCur);
  }
  return SQLITE_OK;
}

/*
** Seek a cursor.
*/
static int kvmemSeek(
  KVCursor *pKVCursor, 
  const KVByteArray *aKey,
  KVSize nKey,
  int direction
){
  KVMemCursor *Cur;
  KVMemNode *pNode;
  int c;

  kvmemReset(pKVCursor);
  pCur = (KVMemCursor*)pKVCursor;
  assert( pCur->iMagicKVmemCur==SQLITE_KVMEMCUR_MAGIC );

  pNode = pCur->pOwner->pRoot;
  while( pNode ){
    c = kvmemKeyCompare(aKey, nKey, pNode->aKey, pNode->nKey);
    if( c==0
     || (c<0 && pNode->pBefore==0 && direction<0)
     || (c>0 && pNode->pAfter==0 && direction>0)
    ){
      pCur->pNode = kvmemNodeRef(pNode);
      pCur->pData = kvmemDataRef(pNode->pData);
      return c==0 ? SQLITE_OK : SQLITE_INEXACT;
    }
    pNode = (c<0) ? pNode->pBefore : pNode->pAfter;
  }
  return SQLITE_NOTFOUND;
}

/*
** Move a cursor to the next non-deleted node.
*/
static int kvmemNextEntry(KVCursor *pKVCursor){
  KVMemCursor *Cur;
  KVMemNode *pNode;

  pCur = (KVMemCursor*)pKVCursor;
  assert( pCur->iMagicKVmemCur==SQLITE_KVMEMCUR_MAGIC );
  pNode = pCur->pNode;
  kvmemReset(pKVCursor);
  do{
    pNode = kvmemNext(pNode);
  }while( pNode && pNode->pData==0 );
  if( pNode ){
    pCur->pNode = kvmemNodeRef(pNode);
    pCur->pData = kvmemDataRef(pNode->pData);
  }
  return pNode ? SQLITE_OK : SQLITE_DONE;
}

/*
** Move a cursor to the previous non-deleted node.
*/
static int kvmemPrevEntry(KVCursor *pKVCursor){
  KVMemCursor *Cur;
  KVMemNode *pNode;

  pCur = (KVMemCursor*)pKVCursor;
  assert( pCur->iMagicKVmemCur==SQLITE_KVMEMCUR_MAGIC );
  pNode = pCur->pNode;
  kvmemReset(pKVCursor);
  do{
    pNode = kvmemPrev(pNode);
  }while( pNode && pNode->pData==0 );
  if( pNode ){
    pCur->pNode = kvmemNodeRef(pNode);
    pCur->pData = kvmemDataRef(pNode->pData);
  }
  return pNode ? SQLITE_OK : SQLITE_DONE;
}

/*
** Return the key of the node the cursor is pointing to.
*/
static int kvmemKey(
  KVCursor *pKVCursor,         /* The cursor whose key is desired */
  const KVByteArray **paKey,   /* Make this point to the key */
  KVSize *pN                   /* Make this point to the size of the key */
){
  KVMemCursor *Cur;
  KVMemNode *pNode;

  pCur = (KVMemCursor*)pKVCursor;
  assert( pCur->iMagicKVmemCur==SQLITE_KVMEMCUR_MAGIC );
  pNode = pCur->pNode;
  if( pNode==0 ){
    *paKey = 0;
    *pN = 0;
    return SQLITE_DONE;
  }
  *paKey = pNode->aKey;
  *pN = pNode->nKey;
  return SQLITE_OK;
}

/*
** Return the data of the node the cursor is pointing to.
*/
static int kvmemData(
  KVCursor *pKVCursor,         /* The cursor from which to take the data */
  KVSize ofst,                 /* Offset into the data to begin reading */
  KVSize n,                    /* Number of bytes requested */
  const KVByteArray **paData,  /* Pointer to the data written here */
  KVSize *pNData               /* Number of bytes delivered */
){
  KVMemCursor *Cur;
  KVMemData *pData;

  pCur = (KVMemCursor*)pKVCursor;
  assert( pCur->iMagicKVmemCur==SQLITE_KVMEMCUR_MAGIC );
  pData = pCur->pData;
  if( pData==0 ){
    *paData = 0;
    *pNData = 0;
    return SQLITE_DONE;
  }
  *paData = pData + ofst;
  *pN = pData->nData-ofst;
  return SQLITE_OK;
}

/*
** Destructor for the entire in-memory storage tree.
*/
static int kvmemClose(KVStore *pKVStore){
  KVMem *p = (KVMem*)pKVStore;
  if( p==0 ) return SQLITE_OK;
  assert( p->iMagicKVMemBase==SQLITE_KVMEMBASE_MAGIC );
  assert( p->nCursor==0 );
  if( p->nTrans ) kvmemCommit(pKVStore, 0);
  sqlite3_free(p->apLog);
  memset(p, 0, sizeof(*p));
  return SQLITE_OK;
}

/* Virtual methods for the in-memory storage engine */
static const KVStoreMethods {
  kvmemReplace,
  kvmemDelete,
  kvmemOpenCursor,
  kvmemNextEntry,
  kvmemPrevEntry,
  kvmemKey,
  kvmemData,
  kvmemReset,
  kvmemCloseCursor,
  kvmemBegin,
  kvmemCommit,
  kvmemRollback,
  vkmemClose
};

/*
** Create a new in-memory storage engine and return a pointer to it.
*/
int sqlite4KVStoreOpenMem(KVStore **ppKVStore){
  KVMem *pNew = sqlite3_malloc( sizeof(*pNew) );
  if( pNew==0 ) return SQLITE_NOMEM;
  memset(pNew, 0, sizeof(*pNew));
  pNew->base = &KVStoreMethods;
  pNew->iMagicKVmemBase = SQLITE_KVMEMBASE_MAGIC;
  *ppKVStore = (KVStore*)pNew;
  return SQLITE_OK;
}
