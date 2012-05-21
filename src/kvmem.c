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
  KVMemData *pData;     /* Content of the row.  NULL means it is deleted. */
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
  KVMemNode *pNode;     /* The node that is changing */
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
  unsigned openFlags;   /* Flags used at open */
  KVMemChng **apLog;    /* Array of transaction logs */
  int nCursor;          /* Number of outstanding cursors */
  int iMagicKVMemBase;  /* Magic number of sanity */
};
#define SQLITE_KVMEMBASE_MAGIC  0xbfcd47d0

/*
** A cursor used for scanning through the tree
*/
struct KVMemCursor {
  KVCursor base;        /* Base class. Must be first */
  KVMem *pOwner;        /* The tree that owns this cursor */
  KVMemNode *pNode;     /* The entry this cursor points to */
  KVMemData *pData;     /* Data returned by xData */
  int iMagicKVMemCur;   /* Magic number for sanity */
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
  assert( pUp->pAfter==p || pUp->pBefore==p );
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
  if( c==0 ) c = nK1 - nK2;
  return c;
}

/*
** Create a new KVMemData object
*/
static KVMemData *kvmemDataNew(const KVByteArray *aData, KVSize nData){
  KVMemData *p = sqlite4_malloc( sizeof(*p) + nData - 8 );
  if( p ) {
    p->nRef = 1;
    p->n = nData;
    memcpy(p->a, aData, nData);
  }
  return p;
}

/*
** Make a copy of a KVMemData object
*/
static KVMemData *kvmemDataRef(KVMemData *p){
  if( p ) p->nRef++;
  return p;
}

/*
** Dereference a KVMemData object
*/
static void kvmemDataUnref(KVMemData *p){
  if( p && (--p->nRef)<=0 ) sqlite4_free(p);
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
  if( p && (--p->nRef)<=0 ){
    kvmemDataUnref(p->pData);
    sqlite4_free(p);
  }
}

/*
** Recursively delete all nodes in a tree
*/
static void kvmemClearTree(KVMemNode *pNode){
  if( pNode==0 ) return;
  kvmemClearTree(pNode->pBefore);
  kvmemClearTree(pNode->pAfter);
  kvmemNodeUnref(pNode);
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
    p = kvmemFirst(p->pAfter);
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
    p = kvmemLast(p->pBefore);
  }
  return p;
}

/*
** Create a new change record
*/
static KVMemChng *kvmemNewChng(KVMem *p, KVMemNode *pNode){
  KVMemChng *pChng;
  assert( p->base.iTransLevel>=2 );
  pChng = sqlite4_malloc( sizeof(*pChng) );
  if( pChng ){
    pChng->pNext = p->apLog[p->base.iTransLevel-2];
    p->apLog[p->base.iTransLevel-2] = pChng;
    pChng->pNode = pNode;
    pChng->oldTrans = pNode->mxTrans;
    pNode->mxTrans = p->base.iTransLevel;
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
  assert( p->base.iTransLevel>=2 );
  pNode = sqlite4_malloc( sizeof(*pNode)+nKey-2 );
  if( pNode ){
    memset(pNode, 0, sizeof(*pNode));
    memcpy(pNode->aKey, aKey, nKey);
    pNode->nKey = nKey;
    pNode->nRef = 1;
    pChng = kvmemNewChng(p, pNode);
    if( pChng==0 ){
      sqlite4_free(pNode);
      pNode = 0;
    }
    assert( pChng==0 || pChng->pData==0 );
  }
  return pNode;
}

#ifdef SQLITE_DEBUG
/*
** Return the number of times that node pNode occurs in the sub-tree 
** headed by node pSub. This is used to assert() that no node structure
** is linked into the tree more than once.
*/
#if 0
static int countNodeOccurences(KVMemNode *pSub, KVMemNode *pNode){
  int iRet = (pSub==pNode);
  if( pSub ){
    iRet += countNodeOccurences(pSub->pBefore, pNode);
    iRet += countNodeOccurences(pSub->pAfter, pNode);
  }
  return iRet;
}
#endif

/*
** Check that all the pUp pointers in the sub-tree headed by pSub are
** correct. Fail an assert if this is not the case.
*/
static void assertUpPointers(KVMemNode *pSub){
  if( pSub ){
    assert( pSub->pBefore==0 || pSub->pBefore->pUp==pSub );
    assert( pSub->pAfter==0 || pSub->pAfter->pUp==pSub );
    assertUpPointers(pSub->pBefore);
    assertUpPointers(pSub->pAfter);
  }
}

#else
#define assertUpPointers(x)
#endif

/* Remove node pOld from the tree.  pOld must be an element of the tree.
*/
static void kvmemRemoveNode(KVMem *p, KVMemNode *pOld){
  KVMemNode **ppParent;           /* Location of pointer to pOld */
  KVMemNode *pBalance;            /* Node to run kvmemBalance() on */
  kvmemDataUnref(pOld->pData);
  pOld->pData = 0;
  ppParent = kvmemFromPtr(pOld, &p->pRoot);
  if( pOld->pBefore==0 && pOld->pAfter==0 ){
    *ppParent = 0;
    pBalance = pOld->pUp;
  }else if( pOld->pBefore && pOld->pAfter ){
    KVMemNode *pX;                /* Smallest node that is larger than pOld */
    KVMemNode *pY;                /* Left-hand child of pOld */
    pX = kvmemFirst(pOld->pAfter);
    assert( pX->pBefore==0 );
    if( pX==pOld->pAfter ){
      pBalance = pX;
    }else{
      *kvmemFromPtr(pX, 0) = pX->pAfter;
      if( pX->pAfter ) pX->pAfter->pUp = pX->pUp;
      pBalance = pX->pUp;
      pX->pAfter = pOld->pAfter;
      if( pX->pAfter ){
        pX->pAfter->pUp = pX;
      }else{
        assert( pBalance==pOld );
        pBalance = pX;
      }
    }
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
  kvmemNodeUnref(pOld);
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
  assert( iLevel==2 || iLevel==p->base.iTransLevel+1 );
  if( iLevel>=2 ){
    KVMemChng **apNewLog;
    apNewLog = sqlite4_realloc(p->apLog, sizeof(apNewLog[0])*(iLevel-1) );
    if( apNewLog==0 ) return SQLITE_NOMEM;
    p->apLog = apNewLog;
    p->apLog[iLevel-2] = 0;
  }
  p->base.iTransLevel = iLevel;
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
** Commit is divided into two phases.  A rollback is still possible after
** phase one completes.  In this implementation, phase one is a no-op since
** phase two cannot fail.
**
** After this routine returns successfully, the transaction level will be 
** equal to iLevel.
*/
static int kvmemCommitPhaseOne(KVStore *pKVStore, int iLevel){
  return SQLITE_OK;
}
static int kvmemCommitPhaseTwo(KVStore *pKVStore, int iLevel){
  KVMem *p = (KVMem*)pKVStore;
  assert( p->iMagicKVMemBase==SQLITE_KVMEMBASE_MAGIC );
  assert( iLevel>=0 );
  assert( iLevel<p->base.iTransLevel );
  assertUpPointers(p->pRoot);
  while( p->base.iTransLevel>iLevel && p->base.iTransLevel>1 ){
    KVMemChng *pChng, *pNext;

    if( iLevel<2 ){
      for(pChng=p->apLog[p->base.iTransLevel-2]; pChng; pChng=pNext){
        KVMemNode *pNode = pChng->pNode;
        if( pNode->pData ){
          pNode->mxTrans = pChng->oldTrans;
        }else{
          kvmemRemoveNode(p, pNode);
        }
        kvmemDataUnref(pChng->pData);
        pNext = pChng->pNext;
        sqlite4_free(pChng);
      }
    }else{
      KVMemChng **pp;
      int iFrom = p->base.iTransLevel-2;
      int iTo = p->base.iTransLevel-3;
      assert( iTo>=0 );

      for(pp=&p->apLog[iFrom]; *pp; pp=&((*pp)->pNext)){
        assert( (*pp)->pNode->mxTrans==p->base.iTransLevel 
             || (*pp)->pNode->mxTrans==(p->base.iTransLevel-1)
        );
        (*pp)->pNode->mxTrans = p->base.iTransLevel - 1;
      }
      *pp = p->apLog[iTo];
      p->apLog[iTo] = p->apLog[iFrom];
    }

    p->apLog[p->base.iTransLevel-2] = 0;
    p->base.iTransLevel--;
  }
  assertUpPointers(p->pRoot);
  p->base.iTransLevel = iLevel;
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
static int kvmemRollback(KVStore *pKVStore, int iLevel){
  KVMem *p = (KVMem*)pKVStore;
  assert( p->iMagicKVMemBase==SQLITE_KVMEMBASE_MAGIC );
  assert( iLevel>=0 );
  while( p->base.iTransLevel>iLevel && p->base.iTransLevel>1 ){
    KVMemChng *pChng, *pNext;
    for(pChng=p->apLog[p->base.iTransLevel-2]; pChng; pChng=pNext){
      KVMemNode *pNode = pChng->pNode;
      if( pChng->pData || pChng->oldTrans>0 ){
        kvmemDataUnref(pNode->pData);
        pNode->pData = pChng->pData;
        pNode->mxTrans = pChng->oldTrans;
      }else{
        kvmemRemoveNode(p, pNode);
      }
      pNext = pChng->pNext;
      sqlite4_free(pChng);
    }
    p->apLog[p->base.iTransLevel-2] = 0;
    p->base.iTransLevel--;
  }
  p->base.iTransLevel = iLevel;
  return SQLITE_OK;
}

/*
** Revert a transaction back to what it was when it started.
*/
static int kvmemRevert(KVStore *pKVStore, int iLevel){
  int rc = kvmemRollback(pKVStore, iLevel-1);
  if( rc==SQLITE_OK ){
    rc = kvmemBegin(pKVStore, iLevel);
  }
  return rc;
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
  assert( p->base.iTransLevel>=2 );
  pData = kvmemDataNew(aData, nData);
  if( pData==0 ) return SQLITE_NOMEM;
  if( p->pRoot==0 ){
    pNode = pNew = kvmemNewNode(p, aKey, nKey);
    if( pNew==0 ) goto KVMemReplace_nomem;
    pNew->pUp = 0;
  }else{
    pNode = p->pRoot;
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
        if( pNode->mxTrans==p->base.iTransLevel ){
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
  p->pRoot = kvmemBalance(pNew);
  return SQLITE_OK;

KVMemReplace_nomem:
  kvmemDataUnref(pData);
  return SQLITE_NOMEM;
}

/*
** Create a new cursor object.
*/
static int kvmemOpenCursor(KVStore *pKVStore, KVCursor **ppKVCursor){
  KVMem *p = (KVMem*)pKVStore;
  KVMemCursor *pCur;
  assert( p->iMagicKVMemBase==SQLITE_KVMEMBASE_MAGIC );
  pCur = sqlite4_malloc( sizeof(*pCur) );
  if( pCur==0 ){
    *ppKVCursor = 0;
    return SQLITE_NOMEM;
  }
  memset(pCur, 0, sizeof(*pCur));
  pCur->pOwner = p;
  p->nCursor++;
  pCur->iMagicKVMemCur = SQLITE_KVMEMCUR_MAGIC;
  pCur->base.pStore = pKVStore;
  pCur->base.pStoreVfunc = pKVStore->pStoreVfunc;
  *ppKVCursor = (KVCursor*)pCur;
  return SQLITE_OK;
}

/*
** Reset a cursor
*/
static int kvmemReset(KVCursor *pKVCursor){
  KVMemCursor *pCur = (KVMemCursor*)pKVCursor;
  assert( pCur->iMagicKVMemCur==SQLITE_KVMEMCUR_MAGIC );
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
    assert( pCur->iMagicKVMemCur==SQLITE_KVMEMCUR_MAGIC );
    assert( pCur->pOwner->iMagicKVMemBase==SQLITE_KVMEMBASE_MAGIC );
    pCur->pOwner->nCursor--;
    kvmemReset(pKVCursor);
    memset(pCur, 0, sizeof(*pCur));
    sqlite4_free(pCur);
  }
  return SQLITE_OK;
}

/*
** Move a cursor to the next non-deleted node.
*/
static int kvmemNextEntry(KVCursor *pKVCursor){
  KVMemCursor *pCur;
  KVMemNode *pNode;

  pCur = (KVMemCursor*)pKVCursor;
  assert( pCur->iMagicKVMemCur==SQLITE_KVMEMCUR_MAGIC );
  pNode = pCur->pNode;
  kvmemReset(pKVCursor);
  do{
    pNode = kvmemNext(pNode);
  }while( pNode && pNode->pData==0 );
  if( pNode ){
    pCur->pNode = kvmemNodeRef(pNode);
    pCur->pData = kvmemDataRef(pNode->pData);
  }
  return pNode ? SQLITE_OK : SQLITE_NOTFOUND;
}

/*
** Move a cursor to the previous non-deleted node.
*/
static int kvmemPrevEntry(KVCursor *pKVCursor){
  KVMemCursor *pCur;
  KVMemNode *pNode;

  pCur = (KVMemCursor*)pKVCursor;
  assert( pCur->iMagicKVMemCur==SQLITE_KVMEMCUR_MAGIC );
  pNode = pCur->pNode;
  kvmemReset(pKVCursor);
  do{
    pNode = kvmemPrev(pNode);
  }while( pNode && pNode->pData==0 );
  if( pNode ){
    pCur->pNode = kvmemNodeRef(pNode);
    pCur->pData = kvmemDataRef(pNode->pData);
  }
  return pNode ? SQLITE_OK : SQLITE_NOTFOUND;
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
  KVMemCursor *pCur;
  KVMemNode *pNode;
  KVMemNode *pBest = 0;
  int c;
  int rc = SQLITE_NOTFOUND;

  kvmemReset(pKVCursor);
  pCur = (KVMemCursor*)pKVCursor;
  assert( pCur->iMagicKVMemCur==SQLITE_KVMEMCUR_MAGIC );

  pNode = pCur->pOwner->pRoot;
  while( pNode ){
    c = kvmemKeyCompare(aKey, nKey, pNode->aKey, pNode->nKey);
    if( c==0 ){
      pBest = pNode;
      rc = SQLITE_OK;
      pNode = 0;
    }else if( c>0 ){
      if( direction<0 ){
        pBest = pNode;
        rc = SQLITE_INEXACT;
      }
      pNode = pNode->pAfter;
    }else{
      if( direction>0 ){
        pBest = pNode;
        rc = SQLITE_INEXACT;
      }
      pNode = pNode->pBefore;
    }
  }
  kvmemNodeUnref(pCur->pNode);
  kvmemDataUnref(pCur->pData);
  if( pBest ){
    pCur->pNode = kvmemNodeRef(pBest);
    pCur->pData = kvmemDataRef(pBest->pData);

    /* The cursor currently points to a deleted node. If parameter 'direction'
    ** was zero (exact matches only), then the search has failed - return
    ** SQLITE_NOTFOUND. Otherwise, advance to the next (if direction is +ve)
    ** or the previous (if direction is -ve) undeleted node in the tree.  */
    if( pCur->pData==0 ){
      if( direction==0 ){
        rc = SQLITE_NOTFOUND;
      }else{ 
        if( (direction>0 ? kvmemNextEntry : kvmemPrevEntry)(pKVCursor) ){
          rc = SQLITE_NOTFOUND;
        }else{
          rc = SQLITE_INEXACT;
        }
      }
    }

  }else{
    pCur->pNode = 0;
    pCur->pData = 0;
  }
  assert( rc!=SQLITE_DONE );
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
static int kvmemDelete(KVCursor *pKVCursor){
  KVMemCursor *pCur;
  KVMemNode *pNode;
  KVMemChng *pChng;
  KVMem *p;

  pCur = (KVMemCursor*)pKVCursor;
  assert( pCur->iMagicKVMemCur==SQLITE_KVMEMCUR_MAGIC );
  p = pCur->pOwner;
  assert( p->iMagicKVMemBase==SQLITE_KVMEMBASE_MAGIC );
  assert( p->base.iTransLevel>=2 );
  pNode = pCur->pNode;
  if( pNode==0 ) return SQLITE_OK;
  if( pNode->pData==0 ) return SQLITE_OK;
  if( pNode->mxTrans<p->base.iTransLevel ){
    pChng = kvmemNewChng(p, pNode);
    if( pChng==0 ) return SQLITE_NOMEM;
    assert( pNode->pData==0 );
  }else{
    kvmemDataUnref(pNode->pData);
    pNode->pData = 0;
  }
  return SQLITE_OK;
}

/*
** Return the key of the node the cursor is pointing to.
*/
static int kvmemKey(
  KVCursor *pKVCursor,         /* The cursor whose key is desired */
  const KVByteArray **paKey,   /* Make this point to the key */
  KVSize *pN                   /* Make this point to the size of the key */
){
  KVMemCursor *pCur;
  KVMemNode *pNode;

  pCur = (KVMemCursor*)pKVCursor;
  assert( pCur->iMagicKVMemCur==SQLITE_KVMEMCUR_MAGIC );
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
  KVMemCursor *pCur;
  KVMemData *pData;

  pCur = (KVMemCursor*)pKVCursor;
  assert( pCur->iMagicKVMemCur==SQLITE_KVMEMCUR_MAGIC );
  pData = pCur->pData;
  if( pData==0 ){
    *paData = 0;
    *pNData = 0;
    return SQLITE_DONE;
  }
  *paData = pData->a + ofst;
  *pNData = pData->n - ofst;
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
  if( p->base.iTransLevel ){
    kvmemCommitPhaseOne(pKVStore, 0);
    kvmemCommitPhaseTwo(pKVStore, 0);
  }
  sqlite4_free(p->apLog);
  kvmemClearTree(p->pRoot);
  memset(p, 0, sizeof(*p));
  sqlite4_free(p);
  return SQLITE_OK;
}

static int kvmemControl(KVStore *pKVStore, int op, void *pArg){
  return SQLITE_NOTFOUND;
}

/* Virtual methods for the in-memory storage engine */
static const KVStoreMethods kvmemMethods = {
  kvmemReplace,
  kvmemOpenCursor,
  kvmemSeek,
  kvmemNextEntry,
  kvmemPrevEntry,
  kvmemDelete,
  kvmemKey,
  kvmemData,
  kvmemReset,
  kvmemCloseCursor,
  kvmemBegin,
  kvmemCommitPhaseOne,
  kvmemCommitPhaseTwo,
  kvmemRollback,
  kvmemRevert,
  kvmemClose,
  kvmemControl
};

/*
** Create a new in-memory storage engine and return a pointer to it.
*/
int sqlite4KVStoreOpenMem(
  KVStore **ppKVStore, 
  const char *zName,
  unsigned openFlags
){
  KVMem *pNew = sqlite4_malloc( sizeof(*pNew) );
  if( pNew==0 ) return SQLITE_NOMEM;
  memset(pNew, 0, sizeof(*pNew));
  pNew->base.pStoreVfunc = &kvmemMethods;
  pNew->iMagicKVMemBase = SQLITE_KVMEMBASE_MAGIC;
  pNew->openFlags = openFlags;
  *ppKVStore = (KVStore*)pNew;
  return SQLITE_OK;
}
