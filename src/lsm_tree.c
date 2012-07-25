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
** This file contains the implementation of an in-memory tree structure.
**
** Technically the tree is a B-tree of order 4 (in the Knuth sense - each 
** node may have up to 4 children). Keys are stored within B-tree nodes by
** reference. This may be slightly slower than a conventional red-black
** tree, but it is simpler. It is also an easier structure to modify to 
** create a version that supports nested transaction rollback.
**
** This tree does not currently support a delete operation. One is not 
** required. When LSM deletes a key from a database, it inserts a DELETE
** marker into the data structure. As a result, although the value associated
** with a key stored in the in-memory tree structure may be modified, no
** keys are ever removed. 
*/

/*
** MVCC NOTES
**
**   The in-memory tree structure supports SQLite-style MVCC. This means
**   that while one client is writing to the tree structure, other clients
**   may still be querying an older snapshot of the tree.
**
**   One way to implement this is to use an append-only b-tree. In this 
**   case instead of modifying nodes in-place, a copy of the node is made
**   and the required modifications made to the copy. The parent of the
**   node is then modified (to update the pointer so that it points to
**   the new copy), which causes a copy of the parent to be made, and so on.
**   This means that each time the tree is written to a new root node is
**   created. A snapshot is identified by the root node that it uses.
**
**   The problem with the above is that each time the tree is written to,
**   a copy of the node structure modified and all of its ancestor nodes
**   is made. This may prove excessive with large tree structures.
**
**   To reduce this overhead, the data structure used for a tree node is
**   designed so that it may be edited in place exactly once without 
**   affecting existing users. In other words, the node structure is capable
**   of storing two separate versions of the node at the same time.
**   When a node is to be edited, if the node structure already contains 
**   two versions, a copy is made as in the append-only approach. Or, if
**   it only contains a single version, it may be edited in place.
**
**   This reduces the overhead so that, roughly, one new node structure
**   must be allocated for each write (on top of those allocations that 
**   would have been required by a non-MVCC tree). Logic: Assume that at 
**   any time, 50% of nodes in the tree already contain 2 versions. When
**   a new entry is written to a node, there is a 50% chance that a copy
**   of the node will be required. And a 25% chance that a copy of its 
**   parent is required. And so on.
**
** ROLLBACK
**
**   The in-memory tree also supports transaction and sub-transaction 
**   rollback. In order to rollback to point in time X, the following is
**   necessary:
**
**     1. All memory allocated since X must be freed, and 
**     2. All "v2" data adding to nodes that existed at X should be zeroed.
**     3. The root node must be restored to its X value.
**
**   The Mempool object used to allocate memory for the tree supports 
**   operation (1) - see the lsmPoolMark() and lsmPoolRevert() functions.
**
**   To support (2), all nodes that have v2 data are part of a singly linked 
**   list, sorted by the age of the v2 data (nodes that have had data added 
**   most recently are at the end of the list). So to zero all v2 data added
**   since X, the linked list is traversed from the first node added following
**   X onwards.
**
*/

#ifndef _LSM_INT_H
# include "lsmInt.h"
#endif

#include <string.h>

#define MAX_DEPTH 32

typedef struct TreeKey TreeKey;
typedef struct TreeNode TreeNode;
typedef struct TreeLeaf TreeLeaf;
typedef struct NodeVersion NodeVersion;

/*
** Container for a key-value pair. Within the *-shm file, each key/value
** pair is stored in a single allocation (which may not actually be 
** contiguous in memory). Layout is the TreeKey structure, followed by
** the nKey bytes of key blob, followed by the nValue bytes of value blob
** (if nValue is non-negative).
*/
struct TreeKey {
  int nKey;                       /* Size of pKey in bytes */
  int nValue;                     /* Size of pValue. Or negative. */
};

#define TK_KEY(p) ((void *)&(p)[1])
#define TK_VAL(p) ((void *)(((u8 *)&(p)[1]) + (p)->nKey))

/*
** A single tree node. A node structure may contain up to 3 key/value
** pairs. Internal (non-leaf) nodes have up to 4 children.
**
** TODO: Update the format of this to be more compact. Get it working
** first though...
*/
struct TreeNode {
  u32 aiKeyPtr[3];                /* Array of pointers to TreeKey objects */

  /* The following fields are present for interior nodes only, not leaves. */
  u32 aiChildPtr[4];              /* Array of pointers to child nodes */

  /* The extra child pointer slot. */
  u32 iV2;                        /* Transaction number of v2 */
  u8 iV2Child;                    /* apChild[] entry replaced by pV2Ptr */
  u32 iV2Ptr;                     /* Substitute pointer */
};

struct TreeLeaf {
  u32 aiKeyPtr[3];                /* Array of pointers to TreeKey objects */
};

/*
** A handle used by a client to access a Tree structure.
*/
struct TreeVersion {
  Tree *pTree;                    /* The tree structure to which this belongs */
  int nRef;                       /* Number of pointers to this */
  TreeNode *pRoot;                /* Pointer to root of tree structure */
  int nHeight;                    /* Current height of tree pRoot */
  int iVersion;                   /* Current version */
};

#define WORKING_VERSION (1<<30)

/*
** A tree structure.
**
** iVersion:
**   When the tree is first created, this is set to 1. Thereafter it is
**   incremented each time lsmTreeMark() is called. The tree must be 
**   destroyed (i.e. flushed to disk) before it wraps around (todo!).
**
**   When v2 data is written to a tree-node, the iV2 field of the node
**   is set to the current value of Tree.iVersion.
**
** nRef:
**   Number of references to this tree structure. When it is first created,
**   (in lsmTreeNew()) nRef is set to 1. There after the ref-count may be
**   incremented and decremented using treeIncrRefcount() and 
**   DecrRefcount(). When the ref-count of a tree structure reaches zero
**   it is freed.
**
** xCmp:
**   Pointer to the compare function. This is a copy of some pDb->xCmp.
**
*/
struct Tree {
  int nTreeRef;                   /* Current number of pointers to this */
  Mempool *pPool;                 /* Memory pool to allocate from */
  int (*xCmp)(void *, int, void *, int);         /* Compare function */
  TreeVersion *pCommit;           /* Committed version of tree (for readers) */

  TreeVersion *pWorking;          /* Working verson (for writers) */
#if 0
  TreeVersion tvWorking;          /* Working verson (for writers) */
#endif

  TreeNode *pRbFirst;
  TreeNode *pRbLast;
};

/*
** The pointer passed as the first argument points to an interior node,
** not a leaf. This function returns the value of the iCell'th child
** sub-tree of the node.
*/
#if 0
static TreeNode *getChildPtr(TreeNode *p, int iVersion, int iCell){
  if( p->iV2 && p->iV2<=iVersion && iCell==p->iV2Ptr ) return p->pV2Ptr;
  return p->apChild[iCell];
}
#endif

static u32 getChildPtr(TreeNode *p, int iVersion, int iCell){
  assert( iCell>=0 && iCell<=array_size(p->aiChildPtr) );
  if( p->iV2 && p->iV2<=iVersion && iCell==p->iV2Child ) return p->iV2Ptr;
  return p->aiChildPtr[iCell];
}

/*
** Cursor for searching a tree structure.
**
** If a cursor does not point to any element (a.k.a. EOF), then the
** TreeCursor.iNode variable is set to a negative value. Otherwise, the
** cursor currently points to key aiCell[iNode] on node apTreeNode[iNode].
**
** Entries in the apTreeNode[] and aiCell[] arrays contain the node and
** index of the TreeNode.apChild[] pointer followed to descend to the 
** current element. Hence apTreeNode[0] always contains the root node of
** the tree.
*/
struct TreeCursor {
  lsm_db *pDb;                    /* Database handle for this cursor */
  int iNode;                      /* Cursor points at apTreeNode[iNode] */
  TreeNode *apTreeNode[MAX_DEPTH];/* Current position in tree */
  u8 aiCell[MAX_DEPTH];           /* Current position in tree */
  TreeKey *pSave;                 /* Saved key */
};



static void *treeShmptr(lsm_db *pDb, u32 iPtr, int *pRc){
  /* TODO: This will likely be way too slow. If it is, chunks should be
  ** cached as part of the db handle.  */
  if( iPtr && *pRc==0 ){
    int rc;
    void *pChunk;
    assert( LSM_SHM_CHUNK_SIZE==(1<<15) );
    rc = lsmShmChunk(pDb, (iPtr>>15), &pChunk);
    if( rc==LSM_OK ){
      return &((u8 *)pChunk)[iPtr & (LSM_SHM_CHUNK_SIZE-1)];
    }
    *pRc = rc;
  }
  return 0;
}



#if defined(LSM_DEBUG) && defined(LSM_EXPENSIVE_ASSERT)

void assert_leaf_looks_ok(TreeNode *pNode){
  assert( pNode->apKey[1] );
}

void assert_node_looks_ok(TreeNode *pNode, int nHeight){
  if( pNode ){
    assert( pNode->apKey[1] );
    if( nHeight>1 ){
      int i;
      assert( getChildPtr(pNode, WORKING_VERSION, 1) );
      assert( getChildPtr(pNode, WORKING_VERSION, 2) );
      for(i=0; i<4; i++){
        assert_node_looks_ok(getChildPtr(pNode, WORKING_VERSION, i), nHeight-1);
      }
    }
  }
}

/*
** Run various assert() statements to check that the working-version of the
** tree is correct in the following respects:
**
**   * todo...
*/
void assert_tree_looks_ok(int rc, Tree *pTree){
  if( rc==LSM_OK ){
    TreeVersion *p = pTree->pWorking ? pTree->pWorking : pTree->pCommit;
    if( p ){
      assert( (p->nHeight==0)==(p->pRoot==0) );
      assert_node_looks_ok(p->pRoot, p->nHeight);
    }
  }
}
#else
# define assert_tree_looks_ok(x,y)
#endif

#ifdef LSM_DEBUG
static void lsmAppendStrBlob(LsmString *pStr, void *pBlob, int nBlob){
  int i;
  lsmStringExtend(pStr, nBlob*2);
  if( pStr->nAlloc==0 ) return;
  for(i=0; i<nBlob; i++){
    u8 c = ((u8*)pBlob)[i];
    pStr->z[pStr->n++] = "0123456789abcdef"[(c>>4)&0xf];
    pStr->z[pStr->n++] = "0123456789abcdef"[c&0xf];
  }
  pStr->z[pStr->n] = 0;
}

static void lsmAppendIndent(LsmString *pStr, int nIndent){
  int i;
  lsmStringExtend(pStr, nIndent);
  for(i=0; i<nIndent; i++) lsmStringAppend(pStr, " ", 1);
}

#if 0
static void lsmAppendKeyValue(LsmString *pStr, TreeKey *pKey){
  int i;

  for(i=0; i<pKey->nKey; i++){
    lsmStringAppendf(pStr, "%2X ", ((u8 *)(pKey->pKey))[i]);
  }
  lsmStringAppend(pStr, "      ", -1);

  if( pKey->nValue<0 ){
    lsmStringAppend(pStr, "<deleted>", -1);
  }else{
    lsmAppendStrBlob(pStr, pKey->pValue, pKey->nValue);
  }
}
#endif

void dump_node(TreeNode *pNode, int nIndent, int isNode){
#if 0
  if( pNode ){
    LsmString s;
    int i;

    lsmStringInit(&s, NEED_ENV);
    lsmAppendIndent(&s, nIndent);
    lsmStringAppendf(&s, "0x%p", (void*)pNode);
    printf("%s\n", s.z);
    lsmStringClear(&s);

    for(i=0; i<4; i++){

      if( isNode ){
        if( pNode->iV2 && i==pNode->iV2Ptr ){
          lsmAppendIndent(&s, nIndent+2);
          lsmStringAppendf(&s, "if( version>=%d )", pNode->iV2);
          printf("%s\n", s.z);
          lsmStringClear(&s);
          dump_node(pNode->pV2Ptr, nIndent + 4, isNode-1);
          if( pNode->apChild[i] ){
            lsmAppendIndent(&s, nIndent+2);
            lsmStringAppendf(&s, "else");
            printf("%s\n", s.z);
            lsmStringClear(&s);
          }
        }

        dump_node(pNode->apChild[i], nIndent + 4, isNode-1);
      }

      if( i<3 && pNode->apKey[i] ){
        lsmAppendIndent(&s, nIndent);
        lsmStringAppendf(&s, "k%d: ", i);
        lsmAppendKeyValue(&s, pNode->apKey[i]);
        printf("%s\n", s.z);
        lsmStringClear(&s);
      }

    }
  }
#endif
}

void dump_node_contents(
  lsm_db *pDb,
  u32 iNode,                      /* Print out hte contents of this node */
  int nIndent,                    /* Number of spaces indentation */
  int nHeight                     /* Height: (0==leaf) (1==parent-of-leaf) */
){
  int i;
  int rc = LSM_OK;
  LsmString s;
  TreeNode *pNode;

  /* Append the nIndent bytes of space to string s. */
  lsmStringInit(&s, pDb->pEnv);
  if( nIndent ) lsmAppendIndent(&s, nIndent);

  pNode = (TreeNode *)treeShmptr(pDb, iNode, &rc);

  /* Append each key to string s. */
  for(i=0; i<3; i++){
    u32 iPtr = pNode->aiKeyPtr[i];
    if( iPtr ){
      TreeKey *pKey = (TreeKey *)treeShmptr(pDb, pNode->aiKeyPtr[i], &rc);
      lsmAppendStrBlob(&s, TK_KEY(pKey), pKey->nKey);
      lsmStringAppend(&s, "     ", -1);
    }
  }

  printf("%s\n", s.z);
  lsmStringClear(&s);

  for(i=0; i<4 && nHeight>0; i++){
    u32 iPtr = getChildPtr(pNode, pDb->treehdr.iTransId, i);
    if( iPtr ){
      dump_node_contents(pDb, iPtr, nIndent + 2, nHeight-1);
    }
  }
}

void dump_tree_contents(lsm_db *pDb, const char *zCaption){
  printf("\n%s\n", zCaption);
  if( pDb->treehdr.iRoot ){
    dump_node_contents(pDb, pDb->treehdr.iRoot, 0, pDb->treehdr.nHeight-1);
  }
  fflush(stdout);
}

void dump_tv_contents(TreeVersion *pTV, const char *zCaption){
  printf("\n%s\n", zCaption);
  if( pTV->pRoot ){
    dump_node(pTV->pRoot, 2, pTV->nHeight-1);
  }
  fflush(stdout);
}

#endif

/*
** Allocate a new tree structure.
*/
int lsmTreeNew(
  lsm_env *pEnv,                            /* Environment handle */
  int (*xCmp)(void *, int, void *, int),    /* Compare function */
  Tree **ppTree                             /* OUT: New tree object */
){
  int rc;
  Tree *pTree = 0;
  Mempool *pPool;                 /* Memory pool used by the new tree */
  TreeVersion *pClient = 0;       /* Initial client access handle */

  rc = lsmPoolNew(pEnv, &pPool);
  pClient = (TreeVersion *)lsmMallocZeroRc(pEnv, sizeof(TreeVersion), &rc);

  if( rc==LSM_OK ){
    pTree = (Tree *)lsmPoolMallocZero(pEnv, pPool, sizeof(Tree));
    assert( pTree );
    pTree->pPool = pPool;
    pTree->xCmp = xCmp;
    pTree->nTreeRef = 1;

    pClient->iVersion = 1;
    pClient->pTree = pTree;
    pClient->nRef = 1;
    pTree->pCommit = pClient;
  }else{
    assert( pClient==0 );
    lsmPoolDestroy(pEnv, pPool);
  }

  *ppTree = pTree;
  return rc;
}

/*
** Destroy a tree structure allocated by lsmTreeNew().
*/
static void treeDestroy(lsm_env *pEnv, Tree *pTree){
  if( pTree ){
    assert( pTree->pWorking==0 );
    lsmPoolDestroy(pEnv, pTree->pPool);
  }
}

/*
** Initialize a cursor object, the space for which has already been
** allocated.
*/
static void treeCursorInit(lsm_db *pDb, TreeCursor *pCsr){
  memset(pCsr, 0, sizeof(TreeCursor));
  pCsr->pDb = pDb;
  pCsr->iNode = -1;
}

static TreeKey *csrGetKey(TreeCursor *pCsr, int *pRc){
  return (TreeKey *)treeShmptr(pCsr->pDb,
      pCsr->apTreeNode[pCsr->iNode]->aiKeyPtr[pCsr->aiCell[pCsr->iNode]], pRc
  );
}

/*
** Save the current position of tree cursor pCsr.
*/
int lsmTreeCursorSave(TreeCursor *pCsr){
  int rc = LSM_OK;
  if( pCsr->pSave==0 ){
    int iNode = pCsr->iNode;
    if( iNode>=0 ){
      pCsr->pSave = csrGetKey(pCsr, &rc);
    }
    pCsr->iNode = -1;
  }
  return rc;
}

/*
** Restore the position of a saved tree cursor.
*/
static int treeCursorRestore(TreeCursor *pCsr, int *pRes){
  int rc = LSM_OK;
  if( pCsr->pSave ){
    TreeKey *pKey = pCsr->pSave;
    pCsr->pSave = 0;
    if( pRes ){
      rc = lsmTreeCursorSeek(pCsr, TK_KEY(pKey), pKey->nKey, pRes);
    }
  }
  return rc;
}

/*
** Allocate nByte bytes of space within the *-shm file. If successful, 
** return LSM_OK and set *piPtr to the offset within the file at which
** the allocated space is located.
*/
static u32 treeShmalloc(lsm_db *pDb, int nByte, int *pRc){
  u32 iRet = 0;
  if( *pRc==LSM_OK ){
    const static int CHUNK_SIZE = LSM_SHM_CHUNK_SIZE;
    const static int CHUNK_HDR = LSM_SHM_CHUNK_HDR;

    u32 iEof;                     /* End of current chunk */
    u32 iWrite = pDb->treehdr.iWrite;

    /* Round all allocations up to a multiple of 4 bytes. So that all
     ** integer fields in shared memory are 32-bit aligned. */
    nByte = ((nByte + 3) / 4) * 4;

    /* TODO: Remove this limit by allowing non-contiguous allocations. */
    assert( nByte <= (CHUNK_SIZE-CHUNK_HDR) );

    iEof = (1 + ((iWrite-1) / CHUNK_SIZE)) * CHUNK_SIZE;
    assert( iEof>=iWrite && (iEof-iWrite)<CHUNK_SIZE );

    if( (iWrite+nByte)>iEof ){
      /* Advance to the next chunk */
      iWrite = iEof + CHUNK_HDR;
    }

    /* Allocate space at iWrite. */
    iRet = iWrite;
    pDb->treehdr.iWrite = iWrite + nByte;
  }
  return iRet;
}

static void *treeShmallocZero(lsm_db *pDb, int nByte, u32 *piPtr, int *pRc){
  u32 iPtr;
  void *p;
  iPtr = treeShmalloc(pDb, nByte, pRc);
  p = treeShmptr(pDb, iPtr, pRc);
  if( p ){
    assert( *pRc==LSM_OK );
    memset(p, 0, nByte);
    *piPtr = iPtr;
  }
  return p;
}

TreeNode *newTreeNode(lsm_db *pDb, u32 *piPtr, int *pRc){
  return treeShmallocZero(pDb, sizeof(TreeNode), piPtr, pRc);
}

TreeLeaf *newTreeLeaf(lsm_db *pDb, u32 *piPtr, int *pRc){
  return treeShmallocZero(pDb, sizeof(TreeLeaf), piPtr, pRc);
}

static TreeNode *copyTreeNode(
  lsm_db *pDb, 
  TreeNode *pOld, 
  u32 *piNew, 
  int *pRc
){
  TreeNode *pNew;

  pNew = newTreeNode(pDb, piNew, pRc);
  if( pNew ){
    memcpy(pNew->aiKeyPtr, pOld->aiKeyPtr, sizeof(pNew->aiKeyPtr));
    memcpy(pNew->aiChildPtr, pOld->aiChildPtr, sizeof(pNew->aiChildPtr));
    if( pOld->iV2 ) pNew->aiChildPtr[pOld->iV2Child] = pOld->iV2Ptr;
  }
  return pNew;
}

static TreeNode *copyTreeLeaf(
  lsm_db *pDb, 
  TreeLeaf *pOld, 
  u32 *piNew, 
  int *pRc
){
  TreeLeaf *pNew;
  pNew = newTreeLeaf(pDb, piNew, pRc);
  if( pNew ){
    memcpy(pNew, pOld, sizeof(TreeLeaf));
  }
  return (TreeNode *)pNew;
}

/*
** The tree cursor passed as the second argument currently points to an 
** internal node (not a leaf). Specifically, to a sub-tree pointer. This
** function replaces the sub-tree that the cursor currently points to
** with sub-tree pNew.
**
** The sub-tree may be replaced either by writing the "v2 data" on the
** internal node, or by allocating a new TreeNode structure and then 
** calling this function on the parent of the internal node.
*/
static int treeUpdatePtr(lsm_db *pDb, TreeCursor *pCsr, u32 iNew){
  int rc = LSM_OK;
  if( pCsr->iNode<0 ){
    /* iNew is the new root node */
    pDb->treehdr.iRoot = iNew;
  }else{
    /* If this node already has version 2 content, allocate a copy and
    ** update the copy with the new pointer value. Otherwise, store the
    ** new pointer as v2 data within the current node structure.  */

    TreeNode *p;                  /* The node to be modified */
    int iChildPtr;                /* apChild[] entry to modify */

    p = pCsr->apTreeNode[pCsr->iNode];
    iChildPtr = pCsr->aiCell[pCsr->iNode];

    if( p->iV2 ){
      /* The "allocate new TreeNode" option */
      u32 iCopy;
      TreeNode *pCopy;
      pCopy = copyTreeNode(pDb, p, &iCopy, &rc);
      if( pCopy ){
        assert( rc==LSM_OK );
        pCopy->aiChildPtr[iChildPtr] = iNew;
        pCsr->iNode--;
        rc = treeUpdatePtr(pDb, pCsr, iCopy);
      }
    }else{
      /* The "v2 data" option */
      p->iV2 = pDb->treehdr.iTransId;
      p->iV2Child = (u8)iChildPtr;
      p->iV2Ptr = iNew;

      /* TODO: Add node p to connections rollback list */
    }
  }

  return rc;
}

/*
** Cursor pCsr points at a node that is part of pTree. This function
** inserts a new key and optionally child node pointer into that node.
**
** The position into which the new key and pointer are inserted is
** determined by the iSlot parameter. The new key will be inserted to
** the left of the key currently stored in apKey[iSlot]. Or, if iSlot is
** greater than the index of the rightmost key in the node.
**
** Pointer pLeftPtr points to a child tree that contains keys that are
** smaller than pTreeKey.
*/
static int treeInsert(
  lsm_db *pDb,                    /* Database handle */
  TreeCursor *pCsr,               /* Cursor indicating path to insert at */
  u32 iLeftPtr,                   /* Left child pointer */
  u32 iTreeKey,                   /* Location of key to insert */
  u32 iRightPtr,                  /* Right child pointer */
  int iSlot                       /* Position to insert key into */
){
  int rc = LSM_OK;
  TreeNode *pNode = pCsr->apTreeNode[pCsr->iNode];

  /* Check if the node is currently full. If so, split pNode in two and
  ** call this function recursively to add a key to the parent. Otherwise, 
  ** insert the new key directly into pNode.  */
  assert( pNode->aiKeyPtr[1] );
  if( pNode->aiKeyPtr[0] && pNode->aiKeyPtr[2] ){
    u32 iLeft; TreeNode *pLeft;   /* New left-hand sibling node */
    u32 iRight; TreeNode *pRight; /* New right-hand sibling node */

    pLeft = newTreeNode(pDb, &iLeft, &rc);
    pRight = newTreeNode(pDb, &iRight, &rc);
    if( rc ) return rc;

    pLeft->aiChildPtr[1] = getChildPtr(pNode, WORKING_VERSION, 0);
    pLeft->aiKeyPtr[1] = pNode->aiKeyPtr[0];
    pLeft->aiChildPtr[2] = getChildPtr(pNode, WORKING_VERSION, 1);

    pRight->aiChildPtr[1] = getChildPtr(pNode, WORKING_VERSION, 2);
    pRight->aiKeyPtr[1] = pNode->aiKeyPtr[2];
    pRight->aiChildPtr[2] = getChildPtr(pNode, WORKING_VERSION, 3);

    if( pCsr->iNode==0 ){
      /* pNode is the root of the tree. Grow the tree by one level. */
      u32 iRoot; TreeNode *pRoot; /* New root node */

      pRoot = newTreeNode(pDb, &iRoot, &rc);
      pRoot->aiKeyPtr[1] = pNode->aiKeyPtr[1];
      pRoot->aiChildPtr[1] = iLeft;
      pRoot->aiChildPtr[2] = iRight;

      pDb->treehdr.iRoot = iRoot;
      pDb->treehdr.nHeight++;
    }else{

      pCsr->iNode--;
      rc = treeInsert(pDb, pCsr, 
          iLeft, pNode->aiKeyPtr[1], iRight, pCsr->aiCell[pCsr->iNode]
      );
    }

    assert( pLeft->iV2==0 );
    assert( pRight->iV2==0 );
    switch( iSlot ){
      case 0:
        pLeft->aiKeyPtr[0] = iTreeKey;
        pLeft->aiChildPtr[0] = iLeftPtr;
        if( iRightPtr ) pLeft->aiChildPtr[1] = iRightPtr;
        break;
      case 1:
        pLeft->aiChildPtr[3] = (iRightPtr ? iRightPtr : pLeft->aiChildPtr[2]);
        pLeft->aiKeyPtr[2] = iTreeKey;
        pLeft->aiChildPtr[2] = iLeftPtr;
        break;
      case 2:
        pRight->aiKeyPtr[0] = iTreeKey;
        pRight->aiChildPtr[0] = iLeftPtr;
        if( iRightPtr ) pRight->aiChildPtr[1] = iRightPtr;
        break;
      case 3:
        pRight->aiChildPtr[3] = (iRightPtr ? iRightPtr : pRight->aiChildPtr[2]);
        pRight->aiKeyPtr[2] = iTreeKey;
        pRight->aiChildPtr[2] = iLeftPtr;
        break;
    }

  }else{
    TreeNode *pNew;
    u32 *piKey;
    u32 *piChild;
    u32 iStore = 0;
    u32 iNew = 0;
    int i;

    /* Allocate a new version of node pNode. */
    pNew = newTreeNode(pDb, &iNew, &rc);
    if( rc ) return rc;

    piKey = pNew->aiKeyPtr;
    piChild = pNew->aiChildPtr;

    for(i=0; i<iSlot; i++){
      if( pNode->aiKeyPtr[i] ){
        *(piKey++) = pNode->aiKeyPtr[i];
        *(piChild++) = getChildPtr(pNode, WORKING_VERSION, i);
      }
    }

    *piKey++ = iTreeKey;
    *piChild++ = iLeftPtr;

    iStore = iRightPtr;
    for(i=iSlot; i<3; i++){
      if( pNode->aiKeyPtr[i] ){
        *(piKey++) = pNode->aiKeyPtr[i];
        *(piChild++) = iStore ? iStore : getChildPtr(pNode, WORKING_VERSION, i);
        iStore = 0;
      }
    }

    if( iStore ){
      *piChild = iStore;
    }else{
      *piChild = getChildPtr(pNode, WORKING_VERSION, 
          (pNode->aiKeyPtr[2] ? 3 : 2)
      );
    }
    pCsr->iNode--;
    rc = treeUpdatePtr(pDb, pCsr, iNew);
  }

  return rc;
}

static int treeInsertLeaf(
  lsm_db *pDb,                    /* Database handle */
  TreeCursor *pCsr,               /* Cursor structure */
  u32 iTreeKey,                   /* Key pointer to insert */
  int iSlot                       /* Insert key to the left of this */
){
  int rc = LSM_OK;                /* Return code */
  TreeNode *pLeaf = pCsr->apTreeNode[pCsr->iNode];
  TreeLeaf *pNew;
  u32 iNew;

  assert( iSlot>=0 && iSlot<=4 );
  assert( pCsr->iNode>0 );
  assert( pLeaf->aiKeyPtr[1] );

  pCsr->iNode--;

  pNew = newTreeLeaf(pDb, &iNew, &rc);
  if( pNew ){
    if( pLeaf->aiKeyPtr[0] && pLeaf->aiKeyPtr[2] ){
      /* The leaf is full. Split it in two. */
      TreeLeaf *pRight;
      u32 iRight;
      pRight = newTreeLeaf(pDb, &iRight, &rc);
      if( pRight ){
        assert( rc==LSM_OK );
        pNew->aiKeyPtr[1] = pLeaf->aiKeyPtr[0];
        pRight->aiKeyPtr[1] = pLeaf->aiKeyPtr[2];
        switch( iSlot ){
          case 0: pNew->aiKeyPtr[0] = iTreeKey; break;
          case 1: pNew->aiKeyPtr[2] = iTreeKey; break;
          case 2: pRight->aiKeyPtr[0] = iTreeKey; break;
          case 3: pRight->aiKeyPtr[2] = iTreeKey; break;
        }

        rc = treeInsert(pDb, pCsr, iNew, pLeaf->aiKeyPtr[1], iRight, 
            pCsr->aiCell[pCsr->iNode]
        );
      }
    }else{
      int iOut = 0;
      int i;
      for(i=0; i<4; i++){
        if( i==iSlot ) pNew->aiKeyPtr[iOut++] = iTreeKey;
        if( i<3 && pLeaf->aiKeyPtr[i] ){
          pNew->aiKeyPtr[iOut++] = pLeaf->aiKeyPtr[i];
        }
      }
      rc = treeUpdatePtr(pDb, pCsr, iNew);
    }
  }

  return rc;
}

/*
** Insert a new entry into the in-memory tree.
**
** If the value of the 5th parameter, nVal, is negative, then a delete-marker
** is inserted into the tree. In this case the value pointer, pVal, must be
** NULL.
*/
int lsmTreeInsert(
  lsm_db *pDb,                    /* Database handle */
  void *pKey,                     /* Pointer to key data */
  int nKey,                       /* Size of key data in bytes */
  void *pVal,                     /* Pointer to value data (or NULL) */
  int nVal                        /* Bytes in value data (or -ve for delete) */
){
  int rc = LSM_OK;                /* Return Code */
  TreeKey *pTreeKey;              /* New key-value being inserted */
  int nTreeKey;                   /* Number of bytes allocated at pTreeKey */
  u32 iTreeKey;
  u8 *a;
  TreeHeader *pHdr = &pDb->treehdr;

  assert( nVal>=0 || pVal==0 );
#if 0
  assert( pTV==pTree->pWorking );
  assert_tree_looks_ok(LSM_OK, pTree);
#endif
  /* dump_tree_contents(pDb, "before"); */

  /* Allocate and populate a new key-value pair structure */
  nTreeKey = sizeof(TreeKey) + nKey + (nVal>0 ? nVal : 0);
  iTreeKey = treeShmalloc(pDb, nTreeKey, &rc);
  pTreeKey = (TreeKey *)treeShmptr(pDb, iTreeKey, &rc);
  if( rc!=LSM_OK ) return rc;
  pTreeKey->nValue = nVal;
  pTreeKey->nKey = nKey;
  a = (u8 *)&pTreeKey[1];
  memcpy(a, pKey, nKey);
  if( nVal>0 ) memcpy(&a[nKey], pVal, nVal);

  if( pHdr->iRoot==0 ){
    /* The tree is completely empty. Add a new root node and install
    ** (pKey/nKey) as the middle entry. Even though it is a leaf at the
    ** moment, use newTreeNode() to allocate the node (i.e. allocate enough
    ** space for the fields used by interior nodes). This is because the
    ** treeInsert() routine may convert this node to an interior node. */
    TreeNode *pRoot = newTreeNode(pDb, &pHdr->iRoot, &rc);
    if( rc==LSM_OK ){
      assert( pHdr->nHeight==0 );
      pRoot->aiKeyPtr[1] = iTreeKey;
      pHdr->nHeight = 1;
    }
  }else{
    TreeCursor csr;
    int res;

    /* Seek to the leaf (or internal node) that the new key belongs on */
    treeCursorInit(pDb, &csr);
    lsmTreeCursorSeek(&csr, pKey, nKey, &res);

    if( res==0 ){
      /* The search found a match within the tree. */
      TreeNode *pNew;
      u32 iNew;
      TreeNode *pNode = csr.apTreeNode[csr.iNode];
      int iCell = csr.aiCell[csr.iNode];

      /* Create a copy of this node */
      if( (csr.iNode>0 && csr.iNode==(pHdr->nHeight-1)) ){
        pNew = copyTreeLeaf(pDb, (TreeLeaf *)pNode, &iNew, &rc);
      }else{
        pNew = copyTreeNode(pDb, pNode, &iNew, &rc);
      }

      if( rc==LSM_OK ){
        /* Modify the value in the new version */
        pNew->aiKeyPtr[iCell] = iTreeKey;

        /* Change the pointer in the parent (if any) to point at the new 
        ** TreeNode */
        csr.iNode--;
        treeUpdatePtr(pDb, &csr, iNew);
      }
    }else{
      /* The cursor now points to the leaf node into which the new entry should
      ** be inserted. There may or may not be a free slot within the leaf for
      ** the new key-value pair. 
      **
      ** iSlot is set to the index of the key within pLeaf that the new key
      ** should be inserted to the left of (or to a value 1 greater than the
      ** index of the rightmost key if the new key is larger than all keys
      ** currently stored in the node).
      */
      int iSlot = csr.aiCell[csr.iNode] + (res<0);
      if( csr.iNode==0 ){
        rc = treeInsert(pDb, &csr, 0, iTreeKey, 0, iSlot);
      }else{
        rc = treeInsertLeaf(pDb, &csr, iTreeKey, iSlot);
      }
    }
  }

  /* dump_tree_contents(pDb, "after"); */
  assert_tree_looks_ok(rc, pTree);
  return rc;
}

/*
** Return, in bytes, the amount of memory currently used by the tree 
** structure.
*/
int lsmTreeSize(lsm_db *pDb){
  return 50;
  return pDb->treehdr.nByte;
}

/*
** Return true if the tree is empty. Otherwise false.
**
** The caller is responsible for ensuring that it has exclusive access
** to the Tree structure for this call.
*/
int lsmTreeIsEmpty(lsm_db *pDb){
  /* TODO: This is not right in a true multi-process system... */
  return (pDb->pShmhdr->hdr1.iRoot==0);
}

/*
** Open a cursor on the in-memory tree pTree.
*/
int lsmTreeCursorNew(lsm_db *pDb, TreeCursor **ppCsr){
  TreeCursor *pCsr;
  *ppCsr = pCsr = lsmMalloc(pDb->pEnv, sizeof(TreeCursor));
  if( pCsr ){
    treeCursorInit(pDb, pCsr);
    return LSM_OK;
  }
  return LSM_NOMEM_BKPT;
}

/*
** Close an in-memory tree cursor.
*/
void lsmTreeCursorDestroy(TreeCursor *pCsr){
  if( pCsr ){
    lsmFree(pCsr->pDb->pEnv, pCsr);
  }
}

void lsmTreeCursorReset(TreeCursor *pCsr){
  pCsr->iNode = -1;
  pCsr->pSave = 0;
}

#ifndef NDEBUG
static int treeCsrCompare(TreeCursor *pCsr, void *pKey, int nKey){
  TreeKey *p;
  int cmp = 0;
  int rc = LSM_OK;
  assert( pCsr->iNode>=0 );
  p = csrGetKey(pCsr, &rc);
  if( p ){
    cmp = pCsr->pDb->xCmp(TK_KEY(p), p->nKey, pKey, nKey);
  }
  return cmp;
}
#endif


/*
** Attempt to seek the cursor passed as the first argument to key (pKey/nKey)
** in the tree structure. If an exact match for the key is found, leave the
** cursor pointing to it and set *pRes to zero before returning. If an
** exact match cannot be found, do one of the following:
**
**   * Leave the cursor pointing to the smallest element in the tree that 
**     is larger than the key and set *pRes to +1, or
**
**   * Leave the cursor pointing to the largest element in the tree that 
**     is smaller than the key and set *pRes to -1, or
**
**   * If the tree is empty, leave the cursor at EOF and set *pRes to -1.
*/
int lsmTreeCursorSeek(TreeCursor *pCsr, void *pKey, int nKey, int *pRes){
  int rc = LSM_OK;                /* Return code */
  lsm_db *pDb = pCsr->pDb;
  TreeHeader *pHdr = &pCsr->pDb->treehdr;
  int (*xCmp)(void *, int, void *, int) = pDb->xCmp;

  u32 iNodePtr;                   /* Location of current node in search */

  /* Discard any saved position data */
  treeCursorRestore(pCsr, 0);

  iNodePtr = pDb->treehdr.iRoot;
  if( iNodePtr==0 ){
    /* Either an error occurred or the tree is completely empty. */
    assert( rc!=LSM_OK || pDb->treehdr.iRoot==0 );
    *pRes = -1;
    pCsr->iNode = -1;
  }else{
    int res = 0;                  /* Result of comparison function */
    int iNode = -1;
    while( iNodePtr ){
      TreeNode *pNode;            /* Node at location iNodePtr */
      int iTest;                  /* Index of second key to test (0 or 2) */
      TreeKey *pTreeKey;          /* Key to compare against */

      pNode = (TreeNode *)treeShmptr(pDb, iNodePtr, &rc);
      iNode++;
      pCsr->apTreeNode[iNode] = pNode;

      /* Compare (pKey/nKey) with the key in the middle slot of B-tree node
      ** pNode. The middle slot is never empty. If the comparison is a match,
      ** then the search is finished. Break out of the loop. */
      pTreeKey = (TreeKey *)treeShmptr(pDb, pNode->aiKeyPtr[1], &rc);
      if( rc ) return rc;
      res = xCmp((void *)&pTreeKey[1], pTreeKey->nKey, pKey, nKey);
      if( res==0 ){
        pCsr->aiCell[iNode] = 1;
        break;
      }

      /* Based on the results of the previous comparison, compare (pKey/nKey)
      ** to either the left or right key of the B-tree node, if such a key
      ** exists. */
      iTest = (res>0 ? 0 : 2);
      pTreeKey = (TreeKey *)treeShmptr(pDb, pNode->aiKeyPtr[iTest], &rc);
      if( rc ) return rc;
      if( pTreeKey==0 ){
        iTest = 1;
      }else{
        res = xCmp((void *)&pTreeKey[1], pTreeKey->nKey, pKey, nKey);
        if( res==0 ){
          pCsr->aiCell[iNode] = iTest;
          break;
        }
      }

      if( iNode<(pHdr->nHeight-1) ){
        iNodePtr = getChildPtr(pNode, pDb->treehdr.iTransId, iTest + (res<0));
      }else{
        iNodePtr = 0;
      }
      pCsr->aiCell[iNode] = iTest + (iNodePtr && (res<0));
    }

    *pRes = res;
    pCsr->iNode = iNode;
  }

  /* assert() that *pRes has been set properly */
#ifndef NDEBUG
  if( lsmTreeCursorValid(pCsr) ){
    int cmp = treeCsrCompare(pCsr, pKey, nKey);
    assert( *pRes==cmp || (*pRes ^ cmp)>0 );
  }
#endif

  return rc;
}

int lsmTreeCursorNext(TreeCursor *pCsr){
#ifndef NDEBUG
  TreeKey *pK1;
#endif
  lsm_db *pDb = pCsr->pDb;
  const int iLeaf = pDb->treehdr.nHeight-1;
  int iCell; 
  int rc = LSM_OK; 
  TreeNode *pNode; 

  /* Restore the cursor position, if required */
  int iRestore = 0;
  treeCursorRestore(pCsr, &iRestore);
  if( iRestore>0 ) return LSM_OK;

  /* Save a pointer to the current key. This is used in an assert() at the
  ** end of this function - to check that the 'next' key really is larger
  ** than the current key. */
#ifndef NDEBUG
  pK1 = csrGetKey(pCsr, &rc);
  if( rc!=LSM_OK ) return rc;
#endif

  assert( lsmTreeCursorValid(pCsr) );
  assert( pCsr->aiCell[pCsr->iNode]<3 );

  pNode = pCsr->apTreeNode[pCsr->iNode];
  iCell = ++pCsr->aiCell[pCsr->iNode];

  /* If the current node is not a leaf, and the current cell has sub-tree
  ** associated with it, descend to the left-most key on the left-most
  ** leaf of the sub-tree.  */
  if( pCsr->iNode<iLeaf && getChildPtr(pNode, pDb->treehdr.iTransId, iCell) ){
    do {
      u32 iNodePtr;
      pCsr->iNode++;
      iNodePtr = getChildPtr(pNode, pDb->treehdr.iTransId, iCell);
      pNode = (TreeNode *)treeShmptr(pDb, iNodePtr, &rc);
      pCsr->apTreeNode[pCsr->iNode] = pNode;
      iCell = pCsr->aiCell[pCsr->iNode] = (pNode->aiKeyPtr[0]==0);
    }while( pCsr->iNode < iLeaf );
  }

  /* Otherwise, the next key is found by following pointer up the tree 
  ** until there is a key immediately to the right of the pointer followed 
  ** to reach the sub-tree containing the current key. */
  else if( iCell>=3 || pNode->aiKeyPtr[iCell]==0 ){
    while( (--pCsr->iNode)>=0 ){
      iCell = pCsr->aiCell[pCsr->iNode];
      if( iCell<3 && pCsr->apTreeNode[pCsr->iNode]->aiKeyPtr[iCell] ) break;
    }
  }

#ifndef NDEBUG
  if( pCsr->iNode>=0 ){
    TreeKey *pK2 = csrGetKey(pCsr, &rc);
    assert( rc || pDb->xCmp(TK_KEY(pK2), pK2->nKey, TK_KEY(pK1), pK1->nKey)>0 );
  }
#endif

  return rc;
}

int lsmTreeCursorPrev(TreeCursor *pCsr){
#ifndef NDEBUG
  TreeKey *pK1;
#endif
  lsm_db *pDb = pCsr->pDb;
  const int iLeaf = pDb->treehdr.nHeight-1;
  int iCell; 
  int rc = LSM_OK; 
  TreeNode *pNode; 

  /* Restore the cursor position, if required */
  int iRestore = 0;
  treeCursorRestore(pCsr, &iRestore);
  if( iRestore<0 ) return LSM_OK;

  /* Save a pointer to the current key. This is used in an assert() at the
  ** end of this function - to check that the 'next' key really is smaller
  ** than the current key. */
#ifndef NDEBUG
  pK1 = csrGetKey(pCsr, &rc);
  if( rc!=LSM_OK ) return rc;
#endif

  assert( lsmTreeCursorValid(pCsr) );
  pNode = pCsr->apTreeNode[pCsr->iNode];
  iCell = pCsr->aiCell[pCsr->iNode];
  assert( iCell>=0 && iCell<3 );

  /* If the current node is not a leaf, and the current cell has sub-tree
  ** associated with it, descend to the right-most key on the right-most
  ** leaf of the sub-tree.  */
  if( pCsr->iNode<iLeaf && getChildPtr(pNode, pDb->treehdr.iTransId, iCell) ){
    do {
      u32 iNodePtr;
      pCsr->iNode++;
      iNodePtr = getChildPtr(pNode, pDb->treehdr.iTransId, iCell);
      pNode = (TreeNode *)treeShmptr(pDb, iNodePtr, &rc);
      if( rc!=LSM_OK ) break;
      pCsr->apTreeNode[pCsr->iNode] = pNode;
      iCell = 1 + (pNode->aiKeyPtr[2]!=0) + (pCsr->iNode < iLeaf);
      pCsr->aiCell[pCsr->iNode] = iCell;
    }while( pCsr->iNode < iLeaf );
  }

  /* Otherwise, the next key is found by following pointer up the tree until
  ** there is a key immediately to the left of the pointer followed to reach
  ** the sub-tree containing the current key. */
  else{
    do {
      iCell = pCsr->aiCell[pCsr->iNode]-1;
      if( iCell>=0 && pCsr->apTreeNode[pCsr->iNode]->aiKeyPtr[iCell] ) break;
    }while( (--pCsr->iNode)>=0 );
    pCsr->aiCell[pCsr->iNode] = iCell;
  }

#ifndef NDEBUG
  if( pCsr->iNode>=0 ){
    TreeKey *pK2 = csrGetKey(pCsr, &rc);
    assert( rc || pDb->xCmp(TK_KEY(pK2), pK2->nKey, TK_KEY(pK1), pK1->nKey)<0 );
  }
#endif

  return rc;
}

/*
** Move the cursor to the first (bLast==0) or last (bLast!=0) entry in the
** in-memory tree.
*/
int lsmTreeCursorEnd(TreeCursor *pCsr, int bLast){
  lsm_db *pDb = pCsr->pDb;
  TreeHeader *pHdr = &pDb->treehdr;
  int rc = LSM_OK;

  u32 iNodePtr;
  pCsr->iNode = -1;

  /* Discard any saved position data */
  treeCursorRestore(pCsr, 0);

  iNodePtr = pHdr->iRoot;
  while( iNodePtr ){
    int iCell;
    TreeNode *pNode;

    pNode = (TreeNode *)treeShmptr(pDb, iNodePtr, &rc);
    if( rc ) break;

    if( bLast ){
      iCell = ((pNode->aiKeyPtr[2]==0) ? 2 : 3);
    }else{
      iCell = ((pNode->aiKeyPtr[0]==0) ? 1 : 0);
    }
    pCsr->iNode++;
    pCsr->apTreeNode[pCsr->iNode] = pNode;

    if( pCsr->iNode<pHdr->nHeight-1 ){
      iNodePtr = getChildPtr(pNode, pHdr->iTransId, iCell);
    }else{
      iNodePtr = 0;
    }
    pCsr->aiCell[pCsr->iNode] = iCell - (iNodePtr==0 && bLast);
  }

  return rc;
}

int lsmTreeCursorKey(TreeCursor *pCsr, void **ppKey, int *pnKey){
  TreeKey *pTreeKey;
  int rc = LSM_OK;

  assert( lsmTreeCursorValid(pCsr) );

  pTreeKey = pCsr->pSave;
  if( !pTreeKey ){
    pTreeKey = (TreeKey *)treeShmptr(pCsr->pDb,
        pCsr->apTreeNode[pCsr->iNode]->aiKeyPtr[pCsr->aiCell[pCsr->iNode]], &rc
    );
  }
  if( rc==LSM_OK ){
    *pnKey = pTreeKey->nKey;
    *ppKey = (void *)&pTreeKey[1];
  }

  return rc;
}

int lsmTreeCursorValue(TreeCursor *pCsr, void **ppVal, int *pnVal){
  int res = 0;
  int rc;

  rc = treeCursorRestore(pCsr, &res);
  if( res==0 ){
    TreeKey *pTreeKey;
    pTreeKey = (TreeKey *)treeShmptr(pCsr->pDb,
        pCsr->apTreeNode[pCsr->iNode]->aiKeyPtr[pCsr->aiCell[pCsr->iNode]], &rc
    );
    if( rc==LSM_OK ){
      *pnVal = pTreeKey->nValue;
      if( pTreeKey->nValue>=0 ){
        *ppVal = TK_VAL(pTreeKey);
      }else{
        *ppVal = 0;
      }
    }
  }else{
    *ppVal = 0;
    *pnVal = 0;
  }

  return rc;
}

/*
** Return true if the cursor currently points to a valid entry. 
*/
int lsmTreeCursorValid(TreeCursor *pCsr){
  return (pCsr && (pCsr->pSave || pCsr->iNode>=0));
}

/*
** Roll back to mark pMark. Structure *pMark should have been previously
** populated by a call to lsmTreeMark().
*/
void lsmTreeRollback(lsm_db *pDb, TreeMark *pMark){
  assert( 0 );
#if 0
  TreeVersion *pWorking = pDb->pTV;
  Tree *pTree = pWorking->pTree;
  TreeNode *p;

  assert( lsmTreeIsWriteVersion(pWorking) );

  pWorking->pRoot = (TreeNode *)pMark->pRoot;
  pWorking->nHeight = pMark->nHeight;

  if( pMark->pRollback ){
    p = ((TreeNode *)pMark->pRollback)->pNext;
  }else{
    p = pTree->pRbFirst;
  }

  while( p ){
    TreeNode *pNext = p->pNext;
    assert( p->iV2!=0 );
    assert( pNext || p==pTree->pRbLast );
    p->iV2 = 0;
    p->iV2Ptr = 0;
    p->pV2Ptr = 0;
    p->pNext = 0;
    p = pNext;
  }

  pTree->pRbLast = (TreeNode *)pMark->pRollback;
  if( pTree->pRbLast ){
    pTree->pRbLast->pNext = 0;
  }else{
    pTree->pRbFirst = 0;
  }

  lsmPoolRollback(pDb->pEnv, pTree->pPool, pMark->pMpChunk, pMark->iMpOff);
#endif
}

/*
** Store a mark in *pMark. Later on, a call to lsmTreeRollback() with a
** pointer to the same TreeMark structure may be used to roll the tree
** contents back to their current state.
*/
void lsmTreeMark(lsm_db *pDb, TreeMark *pMark){
#if 0
  Tree *pTree = pTV->pTree;
  memset(pMark, 0, sizeof(TreeMark));
  pMark->pRoot = (void *)pTV->pRoot;
  pMark->nHeight = pTV->nHeight;
  pMark->pRollback = (void *)pTree->pRbLast;
  lsmPoolMark(pTree->pPool, &pMark->pMpChunk, &pMark->iMpOff);

  assert( lsmTreeIsWriteVersion(pTV) );
  pTV->iVersion++;
#endif
}

/*
** This is called when a client wishes to upgrade from a read to a write
** transaction. If the read-version passed as the second version is the
** most recent one, decrement its ref-count and return a pointer to
** the write-version object. Otherwise return null. So we can do:
**
**     // Open read-transaction
**     pReadVersion = lsmTreeReadVersion(pTree);
**
**     // Later on, attempt to upgrade to write transaction
**     if( pWriteVersion = lsmTreeWriteVersion(pTree, pReadVersion) ){
**       // Have upgraded to a write transaction!
**     }else{
**       // Reading an out-of-date snapshot. Upgrade fails.
**     }
**
** The caller must take care of rejecting a clients attempt to upgrade to
** a write transaction *while* another client has a write transaction 
** underway. This mechanism merely prevents writing to an out-of-date
** snapshot.
*/
int lsmTreeWriteVersion(
  lsm_env *pEnv,
  Tree *pTree, 
  TreeVersion **ppVersion
){
  TreeVersion *pRead = *ppVersion;
  TreeVersion *pRet;

  /* The caller must ensure that no other write transaction is underway. */
  assert( pTree->pWorking==0 );
  
  if( pRead && pTree->pCommit!=pRead ) return LSM_BUSY;
  pRet = lsmMallocZero(pEnv, sizeof(TreeVersion));
  if( pRet==0 ) return LSM_NOMEM_BKPT;
  pTree->pWorking = pRet;

  memcpy(pRet, pTree->pCommit, sizeof(TreeVersion));
  pRet->nRef = 1;
  if( pRead ) pRead->nRef--;
  *ppVersion = pRet;
  assert( pRet->pTree==pTree );
  return LSM_OK;
}

static void treeIncrRefcount(Tree *pTree){
  pTree->nTreeRef++;
}

static void treeDecrRefcount(lsm_env *pEnv, Tree *pTree){
  assert( pTree->nTreeRef>0 );
  pTree->nTreeRef--;
  if( pTree->nTreeRef==0 ){
    assert( pTree->pWorking==0 );
    treeDestroy(pEnv, pTree);
  }
}

/*
** Release a reference to the write-version.
*/
int lsmTreeReleaseWriteVersion(
  lsm_env *pEnv,
  TreeVersion *pWorking,          /* Write-version reference */
  int bCommit,                    /* True for a commit */
  TreeVersion **ppReadVersion     /* OUT: Read-version reference */
){
  assert( 0 );
#if 0
  Tree *pTree = pWorking->pTree;

  assert( lsmTreeIsWriteVersion(pWorking) );
  assert( pWorking->nRef==1 );

  if( bCommit ){
    treeIncrRefcount(pTree);
    lsmTreeReleaseReadVersion(pEnv, pTree->pCommit);
    pTree->pCommit = pWorking;
  }else{
    lsmFree(pEnv, pWorking);
  }

  pTree->pWorking = 0;
  if( ppReadVersion ){
    *ppReadVersion = lsmTreeReadVersion(pTree);
  }
#endif
  return LSM_OK;
}

static void treeHeaderChecksum(
  TreeHeader *pHdr, 
  u32 *aCksum
){
  u32 cksum1 = 0x12345678;
  u32 cksum2 = 0x9ABCDEF0;
  u32 *a = (u32 *)pHdr;
  int i;

  assert( (offsetof(TreeHeader, aCksum) + sizeof(u32)*2)==sizeof(TreeHeader) );
  assert( (sizeof(TreeHeader) % (sizeof(u32)*2))==0 );

  for(i=0; i<(offsetof(TreeHeader, aCksum) / sizeof(u32)); i+=2){
    cksum1 += a[i];
    cksum2 += (cksum1 + a[i+1]);
  }
  aCksum[0] = cksum1;
  aCksum[1] = cksum2;
}

/*
** Return true if the checksum stored in TreeHeader object *pHdr is 
** consistent with the contents of its other fields.
*/
static int treeHeaderChecksumOk(TreeHeader *pHdr){
  u32 aCksum[2];
  treeHeaderChecksum(pHdr, aCksum);
  return (0==memcmp(aCksum, pHdr->aCksum, sizeof(aCksum)));
}

/*
** Load the in-memory tree header from shared-memory into pDb->treehdr.
** If the header cannot be loaded, return LSM_BUSY.
*/
int lsmTreeLoadHeader(lsm_db *pDb){
  while( 1 ){
    int rc;
    ShmHeader *pShm = pDb->pShmhdr;

    memcpy(&pDb->treehdr, &pShm->hdr1, sizeof(TreeHeader));
    if( treeHeaderChecksumOk(&pDb->treehdr) ) return LSM_OK;

    rc = lsmShmLock(pDb, LSM_LOCK_WRITER, LSM_LOCK_EXCL);
    if( rc==LSM_BUSY ){
      usleep(50);
    }else{
      if( rc==LSM_OK ){
        if( treeHeaderChecksumOk(&pShm->hdr1)==0 ){
          memcpy(&pShm->hdr1, &pShm->hdr2, sizeof(TreeHeader));
        }
        memcpy(&pDb->treehdr, &pShm->hdr1, sizeof(TreeHeader));
        lsmShmLock(pDb, LSM_LOCK_WRITER, LSM_LOCK_UNLOCK);

        if( treeHeaderChecksumOk(&pDb->treehdr)==0 ){
          rc = LSM_CORRUPT_BKPT;
        }
      }
      return rc;
    }
  }
}

/*
** This function is called to conclude a transaction. If argument bCommit
** is true, the transaction is committed. Otherwise it is rolled back.
*/
int lsmTreeEndTransaction(lsm_db *pDb, int bCommit){
  if( bCommit ){
    ShmHeader *pShm = pDb->pShmhdr;

    treeHeaderChecksum(&pDb->treehdr, pDb->treehdr.aCksum);
    memcpy(&pShm->hdr2, &pDb->treehdr, sizeof(TreeHeader));
    lsmShmBarrier(pDb);
    memcpy(&pShm->hdr1, &pDb->treehdr, sizeof(TreeHeader));
    pShm->bWriter = 0;
  }

  return LSM_OK;
}

int lsmTreeBeginTransaction(lsm_db *pDb){
  pDb->treehdr.iTransId++;
  return LSM_OK;
}

TreeVersion *lsmTreeRecoverVersion(Tree *pTree){
  return pTree->pCommit;
}

/*
** Return a reference to a TreeVersion structure that may be used to read
** the database. The reference should be released at some point in the future
** by calling lsmTreeReleaseReadVersion().
*/
TreeVersion *lsmTreeReadVersion(Tree *pTree){
  TreeVersion *pRet = pTree->pCommit;
  assert( pRet->nRef>0 );
  pRet->nRef++;
  return pRet;
}

/*
** Release a reference to a read-version.
*/
void lsmTreeReleaseReadVersion(lsm_env *pEnv, TreeVersion *pTreeVersion){
  if( pTreeVersion ){
    assert( pTreeVersion->nRef>0 );
    pTreeVersion->nRef--;
    if( pTreeVersion->nRef==0 ){
      Tree *pTree = pTreeVersion->pTree;
      lsmFree(pEnv, pTreeVersion);
      treeDecrRefcount(pEnv, pTree);
    }
  }
}

/*
** Return true if the tree-version passed as the first argument is writable. 
*/
int lsmTreeIsWriteVersion(TreeVersion *pTV){
  return (pTV==pTV->pTree->pWorking);
}

void lsmTreeRelease(lsm_env *pEnv, Tree *pTree){
  if( pTree ){
    assert( pTree->nTreeRef>0 && pTree->pCommit );
    lsmTreeReleaseReadVersion(pEnv, pTree->pCommit);
  }
}






