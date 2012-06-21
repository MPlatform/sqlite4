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
** Container for a key-value pair.
*/
struct TreeKey {
  void *pKey;                     /* Pointer to key */
  void *pValue;                   /* Pointer to value. May be NULL. */
  int nKey;                       /* Size of pKey in bytes */
  int nValue;                     /* Size of pValue. Or negative. */
};

/*
** A single tree node. A node structure may contain up to 3 key/value
** pairs. Internal (non-leaf) nodes have up to 4 children.
**
** TODO: Update the format of this to be more compact. Get it working
** first though...
*/
struct TreeNode {
  TreeKey *apKey[3];              /* Array of pointers to key-value pairs */

  /* The following fields are present for interior nodes only, not leaves. */
  TreeNode *apChild[4];           /* Array of pointers to child nodes */

  int iV2;                        /* Version number of v2 */
  u8 iV2Ptr;                      /* apChild[] entry replaced by pV2Ptr */
  TreeNode *pV2Ptr;               /* Substitute pointer */
  TreeNode *pNext;                /* Next in interior node rollback list */
};

struct TreeLeaf {
  TreeKey *apKey[3];              /* Array of pointers to key-value pairs */
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
static TreeNode *getChildPtr(TreeNode *p, int iVersion, int iCell){
  if( p->iV2 && p->iV2<=iVersion && iCell==p->iV2Ptr ) return p->pV2Ptr;
  return p->apChild[iCell];
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
  lsmStringExtend(pStr, nBlob);
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

void dump_node(TreeNode *pNode, int nIndent, int isNode){
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
}

void dump_node_contents(TreeNode *pNode, int iVersion, int nIndent, int isNode){
  int i;
  LsmString s;

  lsmStringInit(&s, NEED_ENV);
  lsmAppendIndent(&s, nIndent);
  for(i=0; i<3; i++){
    if( pNode->apKey[i] ){
      TreeKey *pKey = pNode->apKey[i];
      lsmAppendStrBlob(&s, pKey->pKey, pKey->nKey);
      lsmStringAppend(&s, "     ", -1);
    }
  }

  printf("%s\n", s.z);
  lsmStringClear(&s);

  for(i=0; i<4 && isNode>0; i++){
    TreeNode *pChild = getChildPtr(pNode, iVersion, i);
    if( pChild ){
      dump_node_contents(pChild, iVersion, nIndent + 2, isNode-1);
    }
  }
}

void dump_tree_contents(Tree *pTree, const char *zCaption){
  TreeVersion *p = pTree->pWorking ? pTree->pWorking : pTree->pCommit;
  printf("\n%s\n", zCaption);
  if( p->pRoot ){
    dump_node_contents(p->pRoot, WORKING_VERSION, 0, p->nHeight-1);
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

static TreeNode *newTreeLeaf(lsm_env *pEnv, Tree *pTree){
  return (TreeNode *)lsmPoolMallocZero(pEnv, pTree->pPool, sizeof(TreeLeaf));
}

static TreeNode *newTreeNode(lsm_env *pEnv, Tree *pTree){
  return (TreeNode *)lsmPoolMallocZero(pEnv, pTree->pPool, sizeof(TreeNode));
}

static TreeNode *copyTreeNode(lsm_env *pEnv, Tree *pTree, TreeNode *pOld){
  TreeNode *pNew;
  pNew = (TreeNode *)lsmPoolMallocZero(pEnv, pTree->pPool, sizeof(TreeNode));

  memcpy(pNew->apKey, pOld->apKey, sizeof(pNew->apKey));
  memcpy(pNew->apChild, pOld->apChild, sizeof(pNew->apChild));
  if( pOld->iV2 ) pNew->apChild[pOld->iV2Ptr] = pOld->pV2Ptr;

  return pNew;
}

static TreeNode *copyTreeLeaf(lsm_env *pEnv, Tree *pTree, TreeNode *pOld){
  TreeNode *pNew;
  pNew = newTreeLeaf(pEnv, pTree);
  memcpy(pNew, pOld, sizeof(TreeLeaf));
  return pNew;
}

/*
** Save the current position of tree cursor pCsr.
*/
void lsmTreeCursorSave(TreeCursor *pCsr){
  if( pCsr->pSave==0 ){
    int iNode = pCsr->iNode;
    if( iNode>=0 ){
      pCsr->pSave = pCsr->apTreeNode[iNode]->apKey[pCsr->aiCell[iNode]];
    }
    pCsr->iNode = -1;
  }
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
      rc = lsmTreeCursorSeek(pCsr, pKey->pKey, pKey->nKey, pRes);
    }
  }
  return rc;
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
static int treeUpdatePtr(Tree *pTree, TreeCursor *pCsr, TreeNode *pNew){
  int rc = LSM_OK;
  if( pCsr->iNode<0 ){
    /* pNew is the new root node */
    pTree->pWorking->pRoot = pNew;
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
      TreeNode *pCopy = copyTreeNode(pCsr->pDb->pEnv, pTree, p);
      if( pCopy ){
        pCopy->apChild[iChildPtr] = pNew;
        pCsr->iNode--;
        rc = treeUpdatePtr(pTree, pCsr, pCopy);
      }else{
        rc = LSM_NOMEM_BKPT;
      }
    }else{
      /* The "v2 data" option */
      p->iV2 = pTree->pWorking->iVersion;
      p->iV2Ptr = (u8)iChildPtr;
      p->pV2Ptr = (void *)pNew;
      if( pTree->pRbLast ){
        pTree->pRbLast->pNext = p;
      }else{
        pTree->pRbFirst = p;
      }
      pTree->pRbLast = p;
      assert( pTree->pRbLast->pNext==0 );
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
  lsm_env *pEnv,
  Tree *pTree, 
  TreeCursor *pCsr,               /* Cursor indicating path to insert at */
  TreeNode *pLeftPtr,             /* New child pointer (or NULL for leaves) */
  TreeKey *pTreeKey,              /* New key to insert */
  TreeNode *pRightPtr,            /* New child pointer (or NULL for leaves) */
  int iSlot                       /* Position to insert key into */
){
  int rc = LSM_OK;
  TreeNode *pNode = pCsr->apTreeNode[pCsr->iNode];

  /* Check if the leaf is currently full. If so, allocate a sibling node. */
  if( pNode->apKey[0] && pNode->apKey[2] ){
    TreeNode *pLeft;              /* New sibling node. */
    TreeNode *pRight;             /* Sibling of pLeft (either new or pNode) */

    pLeft = newTreeNode(pEnv, pTree);
    pRight = newTreeNode(pEnv, pTree);

    if( pCsr->iNode==0 ){
      /* pNode is the root of the tree. Grow the tree by one level. */
      TreeNode *pRoot;            /* New root node */

      pRoot = newTreeNode(pEnv, pTree);

      pLeft->apChild[1] = getChildPtr(pNode, WORKING_VERSION, 0);
      pLeft->apKey[1] = pNode->apKey[0];
      pLeft->apChild[2] = getChildPtr(pNode, WORKING_VERSION, 1);

      pRight->apChild[1] = getChildPtr(pNode, WORKING_VERSION, 2);
      pRight->apKey[1] = pNode->apKey[2];
      pRight->apChild[2] = getChildPtr(pNode, WORKING_VERSION, 3);

      pRoot->apKey[1] = pNode->apKey[1];
      pRoot->apChild[1] = pLeft;
      pRoot->apChild[2] = pRight;

      pTree->pWorking->pRoot = pRoot;
      pTree->pWorking->nHeight++;
    }else{
      TreeKey *pParentKey;        /* Key to insert into parent node */
      pParentKey = pNode->apKey[1];

      pLeft->apChild[1] = getChildPtr(pNode, WORKING_VERSION, 0);
      pLeft->apKey[1] = pNode->apKey[0];
      pLeft->apChild[2] = getChildPtr(pNode, WORKING_VERSION, 1);

      pRight->apChild[1] = getChildPtr(pNode, WORKING_VERSION, 2);
      pRight->apKey[1] = pNode->apKey[2];
      pRight->apChild[2] = getChildPtr(pNode, WORKING_VERSION, 3);

      pCsr->iNode--;
      treeInsert(pEnv, 
          pTree, pCsr, pLeft, pParentKey, pRight, pCsr->aiCell[pCsr->iNode]
      );
    }

    assert( pLeft->iV2==0 );
    assert( pRight->iV2==0 );
    switch( iSlot ){
      case 0:
        pLeft->apKey[0] = pTreeKey;
        pLeft->apChild[0] = pLeftPtr;
        if( pRightPtr ) pLeft->apChild[1] = pRightPtr;
        break;
      case 1:
        pLeft->apChild[3] = (pRightPtr ? pRightPtr : pLeft->apChild[2]);
        pLeft->apKey[2] = pTreeKey;
        pLeft->apChild[2] = pLeftPtr;
        break;
      case 2:
        pRight->apKey[0] = pTreeKey;
        pRight->apChild[0] = pLeftPtr;
        if( pRightPtr ) pRight->apChild[1] = pRightPtr;
        break;
      case 3:
        pRight->apChild[3] = (pRightPtr ? pRightPtr : pRight->apChild[2]);
        pRight->apKey[2] = pTreeKey;
        pRight->apChild[2] = pLeftPtr;
        break;
    }

  }else{
    TreeNode *pNew;
    TreeKey **pOut;
    TreeNode **pPtr;
    int i;

    pNew = newTreeNode(pEnv, pTree);
    if( pNew ){
      TreeNode *pStore = 0;
      pOut = pNew->apKey;
      pPtr = pNew->apChild;

      for(i=0; i<iSlot; i++){
        if( pNode->apKey[i] ){
          *(pOut++) = pNode->apKey[i];
          *(pPtr++) = getChildPtr(pNode, WORKING_VERSION, i);
        }
      }

      *pOut++ = pTreeKey;
      *pPtr++ = pLeftPtr;

      pStore = pRightPtr;
      for(i=iSlot; i<3; i++){
        if( pNode->apKey[i] ){
          *(pOut++) = pNode->apKey[i];
          *(pPtr++) = pStore ? pStore : getChildPtr(pNode, WORKING_VERSION, i);
          pStore = 0;
        }
      }
      if( pStore ){
        *pPtr = pStore;
      }else{
        *pPtr = getChildPtr(pNode, WORKING_VERSION, (pNode->apKey[2] ? 3 : 2));
      }

      pCsr->iNode--;
      rc = treeUpdatePtr(pTree, pCsr, pNew);
    }else{
      rc = LSM_NOMEM_BKPT;
    }
  }

  return rc;
}

static int treeInsertLeaf(
  lsm_env *pEnv,
  Tree *pTree,                    /* Tree structure */
  TreeCursor *pCsr,               /* Cursor structure */
  TreeKey *pTreeKey,              /* Key to insert */
  int iSlot                       /* Insert key to the left of this */
){
  int rc;                         /* Return code */
  TreeNode *pLeaf = pCsr->apTreeNode[pCsr->iNode];
  TreeNode *pNew;

  assert( iSlot>=0 && iSlot<=4 );
  assert( pCsr->iNode>0 );
  assert( pLeaf->apKey[1] );

  pCsr->iNode--;

  pNew = newTreeLeaf(pEnv, pTree);
  if( !pNew ){
    rc = LSM_NOMEM_BKPT;
  }else if( pLeaf->apKey[0] && pLeaf->apKey[2] ){
    TreeNode *pRight;

    pRight = newTreeLeaf(pEnv, pTree);
    if( pRight==0 ){
      rc = LSM_NOMEM_BKPT;
    }else{
      pNew->apKey[1] = pLeaf->apKey[0];
      pRight->apKey[1] = pLeaf->apKey[2];
      switch( iSlot ){
        case 0: pNew->apKey[0] = pTreeKey; break;
        case 1: pNew->apKey[2] = pTreeKey; break;
        case 2: pRight->apKey[0] = pTreeKey; break;
        case 3: pRight->apKey[2] = pTreeKey; break;
      }
      rc = treeInsert(pEnv, pTree, pCsr, pNew, pLeaf->apKey[1], pRight, 
          pCsr->aiCell[pCsr->iNode]
      );
    }
  }else{
    int iOut = 0;
    int i;
    for(i=0; i<4; i++){
      if( i==iSlot ) pNew->apKey[iOut++] = pTreeKey;
      if( i<3 && pLeaf->apKey[i] ) pNew->apKey[iOut++] = pLeaf->apKey[i];
    }
    rc = treeUpdatePtr(pTree, pCsr, pNew);
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
  lsm_env *pEnv = pDb->pEnv;
  TreeVersion *pTV = pDb->pTV;
  Tree *pTree = pTV->pTree;
  int rc = LSM_OK;                /* Return Code */
  TreeKey *pTreeKey;              /* New key-value being inserted */
  int nTreeKey;                   /* Number of bytes allocated at pTreeKey */

  assert( nVal>=0 || pVal==0 );
  assert( pTV==pTree->pWorking );
  assert_tree_looks_ok(LSM_OK, pTree);
  /* dump_tree_contents(pTree, "before"); */

  /* Allocate and populate a new key-value pair structure */
  nTreeKey = sizeof(TreeKey) + nKey + (nVal>0 ? nVal : 0);
  pTreeKey = (TreeKey *)lsmPoolMalloc(pDb->pEnv, pTree->pPool, nTreeKey);
  if( !pTreeKey ) return LSM_NOMEM_BKPT;
  pTreeKey->pKey = (void *)&pTreeKey[1];
  memcpy(pTreeKey->pKey, pKey, nKey);
  if( nVal>0 ){
    pTreeKey->pValue = (void *)&((u8 *)(pTreeKey->pKey))[nKey];
    memcpy(pTreeKey->pValue, pVal, nVal);
  }else{
    pTreeKey->pValue = 0;
  }
  pTreeKey->nValue = nVal;
  pTreeKey->nKey = nKey;

  if( pTree->pWorking->pRoot==0 ){
    /* The tree is completely empty. Add a new root node and install
    ** (pKey/nKey) as the middle entry. Even though it is a leaf at the
    ** moment, use newTreeNode() to allocate the node (i.e. allocate enough
    ** space for the fields used by interior nodes). This is because the
    ** treeInsert() routine may convert this node to an interior node.  
    */
    TreeNode *pRoot;              /* New tree root node */
    pRoot = newTreeNode(pEnv, pTree);
    if( !pRoot ){
      rc = LSM_NOMEM_BKPT;
    }else{
      pRoot->apKey[1] = pTreeKey;
      pTree->pWorking->pRoot = pRoot;
      assert( pTree->pWorking->nHeight==0 );
      pTree->pWorking->nHeight = 1;
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
      TreeNode *pNode = csr.apTreeNode[csr.iNode];
      int iCell = csr.aiCell[csr.iNode];

      /* Create a copy of this node */
      if( (csr.iNode>0 && csr.iNode==(pTree->pWorking->nHeight-1)) ){
        pNew = copyTreeLeaf(pEnv, pTree, pNode);
      }else{
        pNew = copyTreeNode(pEnv, pTree, pNode);
      }

      /* Modify the value in the new version */
      pNew->apKey[iCell] = pTreeKey;

      /* Change the pointer in the parent (if any) to point at the new 
      ** TreeNode */
      csr.iNode--;
      treeUpdatePtr(pTree, &csr, pNew);
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
        rc = treeInsert(pEnv, pTree, &csr, 0, pTreeKey, 0, iSlot);
      }else{
        rc = treeInsertLeaf(pEnv, pTree, &csr, pTreeKey, iSlot);
      }
    }
  }

  /* dump_tree_contents(pTree, "after"); */
  assert_tree_looks_ok(rc, pTree);
  return rc;
}

/*
** Return, in bytes, the amount of memory currently used by the tree 
** structure.
*/
int lsmTreeSize(TreeVersion *pTV){
  return (lsmPoolUsed(pTV->pTree->pPool) - ROUND8(sizeof(Tree)));
}

/*
** Return true if the tree is empty. Otherwise false.
**
** The caller is responsible for ensuring that it has exclusive access
** to the Tree structure for this call.
*/
int lsmTreeIsEmpty(Tree *pTree){
  assert( pTree==0 || pTree->pWorking==0 );
  return (pTree==0 || pTree->pCommit->pRoot==0);
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
  int cmp;
  assert( pCsr->iNode>=0 );
  p = pCsr->apTreeNode[pCsr->iNode]->apKey[pCsr->aiCell[pCsr->iNode]];
  cmp = memcmp(p->pKey, pKey, LSM_MIN(p->nKey, nKey));
  if( cmp==0 ){
    cmp = p->nKey - nKey;
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
  TreeVersion *p = pCsr->pDb->pTV;
  int (*xCmp)(void *, int, void *, int) = p->pTree->xCmp;
  TreeNode *pNode = p->pRoot;     /* Current node in search */

  /* Discard any saved position data */
  treeCursorRestore(pCsr, 0);

  if( pNode==0 ){
    /* A special case - the tree is completely empty. */
    *pRes = -1;
    pCsr->iNode = -1;
  }else{
    int res = 0;                  /* Result of comparison function */
    int iNode = -1;
    while( pNode ){
      int iTest;                  /* Index of second key to test (0 or 2) */
      TreeKey *pTreeKey;          /* Key to compare against */

      iNode++;
      pCsr->apTreeNode[iNode] = pNode;

      /* Compare (pKey/nKey) with the key in the middle slot of B-tree node
      ** pNode. The middle slot is never empty. If the comparison is a match,
      ** then the search is finished. Break out of the loop. */
      pTreeKey = pNode->apKey[1];
      res = xCmp(pTreeKey->pKey, pTreeKey->nKey, pKey, nKey);
      if( res==0 ){
        pCsr->aiCell[iNode] = 1;
        break;
      }

      /* Based on the results of the previous comparison, compare (pKey/nKey)
      ** to either the left or right key of the B-tree node, if such a key
      ** exists. */
      iTest = (res>0 ? 0 : 2);
      pTreeKey = pNode->apKey[iTest];
      if( pTreeKey==0 ){
        iTest = 1;
      }else{
        res = xCmp(pTreeKey->pKey, pTreeKey->nKey, pKey, nKey);
        if( res==0 ){
          pCsr->aiCell[iNode] = iTest;
          break;
        }
      }

      if( iNode<(p->nHeight-1) ){
        pNode = getChildPtr(pNode, p->iVersion, iTest + (res<0));
      }else{
        pNode = 0;
      }
      pCsr->aiCell[iNode] = iTest + (pNode && (res<0));
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

  return LSM_OK;
}

int lsmTreeCursorNext(TreeCursor *pCsr){
#ifndef NDEBUG
  TreeKey *pK1;
#endif

  TreeVersion *p = pCsr->pDb->pTV;
  const int iLeaf = p->nHeight-1;
  int iCell; 
  TreeNode *pNode; 

  /* Restore the cursor position, if required */
  int iRestore = 0;
  treeCursorRestore(pCsr, &iRestore);
  if( iRestore>0 ) return LSM_OK;

  /* Save a pointer to the current key. This is used in an assert() at the
  ** end of this function - to check that the 'next' key really is larger
  ** than the current key. */
#ifndef NDEBUG
  pK1 = pCsr->apTreeNode[pCsr->iNode]->apKey[pCsr->aiCell[pCsr->iNode]];
#endif

  assert( lsmTreeCursorValid(pCsr) );
  assert( pCsr->aiCell[pCsr->iNode]<3 );

  pNode = pCsr->apTreeNode[pCsr->iNode];
  iCell = ++pCsr->aiCell[pCsr->iNode];

  /* If the current node is not a leaf, and the current cell has sub-tree
  ** associated with it, descend to the left-most key on the left-most
  ** leaf of the sub-tree.  */
  if( pCsr->iNode<iLeaf && getChildPtr(pNode, p->iVersion, iCell) ){
    do {
      pCsr->iNode++;
      pNode = getChildPtr(pNode, p->iVersion, iCell);
      pCsr->apTreeNode[pCsr->iNode] = pNode;
      iCell = pCsr->aiCell[pCsr->iNode] = (pNode->apKey[0]==0);
    }while( pCsr->iNode < iLeaf );
  }

  /* Otherwise, the next key is found by following pointer up the tree 
  ** until there is a key immediately to the right of the pointer followed 
  ** to reach the sub-tree containing the current key. */
  else if( iCell>=3 || pNode->apKey[iCell]==0 ){
    while( (--pCsr->iNode)>=0 ){
      iCell = pCsr->aiCell[pCsr->iNode];
      if( iCell<3 && pCsr->apTreeNode[pCsr->iNode]->apKey[iCell] ) break;
    }
  }

#ifndef NDEBUG
  if( pCsr->iNode>=0 ){
    TreeKey *pK2;
    int (*xCmp)(void *, int, void *, int) = pCsr->pDb->xCmp;
    pK2 = pCsr->apTreeNode[pCsr->iNode]->apKey[pCsr->aiCell[pCsr->iNode]];
    assert( xCmp(pK2->pKey, pK2->nKey, pK1->pKey, pK1->nKey)>0 );
  }
#endif

  return LSM_OK;
}

int lsmTreeCursorPrev(TreeCursor *pCsr){
#ifndef NDEBUG
  TreeKey *pK1;
#endif

  TreeVersion *p = pCsr->pDb->pTV;
  const int iLeaf = p->nHeight-1;
  int iCell; 
  TreeNode *pNode; 

  /* Restore the cursor position, if required */
  int iRestore = 0;
  treeCursorRestore(pCsr, &iRestore);
  if( iRestore<0 ) return LSM_OK;

  /* Save a pointer to the current key. This is used in an assert() at the
  ** end of this function - to check that the 'next' key really is smaller
  ** than the current key. */
#ifndef NDEBUG
  pK1 = pCsr->apTreeNode[pCsr->iNode]->apKey[pCsr->aiCell[pCsr->iNode]];
#endif

  assert( lsmTreeCursorValid(pCsr) );
  pNode = pCsr->apTreeNode[pCsr->iNode];
  iCell = pCsr->aiCell[pCsr->iNode];
  assert( iCell>=0 && iCell<3 );

  /* If the current node is not a leaf, and the current cell has sub-tree
  ** associated with it, descend to the right-most key on the right-most
  ** leaf of the sub-tree.  */
  if( pCsr->iNode<iLeaf && getChildPtr(pNode, p->iVersion, iCell) ){
    do {
      pCsr->iNode++;
      pNode = getChildPtr(pNode, p->iVersion, iCell);
      pCsr->apTreeNode[pCsr->iNode] = pNode;
      iCell = 1 + (pNode->apKey[2]!=0) + (pCsr->iNode < iLeaf);
      pCsr->aiCell[pCsr->iNode] = iCell;
    }while( pCsr->iNode < iLeaf );
  }

  /* Otherwise, the next key is found by following pointer up the tree until
  ** there is a key immediately to the left of the pointer followed to reach
  ** the sub-tree containing the current key. */
  else{
    do {
      iCell = pCsr->aiCell[pCsr->iNode]-1;
      if( iCell>=0 && pCsr->apTreeNode[pCsr->iNode]->apKey[iCell] ) break;
    }while( (--pCsr->iNode)>=0 );
    pCsr->aiCell[pCsr->iNode] = iCell;
  }

#ifndef NDEBUG
  if( pCsr->iNode>=0 ){
    TreeKey *pK2;
    int (*xCmp)(void *, int, void *, int) = pCsr->pDb->xCmp;
    pK2 = pCsr->apTreeNode[pCsr->iNode]->apKey[pCsr->aiCell[pCsr->iNode]];
    assert( xCmp(pK2->pKey, pK2->nKey, pK1->pKey, pK1->nKey)<0 );
  }
#endif

  return LSM_OK;
}

/*
** Move the cursor to the first (bLast==0) or last (bLast!=0) entry in the
** in-memory tree.
*/
int lsmTreeCursorEnd(TreeCursor *pCsr, int bLast){
  TreeVersion *p = pCsr->pDb->pTV;
  TreeNode *pNode = p->pRoot;
  pCsr->iNode = -1;

  /* Discard any saved position data */
  treeCursorRestore(pCsr, 0);

  while( pNode ){
    int iCell;
    if( bLast ){
      iCell = ((pNode->apKey[2]==0) ? 2 : 3);
    }else{
      iCell = ((pNode->apKey[0]==0) ? 1 : 0);
    }

    pCsr->iNode++;
    pCsr->apTreeNode[pCsr->iNode] = pNode;

    if( pCsr->iNode<p->nHeight-1 ){
      pNode = getChildPtr(pNode, p->iVersion, iCell);
    }else{
      pNode = 0;
    }
    pCsr->aiCell[pCsr->iNode] = iCell - (pNode==0 && bLast);
  }
  return LSM_OK;
}

int lsmTreeCursorKey(TreeCursor *pCsr, void **ppKey, int *pnKey){
  TreeKey *pTreeKey;
  assert( lsmTreeCursorValid(pCsr) );

  pTreeKey = pCsr->pSave;
  if( !pTreeKey ){
    pTreeKey = pCsr->apTreeNode[pCsr->iNode]->apKey[pCsr->aiCell[pCsr->iNode]];
  }
  *ppKey = pTreeKey->pKey;
  *pnKey = pTreeKey->nKey;

  return LSM_OK;
}

int lsmTreeCursorValue(TreeCursor *pCsr, void **ppVal, int *pnVal){
  TreeKey *pTreeKey;
  int res = 0;

  treeCursorRestore(pCsr, &res);
  if( res==0 ){
    pTreeKey = pCsr->apTreeNode[pCsr->iNode]->apKey[pCsr->aiCell[pCsr->iNode]];
    *ppVal = pTreeKey->pValue;
    *pnVal = pTreeKey->nValue;
  }else{
    *ppVal = 0;
    *pnVal = 0;
  }

  return LSM_OK;
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
}

/*
** Store a mark in *pMark. Later on, a call to lsmTreeRollback() with a
** pointer to the same TreeMark structure may be used to roll the tree
** contents back to their current state.
*/
void lsmTreeMark(TreeVersion *pTV, TreeMark *pMark){
  Tree *pTree = pTV->pTree;
  memset(pMark, 0, sizeof(TreeMark));
  pMark->pRoot = (void *)pTV->pRoot;
  pMark->nHeight = pTV->nHeight;
  pMark->pRollback = (void *)pTree->pRbLast;
  lsmPoolMark(pTree->pPool, &pMark->pMpChunk, &pMark->iMpOff);

  assert( lsmTreeIsWriteVersion(pTV) );
  pTV->iVersion++;
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
