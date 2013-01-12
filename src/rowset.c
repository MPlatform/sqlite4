/*
** 2008 December 3
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
** This module implements an object called a "RowSet".
**
** The RowSet object is a collection of database keys.  Keys
** are inserted into the RowSet in an arbitrary order.  Inserts
** can be intermixed with tests to see if a given key is present
** in the RowSet.
**
** After all inserts are finished, it is possible to extract the
** elements of the RowSet in sorted order.  Once this extraction
** process has started, no new elements may be inserted.
**
** Hence, the primitive operations for a RowSet are:
**
**    CREATE
**    INSERT
**    TEST
**    SMALLEST
**    DESTROY
**
** The CREATE and DESTROY primitives are the constructor and destructor,
** obviously.  The INSERT primitive adds a new element to the RowSet.
** TEST checks to see if an element is already in the RowSet.  SMALLEST
** extracts the least value from the RowSet.
**
** The INSERT primitive might allocate additional memory.  Memory is
** allocated in chunks so most INSERTs do no allocation.  There is an 
** upper bound on the size of allocated memory.  No memory is freed
** until DESTROY.
**
** The TEST primitive includes a "batch" number.  The TEST primitive
** will only see elements that were inserted before the last change
** in the batch number.  In other words, if an INSERT occurs between
** two TESTs where the TESTs have the same batch nubmer, then the
** value added by the INSERT will not be visible to the second TEST.
** The initial batch number is zero, so if the very first TEST contains
** a non-zero batch number, it will see all prior INSERTs.
**
** No INSERTs may occurs after a SMALLEST.  An assertion will fail if
** that is attempted.
**
** The cost of an INSERT is roughly constant.  (Sometime new memory
** has to be allocated on an INSERT.)  The cost of a TEST with a new
** batch number is O(NlogN) where N is the number of elements in the RowSet.
** The cost of a TEST using the same batch number is O(logN).  The cost
** of the first SMALLEST is O(NlogN).  Second and subsequent SMALLEST
** primitives are constant time.  The cost of DESTROY is O(N).
** There is an added cost of O(N) when switching between TEST and
** SMALLEST primitives.
*/
#include "sqliteInt.h"

typedef struct RowSetEntry RowSetEntry;
typedef struct RowSetChunk RowSetChunk;

/*
** Target size for allocation chunks.
*/

/*
** Total size of allocation chunks, and the number of bytes of usable 
** space per chunk.
*/
#define ROWSET_ALLOCATION_SIZE 1024
#define ROWSET_BYTES_PER_CHUNK (ROWSET_ALLOCATION_SIZE - sizeof(RowSetChunk))

/*
** Each entry in a RowSet is an instance of the following object.
*/
struct RowSetEntry {            
  struct RowSetEntry *pRight;     /* Right subtree (larger entries) or list */
  struct RowSetEntry *pLeft;      /* Left subtree (smaller entries) */
  int nKey;                       /* Number of bytes in buffer aKey[] */
  u8 aKey[0];                     /* Buffer containing nKey byte key */
};

/*
** RowSetEntry objects are allocated in large chunks (instances of the
** following structure) to reduce memory allocation overhead.  The
** chunks are kept on a linked list so that they can be deallocated
** when the RowSet is destroyed.
*/
struct RowSetChunk {
  struct RowSetChunk *pNextChunk; /* Next chunk on list of them all */
};

/*
** A RowSet in an instance of the following structure.
**
** A typedef of this structure if found in sqliteInt.h.
*/
struct RowSet {
  struct RowSetChunk *pChunk;    /* List of all chunk allocations */
  sqlite4 *db;                   /* The database connection */
  struct RowSetEntry *pEntry;    /* List of entries using pRight */
  struct RowSetEntry *pLast;     /* Last entry on the pEntry list */
  struct RowSetEntry *pTree;     /* Binary tree of entries */
  u8 *aSpace;                    /* Space for new entries */
  u16 nSpace;                    /* Number of bytes in buffer aSpace */
  u8 isSorted;                   /* True if pEntry is sorted */
  u8 iBatch;                     /* Current insert batch */
};

/*
** Turn bulk memory into a RowSet object.  N bytes of memory
** are available at pSpace.  The db pointer is used as a memory context
** for any subsequent allocations that need to occur.
** Return a pointer to the new RowSet object.
**
** It must be the case that N is sufficient to make a Rowset.  If not
** an assertion fault occurs.
** 
** If N is larger than the minimum, use the surplus as an initial
** allocation of entries available to be filled.
*/
RowSet *sqlite4RowSetInit(sqlite4 *db, void *pSpace, unsigned int N){
  RowSet *p;
  assert( N >= ROUND8(sizeof(*p)) );
  p = pSpace;
  p->pChunk = 0;
  p->db = db;
  p->pEntry = 0;
  p->pLast = 0;
  p->pTree = 0;
  p->aSpace = 0;
  p->nSpace = 0;
  p->isSorted = 1;
  p->iBatch = 0;
  return p;
}

/*
** Deallocate all chunks from a RowSet.  This frees all memory that
** the RowSet has allocated over its lifetime.  This routine is
** the destructor for the RowSet.
*/
void sqlite4RowSetClear(RowSet *p){
  struct RowSetChunk *pChunk, *pNextChunk;
  for(pChunk=p->pChunk; pChunk; pChunk = pNextChunk){
    pNextChunk = pChunk->pNextChunk;
    sqlite4DbFree(p->db, pChunk);
  }
  memset(p, 0, sizeof(RowSet));
  p->isSorted = 1;
}


static u8 *rowsetAllocateChunk(RowSet *p, int nByte){
  int rowChunkSize = ROUND8(sizeof(RowSetChunk));
  RowSetChunk *pNew;              /* New RowSetChunk */
  int nAlloc;                     /* Bytes to request from malloc() */
  nAlloc =  rowChunkSize + nByte;
  pNew = (RowSetChunk *)sqlite4DbMallocRaw(p->db, nAlloc);
  if( !pNew ) return 0;
  pNew->pNextChunk = p->pChunk;
  p->pChunk = pNew;
  return (u8 *)(&pNew[1]);
}

static int rowsetEntryKeyCmp(RowSetEntry *pLeft, const u8 *aKey, int nKey){
  int nCmp = SQLITE4_MIN(pLeft->nKey, nKey);
  int res;
  res = memcmp(pLeft->aKey, aKey, nCmp);
  return (res ? res : (pLeft->nKey - nKey));
}

static int rowsetEntryCmp(RowSetEntry *pLeft, RowSetEntry *pRight){
  return rowsetEntryKeyCmp(pLeft, pRight->aKey, pRight->nKey);
}


/*
** Insert a new database key into a RowSet.
**
** The mallocFailed flag of the database connection is set if a
** memory allocation fails.
*/
void sqlite4RowSetInsert(RowSet *p, u8 *aKey, int nKey){
  int nByte;                   /* Space (in bytes) required by new entry */
  struct RowSetEntry *pEntry;  /* The new entry */
  struct RowSetEntry *pLast;   /* The last prior entry */
  assert( p!=0 );

  nByte = ROUND8(sizeof(RowSetEntry) + nKey);

  if( nByte>(ROWSET_BYTES_PER_CHUNK/4) ){
    /* This is quite a large key. Store it in a chunk of its own. */
    pEntry = (RowSetEntry *)rowsetAllocateChunk(p, nByte);
  }else{
    if( nByte>p->nSpace ){
      p->aSpace = rowsetAllocateChunk(p, ROWSET_BYTES_PER_CHUNK);
      p->nSpace = ROWSET_BYTES_PER_CHUNK;
    }
    pEntry = (RowSetEntry *)p->aSpace;
    p->aSpace += nByte;
    p->nSpace -= nByte;
  }
  if( pEntry==0 ) return;

  pEntry->pRight = 0;
  pEntry->pLeft = 0;
  pEntry->nKey = nKey;
  memcpy(pEntry->aKey, aKey, nKey);

  pLast = p->pLast;
  if( pLast ){
    if( p->isSorted && rowsetEntryCmp(pEntry, pLast)<0 ){
      p->isSorted = 0;
    }
    pLast->pRight = pEntry;
  }else{
    assert( p->pEntry==0 ); /* Fires if INSERT after SMALLEST */
    p->pEntry = pEntry;
  }
  p->pLast = pEntry;
}

/*
** Merge two lists of RowSetEntry objects.  Remove duplicates.
**
** The input lists are connected via pRight pointers and are 
** assumed to each already be in sorted order.
*/
static struct RowSetEntry *rowSetMerge(
  struct RowSetEntry *pA,    /* First sorted list to be merged */
  struct RowSetEntry *pB     /* Second sorted list to be merged */
){
  struct RowSetEntry head;
  struct RowSetEntry *pTail;

  pTail = &head;
  while( pA && pB ){
    int res;
    assert( pA->pRight==0 || rowsetEntryCmp(pA, pA->pRight)<=0 );
    assert( pB->pRight==0 || rowsetEntryCmp(pB, pB->pRight)<=0 );
    res = rowsetEntryCmp(pA, pB);
    if( res<0 ){
      pTail->pRight = pA;
      pA = pA->pRight;
      pTail = pTail->pRight;
    }else if( res>0 ){
      pTail->pRight = pB;
      pB = pB->pRight;
      pTail = pTail->pRight;
    }else{
      pA = pA->pRight;
    }
  }
  if( pA ){
    assert( pA->pRight==0 || rowsetEntryCmp(pA, pA->pRight)<=0 );
    pTail->pRight = pA;
  }else{
    assert( pB==0 || pB->pRight==0 || rowsetEntryCmp(pB, pB->pRight)<=0 );
    pTail->pRight = pB;
  }
  return head.pRight;
}

/*
** Sort all elements on the pEntry list of the RowSet into ascending order.
*/ 
static void rowSetSort(RowSet *p){
  unsigned int i;
  struct RowSetEntry *pEntry;
  struct RowSetEntry *aBucket[40];

  assert( p->isSorted==0 );
  memset(aBucket, 0, sizeof(aBucket));
  while( p->pEntry ){
    pEntry = p->pEntry;
    p->pEntry = pEntry->pRight;
    pEntry->pRight = 0;
    for(i=0; aBucket[i]; i++){
      pEntry = rowSetMerge(aBucket[i], pEntry);
      aBucket[i] = 0;
    }
    aBucket[i] = pEntry;
  }
  pEntry = 0;
  for(i=0; i<sizeof(aBucket)/sizeof(aBucket[0]); i++){
    pEntry = rowSetMerge(pEntry, aBucket[i]);
  }
  p->pEntry = pEntry;
  p->pLast = 0;
  p->isSorted = 1;
}


/*
** The input, pIn, is a binary tree (or subtree) of RowSetEntry objects.
** Convert this tree into a linked list connected by the pRight pointers
** and return pointers to the first and last elements of the new list.
*/
static void rowSetTreeToList(
  struct RowSetEntry *pIn,         /* Root of the input tree */
  struct RowSetEntry **ppFirst,    /* Write head of the output list here */
  struct RowSetEntry **ppLast      /* Write tail of the output list here */
){
  assert( pIn!=0 );
  if( pIn->pLeft ){
    struct RowSetEntry *p;
    rowSetTreeToList(pIn->pLeft, ppFirst, &p);
    p->pRight = pIn;
  }else{
    *ppFirst = pIn;
  }
  if( pIn->pRight ){
    rowSetTreeToList(pIn->pRight, &pIn->pRight, ppLast);
  }else{
    *ppLast = pIn;
  }
  assert( (*ppLast)->pRight==0 );
}


/*
** Convert a sorted list of elements (connected by pRight) into a binary
** tree with depth of iDepth.  A depth of 1 means the tree contains a single
** node taken from the head of *ppList.  A depth of 2 means a tree with
** three nodes.  And so forth.
**
** Use as many entries from the input list as required and update the
** *ppList to point to the unused elements of the list.  If the input
** list contains too few elements, then construct an incomplete tree
** and leave *ppList set to NULL.
**
** Return a pointer to the root of the constructed binary tree.
*/
static struct RowSetEntry *rowSetNDeepTree(
  struct RowSetEntry **ppList,
  int iDepth
){
  struct RowSetEntry *p;         /* Root of the new tree */
  struct RowSetEntry *pLeft;     /* Left subtree */
  if( *ppList==0 ){
    return 0;
  }
  if( iDepth==1 ){
    p = *ppList;
    *ppList = p->pRight;
    p->pLeft = p->pRight = 0;
    return p;
  }
  pLeft = rowSetNDeepTree(ppList, iDepth-1);
  p = *ppList;
  if( p==0 ){
    return pLeft;
  }
  p->pLeft = pLeft;
  *ppList = p->pRight;
  p->pRight = rowSetNDeepTree(ppList, iDepth-1);
  return p;
}

/*
** Convert a sorted list of elements into a binary tree. Make the tree
** as deep as it needs to be in order to contain the entire list.
*/
static struct RowSetEntry *rowSetListToTree(struct RowSetEntry *pList){
  int iDepth;           /* Depth of the tree so far */
  struct RowSetEntry *p;       /* Current tree root */
  struct RowSetEntry *pLeft;   /* Left subtree */

  assert( pList!=0 );
  p = pList;
  pList = p->pRight;
  p->pLeft = p->pRight = 0;
  for(iDepth=1; pList; iDepth++){
    pLeft = p;
    p = pList;
    pList = p->pRight;
    p->pLeft = pLeft;
    p->pRight = rowSetNDeepTree(&pList, iDepth);
  }
  return p;
}

/*
** Convert the list in p->pEntry into a sorted list if it is not
** sorted already.  If there is a binary tree on p->pTree, then
** convert it into a list too and merge it into the p->pEntry list.
*/
static void rowSetToList(RowSet *p){
  if( !p->isSorted ){
    rowSetSort(p);
  }
  if( p->pTree ){
    struct RowSetEntry *pHead, *pTail;
    rowSetTreeToList(p->pTree, &pHead, &pTail);
    p->pTree = 0;
    p->pEntry = rowSetMerge(p->pEntry, pHead);
  }
}

/*
** Extract the smallest element from the RowSet.
** Write the element into *pRowid.  Return 1 on success.  Return
** 0 if the RowSet is already empty.
**
** After this routine has been called, the sqlite4RowSetInsert()
** routine may not be called again.  
*/
int sqlite4RowSetNext(RowSet *p){
  rowSetToList(p);
  assert( p->pEntry );
  p->pEntry = p->pEntry->pRight;
  return (p->pEntry!=0);
}

const u8 *sqlite4RowSetRead(RowSet *p, int *pnKey){
  const u8 *aRet = 0;
  rowSetToList(p);
  if( p->pEntry ){
    *pnKey = p->pEntry->nKey;
    aRet = p->pEntry->aKey;
  }
  return aRet;
}

/*
** Check to see if element aKey/nKey has been inserted into the the 
** rowset as part of any insert batch prior to iBatch.  Return 1 or 0.
*/
int sqlite4RowSetTest(RowSet *pRowSet, u8 iBatch, u8 *aKey, int nKey){
  struct RowSetEntry *p;
  if( iBatch!=pRowSet->iBatch ){
    if( pRowSet->pEntry ){
      rowSetToList(pRowSet);
      pRowSet->pTree = rowSetListToTree(pRowSet->pEntry);
      pRowSet->pEntry = 0;
      pRowSet->pLast = 0;
    }
    pRowSet->iBatch = iBatch;
  }
  p = pRowSet->pTree;
  while( p ){
    int res = rowsetEntryKeyCmp(p, aKey, nKey);
    if( res<0 ){
      p = p->pRight;
    }else if( res>0 ){
      p = p->pLeft;
    }else{
      return 1;
    }
  }
  return 0;
}
