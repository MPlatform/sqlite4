/*
** 2011-08-14
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
** PAGE FORMAT:
**
**   The maximum page size is 65536 bytes.
**
**   Since all records are equal to or larger than 2 bytes in size, and 
**   some space within the page is consumed by the page footer, there must
**   be less than 2^15 records on each page.
**
**   Each page ends with a footer that describes the pages contents. This
**   footer serves as similar purpose to the page header in an SQLite database.
**   A footer is used instead of a header because it makes it easier to
**   populate a new page based on a sorted list of key/value pairs.
**
**   The footer consists of the following values (starting at the end of
**   the page and continuing backwards towards the start). All values are
**   stored as unsigned big-endian integers.
**
**     * Number of records on page (2 bytes).
**     * Flags field (2 bytes).
**     * Left-hand pointer value (8 bytes).
**     * The starting offset of each record (2 bytes per record).
**
**   Records may span pages. Unless it happens to be an exact fit, the part
**   of the final record that starts on page X that does not fit on page X
**   is stored at the start of page (X+1). This means there may be pages where
**   (N==0). And on most pages the first record that starts on the page will
**   not start at byte offset 0. For example:
**
**      aaaaa bbbbb ccc <footer>    cc eeeee fffff g <footer>    gggg....
**
** RECORD FORMAT:
** 
**   The first byte of the record is a flags byte. It is a combination
**   of the following flags (defined in lsmInt.h):
**
**       LSM_START_DELETE
**       LSM_END_DELETE 
**       LSM_POINT_DELETE
**       LSM_INSERT    
**       LSM_SEPARATOR
**       LSM_SYSTEMKEY
**
**   Immediately following the type byte is a pointer to the smallest key 
**   in the next file that is larger than the key in the current record. The 
**   pointer is encoded as a varint. When added to the 32-bit page number 
**   stored in the footer, it is the page number of the page that contains the
**   smallest key in the next sorted file that is larger than this key. 
**
**   Next is the number of bytes in the key, encoded as a varint.
**
**   If the LSM_INSERT flag is set, the number of bytes in the value, as
**   a varint, is next.
**
**   Finally, the blob of data containing the key, and for LSM_INSERT
**   records, the value as well.
*/

#ifndef _LSM_INT_H
# include "lsmInt.h"
#endif
#include "sqlite4.h"            /* only for sqlite4_snprintf() */

/*
** Macros to help decode record types.
*/
#define rtTopic(eType)       ((eType) & LSM_SYSTEMKEY)
#define rtIsDelete(eType)    (((eType) & 0x0F)==LSM_POINT_DELETE)

#define rtIsSeparator(eType) (((eType) & LSM_SEPARATOR)!=0)
#define rtIsWrite(eType)     (((eType) & LSM_INSERT)!=0)
#define rtIsSystem(eType)    (((eType) & LSM_SYSTEMKEY)!=0)

/*
** The following macros are used to access a page footer.
*/
#define SEGMENT_NRECORD_OFFSET(pgsz)        ((pgsz) - 2)
#define SEGMENT_FLAGS_OFFSET(pgsz)          ((pgsz) - 2 - 2)
#define SEGMENT_POINTER_OFFSET(pgsz)        ((pgsz) - 2 - 2 - 8)
#define SEGMENT_CELLPTR_OFFSET(pgsz, iCell) ((pgsz) - 2 - 2 - 8 - 2 - (iCell)*2)

#define SEGMENT_EOF(pgsz, nEntry) SEGMENT_CELLPTR_OFFSET(pgsz, nEntry)

#define SEGMENT_BTREE_FLAG     0x0001
#define PGFTR_SKIP_NEXT_FLAG   0x0002
#define PGFTR_SKIP_THIS_FLAG   0x0004

typedef struct SegmentPtr SegmentPtr;
typedef struct Blob Blob;

struct Blob {
  lsm_env *pEnv;
  void *pData;
  int nData;
  int nAlloc;
};

/*
** A SegmentPtr object may be used for one of two purposes:
**
**   * To iterate and/or seek within a single Segment (the combination of a 
**     main run and an optional sorted run).
**
**   * To iterate through the separators array of a segment.
*/
struct SegmentPtr {
  Level *pLevel;                /* Level object segment is part of */
  Segment *pSeg;                /* Segment to access */

  /* Current page. See segmentPtrLoadPage(). */
  Page *pPg;                    /* Current page */
  u16 flags;                    /* Copy of page flags field */
  int nCell;                    /* Number of cells on pPg */
  Pgno iPtr;                    /* Base cascade pointer */

  /* Current cell. See segmentPtrLoadCell() */
  int iCell;                    /* Current record within page pPg */
  int eType;                    /* Type of current record */
  Pgno iPgPtr;                  /* Cascade pointer offset */
  void *pKey; int nKey;         /* Key associated with current record */
  void *pVal; int nVal;         /* Current record value (eType==WRITE only) */

  /* Blobs used to allocate buffers for pKey and pVal as required */
  Blob blob1;
  Blob blob2;
};

/*
** Used to iterate through the keys stored in a b-tree hierarchy from start
** to finish. Only First() and Next() operations are required.
**
**   btreeCursorNew()
**   btreeCursorFirst()
**   btreeCursorNext()
**   btreeCursorFree()
**   btreeCursorPosition()
**   btreeCursorRestore()
*/
typedef struct BtreePg BtreePg;
typedef struct BtreeCursor BtreeCursor;
struct BtreePg {
  Page *pPage;
  int iCell;
};
struct BtreeCursor {
  Segment *pSeg;                  /* Iterate through this segments btree */
  FileSystem *pFS;                /* File system to read pages from */
  int nDepth;                     /* Allocated size of aPg[] */
  int iPg;                        /* Current entry in aPg[]. -1 -> EOF. */
  BtreePg *aPg;                   /* Pages from root to current location */

  /* Cache of current entry. pKey==0 for EOF. */
  void *pKey;
  int nKey;
  int eType;
  Pgno iPtr;

  /* Storage for key, if not local */
  Blob blob;
};


/*
** A cursor used for merged searches or iterations through up to one
** Tree structure and any number of sorted files.
**
**   lsmMCursorNew()
**   lsmMCursorSeek()
**   lsmMCursorNext()
**   lsmMCursorPrev()
**   lsmMCursorFirst()
**   lsmMCursorLast()
**   lsmMCursorKey()
**   lsmMCursorValue()
**   lsmMCursorValid()
*/
struct MultiCursor {
  lsm_db *pDb;                    /* Connection that owns this cursor */
  MultiCursor *pNext;             /* Next cursor owned by connection pDb */
  int flags;                      /* Mask of CURSOR_XXX flags */

  int eType;                      /* Cache of current key type */
  Blob key;                       /* Cache of current key (or NULL) */
  Blob val;                       /* Cache of current value */

  /* All the component cursors: */
  TreeCursor *apTreeCsr[2];       /* Up to two tree cursors */
  SegmentPtr *aPtr;               /* Array of segment pointers */
  int nPtr;                       /* Size of array aPtr[] */
  BtreeCursor *pBtCsr;            /* b-tree cursor (db writes only) */

  /* Comparison results */
  int nTree;                      /* Size of aTree[] array */
  int *aTree;                     /* Array of comparison results */

  /* Used by cursors flushing the in-memory tree only */
  int *pnOvfl;                    /* Number of free-list entries to store */
  void *pSystemVal;               /* Pointer to buffer to free */

  /* Used by worker cursors only */
  Pgno *pPrevMergePtr;
};

#define CURSOR_DATA_TREE0     0   /* Current tree cursor */
#define CURSOR_DATA_TREE1     1   /* The "old" tree, if any */
#define CURSOR_DATA_SYSTEM    2
#define CURSOR_DATA_SEGMENT   3

/*
** CURSOR_IGNORE_DELETE
**   If set, this cursor will not visit SORTED_DELETE keys.
**
** CURSOR_NEW_SYSTEM
**   If set, then after all user data from the in-memory tree and any other
**   cursors has been visited, the cursor visits the live (uncommitted) 
**   versions of the two system keys: FREELIST AND LEVELS. This is used when 
**   flushing the in-memory tree to disk - the new free-list and levels record
**   are flushed along with it.
**
** CURSOR_AT_FREELIST
**   This flag is set when sub-cursor CURSOR_DATA_SYSTEM is actually
**   pointing at a free list.
**
** CURSOR_IGNORE_SYSTEM
**   If set, this cursor ignores system keys.
**
** CURSOR_NEXT_OK
**   Set if it is Ok to call lsm_csr_next().
**
** CURSOR_PREV_OK
**   Set if it is Ok to call lsm_csr_prev().
**
** CURSOR_READ_SEPARATORS
**   Set if this cursor should visit the separator keys in segment 
**   aPtr[nPtr-1].
**
** CURSOR_SEEK_EQ
**   Cursor has undergone a successful lsm_csr_seek(LSM_SEEK_EQ) operation.
**   The key and value are stored in MultiCursor.key and MultiCursor.val
**   respectively.
*/
#define CURSOR_IGNORE_DELETE    0x00000001
#define CURSOR_NEW_SYSTEM       0x00000002
#define CURSOR_AT_FREELIST      0x00000004
#define CURSOR_IGNORE_SYSTEM    0x00000010
#define CURSOR_NEXT_OK          0x00000020
#define CURSOR_PREV_OK          0x00000040
#define CURSOR_READ_SEPARATORS  0x00000080
#define CURSOR_SEEK_EQ          0x00000100

typedef struct MergeWorker MergeWorker;
typedef struct Hierarchy Hierarchy;

struct Hierarchy {
  Page **apHier;
  int nHier;
};

/*
** aSave:
**   When mergeWorkerNextPage() is called to advance to the next page in
**   the output segment, if the bStore flag for an element of aSave[] is
**   true, it is cleared and the corresponding iPgno value is set to the 
**   page number of the page just completed.
**
**   aSave[0] is used to record the pointer value to be pushed into the
**   b-tree hierarchy. aSave[1] is used to save the page number of the
**   page containing the indirect key most recently written to the b-tree.
**   see mergeWorkerPushHierarchy() for details.
*/
struct MergeWorker {
  lsm_db *pDb;                    /* Database handle */
  Level *pLevel;                  /* Worker snapshot Level being merged */
  MultiCursor *pCsr;              /* Cursor to read new segment contents from */
  int bFlush;                     /* True if this is an in-memory tree flush */
  Hierarchy hier;                 /* B-tree hierarchy under construction */
  Page *pPage;                    /* Current output page */
  int nWork;                      /* Number of calls to mergeWorkerNextPage() */
  Pgno *aGobble;                  /* Gobble point for each input segment */

  Pgno iIndirect;
  struct SavedPgno {
    Pgno iPgno;
    int bStore;
  } aSave[2];
};

#ifdef LSM_DEBUG_EXPENSIVE
static int assertPointersOk(lsm_db *, Segment *, Segment *, int);
static int assertBtreeOk(lsm_db *, Segment *);
static void assertRunInOrder(lsm_db *pDb, Segment *pSeg);
#else
#define assertRunInOrder(x,y)
#define assertBtreeOk(x,y)
#endif


struct FilePage { u8 *aData; int nData; };
static u8 *fsPageData(Page *pPg, int *pnData){
  *pnData = ((struct FilePage *)(pPg))->nData;
  return ((struct FilePage *)(pPg))->aData;
}
/*UNUSED static u8 *fsPageDataPtr(Page *pPg){
  return ((struct FilePage *)(pPg))->aData;
}*/

/*
** Write nVal as a 16-bit unsigned big-endian integer into buffer aOut.
*/
void lsmPutU16(u8 *aOut, u16 nVal){
  aOut[0] = (u8)((nVal>>8) & 0xFF);
  aOut[1] = (u8)(nVal & 0xFF);
}

void lsmPutU32(u8 *aOut, u32 nVal){
  aOut[0] = (u8)((nVal>>24) & 0xFF);
  aOut[1] = (u8)((nVal>>16) & 0xFF);
  aOut[2] = (u8)((nVal>> 8) & 0xFF);
  aOut[3] = (u8)((nVal    ) & 0xFF);
}

int lsmGetU16(u8 *aOut){
  return (aOut[0] << 8) + aOut[1];
}

u32 lsmGetU32(u8 *aOut){
  return ((u32)aOut[0] << 24) 
       + ((u32)aOut[1] << 16) 
       + ((u32)aOut[2] << 8) 
       + ((u32)aOut[3]);
}

u32 lsmGetU64(u8 *aOut){
  return ((u64)aOut[0] << 56) 
       + ((u64)aOut[1] << 48) 
       + ((u64)aOut[2] << 40) 
       + ((u64)aOut[3] << 32) 
       + ((u64)aOut[4] << 24)
       + ((u32)aOut[5] << 16) 
       + ((u32)aOut[6] << 8) 
       + ((u32)aOut[7]);
}

void lsmPutU64(u8 *aOut, u64 nVal){
  aOut[0] = (u8)((nVal>>56) & 0xFF);
  aOut[1] = (u8)((nVal>>48) & 0xFF);
  aOut[2] = (u8)((nVal>>40) & 0xFF);
  aOut[3] = (u8)((nVal>>32) & 0xFF);
  aOut[4] = (u8)((nVal>>24) & 0xFF);
  aOut[5] = (u8)((nVal>>16) & 0xFF);
  aOut[6] = (u8)((nVal>> 8) & 0xFF);
  aOut[7] = (u8)((nVal    ) & 0xFF);
}

static int sortedBlobGrow(lsm_env *pEnv, Blob *pBlob, int nData){
  assert( pBlob->pEnv==pEnv || (pBlob->pEnv==0 && pBlob->pData==0) );
  if( pBlob->nAlloc<nData ){
    pBlob->pData = lsmReallocOrFree(pEnv, pBlob->pData, nData);
    if( !pBlob->pData ) return LSM_NOMEM;
    pBlob->nAlloc = nData;
    pBlob->pEnv = pEnv;
  }
  return LSM_OK;
}

static int sortedBlobSet(lsm_env *pEnv, Blob *pBlob, void *pData, int nData){
  if( sortedBlobGrow(pEnv, pBlob, nData) ) return LSM_NOMEM;
  memcpy(pBlob->pData, pData, nData);
  pBlob->nData = nData;
  return LSM_OK;
}

#if 0
static int sortedBlobCopy(Blob *pDest, Blob *pSrc){
  return sortedBlobSet(pDest, pSrc->pData, pSrc->nData);
}
#endif

static void sortedBlobFree(Blob *pBlob){
  assert( pBlob->pEnv || pBlob->pData==0 );
  if( pBlob->pData ) lsmFree(pBlob->pEnv, pBlob->pData);
  memset(pBlob, 0, sizeof(Blob));
}

static int sortedReadData(
  Page *pPg,
  int iOff,
  int nByte,
  void **ppData,
  Blob *pBlob
){
  int rc = LSM_OK;
  int iEnd;
  int nData;
  int nCell;
  u8 *aData;

  aData = fsPageData(pPg, &nData);
  nCell = lsmGetU16(&aData[SEGMENT_NRECORD_OFFSET(nData)]);
  iEnd = SEGMENT_EOF(nData, nCell);
  assert( iEnd>0 && iEnd<nData );

  if( iOff+nByte<=iEnd ){
    *ppData = (void *)&aData[iOff];
  }else{
    int nRem = nByte;
    int i = iOff;
    u8 *aDest;

    /* Make sure the blob is big enough to store the value being loaded. */
    rc = sortedBlobGrow(lsmPageEnv(pPg), pBlob, nByte);
    if( rc!=LSM_OK ) return rc;
    pBlob->nData = nByte;
    aDest = (u8 *)pBlob->pData;
    *ppData = pBlob->pData;

    /* Increment the pointer pages ref-count. */
    lsmFsPageRef(pPg);

    while( rc==LSM_OK ){
      Page *pNext;
      int flags;

      /* Copy data from pPg into the output buffer. */
      int nCopy = LSM_MIN(nRem, iEnd-i);
      if( nCopy>0 ){
        memcpy(&aDest[nByte-nRem], &aData[i], nCopy);
        nRem -= nCopy;
        i += nCopy;
        assert( nRem==0 || i==iEnd );
      }
      assert( nRem>=0 );
      if( nRem==0 ) break;
      i -= iEnd;

      /* Grab the next page in the segment */

      do {
        rc = lsmFsDbPageNext(0, pPg, 1, &pNext);
        if( rc==LSM_OK && pNext==0 ){
          rc = LSM_CORRUPT_BKPT;
        }
        if( rc ) break;
        lsmFsPageRelease(pPg);
        pPg = pNext;
        aData = fsPageData(pPg, &nData);
        flags = lsmGetU16(&aData[SEGMENT_FLAGS_OFFSET(nData)]);
      }while( flags&SEGMENT_BTREE_FLAG );

      iEnd = SEGMENT_EOF(nData, lsmGetU16(&aData[nData-2]));
      assert( iEnd>0 && iEnd<nData );
    }

    lsmFsPageRelease(pPg);
  }

  return rc;
}

static int pageGetNRec(u8 *aData, int nData){
  return (int)lsmGetU16(&aData[SEGMENT_NRECORD_OFFSET(nData)]);
}

static Pgno pageGetPtr(u8 *aData, int nData){
  return (Pgno)lsmGetU64(&aData[SEGMENT_POINTER_OFFSET(nData)]);
}

static int pageGetFlags(u8 *aData, int nData){
  return (int)lsmGetU16(&aData[SEGMENT_FLAGS_OFFSET(nData)]);
}

static u8 *pageGetCell(u8 *aData, int nData, int iCell){
  return &aData[lsmGetU16(&aData[SEGMENT_CELLPTR_OFFSET(nData, iCell)])];
}

/*
** Return the number of cells on page pPg.
*/
static int pageObjGetNRec(Page *pPg){
  int nData;
  u8 *aData = lsmFsPageData(pPg, &nData);
  return pageGetNRec(aData, nData);
}

/*
** Return the decoded (possibly relative) pointer value stored in cell 
** iCell from page aData/nData.
*/
static Pgno pageGetRecordPtr(u8 *aData, int nData, int iCell){
  Pgno iRet;                      /* Return value */
  u8 *aCell;                      /* Pointer to cell iCell */

  assert( iCell<pageGetNRec(aData, nData) && iCell>=0 );
  aCell = pageGetCell(aData, nData, iCell);
  lsmVarintGet64(&aCell[1], &iRet);
  return iRet;
}

static u8 *pageGetKey(
  Page *pPg,                      /* Page to read from */
  int iCell,                      /* Index of cell on page to read */
  int *piTopic,                   /* OUT: Topic associated with this key */
  int *pnKey,                     /* OUT: Size of key in bytes */
  Blob *pBlob                     /* If required, use this for dynamic memory */
){
  u8 *pKey;
  int nDummy;
  int eType;
  u8 *aData;
  int nData;

  aData = fsPageData(pPg, &nData);

  assert( !(pageGetFlags(aData, nData) & SEGMENT_BTREE_FLAG) );
  assert( iCell<pageGetNRec(aData, nData) );

  pKey = pageGetCell(aData, nData, iCell);
  eType = *pKey++;
  pKey += lsmVarintGet32(pKey, &nDummy);
  pKey += lsmVarintGet32(pKey, pnKey);
  if( rtIsWrite(eType) ){
    pKey += lsmVarintGet32(pKey, &nDummy);
  }
  *piTopic = rtTopic(eType);

  sortedReadData(pPg, pKey-aData, *pnKey, (void **)&pKey, pBlob);
  return pKey;
}

static int pageGetKeyCopy(
  lsm_env *pEnv,                  /* Environment handle */
  Page *pPg,                      /* Page to read from */
  int iCell,                      /* Index of cell on page to read */
  int *piTopic,                   /* OUT: Topic associated with this key */
  Blob *pBlob                     /* If required, use this for dynamic memory */
){
  int rc = LSM_OK;
  int nKey;
  u8 *aKey;

  aKey = pageGetKey(pPg, iCell, piTopic, &nKey, pBlob);
  assert( (void *)aKey!=pBlob->pData || nKey==pBlob->nData );
  if( (void *)aKey!=pBlob->pData ){
    rc = sortedBlobSet(pEnv, pBlob, aKey, nKey);
  }

  return rc;
}

static Pgno pageGetBtreeRef(Page *pPg, int iKey){
  Pgno iRef;
  u8 *aData;
  int nData;
  u8 *aCell;

  aData = fsPageData(pPg, &nData);
  aCell = pageGetCell(aData, nData, iKey);
  assert( aCell[0]==0 );
  aCell++;
  aCell += lsmVarintGet64(aCell, &iRef);
  lsmVarintGet64(aCell, &iRef);
  assert( iRef>0 );
  return iRef;
}

#define GETVARINT64(a, i) (((i)=((u8*)(a))[0])<=240?1:lsmVarintGet64((a), &(i)))
#define GETVARINT32(a, i) (((i)=((u8*)(a))[0])<=240?1:lsmVarintGet32((a), &(i)))

static int pageGetBtreeKey(
  Page *pPg,
  int iKey, 
  Pgno *piPtr, 
  int *piTopic, 
  void **ppKey,
  int *pnKey,
  Blob *pBlob
){
  u8 *aData;
  int nData;
  u8 *aCell;
  int eType;

  aData = fsPageData(pPg, &nData);
  assert( SEGMENT_BTREE_FLAG & pageGetFlags(aData, nData) );
  assert( iKey>=0 && iKey<pageGetNRec(aData, nData) );

  aCell = pageGetCell(aData, nData, iKey);
  eType = *aCell++;
  aCell += GETVARINT64(aCell, *piPtr);

  if( eType==0 ){
    int rc;
    Pgno iRef;                  /* Page number of referenced page */
    Page *pRef;
    aCell += GETVARINT64(aCell, iRef);
    rc = lsmFsDbPageGet(lsmPageFS(pPg), iRef, &pRef);
    if( rc!=LSM_OK ) return rc;
    pageGetKeyCopy(lsmPageEnv(pPg), pRef, 0, &eType, pBlob);
    lsmFsPageRelease(pRef);
    *ppKey = pBlob->pData;
    *pnKey = pBlob->nData;
  }else{
    aCell += GETVARINT32(aCell, *pnKey);
    *ppKey = aCell;
  }
  if( piTopic ) *piTopic = rtTopic(eType);

  return LSM_OK;
}

static int btreeCursorLoadKey(BtreeCursor *pCsr){
  int rc = LSM_OK;
  if( pCsr->iPg<0 ){
    pCsr->pKey = 0;
    pCsr->nKey = 0;
    pCsr->eType = 0;
  }else{
    Pgno dummy;
    int iPg = pCsr->iPg;
    int iCell = pCsr->aPg[iPg].iCell;
    while( iCell<0 && (--iPg)>=0 ){
      iCell = pCsr->aPg[iPg].iCell-1;
    }
    if( iPg<0 || iCell<0 ) return LSM_CORRUPT_BKPT;

    rc = pageGetBtreeKey(
        pCsr->aPg[iPg].pPage, iCell,
        &dummy, &pCsr->eType, &pCsr->pKey, &pCsr->nKey, &pCsr->blob
    );
    pCsr->eType |= LSM_SEPARATOR;
  }

  return rc;
}

static int btreeCursorPtr(u8 *aData, int nData, int iCell){
  int nCell;

  nCell = pageGetNRec(aData, nData);
  if( iCell>=nCell ){
    return pageGetPtr(aData, nData);
  }
  return pageGetRecordPtr(aData, nData, iCell);
}

static int btreeCursorNext(BtreeCursor *pCsr){
  int rc = LSM_OK;

  BtreePg *pPg = &pCsr->aPg[pCsr->iPg];
  int nCell; 
  u8 *aData;
  int nData;

  assert( pCsr->iPg>=0 );
  assert( pCsr->iPg==pCsr->nDepth-1 );

  aData = fsPageData(pPg->pPage, &nData);
  nCell = pageGetNRec(aData, nData);
  assert( pPg->iCell<=nCell );
  pPg->iCell++;
  if( pPg->iCell==nCell ){
    Pgno iLoad;

    /* Up to parent. */
    lsmFsPageRelease(pPg->pPage);
    pPg->pPage = 0;
    pCsr->iPg--;
    while( pCsr->iPg>=0 ){
      pPg = &pCsr->aPg[pCsr->iPg];
      aData = fsPageData(pPg->pPage, &nData);
      if( pPg->iCell<pageGetNRec(aData, nData) ) break;
      lsmFsPageRelease(pPg->pPage);
      pCsr->iPg--;
    }

    /* Read the key */
    rc = btreeCursorLoadKey(pCsr);

    /* Unless the cursor is at EOF, descend to cell -1 (yes, negative one) of 
    ** the left-most most descendent. */
    if( pCsr->iPg>=0 ){
      pCsr->aPg[pCsr->iPg].iCell++;

      iLoad = btreeCursorPtr(aData, nData, pPg->iCell);
      do {
        Page *pLoad;
        pCsr->iPg++;
        rc = lsmFsDbPageGet(pCsr->pFS, iLoad, &pLoad);
        pCsr->aPg[pCsr->iPg].pPage = pLoad;
        pCsr->aPg[pCsr->iPg].iCell = 0;
        if( rc==LSM_OK ){
          if( pCsr->iPg==(pCsr->nDepth-1) ) break;
          aData = fsPageData(pLoad, &nData);
          iLoad = btreeCursorPtr(aData, nData, 0);
        }
      }while( rc==LSM_OK && pCsr->iPg<(pCsr->nDepth-1) );
      pCsr->aPg[pCsr->iPg].iCell = -1;
    }

  }else{
    rc = btreeCursorLoadKey(pCsr);
  }

  if( rc==LSM_OK && pCsr->iPg>=0 ){
    aData = fsPageData(pCsr->aPg[pCsr->iPg].pPage, &nData);
    pCsr->iPtr = btreeCursorPtr(aData, nData, pCsr->aPg[pCsr->iPg].iCell+1);
  }

  return rc;
}

static void btreeCursorFree(BtreeCursor *pCsr){
  if( pCsr ){
    int i;
    lsm_env *pEnv = lsmFsEnv(pCsr->pFS);
    for(i=0; i<=pCsr->iPg; i++){
      lsmFsPageRelease(pCsr->aPg[i].pPage);
    }
    sortedBlobFree(&pCsr->blob);
    lsmFree(pEnv, pCsr->aPg);
    lsmFree(pEnv, pCsr);
  }
}

static int btreeCursorFirst(BtreeCursor *pCsr){
  int rc;

  Page *pPg = 0;
  FileSystem *pFS = pCsr->pFS;
  int iPg = pCsr->pSeg->iRoot;

  do {
    rc = lsmFsDbPageGet(pFS, iPg, &pPg);
    assert( (rc==LSM_OK)==(pPg!=0) );
    if( rc==LSM_OK ){
      u8 *aData;
      int nData;
      int flags;

      aData = fsPageData(pPg, &nData);
      flags = pageGetFlags(aData, nData);
      if( (flags & SEGMENT_BTREE_FLAG)==0 ) break;

      if( (pCsr->nDepth % 8)==0 ){
        int nNew = pCsr->nDepth + 8;
        pCsr->aPg = (BtreePg *)lsmReallocOrFreeRc(
            lsmFsEnv(pFS), pCsr->aPg, sizeof(BtreePg) * nNew, &rc
        );
        if( rc==LSM_OK ){
          memset(&pCsr->aPg[pCsr->nDepth], 0, sizeof(BtreePg) * 8);
        }
      }

      if( rc==LSM_OK ){
        assert( pCsr->aPg[pCsr->nDepth].iCell==0 );
        pCsr->aPg[pCsr->nDepth].pPage = pPg;
        pCsr->nDepth++;
        iPg = pageGetRecordPtr(aData, nData, 0);
      }
    }
  }while( rc==LSM_OK );
  lsmFsPageRelease(pPg);
  pCsr->iPg = pCsr->nDepth-1;

  if( rc==LSM_OK && pCsr->nDepth ){
    pCsr->aPg[pCsr->iPg].iCell = -1;
    rc = btreeCursorNext(pCsr);
  }

  return rc;
}

static void btreeCursorPosition(BtreeCursor *pCsr, MergeInput *p){
  if( pCsr->iPg>=0 ){
    p->iPg = lsmFsPageNumber(pCsr->aPg[pCsr->iPg].pPage);
    p->iCell = ((pCsr->aPg[pCsr->iPg].iCell + 1) << 8) + pCsr->nDepth;
  }else{
    p->iPg = 0;
    p->iCell = 0;
  }
}

static void btreeCursorSplitkey(BtreeCursor *pCsr, MergeInput *p){
  int iCell = pCsr->aPg[pCsr->iPg].iCell;
  if( iCell>=0 ){
    p->iCell = iCell;
    p->iPg = lsmFsPageNumber(pCsr->aPg[pCsr->iPg].pPage);
  }else{
    int i;
    for(i=pCsr->iPg-1; i>=0; i--){
      if( pCsr->aPg[i].iCell>0 ) break;
    }
    assert( i>=0 );
    p->iCell = pCsr->aPg[i].iCell-1;
    p->iPg = lsmFsPageNumber(pCsr->aPg[i].pPage);
  }
}

static int sortedKeyCompare(
  int (*xCmp)(void *, int, void *, int),
  int iLhsTopic, void *pLhsKey, int nLhsKey,
  int iRhsTopic, void *pRhsKey, int nRhsKey
){
  int res = iLhsTopic - iRhsTopic;
  if( res==0 ){
    res = xCmp(pLhsKey, nLhsKey, pRhsKey, nRhsKey);
  }
  return res;
}

static int btreeCursorRestore(
  BtreeCursor *pCsr, 
  int (*xCmp)(void *, int, void *, int),
  MergeInput *p
){
  int rc = LSM_OK;

  if( p->iPg ){
    lsm_env *pEnv = lsmFsEnv(pCsr->pFS);
    int iCell;                    /* Current cell number on leaf page */
    Pgno iLeaf;                   /* Page number of current leaf page */
    int nDepth;                   /* Depth of b-tree structure */

    /* Decode the MergeInput structure */
    iLeaf = p->iPg;
    nDepth = (p->iCell & 0x00FF);
    iCell = (p->iCell >> 8) - 1;

    /* Allocate the BtreeCursor.aPg[] array */
    assert( pCsr->aPg==0 );
    pCsr->aPg = (BtreePg *)lsmMallocZeroRc(pEnv, sizeof(BtreePg) * nDepth, &rc);

    /* Populate the last entry of the aPg[] array */
    if( rc==LSM_OK ){
      pCsr->iPg = nDepth-1;
      pCsr->nDepth = nDepth;
      pCsr->aPg[pCsr->iPg].iCell = iCell;
      rc = lsmFsDbPageGet(pCsr->pFS, iLeaf, &pCsr->aPg[nDepth-1].pPage);
    }

    /* Populate any other aPg[] array entries */
    if( rc==LSM_OK && nDepth>1 ){
      Blob blob = {0,0,0};
      void *pSeek;
      int nSeek;
      int iTopicSeek;
      int iPg = 0;
      int iLoad = pCsr->pSeg->iRoot;
      Page *pPg = pCsr->aPg[nDepth-1].pPage;
 
      if( pageObjGetNRec(pPg)==0 ){
        /* This can happen when pPg is the right-most leaf in the b-tree.
        ** In this case, set the iTopicSeek/pSeek/nSeek key to a value
        ** greater than any real key.  */
        assert( iCell==-1 );
        iTopicSeek = 1000;
        pSeek = 0;
        nSeek = 0;
      }else{
        Pgno dummy;
        rc = pageGetBtreeKey(pPg,
            0, &dummy, &iTopicSeek, &pSeek, &nSeek, &pCsr->blob
        );
      }

      do {
        Page *pPg;
        rc = lsmFsDbPageGet(pCsr->pFS, iLoad, &pPg);
        assert( rc==LSM_OK || pPg==0 );
        if( rc==LSM_OK ){
          u8 *aData;                  /* Buffer containing page data */
          int nData;                  /* Size of aData[] in bytes */
          int iMin;
          int iMax;
          int iCell;

          aData = fsPageData(pPg, &nData);
          assert( (pageGetFlags(aData, nData) & SEGMENT_BTREE_FLAG) );

          iLoad = pageGetPtr(aData, nData);
          iCell = pageGetNRec(aData, nData); 
          iMax = iCell-1;
          iMin = 0;

          while( iMax>=iMin ){
            int iTry = (iMin+iMax)/2;
            void *pKey; int nKey;         /* Key for cell iTry */
            int iTopic;                   /* Topic for key pKeyT/nKeyT */
            Pgno iPtr;                    /* Pointer for cell iTry */
            int res;                      /* (pSeek - pKeyT) */

            rc = pageGetBtreeKey(pPg, iTry, &iPtr, &iTopic, &pKey, &nKey,&blob);
            if( rc!=LSM_OK ) break;

            res = sortedKeyCompare(
                xCmp, iTopicSeek, pSeek, nSeek, iTopic, pKey, nKey
            );
            assert( res!=0 );

            if( res<0 ){
              iLoad = iPtr;
              iCell = iTry;
              iMax = iTry-1;
            }else{
              iMin = iTry+1;
            }
          }

          pCsr->aPg[iPg].pPage = pPg;
          pCsr->aPg[iPg].iCell = iCell;
          iPg++;
          assert( iPg!=nDepth-1 || iLoad==iLeaf );
        }
      }while( rc==LSM_OK && iPg<(nDepth-1) );
      sortedBlobFree(&blob);
    }

    /* Load the current key and pointer */
    if( rc==LSM_OK ){
      BtreePg *pBtreePg;
      u8 *aData;
      int nData;

      pBtreePg = &pCsr->aPg[pCsr->iPg];
      aData = fsPageData(pBtreePg->pPage, &nData);
      pCsr->iPtr = btreeCursorPtr(aData, nData, pBtreePg->iCell+1);
      if( pBtreePg->iCell<0 ){
        Pgno dummy;
        int i;
        for(i=pCsr->iPg-1; i>=0; i--){
          if( pCsr->aPg[i].iCell>0 ) break;
        }
        assert( i>=0 );
        rc = pageGetBtreeKey(
            pCsr->aPg[i].pPage, pCsr->aPg[i].iCell-1,
            &dummy, &pCsr->eType, &pCsr->pKey, &pCsr->nKey, &pCsr->blob
        );
        pCsr->eType |= LSM_SEPARATOR;

      }else{
        rc = btreeCursorLoadKey(pCsr);
      }
    }
  }
  return rc;
}

static int btreeCursorNew(
  lsm_db *pDb,
  Segment *pSeg,
  BtreeCursor **ppCsr
){
  int rc = LSM_OK;
  BtreeCursor *pCsr;
  
  assert( pSeg->iRoot );
  pCsr = lsmMallocZeroRc(pDb->pEnv, sizeof(BtreeCursor), &rc);
  if( pCsr ){
    pCsr->pFS = pDb->pFS;
    pCsr->pSeg = pSeg;
    pCsr->iPg = -1;
  }

  *ppCsr = pCsr;
  return rc;
}

static void segmentPtrSetPage(SegmentPtr *pPtr, Page *pNext){
  lsmFsPageRelease(pPtr->pPg);
  if( pNext ){
    int nData;
    u8 *aData = fsPageData(pNext, &nData);
    pPtr->nCell = pageGetNRec(aData, nData);
    pPtr->flags = pageGetFlags(aData, nData);
    pPtr->iPtr = pageGetPtr(aData, nData);
  }
  pPtr->pPg = pNext;
}

/*
** Load a new page into the SegmentPtr object pPtr.
*/
static int segmentPtrLoadPage(
  FileSystem *pFS,
  SegmentPtr *pPtr,              /* Load page into this SegmentPtr object */
  int iNew                       /* Page number of new page */
){
  Page *pPg = 0;                 /* The new page */
  int rc;                        /* Return Code */

  rc = lsmFsDbPageGet(pFS, iNew, &pPg);
  assert( rc==LSM_OK || pPg==0 );
  segmentPtrSetPage(pPtr, pPg);

  return rc;
}

static int segmentPtrReadData(
  SegmentPtr *pPtr,
  int iOff,
  int nByte,
  void **ppData,
  Blob *pBlob
){
  return sortedReadData(pPtr->pPg, iOff, nByte, ppData, pBlob);
}

static int segmentPtrNextPage(
  SegmentPtr *pPtr,              /* Load page into this SegmentPtr object */
  int eDir                       /* +1 for next(), -1 for prev() */
){
  Page *pNext;                   /* New page to load */
  int rc;                        /* Return code */

  assert( eDir==1 || eDir==-1 );
  assert( pPtr->pPg );
  assert( pPtr->pSeg || eDir>0 );

  rc = lsmFsDbPageNext(pPtr->pSeg, pPtr->pPg, eDir, &pNext);
  assert( rc==LSM_OK || pNext==0 );
  segmentPtrSetPage(pPtr, pNext);
  return rc;
}

static int segmentPtrLoadCell(
  SegmentPtr *pPtr,              /* Load page into this SegmentPtr object */
  int iNew                       /* Cell number of new cell */
){
  int rc = LSM_OK;
  if( pPtr->pPg ){
    u8 *aData;                    /* Pointer to page data buffer */
    int iOff;                     /* Offset in aData[] to read from */
    int nPgsz;                    /* Size of page (aData[]) in bytes */

    assert( iNew<pPtr->nCell );
    pPtr->iCell = iNew;
    aData = fsPageData(pPtr->pPg, &nPgsz);
    iOff = lsmGetU16(&aData[SEGMENT_CELLPTR_OFFSET(nPgsz, pPtr->iCell)]);
    pPtr->eType = aData[iOff];
    iOff++;
    iOff += GETVARINT64(&aData[iOff], pPtr->iPgPtr);
    iOff += GETVARINT32(&aData[iOff], pPtr->nKey);
    if( rtIsWrite(pPtr->eType) ){
      iOff += GETVARINT32(&aData[iOff], pPtr->nVal);
    }
    assert( pPtr->nKey>=0 );

    rc = segmentPtrReadData(
        pPtr, iOff, pPtr->nKey, &pPtr->pKey, &pPtr->blob1
    );
    if( rc==LSM_OK && rtIsWrite(pPtr->eType) ){
      rc = segmentPtrReadData(
          pPtr, iOff+pPtr->nKey, pPtr->nVal, &pPtr->pVal, &pPtr->blob2
      );
    }else{
      pPtr->nVal = 0;
      pPtr->pVal = 0;
    }
  }

  return rc;
}

void lsmSortedSplitkey(lsm_db *pDb, Level *pLevel, int *pRc){
  Page *pPg = 0;
  lsm_env *pEnv = pDb->pEnv;      /* Environment handle */
  int rc = *pRc;
  Merge *pMerge = pLevel->pMerge;

  if( rc==LSM_OK ){
    rc = lsmFsDbPageGet(pDb->pFS, pMerge->splitkey.iPg, &pPg);
  }
  if( rc==LSM_OK ){
    int iTopic;
    Blob blob = {0, 0, 0, 0};
    u8 *aData;
    int nData;
  
    aData = lsmFsPageData(pPg, &nData);
    if( pageGetFlags(aData, nData) & SEGMENT_BTREE_FLAG ){
      void *pKey;
      int nKey;
      Pgno dummy;
      rc = pageGetBtreeKey(
          pPg, pMerge->splitkey.iCell, &dummy, &iTopic, &pKey, &nKey, &blob
      );
      if( rc==LSM_OK && blob.pData!=pKey ){
        rc = sortedBlobSet(pEnv, &blob, pKey, nKey);
      }
    }else{
      rc = pageGetKeyCopy(pEnv, pPg, pMerge->splitkey.iCell, &iTopic, &blob);
    }

    pLevel->iSplitTopic = iTopic;
    pLevel->pSplitKey = blob.pData;
    pLevel->nSplitKey = blob.nData;
    lsmFsPageRelease(pPg);
  }

  *pRc = rc;
}

static void segmentPtrReset(SegmentPtr *pPtr){
  lsmFsPageRelease(pPtr->pPg);
  pPtr->pPg = 0;
  pPtr->nCell = 0;
  pPtr->pKey = 0;
  pPtr->nKey = 0;
  pPtr->pVal = 0;
  pPtr->nVal = 0;
  pPtr->eType = 0;
  pPtr->iCell = 0;
  sortedBlobFree(&pPtr->blob1);
  sortedBlobFree(&pPtr->blob2);
}

static int segmentPtrIgnoreSeparators(MultiCursor *pCsr, SegmentPtr *pPtr){
  return (pCsr->flags & CURSOR_READ_SEPARATORS)==0
      || (pPtr!=&pCsr->aPtr[pCsr->nPtr-1]);
}

static int segmentPtrAdvance(
  MultiCursor *pCsr, 
  SegmentPtr *pPtr,
  int bReverse
){
  int eDir = (bReverse ? -1 : 1);
  Level *pLvl = pPtr->pLevel;
  do {
    int rc;
    int iCell;                    /* Number of new cell in page */
    int svFlags = 0;              /* SegmentPtr.eType before advance */

    iCell = pPtr->iCell + eDir;
    assert( pPtr->pPg );
    assert( iCell<=pPtr->nCell && iCell>=-1 );

    if( bReverse && pPtr->pSeg!=&pPtr->pLevel->lhs ){
      svFlags = pPtr->eType;
      assert( svFlags );
    }

    if( iCell>=pPtr->nCell || iCell<0 ){
      do {
        rc = segmentPtrNextPage(pPtr, eDir); 
      }while( rc==LSM_OK 
           && pPtr->pPg 
           && (pPtr->nCell==0 || (pPtr->flags & SEGMENT_BTREE_FLAG) ) 
      );
      if( rc!=LSM_OK ) return rc;
      iCell = bReverse ? (pPtr->nCell-1) : 0;
    }
    rc = segmentPtrLoadCell(pPtr, iCell);
    if( rc!=LSM_OK ) return rc;

    if( svFlags && pPtr->pPg ){
      int res = sortedKeyCompare(pCsr->pDb->xCmp,
          rtTopic(pPtr->eType), pPtr->pKey, pPtr->nKey,
          pLvl->iSplitTopic, pLvl->pSplitKey, pLvl->nSplitKey
      );
      if( res<0 ) segmentPtrReset(pPtr);
    }
    if( pPtr->pPg==0 && (svFlags & LSM_END_DELETE) ){
      rc = lsmFsDbPageGet(pCsr->pDb->pFS, pPtr->pSeg->iFirst, &pPtr->pPg);
      if( rc!=LSM_OK ) return rc;
      pPtr->eType = LSM_START_DELETE | (pLvl->iSplitTopic ? LSM_SYSTEMKEY : 0);
      pPtr->pKey = pLvl->pSplitKey;
      pPtr->nKey = pLvl->nSplitKey;
    }

  }while( pCsr 
       && pPtr->pPg 
       && segmentPtrIgnoreSeparators(pCsr, pPtr)
       && rtIsSeparator(pPtr->eType)
  );

  return LSM_OK;
}

static void segmentPtrEndPage(
  FileSystem *pFS, 
  SegmentPtr *pPtr, 
  int bLast, 
  int *pRc
){
  if( *pRc==LSM_OK ){
    Page *pNew = 0;
    if( bLast ){
      *pRc = lsmFsDbPageLast(pFS, pPtr->pSeg, &pNew);
    }else{
      *pRc = lsmFsDbPageGet(pFS, pPtr->pSeg->iFirst, &pNew);
    }
    segmentPtrSetPage(pPtr, pNew);
  }
}


/*
** Try to move the segment pointer passed as the second argument so that it
** points at either the first (bLast==0) or last (bLast==1) cell in the valid
** region of the segment defined by pPtr->iFirst and pPtr->iLast.
**
** Return LSM_OK if successful or an lsm error code if something goes
** wrong (IO error, OOM etc.).
*/
static int segmentPtrEnd(MultiCursor *pCsr, SegmentPtr *pPtr, int bLast){
  Level *pLvl = pPtr->pLevel;
  int rc = LSM_OK;
  FileSystem *pFS = pCsr->pDb->pFS;
  int bIgnore;

  segmentPtrEndPage(pFS, pPtr, bLast, &rc);
  while( rc==LSM_OK && pPtr->pPg 
      && (pPtr->nCell==0 || (pPtr->flags & SEGMENT_BTREE_FLAG))
  ){
    rc = segmentPtrNextPage(pPtr, (bLast ? -1 : 1));
  }

  if( rc==LSM_OK && pPtr->pPg ){
    rc = segmentPtrLoadCell(pPtr, bLast ? (pPtr->nCell-1) : 0);
  }
  
  bIgnore = segmentPtrIgnoreSeparators(pCsr, pPtr);
  if( rc==LSM_OK && pPtr->pPg && bIgnore && rtIsSeparator(pPtr->eType) ){
    rc = segmentPtrAdvance(pCsr, pPtr, bLast);
  }


  if( bLast && rc==LSM_OK && pPtr->pPg
   && pPtr->pSeg==&pLvl->lhs 
   && pLvl->nRight && (pPtr->eType & LSM_START_DELETE)
  ){
    pPtr->iCell++;
    pPtr->eType = LSM_END_DELETE | (pLvl->iSplitTopic);
    pPtr->pKey = pLvl->pSplitKey;
    pPtr->nKey = pLvl->nSplitKey;
    pPtr->pVal = 0;
    pPtr->nVal = 0;
  }

  return rc;
}

static void segmentPtrKey(SegmentPtr *pPtr, void **ppKey, int *pnKey){
  assert( pPtr->pPg );
  *ppKey = pPtr->pKey;
  *pnKey = pPtr->nKey;
}

#ifdef LSM_DEBUG

static char *keyToString(lsm_env *pEnv, void *pKey, int nKey){
  int i;
  u8 *aKey = (u8 *)pKey;
  char *zRet = (char *)lsmMalloc(pEnv, nKey+1);

  for(i=0; i<nKey; i++){
    zRet[i] = (char)(isalnum(aKey[i]) ? aKey[i] : '.');
  }
  zRet[nKey] = '\0';
  return zRet;
}

/*
** Check that the page that pPtr currently has loaded is the correct page
** to search for key (pKey/nKey). If it is, return 1. Otherwise, an assert
** fails and this function does not return.
*/
static int assertKeyLocation(
  MultiCursor *pCsr, 
  SegmentPtr *pPtr, 
  void *pKey, int nKey
){
  lsm_env *pEnv = lsmFsEnv(pCsr->pDb->pFS);
  Blob blob = {0, 0, 0};
  int eDir;
  int iTopic = 0;                 /* TODO: Fix me */

  for(eDir=-1; eDir<=1; eDir+=2){
    Page *pTest = pPtr->pPg;

    lsmFsPageRef(pTest);
    while( pTest ){
      Page *pNext;

      int rc = lsmFsDbPageNext(pPtr->pSeg, pTest, eDir, &pNext);
      lsmFsPageRelease(pTest);
      if( rc ) return 1;
      pTest = pNext;

      if( pTest ){
        int nData;
        u8 *aData = fsPageData(pTest, &nData);
        int nCell = pageGetNRec(aData, nData);
        int flags = pageGetFlags(aData, nData);
        if( nCell && 0==(flags&SEGMENT_BTREE_FLAG) ){
          int nPgKey;
          int iPgTopic;
          u8 *pPgKey;
          int res;
          int iCell;

          iCell = ((eDir < 0) ? (nCell-1) : 0);
          pPgKey = pageGetKey(pTest, iCell, &iPgTopic, &nPgKey, &blob);
          res = iTopic - iPgTopic;
          if( res==0 ) res = pCsr->pDb->xCmp(pKey, nKey, pPgKey, nPgKey);
          if( (eDir==1 && res>0) || (eDir==-1 && res<0) ){
            /* Taking this branch means something has gone wrong. */
            char *zMsg = lsmMallocPrintf(pEnv, "Key \"%s\" is not on page %d", 
                keyToString(pEnv, pKey, nKey), lsmFsPageNumber(pPtr->pPg)
            );
            fprintf(stderr, "%s\n", zMsg);
            assert( !"assertKeyLocation() failed" );
          }
          lsmFsPageRelease(pTest);
          pTest = 0;
        }
      }
    }
  }

  sortedBlobFree(&blob);
  return 1;
}
#endif

#ifndef NDEBUG
static int assertSeekResult(
  MultiCursor *pCsr,
  SegmentPtr *pPtr,
  int iTopic,
  void *pKey,
  int nKey,
  int eSeek
){
  if( pPtr->pPg ){
    int res;
    res = sortedKeyCompare(pCsr->pDb->xCmp, iTopic, pKey, nKey,
        rtTopic(pPtr->eType), pPtr->pKey, pPtr->nKey
    );

    if( eSeek==LSM_SEEK_EQ ) return (res==0);
    if( eSeek==LSM_SEEK_LE ) return (res>=0);
    if( eSeek==LSM_SEEK_GE ) return (res<=0);
  }

  return 1;
}
#endif

static int segmentPtrSearchOversized(
  MultiCursor *pCsr,              /* Cursor context */
  SegmentPtr *pPtr,               /* Pointer to seek */
  void *pKey, int nKey            /* Key to seek to */
){
  int (*xCmp)(void *, int, void *, int) = pCsr->pDb->xCmp;
  int rc = LSM_OK;

  /* If the OVERSIZED flag is set, then there is no pointer in the
  ** upper level to the next page in the segment that contains at least
  ** one key. So compare the largest key on the current page with the
  ** key being sought (pKey/nKey). If (pKey/nKey) is larger, advance
  ** to the next page in the segment that contains at least one key. 
  */
  while( rc==LSM_OK && (pPtr->flags & PGFTR_SKIP_NEXT_FLAG) ){
    u8 *pLastKey;
    int nLastKey;
    int iLastTopic;
    int res;                      /* Result of comparison */
    Page *pNext;

    /* Load the last key on the current page. */
    pLastKey = pageGetKey(
        pPtr->pPg, pPtr->nCell-1, &iLastTopic, &nLastKey, &pPtr->blob1
    );

    /* If the loaded key is >= than (pKey/nKey), break out of the loop.
    ** If (pKey/nKey) is present in this array, it must be on the current 
    ** page.  */
    res = sortedKeyCompare(
        xCmp, iLastTopic, pLastKey, nLastKey, 0, pKey, nKey
    );
    if( res>=0 ) break;

    /* Advance to the next page that contains at least one key. */
    pNext = pPtr->pPg;
    lsmFsPageRef(pNext);
    while( 1 ){
      Page *pLoad;
      u8 *aData; int nData;

      rc = lsmFsDbPageNext(pPtr->pSeg, pNext, 1, &pLoad);
      lsmFsPageRelease(pNext);
      pNext = pLoad;
      if( pNext==0 ) break;

      assert( rc==LSM_OK );
      aData = lsmFsPageData(pNext, &nData);
      if( (pageGetFlags(aData, nData) & SEGMENT_BTREE_FLAG)==0
       && pageGetNRec(aData, nData)>0
      ){
        break;
      }
    }
    if( pNext==0 ) break;
    segmentPtrSetPage(pPtr, pNext);

    /* This should probably be an LSM_CORRUPT error. */
    assert( rc!=LSM_OK || (pPtr->flags & PGFTR_SKIP_THIS_FLAG) );
  }

  return rc;
}

static int ptrFwdPointer(
  Page *pPage,
  int iCell,
  Segment *pSeg,
  Pgno *piPtr,
  int *pbFound
){
  Page *pPg = pPage;
  int iFirst = iCell;
  int rc = LSM_OK;

  do {
    Page *pNext = 0;
    u8 *aData;
    int nData;

    aData = lsmFsPageData(pPg, &nData);
    if( (pageGetFlags(aData, nData) & SEGMENT_BTREE_FLAG)==0 ){
      int i;
      int nCell = pageGetNRec(aData, nData);
      for(i=iFirst; i<nCell; i++){
        u8 eType = *pageGetCell(aData, nData, i);
        if( (eType & LSM_START_DELETE)==0 ){
          *pbFound = 1;
          *piPtr = pageGetRecordPtr(aData, nData, i) + pageGetPtr(aData, nData);
          lsmFsPageRelease(pPg);
          return LSM_OK;
        }
      }
    }

    rc = lsmFsDbPageNext(pSeg, pPg, 1, &pNext);
    lsmFsPageRelease(pPg);
    pPg = pNext;
    iFirst = 0;
  }while( pPg && rc==LSM_OK );
  lsmFsPageRelease(pPg);

  *pbFound = 0;
  return rc;
}

static int sortedRhsFirst(MultiCursor *pCsr, Level *pLvl, SegmentPtr *pPtr){
  int rc;
  rc = segmentPtrEnd(pCsr, pPtr, 0);
  while( pPtr->pPg && rc==LSM_OK ){
    int res = sortedKeyCompare(pCsr->pDb->xCmp,
        pLvl->iSplitTopic, pLvl->pSplitKey, pLvl->nSplitKey,
        rtTopic(pPtr->eType), pPtr->pKey, pPtr->nKey
    );
    if( res<=0 ) break;
    rc = segmentPtrAdvance(pCsr, pPtr, 0);
  }
  return rc;
}


/*
** This function is called as part of a SEEK_GE op on a multi-cursor if the 
** FC pointer read from segment *pPtr comes from an entry with the 
** LSM_START_DELETE flag set. In this case the pointer value cannot be 
** trusted. Instead, the pointer that should be followed is that associated
** with the next entry in *pPtr that does not have LSM_START_DELETE set.
**
** Why the pointers can't be trusted:
**
**
**
** TODO: This is a stop-gap solution:
** 
**   At the moment, this function is called from within segmentPtrSeek(), 
**   as part of the initial lsmMCursorSeek() call. However, consider a 
**   database where the following has occurred:
**
**      1. A range delete removes keys 1..9999 using a range delete.
**      2. Keys 1 through 9999 are reinserted.
**      3. The levels containing the ops in 1. and 2. above are merged. Call
**         this level N. Level N contains FC pointers to level N+1.
**
**   Then, if the user attempts to query for (key>=2 LIMIT 10), the 
**   lsmMCursorSeek() call will iterate through 9998 entries searching for a 
**   pointer down to the level N+1 that is never actually used. It would be
**   much better if the multi-cursor could do this lazily - only seek to the
**   level (N+1) page after the user has moved the cursor on level N passed
**   the big range-delete.
*/
static int segmentPtrFwdPointer(
  MultiCursor *pCsr,              /* Multi-cursor pPtr belongs to */
  SegmentPtr *pPtr,               /* Segment-pointer to extract FC ptr from */
  Pgno *piPtr                     /* OUT: FC pointer value */
){
  Level *pLvl = pPtr->pLevel;
  Level *pNext = pLvl->pNext;
  Page *pPg = pPtr->pPg;
  int rc;
  int bFound;
  Pgno iOut = 0;

  if( pPtr->pSeg==&pLvl->lhs || pPtr->pSeg==&pLvl->aRhs[pLvl->nRight-1] ){
    if( pNext==0 
        || (pNext->nRight==0 && pNext->lhs.iRoot)
        || (pNext->nRight!=0 && pNext->aRhs[0].iRoot)
      ){
      /* Do nothing. The pointer will not be used anyway. */
      return LSM_OK;
    }
  }else{
    if( pPtr[1].pSeg->iRoot ){
      return LSM_OK;
    }
  }

  /* Search for a pointer within the current segment. */
  lsmFsPageRef(pPg);
  rc = ptrFwdPointer(pPg, pPtr->iCell, pPtr->pSeg, &iOut, &bFound);

  if( rc==LSM_OK && bFound==0 ){
    /* This case happens when pPtr points to the left-hand-side of a segment
    ** currently undergoing an incremental merge. In this case, jump to the
    ** oldest segment in the right-hand-side of the same level and continue
    ** searching. But - do not consider any keys smaller than the levels
    ** split-key. */
    SegmentPtr ptr;

    if( pPtr->pLevel->nRight==0 || pPtr->pSeg!=&pPtr->pLevel->lhs ){
      return LSM_CORRUPT_BKPT;
    }

    memset(&ptr, 0, sizeof(SegmentPtr));
    ptr.pLevel = pPtr->pLevel;
    ptr.pSeg = &ptr.pLevel->aRhs[ptr.pLevel->nRight-1];
    rc = sortedRhsFirst(pCsr, ptr.pLevel, &ptr);
    if( rc==LSM_OK ){
      rc = ptrFwdPointer(ptr.pPg, ptr.iCell, ptr.pSeg, &iOut, &bFound);
      ptr.pPg = 0;
    }
    segmentPtrReset(&ptr);
  }

  *piPtr = iOut;
  return rc;
}

static int segmentPtrSeek(
  MultiCursor *pCsr,              /* Cursor context */
  SegmentPtr *pPtr,               /* Pointer to seek */
  void *pKey, int nKey,           /* Key to seek to */
  int eSeek,                      /* Search bias - see above */
  int *piPtr,                     /* OUT: FC pointer */
  int *pbStop
){
  int (*xCmp)(void *, int, void *, int) = pCsr->pDb->xCmp;
  int res;                        /* Result of comparison operation */
  int rc = LSM_OK;
  int iMin;
  int iMax;
  Pgno iPtrOut = 0;
  const int iTopic = 0;

  /* If the current page contains an oversized entry, then there are no
  ** pointers to one or more of the subsequent pages in the sorted run.
  ** The following call ensures that the segment-ptr points to the correct 
  ** page in this case.  */
  rc = segmentPtrSearchOversized(pCsr, pPtr, pKey, nKey);
  iPtrOut = pPtr->iPtr;

  /* Assert that this page is the right page of this segment for the key
  ** that we are searching for. Do this by loading page (iPg-1) and testing
  ** that pKey/nKey is greater than all keys on that page, and then by 
  ** loading (iPg+1) and testing that pKey/nKey is smaller than all
  ** the keys it houses.  
  **
  ** TODO: With range-deletes in the tree, the test described above may fail.
  */
#if 0
  assert( assertKeyLocation(pCsr, pPtr, pKey, nKey) );
#endif

  assert( pPtr->nCell>0 
       || pPtr->pSeg->nSize==1 
       || lsmFsDbPageIsLast(pPtr->pPg, pPtr->pSeg)
  );
  if( pPtr->nCell==0 ){
    segmentPtrReset(pPtr);
  }else{
    iMin = 0;
    iMax = pPtr->nCell-1;

    while( 1 ){
      int iTry = (iMin+iMax)/2;
      void *pKeyT; int nKeyT;       /* Key for cell iTry */
      int iTopicT;

      assert( iTry<iMax || iMin==iMax );

      rc = segmentPtrLoadCell(pPtr, iTry);
      if( rc!=LSM_OK ) break;

      segmentPtrKey(pPtr, &pKeyT, &nKeyT);
      iTopicT = rtTopic(pPtr->eType);

      res = sortedKeyCompare(xCmp, iTopicT, pKeyT, nKeyT, iTopic, pKey, nKey);
      if( res<=0 ){
        iPtrOut = pPtr->iPtr + pPtr->iPgPtr;
      }

      if( res==0 || iMin==iMax ){
        break;
      }else if( res>0 ){
        iMax = LSM_MAX(iTry-1, iMin);
      }else{
        iMin = iTry+1;
      }
    }

    if( rc==LSM_OK ){
      assert( res==0 || (iMin==iMax && iMin>=0 && iMin<pPtr->nCell) );
      if( res ){
        rc = segmentPtrLoadCell(pPtr, iMin);
      }
      assert( rc!=LSM_OK || res>0 || iPtrOut==(pPtr->iPtr + pPtr->iPgPtr) );

      if( rc==LSM_OK ){
        switch( eSeek ){
          case LSM_SEEK_EQ: {
            int eType = pPtr->eType;
            if( (res<0 && (eType & LSM_START_DELETE))
             || (res>0 && (eType & LSM_END_DELETE))
             || (res==0 && (eType & LSM_POINT_DELETE))
            ){
              *pbStop = 1;
            }else if( res==0 && (eType & LSM_INSERT) ){
              lsm_env *pEnv = pCsr->pDb->pEnv;
              *pbStop = 1;
              pCsr->eType = pPtr->eType;
              rc = sortedBlobSet(pEnv, &pCsr->key, pPtr->pKey, pPtr->nKey);
              if( rc==LSM_OK ){
                rc = sortedBlobSet(pEnv, &pCsr->val, pPtr->pVal, pPtr->nVal);
              }
              pCsr->flags |= CURSOR_SEEK_EQ;
            }
            segmentPtrReset(pPtr);
            break;
          }
          case LSM_SEEK_LE:
            if( res>0 ) rc = segmentPtrAdvance(pCsr, pPtr, 1);
            break;
          case LSM_SEEK_GE: {
            /* Figure out if we need to 'skip' the pointer forward or not */
            if( (res<=0 && (pPtr->eType & LSM_START_DELETE)) 
             || (res>0  && (pPtr->eType & LSM_END_DELETE)) 
            ){
              rc = segmentPtrFwdPointer(pCsr, pPtr, &iPtrOut);
            }
            if( res<0 && rc==LSM_OK ){
              rc = segmentPtrAdvance(pCsr, pPtr, 0);
            }
            break;
          }
        }
      }
    }

    /* If the cursor seek has found a separator key, and this cursor is
    ** supposed to ignore separators keys, advance to the next entry.  */
    if( rc==LSM_OK && pPtr->pPg
     && segmentPtrIgnoreSeparators(pCsr, pPtr) 
     && rtIsSeparator(pPtr->eType)
    ){
      assert( eSeek!=LSM_SEEK_EQ );
      rc = segmentPtrAdvance(pCsr, pPtr, eSeek==LSM_SEEK_LE);
    }
  }

  assert( rc!=LSM_OK || assertSeekResult(pCsr,pPtr,iTopic,pKey,nKey,eSeek) );
  *piPtr = iPtrOut;
  return rc;
}

static int seekInBtree(
  MultiCursor *pCsr,              /* Multi-cursor object */
  Segment *pSeg,                  /* Seek within this segment */
  void *pKey, int nKey,           /* Key to seek to */
  Pgno *aPg,                      /* OUT: Page numbers */
  Page **ppPg                     /* OUT: Leaf (sorted-run) page reference */
){
  int i = 0;
  int rc;
  int iPg;
  Page *pPg = 0;
  Blob blob = {0, 0, 0};
  int iTopic = 0;                 /* TODO: Fix me */

  iPg = pSeg->iRoot;
  do {
    Pgno *piFirst = 0;
    if( aPg ){
      aPg[i++] = iPg;
      piFirst = &aPg[i];
    }

    rc = lsmFsDbPageGet(pCsr->pDb->pFS, iPg, &pPg);
    assert( rc==LSM_OK || pPg==0 );
    if( rc==LSM_OK ){
      u8 *aData;                  /* Buffer containing page data */
      int nData;                  /* Size of aData[] in bytes */
      int iMin;
      int iMax;
      int nRec;
      int flags;

      aData = fsPageData(pPg, &nData);
      flags = pageGetFlags(aData, nData);
      if( (flags & SEGMENT_BTREE_FLAG)==0 ) break;

      iPg = pageGetPtr(aData, nData);
      nRec = pageGetNRec(aData, nData);

      iMin = 0;
      iMax = nRec-1;
      while( iMax>=iMin ){
        int iTry = (iMin+iMax)/2;
        void *pKeyT; int nKeyT;       /* Key for cell iTry */
        int iTopicT;                  /* Topic for key pKeyT/nKeyT */
        Pgno iPtr;                    /* Pointer associated with cell iTry */
        int res;                      /* (pKey - pKeyT) */

        rc = pageGetBtreeKey(pPg, iTry, &iPtr, &iTopicT, &pKeyT, &nKeyT, &blob);
        if( rc!=LSM_OK ) break;
        if( piFirst && pKeyT==blob.pData ){
          *piFirst = pageGetBtreeRef(pPg, iTry);
          piFirst = 0;
          i++;
        }

        res = sortedKeyCompare(
            pCsr->pDb->xCmp, iTopic, pKey, nKey, iTopicT, pKeyT, nKeyT
        );
        if( res<0 ){
          iPg = iPtr;
          iMax = iTry-1;
        }else{
          iMin = iTry+1;
        }
      }
      lsmFsPageRelease(pPg);
      pPg = 0;
    }
  }while( rc==LSM_OK );

  sortedBlobFree(&blob);
  assert( (rc==LSM_OK)==(pPg!=0) );
  if( ppPg ){
    *ppPg = pPg;
  }else{
    lsmFsPageRelease(pPg);
  }
  return rc;
}

static int seekInSegment(
  MultiCursor *pCsr, 
  SegmentPtr *pPtr,
  void *pKey, int nKey,
  int iPg,                        /* Page to search */
  int eSeek,                      /* Search bias - see above */
  int *piPtr,                     /* OUT: FC pointer */
  int *pbStop                     /* OUT: Stop search flag */
){
  int iPtr = iPg;
  int rc = LSM_OK;

  if( pPtr->pSeg->iRoot ){
    Page *pPg;
    assert( pPtr->pSeg->iRoot!=0 );
    rc = seekInBtree(pCsr, pPtr->pSeg, pKey, nKey, 0, &pPg);
    if( rc==LSM_OK ) segmentPtrSetPage(pPtr, pPg);
  }else{
    if( iPtr==0 ){
      iPtr = pPtr->pSeg->iFirst;
    }
    if( rc==LSM_OK ){
      rc = segmentPtrLoadPage(pCsr->pDb->pFS, pPtr, iPtr);
    }
  }

  if( rc==LSM_OK ){
    rc = segmentPtrSeek(pCsr, pPtr, pKey, nKey, eSeek, piPtr, pbStop);
  }
  return rc;
}

/*
** Seek each segment pointer in the array of (pLvl->nRight+1) at aPtr[].
**
** pbStop:
**   This parameter is only significant if parameter eSeek is set to
**   LSM_SEEK_EQ. In this case, it is set to true before returning if
**   the seek operation is finished. This can happen in two ways:
**   
**     a) A key matching (pKey/nKey) is found, or
**     b) A point-delete or range-delete deleting the key is found.
**
**   In case (a), the multi-cursor CURSOR_SEEK_EQ flag is set and the pCsr->key
**   and pCsr->val blobs populated before returning.
*/
static int seekInLevel(
  MultiCursor *pCsr,              /* Sorted cursor object to seek */
  SegmentPtr *aPtr,               /* Pointer to array of (nRhs+1) SPs */
  int eSeek,                      /* Search bias - see above */
  void *pKey, int nKey,           /* Key to search for */
  Pgno *piPgno,                   /* IN/OUT: fraction cascade pointer (or 0) */
  int *pbStop                     /* OUT: See above */
){
  Level *pLvl = aPtr[0].pLevel;   /* Level to seek within */
  int rc = LSM_OK;                /* Return code */
  int iOut = 0;                   /* Pointer to return to caller */
  int res = -1;                   /* Result of xCmp(pKey, split) */
  int nRhs = pLvl->nRight;        /* Number of right-hand-side segments */
  int bStop = 0;

  /* If this is a composite level (one currently undergoing an incremental
  ** merge), figure out if the search key is larger or smaller than the
  ** levels split-key.  */
  if( nRhs ){
    res = sortedKeyCompare(pCsr->pDb->xCmp, 0, pKey, nKey, 
        pLvl->iSplitTopic, pLvl->pSplitKey, pLvl->nSplitKey
    );
  }

  /* If (res<0), then key pKey/nKey is smaller than the split-key (or this
  ** is not a composite level and there is no split-key). Search the 
  ** left-hand-side of the level in this case.  */
  if( res<0 ){
    int iPtr = 0;
    if( nRhs==0 ) iPtr = *piPgno;

    rc = seekInSegment(pCsr, &aPtr[0], pKey, nKey, iPtr, eSeek, &iOut, &bStop);
    if( rc==LSM_OK && nRhs>0 && eSeek==LSM_SEEK_GE && aPtr[0].pPg==0 ){
      res = 0;
    }
  }
  
  if( res>=0 ){
    int iPtr = *piPgno;
    int i;
    for(i=1; rc==LSM_OK && i<=nRhs && bStop==0; i++){
      iOut = 0;
      rc = seekInSegment(pCsr, &aPtr[i], pKey, nKey, iPtr, eSeek, &iOut,&bStop);
      iPtr = iOut;
    }

    if( rc==LSM_OK && eSeek==LSM_SEEK_LE ){
      rc = segmentPtrEnd(pCsr, &aPtr[0], 1);
    }
  }

  assert( eSeek==LSM_SEEK_EQ || bStop==0 );
  *piPgno = iOut;
  *pbStop = bStop;
  return rc;
}

static void multiCursorGetKey(
  MultiCursor *pCsr, 
  int iKey,
  int *peType,                    /* OUT: Key type (SORTED_WRITE etc.) */
  void **ppKey,                   /* OUT: Pointer to buffer containing key */
  int *pnKey                      /* OUT: Size of *ppKey in bytes */
){
  int nKey = 0;
  void *pKey = 0;
  int eType = 0;

  switch( iKey ){
    case CURSOR_DATA_TREE0:
    case CURSOR_DATA_TREE1: {
      TreeCursor *pTreeCsr = pCsr->apTreeCsr[iKey-CURSOR_DATA_TREE0];
      if( lsmTreeCursorValid(pTreeCsr) ){
        int nVal;
        void *pVal;

        lsmTreeCursorKey(pTreeCsr, &eType, &pKey, &nKey);
        lsmTreeCursorValue(pTreeCsr, &pVal, &nVal);
      }
      break;
    }

    case CURSOR_DATA_SYSTEM:
      if( pCsr->flags & CURSOR_AT_FREELIST ){
        pKey = (void *)"FREELIST";
        nKey = 8;
        eType = LSM_SYSTEMKEY | LSM_INSERT;
      }
      break;

    default: {
      int iPtr = iKey - CURSOR_DATA_SEGMENT;
      assert( iPtr>=0 );
      if( iPtr==pCsr->nPtr ){
        if( pCsr->pBtCsr ){
          pKey = pCsr->pBtCsr->pKey;
          nKey = pCsr->pBtCsr->nKey;
          eType = pCsr->pBtCsr->eType;
        }
      }else if( iPtr<pCsr->nPtr ){
        SegmentPtr *pPtr = &pCsr->aPtr[iPtr];
        if( pPtr->pPg ){
          pKey = pPtr->pKey;
          nKey = pPtr->nKey;
          eType = pPtr->eType;
        }
      }
      break;
    }
  }

  if( peType ) *peType = eType;
  if( pnKey ) *pnKey = nKey;
  if( ppKey ) *ppKey = pKey;
}

static int sortedDbKeyCompare(
  int (*xCmp)(void *, int, void *, int),
  int iLhsFlags, void *pLhsKey, int nLhsKey,
  int iRhsFlags, void *pRhsKey, int nRhsKey
){
  int res;

  /* Compare the keys, including the system flag. */
  res = sortedKeyCompare(xCmp, 
    rtTopic(iLhsFlags), pLhsKey, nLhsKey,
    rtTopic(iRhsFlags), pRhsKey, nRhsKey
  );

  /* If a key has the LSM_START_DELETE flag set, but not the LSM_INSERT or
  ** LSM_POINT_DELETE flags, it is considered a delta larger. This prevents
  ** the beginning of an open-ended set from masking a database entry or
  ** delete at a lower level.  */
  if( res==0 ){
    const int insdel = LSM_POINT_DELETE|LSM_INSERT;
    int iDel1 = 0;
    int iDel2 = 0;
    if( LSM_START_DELETE==(iLhsFlags & (LSM_START_DELETE|insdel)) ) iDel1 = +1;
    if( LSM_END_DELETE  ==(iLhsFlags & (LSM_END_DELETE  |insdel)) ) iDel1 = -1;
    if( LSM_START_DELETE==(iRhsFlags & (LSM_START_DELETE|insdel)) ) iDel2 = +1;
    if( LSM_END_DELETE  ==(iRhsFlags & (LSM_END_DELETE  |insdel)) ) iDel2 = -1;
    res = (iDel1 - iDel2);
  }

  return res;
}

static void multiCursorDoCompare(MultiCursor *pCsr, int iOut, int bReverse){
  int i1;
  int i2;
  int iRes;
  void *pKey1; int nKey1; int eType1;
  void *pKey2; int nKey2; int eType2;
  const int mul = (bReverse ? -1 : 1);

  assert( pCsr->aTree && iOut<pCsr->nTree );
  if( iOut>=(pCsr->nTree/2) ){
    i1 = (iOut - pCsr->nTree/2) * 2;
    i2 = i1 + 1;
  }else{
    i1 = pCsr->aTree[iOut*2];
    i2 = pCsr->aTree[iOut*2+1];
  }

  multiCursorGetKey(pCsr, i1, &eType1, &pKey1, &nKey1);
  multiCursorGetKey(pCsr, i2, &eType2, &pKey2, &nKey2);

  if( pKey1==0 ){
    iRes = i2;
  }else if( pKey2==0 ){
    iRes = i1;
  }else{
    int res;

    /* Compare the keys */
    res = sortedDbKeyCompare(pCsr->pDb->xCmp, 
        eType1, pKey1, nKey1, eType2, pKey2, nKey2
    );

    res = res * mul;
    if( res==0 ){
      iRes = (rtIsSeparator(eType1) ? i2 : i1);
    }else if( res<0 ){
      iRes = i1;
    }else{
      iRes = i2;
    }
  }

  pCsr->aTree[iOut] = iRes;
}

/*
** This function advances segment pointer iPtr belonging to multi-cursor
** pCsr forward (bReverse==0) or backward (bReverse!=0).
**
** If the segment pointer points to a segment that is part of a composite
** level, then the following special case is handled.
**
**   * If iPtr is the lhs of a composite level, and the cursor is being
**     advanced forwards, and segment iPtr is at EOF, move all pointers
**     that correspond to rhs segments of the same level to the first
**     key in their respective data.
*/
static int segmentCursorAdvance(
  MultiCursor *pCsr, 
  int iPtr,
  int bReverse
){
  int rc;
  SegmentPtr *pPtr = &pCsr->aPtr[iPtr];
  Level *pLvl = pPtr->pLevel;
  int bComposite;

  rc = segmentPtrAdvance(pCsr, pPtr, bReverse);
  if( rc!=LSM_OK ) return rc;
  bComposite = (pLvl->nRight>0 && pCsr->nPtr>pLvl->nRight);

  if( bComposite && pPtr->pSeg==&pLvl->lhs       /* lhs of composite level */
   && bReverse==0                                /* csr advanced forwards */
   && pPtr->pPg==0                               /* segment at EOF */
  ){
    int i;
    for(i=0; rc==LSM_OK && i<pLvl->nRight; i++){
      rc = sortedRhsFirst(pCsr, pLvl, &pCsr->aPtr[iPtr+1+i]);
    }
    for(i=pCsr->nTree-1; i>0; i--){
      multiCursorDoCompare(pCsr, i, 0);
    }
  }

  return rc;
}

static void mcursorFreeComponents(MultiCursor *pCsr){
  int i;
  lsm_env *pEnv = pCsr->pDb->pEnv;

  /* Close the tree cursor, if any. */
  lsmTreeCursorDestroy(pCsr->apTreeCsr[0]);
  lsmTreeCursorDestroy(pCsr->apTreeCsr[1]);

  /* Reset the segment pointers */
  for(i=0; i<pCsr->nPtr; i++){
    segmentPtrReset(&pCsr->aPtr[i]);
  }

  /* And the b-tree cursor, if any */
  btreeCursorFree(pCsr->pBtCsr);

  /* Free allocations */
  lsmFree(pEnv, pCsr->aPtr);
  lsmFree(pEnv, pCsr->aTree);
  lsmFree(pEnv, pCsr->pSystemVal);

  /* Zero fields */
  pCsr->nPtr = 0;
  pCsr->aPtr = 0;
  pCsr->nTree = 0;
  pCsr->aTree = 0;
  pCsr->pSystemVal = 0;
  pCsr->apTreeCsr[0] = 0;
  pCsr->apTreeCsr[1] = 0;
  pCsr->pBtCsr = 0;
}

void lsmMCursorClose(MultiCursor *pCsr){
  if( pCsr ){
    lsm_db *pDb = pCsr->pDb;
    MultiCursor **pp;             /* Iterator variable */

    /* The cursor may or may not be currently part of the linked list 
    ** starting at lsm_db.pCsr. If it is, extract it.  */
    for(pp=&pDb->pCsr; *pp; pp=&((*pp)->pNext)){
      if( *pp==pCsr ){
        *pp = pCsr->pNext;
        break;
      }
    }

    /* Free the allocation used to cache the current key, if any. */
    sortedBlobFree(&pCsr->key);
    sortedBlobFree(&pCsr->val);

    /* Free the component cursors */
    mcursorFreeComponents(pCsr);

    /* Free the cursor structure itself */
    lsmFree(pDb->pEnv, pCsr);
  }
}

#define TREE_NONE 0
#define TREE_OLD  1
#define TREE_BOTH 2

/*
** Parameter eTree is one of TREE_OLD or TREE_BOTH.
*/
static int multiCursorAddTree(MultiCursor *pCsr, Snapshot *pSnap, int eTree){
  int rc = LSM_OK;
  lsm_db *db = pCsr->pDb;

  /* Add a tree cursor on the 'old' tree, if it exists. */
  if( eTree!=TREE_NONE 
   && lsmTreeHasOld(db) 
   && db->treehdr.iOldLog!=pSnap->iLogOff 
  ){
    rc = lsmTreeCursorNew(db, 1, &pCsr->apTreeCsr[1]);
  }

  /* Add a tree cursor on the 'current' tree, if required. */
  if( rc==LSM_OK && eTree==TREE_BOTH ){
    rc = lsmTreeCursorNew(db, 0, &pCsr->apTreeCsr[0]);
  }

  return rc;
}

static int multiCursorAddRhs(MultiCursor *pCsr, Level *pLvl){
  int i;
  int nRhs = pLvl->nRight;

  assert( pLvl->nRight>0 );
  assert( pCsr->aPtr==0 );
  pCsr->aPtr = lsmMallocZero(pCsr->pDb->pEnv, sizeof(SegmentPtr) * nRhs);
  if( !pCsr->aPtr ) return LSM_NOMEM_BKPT;
  pCsr->nPtr = nRhs;

  for(i=0; i<nRhs; i++){
    pCsr->aPtr[i].pSeg = &pLvl->aRhs[i];
    pCsr->aPtr[i].pLevel = pLvl;
  }

  return LSM_OK;
}

static int multiCursorAddAll(MultiCursor *pCsr, Snapshot *pSnap){
  Level *pLvl;
  int nPtr = 0;
  int iPtr = 0;
  int rc = LSM_OK;

  for(pLvl=pSnap->pLevel; pLvl; pLvl=pLvl->pNext){
    nPtr += (1 + pLvl->nRight);
  }

  assert( pCsr->aPtr==0 );
  pCsr->aPtr = lsmMallocZeroRc(pCsr->pDb->pEnv, sizeof(SegmentPtr) * nPtr, &rc);
  if( rc==LSM_OK ) pCsr->nPtr = nPtr;

  for(pLvl=pSnap->pLevel; pLvl && rc==LSM_OK; pLvl=pLvl->pNext){
    int i;
    pCsr->aPtr[iPtr].pLevel = pLvl;
    pCsr->aPtr[iPtr].pSeg = &pLvl->lhs;
    iPtr++;
    for(i=0; i<pLvl->nRight; i++){
      pCsr->aPtr[iPtr].pLevel = pLvl;
      pCsr->aPtr[iPtr].pSeg = &pLvl->aRhs[i];
      iPtr++;
    }

    if( pLvl->nRight && pLvl->pSplitKey==0 ){
      lsmSortedSplitkey(pCsr->pDb, pLvl, &rc);
    }
  }

  return rc;
}

static int multiCursorInit(MultiCursor *pCsr, Snapshot *pSnap){
  int rc;
  rc = multiCursorAddAll(pCsr, pSnap);
  if( rc==LSM_OK ){
    rc = multiCursorAddTree(pCsr, pSnap, TREE_BOTH);
  }
  pCsr->flags |= (CURSOR_IGNORE_SYSTEM | CURSOR_IGNORE_DELETE);
  return rc;
}

static MultiCursor *multiCursorNew(lsm_db *db, int *pRc){
  MultiCursor *pCsr;
  pCsr = (MultiCursor *)lsmMallocZeroRc(db->pEnv, sizeof(MultiCursor), pRc);
  if( pCsr ){
    pCsr->pNext = db->pCsr;
    db->pCsr = pCsr;
    pCsr->pDb = db;
  }
  return pCsr;
}


void lsmSortedRemap(lsm_db *pDb){
  MultiCursor *pCsr;
  for(pCsr=pDb->pCsr; pCsr; pCsr=pCsr->pNext){
    int iPtr;
    if( pCsr->pBtCsr ){
      btreeCursorLoadKey(pCsr->pBtCsr);
    }
    for(iPtr=0; iPtr<pCsr->nPtr; iPtr++){
      segmentPtrLoadCell(&pCsr->aPtr[iPtr], pCsr->aPtr[iPtr].iCell);
    }
  }
}

static void multiCursorReadSeparators(MultiCursor *pCsr){
  if( pCsr->nPtr>0 ){
    pCsr->flags |= CURSOR_READ_SEPARATORS;
  }
}

/*
** Have this cursor skip over SORTED_DELETE entries.
*/
static void multiCursorIgnoreDelete(MultiCursor *pCsr){
  if( pCsr ) pCsr->flags |= CURSOR_IGNORE_DELETE;
}

/*
** If the free-block list is not empty, then have this cursor visit a key
** with (a) the system bit set, and (b) the key "FREELIST" and (c) a value 
** blob containing the serialized free-block list.
*/
static void multiCursorVisitFreelist(MultiCursor *pCsr, int *pnOvfl){
  assert( pCsr );
  pCsr->pnOvfl = pnOvfl;
  pCsr->flags |= CURSOR_NEW_SYSTEM;
}

/*
** Allocate and return a new database cursor.
*/
int lsmMCursorNew(
  lsm_db *pDb,                    /* Database handle */
  MultiCursor **ppCsr             /* OUT: Allocated cursor */
){
  MultiCursor *pCsr = 0;
  int rc = LSM_OK;

  pCsr = multiCursorNew(pDb, &rc);
  if( rc==LSM_OK ) rc = multiCursorInit(pCsr, pDb->pClient);

  if( rc!=LSM_OK ){
    lsmMCursorClose(pCsr);
    pCsr = 0;
  }
  assert( (rc==LSM_OK)==(pCsr!=0) );
  *ppCsr = pCsr;
  return rc;
}

static int multiCursorGetVal(
  MultiCursor *pCsr, 
  int iVal, 
  void **ppVal, 
  int *pnVal
){
  int rc = LSM_OK;

  *ppVal = 0;
  *pnVal = 0;

  switch( iVal ){
    case CURSOR_DATA_TREE0:
    case CURSOR_DATA_TREE1: {
      TreeCursor *pTreeCsr = pCsr->apTreeCsr[iVal-CURSOR_DATA_TREE0];
      if( lsmTreeCursorValid(pTreeCsr) ){
        lsmTreeCursorValue(pTreeCsr, ppVal, pnVal);
      }else{
        *ppVal = 0;
        *pnVal = 0;
      }
      break;
    }

    case CURSOR_DATA_SYSTEM:
      if( pCsr->flags & CURSOR_AT_FREELIST ){
        void *aVal;
        rc = lsmCheckpointOverflow(pCsr->pDb, &aVal, pnVal, pCsr->pnOvfl);
        assert( pCsr->pSystemVal==0 );
        *ppVal = pCsr->pSystemVal = aVal;
      }
      break;

    default: {
      int iPtr = iVal-CURSOR_DATA_SEGMENT;
      if( iPtr<pCsr->nPtr ){
        SegmentPtr *pPtr = &pCsr->aPtr[iPtr];
        if( pPtr->pPg ){
          *ppVal = pPtr->pVal;
          *pnVal = pPtr->nVal;
        }
      }
    }
  }

  assert( rc==LSM_OK || (*ppVal==0 && *pnVal==0) );
  return rc;
}

int lsmSortedLoadFreelist(
  lsm_db *pDb,                    /* Database handle (must be worker) */
  void **ppVal,                   /* OUT: Blob containing LSM free-list */
  int *pnVal                      /* OUT: Size of *ppVal blob in bytes */
){
  MultiCursor *pCsr;              /* Cursor used to retreive free-list */
  int rc = LSM_OK;                /* Return Code */

  assert( pDb->pWorker );
  assert( *ppVal==0 && *pnVal==0 );

  pCsr = multiCursorNew(pDb, &rc);
  if( pCsr ){
    rc = multiCursorAddAll(pCsr, pDb->pWorker);
    pCsr->flags |= CURSOR_IGNORE_DELETE;
  }
  
  if( rc==LSM_OK ){
    rc = lsmMCursorLast(pCsr);
    if( rc==LSM_OK 
     && rtIsWrite(pCsr->eType) && rtIsSystem(pCsr->eType)
     && pCsr->key.nData==8 
     && 0==memcmp(pCsr->key.pData, "FREELIST", 8)
    ){
      void *pVal; int nVal;         /* Value read from database */
      rc = lsmMCursorValue(pCsr, &pVal, &nVal);
      if( rc==LSM_OK ){
        *ppVal = lsmMallocRc(pDb->pEnv, nVal, &rc);
        if( *ppVal ){
          memcpy(*ppVal, pVal, nVal);
          *pnVal = nVal;
        }
      }
    }

    lsmMCursorClose(pCsr);
  }

  return rc;
}

static int multiCursorAllocTree(MultiCursor *pCsr){
  int rc = LSM_OK;
  if( pCsr->aTree==0 ){
    int nByte;                    /* Bytes of space to allocate */
    int nMin;                     /* Total number of cursors being merged */

    nMin = CURSOR_DATA_SEGMENT + pCsr->nPtr + (pCsr->pBtCsr!=0);
    pCsr->nTree = 2;
    while( pCsr->nTree<nMin ){
      pCsr->nTree = pCsr->nTree*2;
    }

    nByte = sizeof(int)*pCsr->nTree*2;
    pCsr->aTree = (int *)lsmMallocZeroRc(pCsr->pDb->pEnv, nByte, &rc);
  }
  return rc;
}

static void multiCursorCacheKey(MultiCursor *pCsr, int *pRc){
  if( *pRc==LSM_OK ){
    void *pKey;
    int nKey;
    multiCursorGetKey(pCsr, pCsr->aTree[1], &pCsr->eType, &pKey, &nKey);
    *pRc = sortedBlobSet(pCsr->pDb->pEnv, &pCsr->key, pKey, nKey);
  }
}

static int mcursorLocationOk(MultiCursor *pCsr, int bDeleteOk){
  int eType = pCsr->eType;
  int iKey;
  int i;
  int rdmask = 0;
  
  assert( pCsr->flags & (CURSOR_NEXT_OK|CURSOR_PREV_OK) );
  if( pCsr->flags & CURSOR_NEXT_OK ){
    rdmask = LSM_END_DELETE;
  }else{
    rdmask = LSM_START_DELETE;
  }

  if( (pCsr->flags & CURSOR_IGNORE_DELETE) && bDeleteOk==0 ){
    if( (eType & LSM_INSERT)==0 ) return 0;
  }
  if( (pCsr->flags & CURSOR_IGNORE_SYSTEM) && rtTopic(eType)!=0 ){
    return 0;
  }

  /* Check if this key has already been deleted by a range-delete */
  iKey = pCsr->aTree[1];
  if( (iKey>0 && (rdmask & lsmTreeCursorFlags(pCsr->apTreeCsr[0]))) 
   || (iKey>1 && (rdmask & lsmTreeCursorFlags(pCsr->apTreeCsr[1]))) 
  ){
    return 0;
  }
  for(i=CURSOR_DATA_SEGMENT; i<iKey; i++){
    int iPtr = i-CURSOR_DATA_SEGMENT;
    if( pCsr->aPtr[iPtr].pPg && (pCsr->aPtr[iPtr].eType & rdmask) ){
      return 0;
    }
  }

  return 1;
}

static int multiCursorEnd(MultiCursor *pCsr, int bLast){
  int rc = LSM_OK;
  int i;

  pCsr->flags &= ~(CURSOR_NEXT_OK | CURSOR_PREV_OK);
  pCsr->flags |= (bLast ? CURSOR_PREV_OK : CURSOR_NEXT_OK);

  if( pCsr->apTreeCsr[0] ){
    rc = lsmTreeCursorEnd(pCsr->apTreeCsr[0], bLast);
  }
  if( rc==LSM_OK && pCsr->apTreeCsr[1] ){
    rc = lsmTreeCursorEnd(pCsr->apTreeCsr[1], bLast);
  }

  if( pCsr->flags & CURSOR_NEW_SYSTEM ){
    assert( bLast==0 );
    pCsr->flags |= CURSOR_AT_FREELIST;
  }

  for(i=0; rc==LSM_OK && i<pCsr->nPtr; i++){
    SegmentPtr *pPtr = &pCsr->aPtr[i];
    Level *pLvl = pPtr->pLevel;

    rc = segmentPtrEnd(pCsr, pPtr, bLast);
    if( rc==LSM_OK && bLast==0 && pLvl->nRight && pPtr->pSeg==&pLvl->lhs ){
      int iRhs;
      for(iRhs=1+i; rc==LSM_OK && iRhs<1+i+pLvl->nRight; iRhs++){
        SegmentPtr *pRhs = &pCsr->aPtr[iRhs];
        if( pPtr->pPg==0 ){
          rc = sortedRhsFirst(pCsr, pLvl, pRhs);
        }else{
          segmentPtrReset(pRhs);
        }
      }
      i += pLvl->nRight;
    }
  }

  if( rc==LSM_OK && pCsr->pBtCsr ){
    assert( bLast==0 );
    rc = btreeCursorFirst(pCsr->pBtCsr);
  }

  if( rc==LSM_OK ){
    rc = multiCursorAllocTree(pCsr);
  }
  if( rc==LSM_OK ){
    for(i=pCsr->nTree-1; i>0; i--){
      multiCursorDoCompare(pCsr, i, bLast);
    }
  }

  multiCursorCacheKey(pCsr, &rc);
  if( rc==LSM_OK && mcursorLocationOk(pCsr, 0)==0 ){
    if( bLast ){
      rc = lsmMCursorPrev(pCsr);
    }else{
      rc = lsmMCursorNext(pCsr);
    }
  }

  return rc;
}


int mcursorSave(MultiCursor *pCsr){
  int rc = LSM_OK;
  if( pCsr->aTree ){
    int iTree = pCsr->aTree[1];
    if( iTree==CURSOR_DATA_TREE0 || iTree==CURSOR_DATA_TREE1 ){
      multiCursorCacheKey(pCsr, &rc);
    }
  }
  mcursorFreeComponents(pCsr);
  return rc;
}

int mcursorRestore(lsm_db *pDb, MultiCursor *pCsr){
  int rc;
  rc = multiCursorInit(pCsr, pDb->pClient);
  if( rc==LSM_OK && pCsr->key.pData ){
    rc = lsmMCursorSeek(pCsr, pCsr->key.pData, pCsr->key.nData, +1);
  }
  return rc;
}

int lsmSaveCursors(lsm_db *pDb){
  int rc = LSM_OK;
  MultiCursor *pCsr;

  for(pCsr=pDb->pCsr; rc==LSM_OK && pCsr; pCsr=pCsr->pNext){
    rc = mcursorSave(pCsr);
  }
  return rc;
}

int lsmRestoreCursors(lsm_db *pDb){
  int rc = LSM_OK;
  MultiCursor *pCsr;

  for(pCsr=pDb->pCsr; rc==LSM_OK && pCsr; pCsr=pCsr->pNext){
    rc = mcursorRestore(pDb, pCsr);
  }
  return rc;
}

int lsmMCursorFirst(MultiCursor *pCsr){
  return multiCursorEnd(pCsr, 0);
}

int lsmMCursorLast(MultiCursor *pCsr){
  return multiCursorEnd(pCsr, 1);
}

lsm_db *lsmMCursorDb(MultiCursor *pCsr){
  return pCsr->pDb;
}

void lsmMCursorReset(MultiCursor *pCsr){
  int i;
  lsmTreeCursorReset(pCsr->apTreeCsr[0]);
  lsmTreeCursorReset(pCsr->apTreeCsr[1]);
  for(i=0; i<pCsr->nPtr; i++){
    segmentPtrReset(&pCsr->aPtr[i]);
  }
  pCsr->key.nData = 0;
}

static int treeCursorSeek(
  MultiCursor *pCsr,
  TreeCursor *pTreeCsr, 
  void *pKey, int nKey, 
  int eSeek,
  int *pbStop
){
  int rc = LSM_OK;
  if( pTreeCsr ){
    int res = 0;
    lsmTreeCursorSeek(pTreeCsr, pKey, nKey, &res);
    switch( eSeek ){
      case LSM_SEEK_EQ: {
        int eType = lsmTreeCursorFlags(pTreeCsr);
        if( (res<0 && (eType & LSM_START_DELETE))
         || (res>0 && (eType & LSM_END_DELETE))
         || (res==0 && (eType & LSM_POINT_DELETE))
        ){
          *pbStop = 1;
        }else if( res==0 && (eType & LSM_INSERT) ){
          lsm_env *pEnv = pCsr->pDb->pEnv;
          void *p; int n;         /* Key/value from tree-cursor */
          *pbStop = 1;
          pCsr->flags |= CURSOR_SEEK_EQ;
          rc = lsmTreeCursorKey(pTreeCsr, &pCsr->eType, &p, &n);
          if( rc==LSM_OK ) rc = sortedBlobSet(pEnv, &pCsr->key, p, n);
          if( rc==LSM_OK ) rc = lsmTreeCursorValue(pTreeCsr, &p, &n);
          if( rc==LSM_OK ) rc = sortedBlobSet(pEnv, &pCsr->val, p, n);
        }
        lsmTreeCursorReset(pTreeCsr);
        break;
      }
      case LSM_SEEK_GE:
        if( res<0 && lsmTreeCursorValid(pTreeCsr) ){
          lsmTreeCursorNext(pTreeCsr);
        }
        break;
      default:
        if( res>0 ){
          assert( lsmTreeCursorValid(pTreeCsr) );
          lsmTreeCursorPrev(pTreeCsr);
        }
        break;
    }
  }
  return rc;
}


/*
** Seek the cursor.
*/
int lsmMCursorSeek(MultiCursor *pCsr, void *pKey, int nKey, int eSeek){
  int eESeek = eSeek;             /* Effective eSeek parameter */
  int bStop = 0;                  /* Set to true to halt search operation */
  int rc = LSM_OK;                /* Return code */
  int iPtr = 0;                   /* Used to iterate through pCsr->aPtr[] */
  Pgno iPgno = 0;                 /* FC pointer value */

  if( eESeek==LSM_SEEK_LEFAST ) eESeek = LSM_SEEK_LE;

  assert( eESeek==LSM_SEEK_EQ || eESeek==LSM_SEEK_LE || eESeek==LSM_SEEK_GE );
  assert( (pCsr->flags & CURSOR_NEW_SYSTEM)==0 );
  assert( (pCsr->flags & CURSOR_AT_FREELIST)==0 );
  assert( pCsr->nPtr==0 || pCsr->aPtr[0].pLevel );

  pCsr->flags &= ~(CURSOR_NEXT_OK | CURSOR_PREV_OK | CURSOR_SEEK_EQ);
  rc = treeCursorSeek(pCsr, pCsr->apTreeCsr[0], pKey, nKey, eESeek, &bStop);
  if( rc==LSM_OK && bStop==0 ){
    rc = treeCursorSeek(pCsr, pCsr->apTreeCsr[1], pKey, nKey, eESeek, &bStop);
  }

  /* Seek all segment pointers. */
  for(iPtr=0; iPtr<pCsr->nPtr && rc==LSM_OK && bStop==0; iPtr++){
    SegmentPtr *pPtr = &pCsr->aPtr[iPtr];
    assert( pPtr->pSeg==&pPtr->pLevel->lhs );
    rc = seekInLevel(pCsr, pPtr, eESeek, pKey, nKey, &iPgno, &bStop);
    iPtr += pPtr->pLevel->nRight;
  }

  if( eSeek!=LSM_SEEK_EQ ){
    if( rc==LSM_OK ){
      rc = multiCursorAllocTree(pCsr);
    }
    if( rc==LSM_OK ){
      int i;
      for(i=pCsr->nTree-1; i>0; i--){
        multiCursorDoCompare(pCsr, i, eESeek==LSM_SEEK_LE);
      }
      if( eSeek==LSM_SEEK_GE ) pCsr->flags |= CURSOR_NEXT_OK;
      if( eSeek==LSM_SEEK_LE ) pCsr->flags |= CURSOR_PREV_OK;
    }

    multiCursorCacheKey(pCsr, &rc);
    if( rc==LSM_OK && eSeek!=LSM_SEEK_LEFAST && 0==mcursorLocationOk(pCsr, 0) ){
      switch( eESeek ){
        case LSM_SEEK_EQ:
          lsmMCursorReset(pCsr);
          break;
        case LSM_SEEK_GE:
          rc = lsmMCursorNext(pCsr);
          break;
        default:
          rc = lsmMCursorPrev(pCsr);
          break;
      }
    }
  }

  return rc;
}

int lsmMCursorValid(MultiCursor *pCsr){
  int res = 0;
  if( pCsr->flags & CURSOR_SEEK_EQ ){
    res = 1;
  }else if( pCsr->aTree ){
    int iKey = pCsr->aTree[1];
    if( iKey==CURSOR_DATA_TREE0 || iKey==CURSOR_DATA_TREE1 ){
      res = lsmTreeCursorValid(pCsr->apTreeCsr[iKey-CURSOR_DATA_TREE0]);
    }else{
      void *pKey; 
      multiCursorGetKey(pCsr, iKey, 0, &pKey, 0);
      res = pKey!=0;
    }
  }
  return res;
}

static int mcursorAdvanceOk(
  MultiCursor *pCsr, 
  int bReverse,
  int *pRc
){
  void *pNew;                     /* Pointer to buffer containing new key */
  int nNew;                       /* Size of buffer pNew in bytes */
  int eNewType;                   /* Type of new record */

  if( *pRc ) return 1;

  /* Check the current key value. If it is not greater than (if bReverse==0)
  ** or less than (if bReverse!=0) the key currently cached in pCsr->key, 
  ** then the cursor has not yet been successfully advanced.  
  */
  multiCursorGetKey(pCsr, pCsr->aTree[1], &eNewType, &pNew, &nNew);
  if( pNew ){
    int res = sortedDbKeyCompare(pCsr->pDb->xCmp, 
        eNewType, pNew, nNew, pCsr->eType, pCsr->key.pData, pCsr->key.nData
    );
    if( (bReverse==0 && res<=0) || (bReverse!=0 && res>=0) ){
      return 0;
    }

    multiCursorCacheKey(pCsr, pRc);
    assert( pCsr->eType==eNewType );

    /* If this cursor is configured to skip deleted keys, and the current
    ** cursor points to a SORTED_DELETE entry, then the cursor has not been 
    ** successfully advanced.  
    **
    ** Similarly, if the cursor is configured to skip system keys and the
    ** current cursor points to a system key, it has not yet been advanced.
     */
    if( *pRc==LSM_OK && 0==mcursorLocationOk(pCsr, 0) ) return 0;
  }
  return 1;
}

static int multiCursorAdvance(MultiCursor *pCsr, int bReverse){
  int rc = LSM_OK;                /* Return Code */
  if( lsmMCursorValid(pCsr) ){
    do {
      int iKey = pCsr->aTree[1];

      /* If this multi-cursor is advancing forwards, and the sub-cursor
      ** being advanced is the one that separator keys may be being read
      ** from, record the current absolute pointer value.  */
      if( pCsr->pPrevMergePtr ){
        if( iKey==(CURSOR_DATA_SEGMENT+pCsr->nPtr) ){
          assert( pCsr->pBtCsr );
          *pCsr->pPrevMergePtr = pCsr->pBtCsr->iPtr;
        }else if( pCsr->pBtCsr==0 && pCsr->nPtr>0
               && iKey==(CURSOR_DATA_SEGMENT+pCsr->nPtr-1) 
        ){
          SegmentPtr *pPtr = &pCsr->aPtr[iKey-CURSOR_DATA_SEGMENT];
          *pCsr->pPrevMergePtr = pPtr->iPtr+pPtr->iPgPtr;
        }
      }

      if( iKey==CURSOR_DATA_TREE0 || iKey==CURSOR_DATA_TREE1 ){
        TreeCursor *pTreeCsr = pCsr->apTreeCsr[iKey-CURSOR_DATA_TREE0];
        if( bReverse ){
          rc = lsmTreeCursorPrev(pTreeCsr);
        }else{
          rc = lsmTreeCursorNext(pTreeCsr);
        }
      }else if( iKey==CURSOR_DATA_SYSTEM ){
        assert( pCsr->flags & CURSOR_AT_FREELIST );
        assert( pCsr->flags & CURSOR_NEW_SYSTEM );
        assert( bReverse==0 );
        pCsr->flags &= ~CURSOR_AT_FREELIST;
      }else if( iKey==(CURSOR_DATA_SEGMENT+pCsr->nPtr) ){
        assert( bReverse==0 && pCsr->pBtCsr );
        rc = btreeCursorNext(pCsr->pBtCsr);
      }else{
        rc = segmentCursorAdvance(pCsr, iKey-CURSOR_DATA_SEGMENT, bReverse);
      }
      if( rc==LSM_OK ){
        int i;
        for(i=(iKey+pCsr->nTree)/2; i>0; i=i/2){
          multiCursorDoCompare(pCsr, i, bReverse);
        }
      }
    }while( mcursorAdvanceOk(pCsr, bReverse, &rc)==0 );
  }
  return rc;
}

int lsmMCursorNext(MultiCursor *pCsr){
  if( (pCsr->flags & CURSOR_NEXT_OK)==0 ) return LSM_MISUSE_BKPT;
  return multiCursorAdvance(pCsr, 0);
}

int lsmMCursorPrev(MultiCursor *pCsr){
  if( (pCsr->flags & CURSOR_PREV_OK)==0 ) return LSM_MISUSE_BKPT;
  return multiCursorAdvance(pCsr, 1);
}

int lsmMCursorKey(MultiCursor *pCsr, void **ppKey, int *pnKey){
  if( pCsr->flags & CURSOR_SEEK_EQ ){
    *pnKey = pCsr->key.nData;
    *ppKey = pCsr->key.pData;
  }else{
    int iKey = pCsr->aTree[1];

    if( iKey==CURSOR_DATA_TREE0 || iKey==CURSOR_DATA_TREE1 ){
      TreeCursor *pTreeCsr = pCsr->apTreeCsr[iKey-CURSOR_DATA_TREE0];
      lsmTreeCursorKey(pTreeCsr, 0, ppKey, pnKey);
    }else{
      int nKey;

#ifndef NDEBUG
      void *pKey;
      int eType;
      multiCursorGetKey(pCsr, iKey, &eType, &pKey, &nKey);
      assert( eType==pCsr->eType );
      assert( nKey==pCsr->key.nData );
      assert( memcmp(pKey, pCsr->key.pData, nKey)==0 );
#endif

      nKey = pCsr->key.nData;
      if( nKey==0 ){
        *ppKey = 0;
      }else{
        *ppKey = pCsr->key.pData;
      }
      *pnKey = nKey; 
    }
  }
  return LSM_OK;
}

int lsmMCursorValue(MultiCursor *pCsr, void **ppVal, int *pnVal){
  void *pVal;
  int nVal;
  int rc;
  if( pCsr->flags & CURSOR_SEEK_EQ ){
    rc = LSM_OK;
    nVal = pCsr->val.nData;
    pVal = pCsr->val.pData;
  }else{

    assert( pCsr->aTree );
    assert( mcursorLocationOk(pCsr, (pCsr->flags & CURSOR_IGNORE_DELETE)) );

    rc = multiCursorGetVal(pCsr, pCsr->aTree[1], &pVal, &nVal);
    if( pVal && rc==LSM_OK ){
      rc = sortedBlobSet(pCsr->pDb->pEnv, &pCsr->val, pVal, nVal);
      pVal = pCsr->val.pData;
    }

    if( rc!=LSM_OK ){
      pVal = 0;
      nVal = 0;
    }
  }
  *ppVal = pVal;
  *pnVal = nVal;
  return rc;
}

int lsmMCursorType(MultiCursor *pCsr, int *peType){
  assert( pCsr->aTree );
  multiCursorGetKey(pCsr, pCsr->aTree[1], peType, 0, 0);
  return LSM_OK;
}

/*
** Buffer aData[], size nData, is assumed to contain a valid b-tree 
** hierarchy page image. Return the offset in aData[] of the next free
** byte in the data area (where a new cell may be written if there is
** space).
*/
static int mergeWorkerPageOffset(u8 *aData, int nData){
  int nRec;
  int iOff;
  int nKey;
  int eType;

  nRec = lsmGetU16(&aData[SEGMENT_NRECORD_OFFSET(nData)]);
  iOff = lsmGetU16(&aData[SEGMENT_CELLPTR_OFFSET(nData, nRec-1)]);
  eType = aData[iOff++];
  assert( eType==0 
       || eType==(LSM_SYSTEMKEY|LSM_SEPARATOR) 
       || eType==(LSM_SEPARATOR)
  );

  iOff += lsmVarintGet32(&aData[iOff], &nKey);
  iOff += lsmVarintGet32(&aData[iOff], &nKey);

  return iOff + (eType ? nKey : 0);
}

/*
** Following a checkpoint operation, database pages that are part of the
** checkpointed state of the LSM are deemed read-only. This includes the
** right-most page of the b-tree hierarchy of any separators array under
** construction, and all pages between it and the b-tree root, inclusive.
** This is a problem, as when further pages are appended to the separators
** array, entries must be added to the indicated b-tree hierarchy pages.
**
** This function copies all such b-tree pages to new locations, so that
** they can be modified as required.
**
** The complication is that not all database pages are the same size - due
** to the way the file.c module works some (the first and last in each block)
** are 4 bytes smaller than the others.
*/
static int mergeWorkerMoveHierarchy(
  MergeWorker *pMW,               /* Merge worker */
  int bSep                        /* True for separators run */
){
  Segment *pSeg;                  /* Segment being written */
  lsm_db *pDb = pMW->pDb;         /* Database handle */
  int rc = LSM_OK;                /* Return code */
  int i;
  Page **apHier = pMW->hier.apHier;
  int nHier = pMW->hier.nHier;

  pSeg = &pMW->pLevel->lhs;

  for(i=0; rc==LSM_OK && i<nHier; i++){
    Page *pNew = 0;
    rc = lsmFsSortedAppend(pDb->pFS, pDb->pWorker, pSeg, &pNew);
    assert( rc==LSM_OK );

    if( rc==LSM_OK ){
      u8 *a1; int n1;
      u8 *a2; int n2;

      a1 = fsPageData(pNew, &n1);
      a2 = fsPageData(apHier[i], &n2);
      assert( n1==n2 || n1+4==n2 || n2+4==n1 );

      if( n1>=n2 ){
        /* If n1 (size of the new page) is equal to or greater than n2 (the
        ** size of the old page), then copy the data into the new page. If
        ** n1==n2, this could be done with a single memcpy(). However, 
        ** since sometimes n1>n2, the page content and footer must be copied 
        ** separately. */
        int nEntry = pageGetNRec(a2, n2);
        int iEof1 = SEGMENT_EOF(n1, nEntry);
        int iEof2 = SEGMENT_EOF(n2, nEntry);
        memcpy(a1, a2, iEof2);
        memcpy(&a1[iEof1], &a2[iEof2], n2 - iEof2);
        lsmFsPageRelease(apHier[i]);
        apHier[i] = pNew;
      }else{
        lsmPutU16(&a1[SEGMENT_FLAGS_OFFSET(n1)], SEGMENT_BTREE_FLAG);
        lsmPutU16(&a1[SEGMENT_NRECORD_OFFSET(n1)], 0);
        lsmPutU64(&a1[SEGMENT_POINTER_OFFSET(n1)], 0);
        i = i - 1;
        lsmFsPageRelease(pNew);
      }
    }
  }

#ifdef LSM_DEBUG
  if( rc==LSM_OK ){
    for(i=0; i<nHier; i++) assert( lsmFsPageWritable(apHier[i]) );
  }
#endif

  return rc;
}

/*
** Allocate and populate the MergeWorker.apHier[] array.
*/
static int mergeWorkerLoadHierarchy(MergeWorker *pMW){
  int rc = LSM_OK;
  Segment *pSeg;
  Hierarchy *p;
 
  pSeg = &pMW->pLevel->lhs;
  p = &pMW->hier;

  if( p->apHier==0 && pSeg->iRoot!=0 ){
    FileSystem *pFS = pMW->pDb->pFS;
    lsm_env *pEnv = pMW->pDb->pEnv;
    Page **apHier = 0;
    int nHier = 0;
    int iPg = pSeg->iRoot;

    do {
      Page *pPg = 0;
      u8 *aData;
      int nData;
      int flags;

      rc = lsmFsDbPageGet(pFS, iPg, &pPg);
      if( rc!=LSM_OK ) break;

      aData = fsPageData(pPg, &nData);
      flags = pageGetFlags(aData, nData);
      if( flags&SEGMENT_BTREE_FLAG ){
        Page **apNew = (Page **)lsmRealloc(
            pEnv, apHier, sizeof(Page *)*(nHier+1)
        );
        if( apNew==0 ){
          rc = LSM_NOMEM_BKPT;
          break;
        }
        apHier = apNew;
        memmove(&apHier[1], &apHier[0], sizeof(Page *) * nHier);
        nHier++;

        apHier[0] = pPg;
        iPg = pageGetPtr(aData, nData);
      }else{
        lsmFsPageRelease(pPg);
        break;
      }
    }while( 1 );

    if( rc==LSM_OK ){
      u8 *aData;
      int nData;
      aData = fsPageData(apHier[0], &nData);
      pMW->aSave[0].iPgno = pageGetPtr(aData, nData);
      p->nHier = nHier;
      p->apHier = apHier;
      rc = mergeWorkerMoveHierarchy(pMW, 0);
    }else{
      int i;
      for(i=0; i<nHier; i++){
        lsmFsPageRelease(apHier[i]);
      }
      lsmFree(pEnv, apHier);
    }
  }

  return rc;
}

/*
** B-tree pages use almost the same format as regular pages. The 
** differences are:
**
**   1. The record format is (usually, see below) as follows:
**
**         + Type byte (always SORTED_SEPARATOR or SORTED_SYSTEM_SEPARATOR),
**         + Absolute pointer value (varint),
**         + Number of bytes in key (varint),
**         + Blob containing key data.
**
**   2. All pointer values are stored as absolute values (not offsets 
**      relative to the footer pointer value).
**
**   3. Each pointer that is part of a record points to a page that 
**      contains keys smaller than the records key (note: not "equal to or
**      smaller than - smaller than").
**
**   4. The pointer in the page footer of a b-tree page points to a page
**      that contains keys equal to or larger than the largest key on the
**      b-tree page.
**
** The reason for having the page footer pointer point to the right-child
** (instead of the left) is that doing things this way makes the 
** mergeWorkerMoveHierarchy() operation less complicated (since the pointers 
** that need to be updated are all stored as fixed-size integers within the 
** page footer, not varints in page records).
**
** Records may not span b-tree pages. If this function is called to add a
** record larger than (page-size / 4) bytes, then a pointer to the indexed
** array page that contains the main record is added to the b-tree instead.
** In this case the record format is:
**
**         + 0x00 byte (1 byte) 
**         + Absolute pointer value (varint),
**         + Absolute page number of page containing key (varint).
**
** See function seekInBtree() for the code that traverses b-tree pages.
*/

static int mergeWorkerBtreeWrite(
  MergeWorker *pMW,
  u8 eType,
  Pgno iPtr,
  Pgno iKeyPg,
  void *pKey,
  int nKey
){
  Segment *pSeg = &pMW->pLevel->lhs;
  Hierarchy *p = &pMW->hier;
  lsm_db *pDb = pMW->pDb;         /* Database handle */
  int rc = LSM_OK;                /* Return Code */
  int iLevel;                     /* Level of b-tree hierachy to write to */
  int nData;                      /* Size of aData[] in bytes */
  u8 *aData;                      /* Page data for level iLevel */
  int iOff;                       /* Offset on b-tree page to write record to */
  int nRec;                       /* Initial number of records on b-tree page */

  /* iKeyPg should be zero for an ordinary b-tree key, or non-zero for an
  ** indirect key. The flags byte for an indirect key is 0x00.  */
  assert( (eType==0)==(iKeyPg!=0) );

  /* The MergeWorker.apHier[] array contains the right-most leaf of the b-tree
  ** hierarchy, the root node, and all nodes that lie on the path between.
  ** apHier[0] is the right-most leaf and apHier[pMW->nHier-1] is the current
  ** root page.
  **
  ** This loop searches for a node with enough space to store the key on,
  ** starting with the leaf and iterating up towards the root. When the loop
  ** exits, the key may be written to apHier[iLevel].  */
  for(iLevel=0; iLevel<=p->nHier; iLevel++){
    int nByte;                    /* Number of free bytes required */

    if( iLevel==p->nHier ){
      /* Extend the array and allocate a new root page. */
      Page **aNew;
      aNew = (Page **)lsmRealloc(
          pMW->pDb->pEnv, p->apHier, sizeof(Page *)*(p->nHier+1)
      );
      if( !aNew ){
        return LSM_NOMEM_BKPT;
      }
      p->apHier = aNew;
    }else{
      Page *pOld;
      int nFree;

      /* If the key will fit on this page, break out of the loop here.
      ** The new entry will be written to page apHier[iLevel]. */
      pOld = p->apHier[iLevel];
      assert( lsmFsPageWritable(pOld) );
      aData = fsPageData(pOld, &nData);
      if( eType==0 ){
        nByte = 2 + 1 + lsmVarintLen32(iPtr) + lsmVarintLen32(iKeyPg);
      }else{
        nByte = 2 + 1 + lsmVarintLen32(iPtr) + lsmVarintLen32(nKey) + nKey;
      }
      nRec = pageGetNRec(aData, nData);
      nFree = SEGMENT_EOF(nData, nRec) - mergeWorkerPageOffset(aData, nData);
      if( nByte<=nFree ) break;

      /* Otherwise, this page is full. Set the right-hand-child pointer
      ** to iPtr and release it.  */
      lsmPutU64(&aData[SEGMENT_POINTER_OFFSET(nData)], iPtr);
      rc = lsmFsPagePersist(pOld);
      if( rc==LSM_OK ){
        iPtr = lsmFsPageNumber(pOld);
        lsmFsPageRelease(pOld);
      }
    }

    /* Allocate a new page for apHier[iLevel]. */
    p->apHier[iLevel] = 0;
    if( rc==LSM_OK ){
      rc = lsmFsSortedAppend(
          pDb->pFS, pDb->pWorker, pSeg, &p->apHier[iLevel]
      );
    }
    if( rc!=LSM_OK ) return rc;

    aData = fsPageData(p->apHier[iLevel], &nData);
    memset(aData, 0, nData);
    lsmPutU16(&aData[SEGMENT_FLAGS_OFFSET(nData)], SEGMENT_BTREE_FLAG);
    lsmPutU16(&aData[SEGMENT_NRECORD_OFFSET(nData)], 0);

    if( iLevel==p->nHier ){
      p->nHier++;
      break;
    }
  }

  /* Write the key into page apHier[iLevel]. */
  aData = fsPageData(p->apHier[iLevel], &nData);
  iOff = mergeWorkerPageOffset(aData, nData);
  nRec = pageGetNRec(aData, nData);
  lsmPutU16(&aData[SEGMENT_CELLPTR_OFFSET(nData, nRec)], iOff);
  lsmPutU16(&aData[SEGMENT_NRECORD_OFFSET(nData)], nRec+1);
  if( eType==0 ){
    aData[iOff++] = 0x00;
    iOff += lsmVarintPut32(&aData[iOff], iPtr);
    iOff += lsmVarintPut32(&aData[iOff], iKeyPg);
  }else{
    aData[iOff++] = eType;
    iOff += lsmVarintPut32(&aData[iOff], iPtr);
    iOff += lsmVarintPut32(&aData[iOff], nKey);
    memcpy(&aData[iOff], pKey, nKey);
  }

  return rc;
}

static int mergeWorkerBtreeIndirect(MergeWorker *pMW){
  int rc = LSM_OK;
  if( pMW->iIndirect ){
    Pgno iKeyPg = pMW->aSave[1].iPgno;
    rc = mergeWorkerBtreeWrite(pMW, 0, pMW->iIndirect, iKeyPg, 0, 0);
    pMW->iIndirect = 0;
  }
  return rc;
}

/*
** Append the database key (iTopic/pKey/nKey) to the b-tree under 
** construction. This key has not yet been written to a segment page.
** The pointer that will accompany the new key in the b-tree - that
** points to the completed segment page that contains keys smaller than
** (pKey/nKey) is currently stored in pMW->aSave[0].iPgno.
*/
static int mergeWorkerPushHierarchy(
  MergeWorker *pMW,               /* Merge worker object */
  int iTopic,                     /* Topic value for this key */
  void *pKey,                     /* Pointer to key buffer */
  int nKey                        /* Size of pKey buffer in bytes */
){
  lsm_db *pDb = pMW->pDb;         /* Database handle */
  int rc = LSM_OK;                /* Return Code */
  int iLevel;                     /* Level of b-tree hierachy to write to */
  int nData;                      /* Size of aData[] in bytes */
  u8 *aData;                      /* Page data for level iLevel */
  int iOff;                       /* Offset on b-tree page to write record to */
  int nRec;                       /* Initial number of records on b-tree page */
  Pgno iPtr;                      /* Pointer value to accompany pKey/nKey */

  Hierarchy *p;
  Segment *pSeg;

  /* If there exists a b-tree hierarchy and it is not loaded into 
  ** memory, load it now.  */
  pSeg = &pMW->pLevel->lhs;
  p = &pMW->hier;

  assert( pMW->aSave[0].bStore==0 );
  assert( pMW->aSave[1].bStore==0 );
  rc = mergeWorkerBtreeIndirect(pMW);

  /* Obtain the absolute pointer value to store along with the key in the
  ** page body. This pointer points to a page that contains keys that are
  ** smaller than pKey/nKey.  */
  iPtr = pMW->aSave[0].iPgno;
  assert( iPtr!=0 );

  /* Determine if the indirect format should be used. */
  if( (nKey*4 > lsmFsPageSize(pMW->pDb->pFS)) ){
    pMW->iIndirect = iPtr;
    pMW->aSave[1].bStore = 1;
  }else{
    rc = mergeWorkerBtreeWrite(
        pMW, (u8)(iTopic | LSM_SEPARATOR), iPtr, 0, pKey, nKey
    );
  }

  /* Ensure that the SortedRun.iRoot field is correct. */
  return rc;
}

static int mergeWorkerFinishHierarchy(
  MergeWorker *pMW                /* Merge worker object */
){
  int i;                          /* Used to loop through apHier[] */
  int rc = LSM_OK;                /* Return code */
  Pgno iPtr;                      /* New right-hand-child pointer value */

  iPtr = pMW->aSave[0].iPgno;
  for(i=0; i<pMW->hier.nHier && rc==LSM_OK; i++){
    Page *pPg = pMW->hier.apHier[i];
    int nData;                    /* Size of aData[] in bytes */
    u8 *aData;                    /* Page data for pPg */

    aData = fsPageData(pPg, &nData);
    lsmPutU64(&aData[SEGMENT_POINTER_OFFSET(nData)], iPtr);

    rc = lsmFsPagePersist(pPg);
    iPtr = lsmFsPageNumber(pPg);
    lsmFsPageRelease(pPg);
  }

  if( pMW->hier.nHier ){
    pMW->pLevel->lhs.iRoot = iPtr;
    lsmFree(pMW->pDb->pEnv, pMW->hier.apHier);
    pMW->hier.apHier = 0;
    pMW->hier.nHier = 0;
  }

  return rc;
}

static int keyszToSkip(FileSystem *pFS, int nKey){
  int nPgsz;                /* Nominal database page size */
  nPgsz = lsmFsPageSize(pFS);
  return LSM_MIN(((nKey * 4) / nPgsz), 3);
}

/*
** Release the reference to the current output page of merge-worker *pMW
** (reference pMW->pPage). Set the page number values in aSave[] as 
** required (see comments above struct MergeWorker for details).
*/
static int mergeWorkerPersistAndRelease(MergeWorker *pMW){
  int rc;
  int i;

  assert( pMW->pPage || (pMW->aSave[0].bStore==0 && pMW->aSave[1].bStore==0) );

  /* Persist the page */
  rc = lsmFsPagePersist(pMW->pPage);

  /* If required, save the page number. */
  for(i=0; i<2; i++){
    if( pMW->aSave[i].bStore ){
      pMW->aSave[i].iPgno = lsmFsPageNumber(pMW->pPage);
      pMW->aSave[i].bStore = 0;
    }
  }

  /* Release the completed output page. */
  lsmFsPageRelease(pMW->pPage);
  pMW->pPage = 0;
  return rc;
}

/*
** Advance to the next page of an output run being populated by merge-worker
** pMW. The footer of the new page is initialized to indicate that it contains
** zero records. The flags field is cleared. The page footer pointer field
** is set to iFPtr.
**
** If successful, LSM_OK is returned. Otherwise, an error code.
*/
static int mergeWorkerNextPage(
  MergeWorker *pMW,               /* Merge worker object to append page to */
  Pgno iFPtr                      /* Pointer value for footer of new page */
){
  int rc = LSM_OK;                /* Return code */
  Page *pNext = 0;                /* New page appended to run */
  lsm_db *pDb = pMW->pDb;         /* Database handle */
  Segment *pSeg;                  /* Run to append to */

  pSeg = &pMW->pLevel->lhs;
  rc = lsmFsSortedAppend(pDb->pFS, pDb->pWorker, pSeg, &pNext);
  assert( rc!=LSM_OK || pSeg->iFirst>0 );

  if( rc==LSM_OK ){
    u8 *aData;                    /* Data buffer belonging to page pNext */
    int nData;                    /* Size of aData[] in bytes */

    rc = mergeWorkerPersistAndRelease(pMW);

    pMW->pPage = pNext;
    pMW->pLevel->pMerge->iOutputOff = 0;
    aData = fsPageData(pNext, &nData);
    lsmPutU16(&aData[SEGMENT_NRECORD_OFFSET(nData)], 0);
    lsmPutU16(&aData[SEGMENT_FLAGS_OFFSET(nData)], 0);
    lsmPutU64(&aData[SEGMENT_POINTER_OFFSET(nData)], iFPtr);
    pMW->nWork++;
  }

  return rc;
}

/*
** Write a blob of data into an output segment being populated by a 
** merge-worker object. If argument bSep is true, write into the separators
** array. Otherwise, the main array.
**
** This function is used to write the blobs of data for keys and values.
*/
static int mergeWorkerData(
  MergeWorker *pMW,               /* Merge worker object */
  int bSep,                       /* True to write to separators run */
  int iFPtr,                      /* Footer ptr for new pages */
  u8 *aWrite,                     /* Write data from this buffer */
  int nWrite                      /* Size of aWrite[] in bytes */
){
  int rc = LSM_OK;                /* Return code */
  int nRem = nWrite;              /* Number of bytes still to write */

  while( nRem>0 ){
    Merge *pMerge = pMW->pLevel->pMerge;
    int nCopy;                    /* Number of bytes to copy */
    u8 *aData;                    /* Pointer to buffer of current output page */
    int nData;                    /* Size of aData[] in bytes */
    int nRec;                     /* Number of records on current output page */
    int iOff;                     /* Offset in aData[] to write to */

    assert( lsmFsPageWritable(pMW->pPage) );
   
    aData = fsPageData(pMW->pPage, &nData);
    nRec = pageGetNRec(aData, nData);
    iOff = pMerge->iOutputOff;
    nCopy = LSM_MIN(nRem, SEGMENT_EOF(nData, nRec) - iOff);

    memcpy(&aData[iOff], &aWrite[nWrite-nRem], nCopy);
    nRem -= nCopy;

    if( nRem>0 ){
      rc = mergeWorkerNextPage(pMW, iFPtr);
    }else{
      pMerge->iOutputOff = iOff + nCopy;
    }
  }

  return rc;
}


/*
** The MergeWorker passed as the only argument is working to merge two or
** more existing segments together (not to flush an in-memory tree). It
** has not yet written the first key to the first page of the output.
*/
static int mergeWorkerFirstPage(MergeWorker *pMW){
  int rc = LSM_OK;                /* Return code */
  Page *pPg = 0;                  /* First page of run pSeg */
  int iFPtr = 0;                  /* Pointer value read from footer of pPg */
  MultiCursor *pCsr = pMW->pCsr;

  assert( pMW->pPage==0 );

  if( pCsr->pBtCsr ){
    rc = LSM_OK;
    iFPtr = pMW->pLevel->pNext->lhs.iFirst;
  }else if( pCsr->nPtr>0 ){
    Segment *pSeg;
    pSeg = pCsr->aPtr[pCsr->nPtr-1].pSeg;
    rc = lsmFsDbPageGet(pMW->pDb->pFS, pSeg->iFirst, &pPg);
    if( rc==LSM_OK ){
      u8 *aData;                    /* Buffer for page pPg */
      int nData;                    /* Size of aData[] in bytes */
      aData = fsPageData(pPg, &nData);
      iFPtr = pageGetPtr(aData, nData);
      lsmFsPageRelease(pPg);
    }
  }

  if( rc==LSM_OK ){
    rc = mergeWorkerNextPage(pMW, iFPtr);
    if( pCsr->pPrevMergePtr ) *pCsr->pPrevMergePtr = iFPtr;
    pMW->aSave[0].bStore = 1;
  }

  return rc;
}

static int mergeWorkerWrite(
  MergeWorker *pMW,               /* Merge worker object to write into */
  int eType,                      /* One of SORTED_SEPARATOR, WRITE or DELETE */
  void *pKey, int nKey,           /* Key value */
  MultiCursor *pCsr,              /* Read value (if any) from here */
  int iPtr                        /* Absolute value of page pointer, or 0 */
){
  int rc = LSM_OK;                /* Return code */
  Merge *pMerge;                  /* Persistent part of level merge state */
  int nHdr;                       /* Space required for this record header */
  Page *pPg;                      /* Page to write to */
  u8 *aData;                      /* Data buffer for page pWriter->pPage */
  int nData;                      /* Size of buffer aData[] in bytes */
  int nRec;                       /* Number of records on page pPg */
  int iFPtr;                      /* Value of pointer in footer of pPg */
  int iRPtr;                      /* Value of pointer written into record */
  int iOff;                       /* Current write offset within page pPg */
  Segment *pSeg;                  /* Segment being written */
  int flags = 0;                  /* If != 0, flags value for page footer */
  int bFirst = 0;                 /* True for first key of output run */
  void *pVal;
  int nVal;

  pMerge = pMW->pLevel->pMerge;    
  pSeg = &pMW->pLevel->lhs;

  if( pSeg->iFirst==0 && pMW->pPage==0 ){
    rc = mergeWorkerFirstPage(pMW);
    bFirst = 1;
  }
  pPg = pMW->pPage;
  if( pPg ){
    aData = fsPageData(pPg, &nData);
    nRec = pageGetNRec(aData, nData);
    iFPtr = pageGetPtr(aData, nData);
    iRPtr = iPtr - iFPtr;
  }
     
  /* Figure out how much space is required by the new record. The space
  ** required is divided into two sections: the header and the body. The
  ** header consists of the intial varint fields. The body are the blobs 
  ** of data that correspond to the key and value data. The entire header 
  ** must be stored on the page. The body may overflow onto the next and
  ** subsequent pages.
  **
  ** The header space is:
  **
  **     1) record type - 1 byte.
  **     2) Page-pointer-offset - 1 varint
  **     3) Key size - 1 varint
  **     4) Value size - 1 varint (only if LSM_INSERT flag is set)
  */
  rc = lsmMCursorValue(pCsr, &pVal, &nVal);
  if( rc==LSM_OK ){
    nHdr = 1 + lsmVarintLen32(iRPtr) + lsmVarintLen32(nKey);
    if( rtIsWrite(eType) ) nHdr += lsmVarintLen32(nVal);

    /* If the entire header will not fit on page pPg, or if page pPg is 
    ** marked read-only, advance to the next page of the output run. */
    iOff = pMerge->iOutputOff;
    if( iOff<0 || pPg==0 || iOff+nHdr > SEGMENT_EOF(nData, nRec+1) ){
      iFPtr = *pCsr->pPrevMergePtr;
      iRPtr = iPtr - iFPtr;
      iOff = 0;
      nRec = 0;
      rc = mergeWorkerNextPage(pMW, iFPtr);
      pPg = pMW->pPage;
    }
  }

  /* If this record header will be the first on the page, and the page is 
  ** not the very first in the entire run, add a copy of the key to the
  ** b-tree hierarchy.
  */
  if( rc==LSM_OK && nRec==0 && bFirst==0 ){
    assert( pMerge->nSkip>=0 );

    if( pMerge->nSkip==0 ){
      rc = mergeWorkerPushHierarchy(pMW, rtTopic(eType), pKey, nKey);
      assert( pMW->aSave[0].bStore==0 );
      pMW->aSave[0].bStore = 1;
      pMerge->nSkip = keyszToSkip(pMW->pDb->pFS, nKey);
    }else{
      pMerge->nSkip--;
      flags = PGFTR_SKIP_THIS_FLAG;
    }

    if( pMerge->nSkip ) flags |= PGFTR_SKIP_NEXT_FLAG;
  }

  /* Update the output segment */
  if( rc==LSM_OK ){
    aData = fsPageData(pPg, &nData);

    /* Update the page footer. */
    lsmPutU16(&aData[SEGMENT_NRECORD_OFFSET(nData)], nRec+1);
    lsmPutU16(&aData[SEGMENT_CELLPTR_OFFSET(nData, nRec)], iOff);
    if( flags ) lsmPutU16(&aData[SEGMENT_FLAGS_OFFSET(nData)], flags);

    /* Write the entry header into the current page. */
    aData[iOff++] = eType;                                               /* 1 */
    iOff += lsmVarintPut32(&aData[iOff], iRPtr);                         /* 2 */
    iOff += lsmVarintPut32(&aData[iOff], nKey);                          /* 3 */
    if( rtIsWrite(eType) ) iOff += lsmVarintPut32(&aData[iOff], nVal);   /* 4 */
    pMerge->iOutputOff = iOff;

    /* Write the key and data into the segment. */
    assert( iFPtr==pageGetPtr(aData, nData) );
    rc = mergeWorkerData(pMW, 0, iFPtr+iRPtr, pKey, nKey);
    if( rc==LSM_OK && rtIsWrite(eType) ){
      if( rtTopic(eType)==0 ) rc = lsmMCursorValue(pCsr, &pVal, &nVal);
      if( rc==LSM_OK ){
        rc = mergeWorkerData(pMW, 0, iFPtr+iRPtr, pVal, nVal);
      }
    }
  }

  return rc;
}


static int multiCursorSetupTree(MultiCursor *pCsr, int bRev){
  int rc;

  assert( pCsr->aTree==0 );

  rc = multiCursorAllocTree(pCsr);
  if( rc==LSM_OK ){
    int i;
    for(i=pCsr->nTree-1; i>0; i--){
      multiCursorDoCompare(pCsr, i, bRev);
    }
  }

  multiCursorCacheKey(pCsr, &rc);
  return rc;
}

/*
** Free all resources allocated by mergeWorkerInit().
*/
static void mergeWorkerShutdown(MergeWorker *pMW, int *pRc){
  int i;                          /* Iterator variable */
  int rc = *pRc;
  MultiCursor *pCsr = pMW->pCsr;
  Hierarchy *p = &pMW->hier;

  /* Unless the merge has finished, save the cursor position in the
  ** Merge.aInput[] array. See function mergeWorkerInit() for the 
  ** code to restore a cursor position based on aInput[].  */
  if( rc==LSM_OK && pCsr && lsmMCursorValid(pCsr) ){
    Merge *pMerge = pMW->pLevel->pMerge;
    int bBtree = (pCsr->pBtCsr!=0);
    int iPtr;

    /* pMerge->nInput==0 indicates that this is a FlushTree() operation. */
    assert( pMerge->nInput==0 || pMW->pLevel->nRight>0 );
    assert( pMerge->nInput==0 || pMerge->nInput==(pCsr->nPtr+bBtree) );

    for(i=0; i<(pMerge->nInput-bBtree); i++){
      SegmentPtr *pPtr = &pCsr->aPtr[i];
      if( pPtr->pPg ){
        pMerge->aInput[i].iPg = lsmFsPageNumber(pPtr->pPg);
        pMerge->aInput[i].iCell = pPtr->iCell;
      }else{
        pMerge->aInput[i].iPg = 0;
        pMerge->aInput[i].iCell = 0;
      }
    }
    if( bBtree && pMerge->nInput ){
      assert( i==pCsr->nPtr );
      btreeCursorPosition(pCsr->pBtCsr, &pMerge->aInput[i]);
    }

    /* Store the location of the split-key */
    iPtr = pCsr->aTree[1] - CURSOR_DATA_SEGMENT;
    if( iPtr<pCsr->nPtr ){
      pMerge->splitkey = pMerge->aInput[iPtr];
    }else{
      btreeCursorSplitkey(pCsr->pBtCsr, &pMerge->splitkey);
    }
    
    pMerge->iOutputOff = -1;
  }

  lsmMCursorClose(pCsr);

  /* Persist and release the output page. */
  if( rc==LSM_OK ) rc = mergeWorkerPersistAndRelease(pMW);
  if( rc==LSM_OK ) rc = mergeWorkerBtreeIndirect(pMW);
  if( rc==LSM_OK ) rc = mergeWorkerFinishHierarchy(pMW);

  lsmFree(pMW->pDb->pEnv, pMW->aGobble);
  pMW->aGobble = 0;
  pMW->pCsr = 0;
  pMW->pPage = 0;
  pMW->pPage = 0;

  *pRc = rc;
}

/*
** The cursor passed as the first argument is being used as the input for
** a merge operation. When this function is called, *piFlags contains the
** database entry flags for the current entry. The entry about to be written
** to the output.
**
*/
static void mergeRangeDeletes(MultiCursor *pCsr, int *piFlags){
  int f = *piFlags;
  int iKey = pCsr->aTree[1];
  int i;

  if( pCsr->flags & CURSOR_IGNORE_DELETE ){
    /* The ignore-delete flag is set when the output of the merge will form
    ** the oldest level in the database. In this case there is no point in
    ** retaining any range-delete flags.  */
    assert( (f & LSM_POINT_DELETE)==0 );
    f &= ~(LSM_START_DELETE|LSM_END_DELETE);
  }else{
    if( iKey==0 ){
      int btreeflags = lsmTreeCursorFlags(pCsr->apTreeCsr[1]);
      if( btreeflags & LSM_END_DELETE ){
        f |= (LSM_START_DELETE|LSM_END_DELETE);
      }
    }

    for(i=LSM_MAX(0, iKey+1-CURSOR_DATA_SEGMENT); i<pCsr->nPtr; i++){
      if( pCsr->aPtr[i].eType & LSM_END_DELETE ){
        f |= (LSM_START_DELETE|LSM_END_DELETE);
      }
    }

    if( (f & LSM_START_DELETE) && (f & LSM_END_DELETE) && (f & LSM_INSERT)==0 ){
      f = 0;
    }
  }

  *piFlags = f;
}

static int mergeWorkerStep(MergeWorker *pMW){
  lsm_db *pDb = pMW->pDb;       /* Database handle */
  MultiCursor *pCsr;            /* Cursor to read input data from */
  int rc = LSM_OK;              /* Return code */
  int eType;                    /* SORTED_SEPARATOR, WRITE or DELETE */
  void *pKey; int nKey;         /* Key */
  Segment *pSeg;                /* Output segment */
  Pgno iPtr;

  pCsr = pMW->pCsr;
  pSeg = &pMW->pLevel->lhs;

  /* Pull the next record out of the source cursor. */
  lsmMCursorKey(pCsr, &pKey, &nKey);
  eType = pCsr->eType;

  /* Figure out if the output record may have a different pointer value
  ** than the previous. This is the case if the current key is identical to
  ** a key that appears in the lowest level run being merged. If so, set 
  ** iPtr to the absolute pointer value. If not, leave iPtr set to zero, 
  ** indicating that the output pointer value should be a copy of the pointer 
  ** value written with the previous key.  */
  iPtr = (pCsr->pPrevMergePtr ? *pCsr->pPrevMergePtr : 0);
  if( pCsr->pBtCsr ){
    BtreeCursor *pBtCsr = pCsr->pBtCsr;
    if( pBtCsr->pKey ){
      int res = rtTopic(pBtCsr->eType) - rtTopic(eType);
      if( res==0 ) res = pDb->xCmp(pBtCsr->pKey, pBtCsr->nKey, pKey, nKey);
      if( 0==res ) iPtr = pBtCsr->iPtr;
      assert( res>=0 );
    }
  }else if( pCsr->nPtr ){
    SegmentPtr *pPtr = &pCsr->aPtr[pCsr->nPtr-1];
    if( pPtr->pPg
     && 0==pDb->xCmp(pPtr->pKey, pPtr->nKey, pKey, nKey)
    ){
      iPtr = pPtr->iPtr+pPtr->iPgPtr;
    }
  }

  mergeRangeDeletes(pCsr, &eType);

  if( eType!=0 ){
    if( pMW->aGobble ){
      int iGobble = pCsr->aTree[1] - CURSOR_DATA_SEGMENT;
      if( iGobble<pCsr->nPtr ){
        SegmentPtr *pGobble = &pCsr->aPtr[iGobble];
        if( (pGobble->flags & PGFTR_SKIP_THIS_FLAG)==0 ){
          pMW->aGobble[iGobble] = lsmFsPageNumber(pGobble->pPg);
        }
      }
    }

    /* If this is a separator key and we know that the output pointer has not
    ** changed, there is no point in writing an output record. Otherwise,
    ** proceed. */
    if( rtIsSeparator(eType)==0 || iPtr!=0 ){
      /* Write the record into the main run. */
      if( rc==LSM_OK ){
        rc = mergeWorkerWrite(pMW, eType, pKey, nKey, pCsr, iPtr);
      }
    }
  }

  /* Advance the cursor to the next input record (assuming one exists). */
  assert( lsmMCursorValid(pMW->pCsr) );
  if( rc==LSM_OK ) rc = lsmMCursorNext(pMW->pCsr);

  /* If the cursor is at EOF, the merge is finished. Release all page
  ** references currently held by the merge worker and inform the 
  ** FileSystem object that no further pages will be appended to either 
  ** the main or separators array. 
  */
  if( rc==LSM_OK && !lsmMCursorValid(pMW->pCsr) ){

    mergeWorkerShutdown(pMW, &rc);
    if( pSeg->iFirst ){
      rc = lsmFsSortedFinish(pDb->pFS, pSeg);
    }

#ifdef LSM_DEBUG_EXPENSIVE
    if( rc==LSM_OK ){
#if 0
      rc = assertBtreeOk(pDb, pSeg);
      if( pMW->pCsr->pBtCsr ){
        Segment *pNext = &pMW->pLevel->pNext->lhs;
        rc = assertPointersOk(pDb, pSeg, pNext, 0);
      }
#endif
    }
#endif
  }
  return rc;
}

static int mergeWorkerDone(MergeWorker *pMW){
  return pMW->pCsr==0 || !lsmMCursorValid(pMW->pCsr);
}

static void sortedFreeLevel(lsm_env *pEnv, Level *p){
  if( p ){
    lsmFree(pEnv, p->pSplitKey);
    lsmFree(pEnv, p->pMerge);
    lsmFree(pEnv, p->aRhs);
    lsmFree(pEnv, p);
  }
}

static void sortedInvokeWorkHook(lsm_db *pDb){
  if( pDb->xWork ){
    pDb->xWork(pDb, pDb->pWorkCtx);
  }
}

static int sortedNewToplevel(
  lsm_db *pDb,                    /* Connection handle */
  int eTree,                      /* One of the TREE_XXX constants */
  int *pnOvfl,                    /* OUT: Number of free-list entries stored */
  int *pnWrite                    /* OUT: Number of database pages written */
){
  int rc = LSM_OK;                /* Return Code */
  MultiCursor *pCsr = 0;
  Level *pNext = 0;               /* The current top level */
  Level *pNew;                    /* The new level itself */
  Segment *pDel = 0;              /* Delete separators from this segment */
  int nWrite = 0;                 /* Number of database pages written */

  assert( pnOvfl );

  /* Allocate the new level structure to write to. */
  pNext = lsmDbSnapshotLevel(pDb->pWorker);
  pNew = (Level *)lsmMallocZeroRc(pDb->pEnv, sizeof(Level), &rc);
  if( pNew ){
    pNew->pNext = pNext;
    lsmDbSnapshotSetLevel(pDb->pWorker, pNew);
  }

  /* Create a cursor to gather the data required by the new segment. The new
  ** segment contains everything in the tree and pointers to the next segment
  ** in the database (if any).  */
  pCsr = multiCursorNew(pDb, &rc);
  if( pCsr ){
    pCsr->pDb = pDb;
    multiCursorVisitFreelist(pCsr, pnOvfl);
    rc = multiCursorAddTree(pCsr, pDb->pWorker, eTree);
    if( rc==LSM_OK && pNext && pNext->pMerge==0 && pNext->lhs.iRoot ){
      pDel = &pNext->lhs;
      rc = btreeCursorNew(pDb, pDel, &pCsr->pBtCsr);
    }

    if( pNext==0 ){
      multiCursorIgnoreDelete(pCsr);
    }
  }

  if( rc!=LSM_OK ){
    lsmMCursorClose(pCsr);
  }else{
    Pgno iLeftPtr = 0;
    Merge merge;                  /* Merge object used to create new level */
    MergeWorker mergeworker;      /* MergeWorker object for the same purpose */

    memset(&merge, 0, sizeof(Merge));
    memset(&mergeworker, 0, sizeof(MergeWorker));

    pNew->pMerge = &merge;
    mergeworker.pDb = pDb;
    mergeworker.pLevel = pNew;
    mergeworker.pCsr = pCsr;
    pCsr->pPrevMergePtr = &iLeftPtr;

    /* Mark the separators array for the new level as a "phantom". */
    mergeworker.bFlush = 1;

    /* Do the work to create the new merged segment on disk */
    if( rc==LSM_OK ) rc = lsmMCursorFirst(pCsr);
    while( rc==LSM_OK && mergeWorkerDone(&mergeworker)==0 ){
      rc = mergeWorkerStep(&mergeworker);
    }

    nWrite = mergeworker.nWork;
    mergeWorkerShutdown(&mergeworker, &rc);
    pNew->pMerge = 0;
  }

  /* Link the new level into the top of the tree. */
  if( rc==LSM_OK ){
    if( pDel ) pDel->iRoot = 0;
  }else{
    lsmDbSnapshotSetLevel(pDb->pWorker, pNext);
    sortedFreeLevel(pDb->pEnv, pNew);
  }

#if 0
  lsmSortedDumpStructure(pDb, pDb->pWorker, 1, 0, "new-toplevel");
#endif

  if( rc==LSM_OK ){
    assertBtreeOk(pDb, &pNew->lhs);
    sortedInvokeWorkHook(pDb);
  }

  if( pnWrite ) *pnWrite = nWrite;
  pDb->pWorker->nWrite += nWrite;
  return rc;
}

/*
** The nMerge levels in the LSM beginning with pLevel consist of a
** left-hand-side segment only. Replace these levels with a single new
** level consisting of a new empty segment on the left-hand-side and the
** nMerge segments from the replaced levels on the right-hand-side.
**
** Also, allocate and populate a Merge object and set Level.pMerge to
** point to it.
*/
static int sortedMergeSetup(
  lsm_db *pDb,                    /* Database handle */
  Level *pLevel,                  /* First level to merge */
  int nMerge,                     /* Merge this many levels together */
  Level **ppNew                   /* New, merged, level */
){
  int rc = LSM_OK;                /* Return Code */
  Level *pNew;                    /* New Level object */
  int bUseNext = 0;               /* True to link in next separators */
  Merge *pMerge;                  /* New Merge object */
  int nByte;                      /* Bytes of space allocated at pMerge */

#ifdef LSM_DEBUG
  int iLevel;
  Level *pX = pLevel;
  for(iLevel=0; iLevel<nMerge; iLevel++){
    assert( pX->nRight==0 );
    pX = pX->pNext;
  }
#endif

  /* Allocate the new Level object */
  pNew = (Level *)lsmMallocZeroRc(pDb->pEnv, sizeof(Level), &rc);
  if( pNew ){
    pNew->aRhs = (Segment *)lsmMallocZeroRc(pDb->pEnv, 
                                        nMerge * sizeof(Segment), &rc);
  }


  /* Populate the new Level object */
  if( rc==LSM_OK ){
    Level *pNext = 0;             /* Level following pNew */
    int i;
    Level *pTopLevel;
    Level *p = pLevel;
    Level **pp;
    pNew->nRight = nMerge;
    pNew->iAge = pLevel->iAge+1;
    for(i=0; i<nMerge; i++){
      assert( p->nRight==0 );
      pNext = p->pNext;
      pNew->aRhs[i] = p->lhs;
      sortedFreeLevel(pDb->pEnv, p);
      p = pNext;
    }

    /* Replace the old levels with the new. */
    pTopLevel = lsmDbSnapshotLevel(pDb->pWorker);
    pNew->pNext = p;
    for(pp=&pTopLevel; *pp!=pLevel; pp=&((*pp)->pNext));
    *pp = pNew;
    lsmDbSnapshotSetLevel(pDb->pWorker, pTopLevel);

    /* Determine whether or not the next separators will be linked in */
    if( pNext && pNext->pMerge==0 && pNext->lhs.iRoot ){
      bUseNext = 1;
    }
  }

  /* Allocate the merge object */
  nByte = sizeof(Merge) + sizeof(MergeInput) * (nMerge + bUseNext);
  pMerge = (Merge *)lsmMallocZeroRc(pDb->pEnv, nByte, &rc);
  if( pMerge ){
    pMerge->aInput = (MergeInput *)&pMerge[1];
    pMerge->nInput = nMerge + bUseNext;
    pNew->pMerge = pMerge;
  }

  *ppNew = pNew;
  return rc;
}

static int mergeWorkerInit(
  lsm_db *pDb,                    /* Db connection to do merge work */
  Level *pLevel,                  /* Level to work on merging */
  MergeWorker *pMW                /* Object to initialize */
){
  int rc = LSM_OK;                /* Return code */
  Merge *pMerge = pLevel->pMerge; /* Persistent part of merge state */
  MultiCursor *pCsr = 0;          /* Cursor opened for pMW */
  Level *pNext = pLevel->pNext;   /* Next level in LSM */

  assert( pDb->pWorker );
  assert( pLevel->pMerge );
  assert( pLevel->nRight>0 );

  memset(pMW, 0, sizeof(MergeWorker));
  pMW->pDb = pDb;
  pMW->pLevel = pLevel;
  pMW->aGobble = lsmMallocZeroRc(pDb->pEnv, sizeof(Pgno) * pLevel->nRight, &rc);

  /* Create a multi-cursor to read the data to write to the new
  ** segment. The new segment contains:
  **
  **   1. Records from LHS of each of the nMerge levels being merged.
  **   2. Separators from either the last level being merged, or the
  **      separators attached to the LHS of the following level, or neither.
  **
  ** If the new level is the lowest (oldest) in the db, discard any
  ** delete keys. Key annihilation.
  */
  pCsr = multiCursorNew(pDb, &rc);
  if( pCsr ){
    rc = multiCursorAddRhs(pCsr, pLevel);
  }
  if( rc==LSM_OK && pMerge->nInput > pLevel->nRight ){
    rc = btreeCursorNew(pDb, &pNext->lhs, &pCsr->pBtCsr);
  }else if( pNext ){
    multiCursorReadSeparators(pCsr);
  }else{
    multiCursorIgnoreDelete(pCsr);
  }

  assert( rc!=LSM_OK || pMerge->nInput==(pCsr->nPtr+(pCsr->pBtCsr!=0)) );
  pMW->pCsr = pCsr;

  /* Load the b-tree hierarchy into memory. */
  if( rc==LSM_OK ) rc = mergeWorkerLoadHierarchy(pMW);
  if( rc==LSM_OK && pMW->hier.nHier==0 ){
    pMW->aSave[0].iPgno = pLevel->lhs.iFirst;
  }

  /* Position the cursor. */
  if( rc==LSM_OK ){
    pCsr->pPrevMergePtr = &pMerge->iCurrentPtr;
    if( pLevel->lhs.iFirst==0 ){
      /* The output array is still empty. So position the cursor at the very 
      ** start of the input.  */
      rc = multiCursorEnd(pCsr, 0);
    }else{
      /* The output array is non-empty. Position the cursor based on the
      ** page/cell data saved in the Merge.aInput[] array.  */
      int i;
      for(i=0; rc==LSM_OK && i<pCsr->nPtr; i++){
        MergeInput *pInput = &pMerge->aInput[i];
        if( pInput->iPg ){
          SegmentPtr *pPtr;
          assert( pCsr->aPtr[i].pPg==0 );
          pPtr = &pCsr->aPtr[i];
          rc = segmentPtrLoadPage(pDb->pFS, pPtr, pInput->iPg);
          if( rc==LSM_OK && pPtr->nCell>0 ){
            rc = segmentPtrLoadCell(pPtr, pInput->iCell);
          }
        }
      }

      if( rc==LSM_OK && pCsr->pBtCsr ){
        int (*xCmp)(void *, int, void *, int) = pCsr->pDb->xCmp;
        assert( i==pCsr->nPtr );
        rc = btreeCursorRestore(pCsr->pBtCsr, xCmp, &pMerge->aInput[i]);
      }

      if( rc==LSM_OK ){
        rc = multiCursorSetupTree(pCsr, 0);
      }
    }
    pCsr->flags |= CURSOR_NEXT_OK;
  }

  return rc;
}

/* TODO: Re-enable this!!! */
static int sortedBtreeGobble(
  lsm_db *pDb, 
  MultiCursor *pCsr, 
  int iGobble
){
  int rc = LSM_OK;
  if( rtTopic(pCsr->eType)==0 ){
    Segment *pSeg = pCsr->aPtr[iGobble].pSeg;
    Blob *p = &pCsr->key;
    Pgno *aPg;
    int nPg;

    assert( pSeg->iRoot>0 );
    aPg = lsmMallocZeroRc(pDb->pEnv, sizeof(Pgno)*32, &rc);
    if( rc==LSM_OK ){
      rc = seekInBtree(pCsr, pSeg, p->pData, p->nData, aPg, 0); 
    }

    for(nPg=0; aPg[nPg]; nPg++);

#if 0
    lsmFsGobble(pDb, pSeg, aPg, nPg);
#endif

    lsmFree(pDb->pEnv, aPg);
  }
  return rc;
}

/*
** Argument p points to a level of age N. Return the number of levels in
** the linked list starting at p that have age=N (always at least 1).
*/
static int sortedCountLevels(Level *p){
  int iAge = p->iAge;
  int nRet = 0;
  do {
    nRet++;
    p = p->pNext;
  }while( p && p->iAge==iAge );
  return nRet;
}

static int sortedSelectLevel(lsm_db *pDb, int bOpt, Level **ppOut){
  Level *pTopLevel = lsmDbSnapshotLevel(pDb->pWorker);
  int rc = LSM_OK;
  Level *pLevel = 0;            /* Output value */
  Level *pBest = 0;             /* Best level to work on found so far */
  int nBest = pDb->nMerge-1;    /* Number of segments merged at pBest */
  Level *pThis = 0;             /* First in run of levels with age=iAge */
  int nThis = 0;                /* Number of levels starting at pThis */

  /* Find the longest contiguous run of levels not currently undergoing a 
  ** merge with the same age in the structure. Or the level being merged
  ** with the largest number of right-hand segments. Work on it. */
  for(pLevel=pTopLevel; pLevel; pLevel=pLevel->pNext){
    if( pLevel->nRight==0 && pThis && pLevel->iAge==pThis->iAge ){
      nThis++;
    }else{
      if( nThis>nBest ){
        if( (pLevel->iAge!=pThis->iAge+1)
            || (pLevel->nRight==0 && sortedCountLevels(pLevel)<=pDb->nMerge)
          ){
          pBest = pThis;
          nBest = nThis;
        }
      }
      if( pLevel->nRight ){
        if( pLevel->nRight>nBest ){
          nBest = pLevel->nRight;
          pBest = pLevel;
        }
        nThis = 0;
        pThis = 0;
      }else{
        pThis = pLevel;
        nThis = 1;
      }
    }
  }
  if( nThis>nBest ){
    assert( pThis );
    pBest = pThis;
    nBest = nThis;
  }

  if( pBest==0 && bOpt && pTopLevel->pNext ){
    pBest = pTopLevel;
    nBest = 2;
  }

  if( pBest ){
    if( pBest->nRight==0 ){
      rc = sortedMergeSetup(pDb, pBest, nBest, ppOut);
    }else{
      *ppOut = pBest;
    }
  }

  return rc;
}

static int sortedDbIsFull(lsm_db *pDb){
  Level *pTop = lsmDbSnapshotLevel(pDb->pWorker);

  if( lsmDatabaseFull(pDb) ) return 1;
  if( pTop && pTop->iAge==0
   && (pTop->nRight || sortedCountLevels(pTop)>=pDb->nMerge)
  ){
    return 1;
  }
  return 0;
}

static int sortedWork(
  lsm_db *pDb,                    /* Database handle. Must be worker. */
  int nWork,                      /* Number of pages of work to do */
  int bOptimize,                  /* True to merge less than nMerge levels */
  int bFlush,                     /* Set if call is to make room for a flush */
  int *pnWrite                    /* OUT: Actual number of pages written */
){
  int rc = LSM_OK;                /* Return Code */
  int nRemaining = nWork;         /* Units of work to do before returning */
  Snapshot *pWorker = pDb->pWorker;

  assert( pWorker );
  if( lsmDbSnapshotLevel(pWorker)==0 ) return LSM_OK;

  while( nRemaining>0 ){
    Level *pLevel = 0;

    /* Find a level to work on. */
    rc = sortedSelectLevel(pDb, bOptimize, &pLevel);
    assert( rc==LSM_OK || pLevel==0 );

    if( pLevel==0 ){
      /* Could not find any work to do. Finished. */
      break;
    }else{
      MergeWorker mergeworker;    /* State used to work on the level merge */

      rc = mergeWorkerInit(pDb, pLevel, &mergeworker);
      assert( mergeworker.nWork==0 );
      while( rc==LSM_OK 
          && 0==mergeWorkerDone(&mergeworker) 
          && mergeworker.nWork<nRemaining 
      ){
        rc = mergeWorkerStep(&mergeworker);
      }
      nRemaining -= LSM_MAX(mergeworker.nWork, 1);

      /* Check if the merge operation is completely finished. If so, the
      ** Merge object and the right-hand-side of the level can be deleted. 
      **
      ** Otherwise, gobble up (declare eligible for recycling) any pages
      ** from rhs segments for which the content has been completely merged
      ** into the lhs of the level.
      */
      if( rc==LSM_OK ){
        if( mergeWorkerDone(&mergeworker)==0 ){
          int i;
          for(i=0; i<pLevel->nRight; i++){
            SegmentPtr *pGobble = &mergeworker.pCsr->aPtr[i];
            if( pGobble->pSeg->iRoot ){
              rc = sortedBtreeGobble(pDb, mergeworker.pCsr, i);
            }else if( mergeworker.aGobble[i] ){
              lsmFsGobble(pDb, pGobble->pSeg, &mergeworker.aGobble[i], 1);
            }
          }
        }else if( pLevel->lhs.iFirst==0 ){
          /* If the new level is completely empty, remove it from the 
          ** database snapshot. This can only happen if all input keys were
          ** annihilated. Since keys are only annihilated if the new level
          ** is the last in the linked list (contains the most ancient of
          ** database content), this guarantees that pLevel->pNext==0.  */ 

          Level *pTop;          /* Top level of worker snapshot */
          Level **pp;           /* Read/write iterator for Level.pNext list */
          assert( pLevel->pNext==0 );

          /* Remove the level from the worker snapshot. */
          pTop = lsmDbSnapshotLevel(pWorker);
          for(pp=&pTop; *pp!=pLevel; pp=&((*pp)->pNext));
          *pp = pLevel->pNext;
          lsmDbSnapshotSetLevel(pWorker, pTop);

          /* Free the Level structure. */
          lsmFsSortedDelete(pDb->pFS, pWorker, 1, &pLevel->lhs);
          sortedFreeLevel(pDb->pEnv, pLevel);
        }else{
          int i;

          /* Free the separators of the next level, if required. */
          if( pLevel->pMerge->nInput > pLevel->nRight ){
            assert( pLevel->pNext->lhs.iRoot );
            pLevel->pNext->lhs.iRoot = 0;
          }

          /* Free the right-hand-side of pLevel */
          for(i=0; i<pLevel->nRight; i++){
            lsmFsSortedDelete(pDb->pFS, pWorker, 1, &pLevel->aRhs[i]);
          }
          lsmFree(pDb->pEnv, pLevel->aRhs);
          pLevel->nRight = 0;
          pLevel->aRhs = 0;

          /* Free the Merge object */
          lsmFree(pDb->pEnv, pLevel->pMerge);
          pLevel->pMerge = 0;
        }
      }

      /* Clean up the MergeWorker object initialized above. If no error
      ** has occurred, invoke the work-hook to inform the application that
      ** the database structure has changed. */
      mergeWorkerShutdown(&mergeworker, &rc);
      if( rc==LSM_OK ) sortedInvokeWorkHook(pDb);

#if 0
      lsmSortedDumpStructure(pDb, pDb->pWorker, 1, 0, "work");
#endif
      assertBtreeOk(pDb, &pLevel->lhs);
      assertRunInOrder(pDb, &pLevel->lhs);

      /* If bFlush is true and the database is no longer considered "full",
      ** break out of the loop even if nRemaining is still greater than
      ** zero. The caller has an in-memory tree to flush to disk.  */
      if( bFlush && sortedDbIsFull(pDb)==0 ) break;
    }
  }

  if( pnWrite ) *pnWrite = (nWork - nRemaining);
  pWorker->nWrite += (nWork - nRemaining);

#ifdef LSM_LOG_WORK
  lsmLogMessage(pDb, rc, "sortedWork(): %d pages", (nWork-nRemaining));
#endif
  return rc;
}

/*
** The database connection passed as the first argument must be a worker
** connection. This function checks if there exists an "old" in-memory tree
** ready to be flushed to disk. If so, true is returned. Otherwise false.
**
** If an error occurs, *pRc is set to an LSM error code before returning.
** It is assumed that *pRc is set to LSM_OK when this function is called.
*/
static int sortedTreeHasOld(lsm_db *pDb, int *pRc){
  int rc = LSM_OK;
  int bRet = 0;

  assert( pDb->pWorker );
  if( *pRc==LSM_OK ){
    if( pDb->nTransOpen==0 ){
      rc = lsmTreeLoadHeader(pDb, 0);
    }
    if( rc==LSM_OK 
        && pDb->treehdr.iOldShmid
        && pDb->treehdr.iOldLog!=pDb->pWorker->iLogOff 
      ){
      bRet = 1;
    }else{
      bRet = 0;
    }
    *pRc = rc;
  }
  assert( *pRc==LSM_OK || bRet==0 );
  return bRet;
}

static int doLsmSingleWork(
  lsm_db *pDb, 
  int bShutdown,
  int flags, 
  int nPage,                      /* Number of pages to write to disk */
  int *pnWrite,                   /* OUT: Pages actually written to disk */
  int *pbCkpt                     /* OUT: True if an auto-checkpoint is req. */
){
  int rc = LSM_OK;                /* Return code */
  int nOvfl = 0;
  int bFlush = 0;
  int nMax = nPage;               /* Maximum pages to write to disk */
  int nRem = nPage;
  int bCkpt = 0;
  int bToplevel = 0;

  /* Open the worker 'transaction'. It will be closed before this function
  ** returns.  */
  assert( pDb->pWorker==0 );
  rc = lsmBeginWork(pDb);
  assert( rc!=8 );
  if( rc!=LSM_OK ) return rc;

  /* If this connection is doing auto-checkpoints, set nMax (and nRem) so
  ** that this call stops writing when the auto-checkpoint is due.  */
  if( bShutdown==0 && pDb->nAutockpt ){
    u32 nSync;
    u32 nUnsync;
    int nPgsz;
    int nMax;

    lsmCheckpointSynced(pDb, 0, 0, &nSync);
    nUnsync = lsmCheckpointNWrite(pDb->pShmhdr->aSnap1, 0);
    nPgsz = lsmCheckpointPgsz(pDb->pShmhdr->aSnap1);

    nMax = (pDb->nAutockpt/nPgsz) - (nUnsync-nSync);
    if( nMax<nRem ){
      bCkpt = 1;
      nRem = LSM_MAX(nMax, 0);
    }
  }

  /* If there exists in-memory data ready to be flushed to disk, attempt
  ** to flush it now.  */
  if( sortedTreeHasOld(pDb, &rc) ){
    if( sortedDbIsFull(pDb) ){
      int nPg = 0;
      rc = sortedWork(pDb, nRem, 0, 1, &nPg);
      nRem -= nPg;
      assert( rc!=LSM_OK || nRem<=0 || !sortedDbIsFull(pDb) );
      bToplevel = 1;
    }
    if( rc==LSM_OK && nRem>0 ){
      int nPg = 0;
      rc = sortedNewToplevel(pDb, TREE_OLD, &nOvfl, &nPg);
      nRem -= nPg;
      if( rc==LSM_OK && pDb->nTransOpen>0 ){
        lsmTreeDiscardOld(pDb);
      }
      bFlush = 1;
      bToplevel = 0;
    }
  }

  /* If nPage is still greater than zero, do some merging. */
  if( rc==LSM_OK && nRem>0 && bShutdown==0 ){
    int nPg = 0;
    int bOptimize = ((flags & LSM_WORK_OPTIMIZE) ? 1 : 0);
    rc = sortedWork(pDb, nRem, bOptimize, 0, &nPg);
    nRem -= nPg;
    if( nPg ){
      bToplevel = 1;
      nOvfl = 0;
    }
  }

  if( rc==LSM_OK && bToplevel && lsmCheckpointOverflowRequired(pDb) ){
    while( rc==LSM_OK && sortedDbIsFull(pDb) ){
      int nPg = 0;
      rc = sortedWork(pDb, 16, 0, 1, &nPg);
    }
    if( rc==LSM_OK && lsmCheckpointOverflowRequired(pDb) ){
      rc = sortedNewToplevel(pDb, TREE_NONE, &nOvfl, 0);
    }
  }

  if( rc==LSM_OK && (nRem!=nMax) ){
    lsmFinishWork(pDb, bFlush, nOvfl, &rc);
  }else{
    int rcdummy = LSM_BUSY;
    assert( rc!=LSM_OK || bFlush==0 );
    lsmFinishWork(pDb, 0, 0, &rcdummy);
  }
  assert( pDb->pWorker==0 );

  if( rc==LSM_OK ){
    if( pnWrite ) *pnWrite = (nMax - nRem);
    if( pbCkpt ) *pbCkpt = (bCkpt && nRem<=0);
  }else{
    if( pnWrite ) *pnWrite = 0;
    if( pbCkpt ) *pbCkpt = 0;
  }

  return rc;
}

static int doLsmWork(lsm_db *pDb, int flags, int nPage, int *pnWrite){
  int rc;
  int nWrite = 0;
  int bCkpt = 0;

  do {
    int nThis = 0;
    bCkpt = 0;
    rc = doLsmSingleWork(pDb, 0, flags, nPage-nWrite, &nThis, &bCkpt);
    nWrite += nThis;
    if( rc==LSM_OK && bCkpt ){
      rc = lsm_checkpoint(pDb, 0);
    }
  }while( rc==LSM_OK && (nWrite<nPage && bCkpt) );

  if( pnWrite ){
    if( rc==LSM_OK ){
      *pnWrite = nWrite;
    }else{
      *pnWrite = 0;
    }
  }
  return rc;
}

/*
** Perform work to merge database segments together.
*/
int lsm_work(lsm_db *pDb, int flags, int nPage, int *pnWrite){

  /* This function may not be called if pDb has an open read or write
  ** transaction. Return LSM_MISUSE if an application attempts this.  */
  if( pDb->nTransOpen || pDb->pCsr ) return LSM_MISUSE_BKPT;

  return doLsmWork(pDb, flags, nPage, pnWrite);
}

/*
** This function is called in auto-work mode to perform merging work on
** the data structure. It performs enough merging work to prevent the
** height of the tree from growing indefinitely assuming that roughly
** nUnit database pages worth of data have been written to the database
** (i.e. the in-memory tree) since the last call.
*/
int lsmSortedAutoWork(
  lsm_db *pDb,                    /* Database handle */
  int nUnit                       /* Pages of data written to in-memory tree */
){
  int rc = LSM_OK;                /* Return code */
  int nDepth = 0;                 /* Current height of tree (longest path) */
  Level *pLevel;                  /* Used to iterate through levels */
  int bRestore = 0;

  assert( pDb->pWorker==0 );
  assert( pDb->nTransOpen>0 );

  /* Determine how many units of work to do before returning. One unit of
  ** work is achieved by writing one page (~4KB) of merged data.  */
  for(pLevel=lsmDbSnapshotLevel(pDb->pClient); pLevel; pLevel=pLevel->pNext){
    /* nDepth += LSM_MAX(1, pLevel->nRight); */
    nDepth += 1;
  }
  if( lsmTreeHasOld(pDb) ){
    nDepth += 1;
    bRestore = 1;
    rc = lsmSaveCursors(pDb);
    if( rc!=LSM_OK ) return rc;
  }

  if( nDepth>0 ){
    int nRemaining;               /* Units of work to do before returning */

    nRemaining = nUnit * nDepth;
#ifdef LSM_LOG_WORK
    lsmLogMessage(pDb, rc, "lsmSortedAutoWork(): %d*%d = %d pages", 
        nUnit, nDepth, nRemaining);
#endif
    rc = doLsmWork(pDb, LSM_WORK_FLUSH, nRemaining, 0);
    if( rc==LSM_BUSY ) rc = LSM_OK;

    if( bRestore && pDb->pCsr ){
      lsmFreeSnapshot(pDb->pEnv, pDb->pClient);
      pDb->pClient = 0;
      rc = lsmCheckpointLoad(pDb, 0);
      if( rc==LSM_OK ){
        rc = lsmCheckpointDeserialize(pDb, 0, pDb->aSnapshot, &pDb->pClient);
      }
      if( rc==LSM_OK ){
        rc = lsmRestoreCursors(pDb);
      }
    }
  }

  return rc;
}

/*
** This function is only called during system shutdown. The contents of
** any in-memory trees present (old or current) are written out to disk.
*/
int lsmFlushTreeToDisk(lsm_db *pDb){
  int rc;
  int nOvfl = 0;

  rc = lsmBeginWork(pDb);
  while( rc==LSM_OK && sortedDbIsFull(pDb) ){
    rc = sortedWork(pDb, 256, 0, 1, 0);
  }

  if( rc==LSM_OK ){
    rc = sortedNewToplevel(pDb, TREE_BOTH, &nOvfl, 0);
  }
  lsmFinishWork(pDb, 1, nOvfl, &rc);
  return rc;
}

/*
** Return a string representation of the segment passed as the only argument.
** Space for the returned string is allocated using lsmMalloc(), and should
** be freed by the caller using lsmFree().
*/
static char *segToString(lsm_env *pEnv, Segment *pSeg, int nMin){
  int nSize = pSeg->nSize;
  Pgno iRoot = pSeg->iRoot;
  Pgno iFirst = pSeg->iFirst;
  Pgno iLast = pSeg->iLastPg;
  char *z;

  char *z1;
  char *z2;
  int nPad;

  z1 = lsmMallocPrintf(pEnv, "%d.%d", iFirst, iLast);
  if( iRoot ){
    z2 = lsmMallocPrintf(pEnv, "root=%d", iRoot);
  }else{
    z2 = lsmMallocPrintf(pEnv, "size=%d", nSize);
  }

  nPad = nMin - 2 - strlen(z1) - 1 - strlen(z2);
  nPad = LSM_MAX(0, nPad);

  if( iRoot ){
    z = lsmMallocPrintf(pEnv, "/%s %*s%s\\", z1, nPad, "", z2);
  }else{
    z = lsmMallocPrintf(pEnv, "|%s %*s%s|", z1, nPad, "", z2);
  }
  lsmFree(pEnv, z1);
  lsmFree(pEnv, z2);

  return z;
}

static int fileToString(
  lsm_env *pEnv,                  /* For xMalloc() */
  char *aBuf, 
  int nBuf, 
  int nMin,
  Segment *pSeg
){
  int i = 0;
  char *zSeg;

  zSeg = segToString(pEnv, pSeg, nMin);
  i += sqlite4_snprintf(&aBuf[i], nBuf-i, "%s", zSeg);
  lsmFree(pEnv, zSeg);

  return i;
}

void sortedDumpPage(lsm_db *pDb, Segment *pRun, Page *pPg, int bVals){
  Blob blob = {0, 0, 0};         /* Blob used for keys */
  LsmString s;
  int i;

  int nRec;
  int iPtr;
  int flags;
  u8 *aData;
  int nData;

  aData = fsPageData(pPg, &nData);

  nRec = pageGetNRec(aData, nData);
  iPtr = pageGetPtr(aData, nData);
  flags = pageGetFlags(aData, nData);

  lsmStringInit(&s, pDb->pEnv);
  lsmStringAppendf(&s,"nCell=%d iPtr=%d flags=%d {", nRec, iPtr, flags);
  if( flags&SEGMENT_BTREE_FLAG ) iPtr = 0;

  for(i=0; i<nRec; i++){
    Page *pRef = 0;               /* Pointer to page iRef */
    int iChar;
    u8 *aKey; int nKey = 0;       /* Key */
    u8 *aVal; int nVal = 0;       /* Value */
    int iTopic;
    u8 *aCell;
    int iPgPtr;
    int eType;

    aCell = pageGetCell(aData, nData, i);
    eType = *aCell++;
    assert( (flags & SEGMENT_BTREE_FLAG) || eType!=0 );
    aCell += lsmVarintGet32(aCell, &iPgPtr);

    if( eType==0 ){
      Pgno iRef;                  /* Page number of referenced page */
      aCell += lsmVarintGet64(aCell, &iRef);
      lsmFsDbPageGet(pDb->pFS, iRef, &pRef);
      aKey = pageGetKey(pRef, 0, &iTopic, &nKey, &blob);
    }else{
      aCell += lsmVarintGet32(aCell, &nKey);
      if( rtIsWrite(eType) ) aCell += lsmVarintGet32(aCell, &nVal);
      sortedReadData(pPg, (aCell-aData), nKey+nVal, (void **)&aKey, &blob);
      aVal = &aKey[nKey];
      iTopic = eType;
    }

    lsmStringAppendf(&s, "%s%2X:", (i==0?"":" "), iTopic);
    for(iChar=0; iChar<nKey; iChar++){
      lsmStringAppendf(&s, "%c", isalnum(aKey[iChar]) ? aKey[iChar] : '.');
    }
    if( nVal>0 && bVals ){
      lsmStringAppendf(&s, "##");
      for(iChar=0; iChar<nVal; iChar++){
        lsmStringAppendf(&s, "%c", isalnum(aVal[iChar]) ? aVal[iChar] : '.');
      }
    }

    lsmStringAppendf(&s, " %d", iPgPtr+iPtr);
    lsmFsPageRelease(pRef);
  }
  lsmStringAppend(&s, "}", 1);

  lsmLogMessage(pDb, LSM_OK, "      Page %d: %s", lsmFsPageNumber(pPg), s.z);
  lsmStringClear(&s);

  sortedBlobFree(&blob);
}

static void infoCellDump(
  lsm_db *pDb,
  int bIndirect,                  /* True to follow indirect refs */
  Page *pPg,
  int iCell,
  int *peType,
  int *piPgPtr,
  u8 **paKey, int *pnKey,
  u8 **paVal, int *pnVal,
  Blob *pBlob
){
  u8 *aData; int nData;           /* Page data */
  u8 *aKey; int nKey = 0;         /* Key */
  u8 *aVal; int nVal = 0;         /* Value */
  int eType;
  int iPgPtr;
  Page *pRef = 0;                 /* Pointer to page iRef */
  u8 *aCell;

  aData = fsPageData(pPg, &nData);

  aCell = pageGetCell(aData, nData, iCell);
  eType = *aCell++;
  aCell += lsmVarintGet32(aCell, &iPgPtr);

  if( eType==0 ){
    int dummy;
    Pgno iRef;                  /* Page number of referenced page */
    aCell += lsmVarintGet64(aCell, &iRef);
    if( bIndirect ){
      lsmFsDbPageGet(pDb->pFS, iRef, &pRef);
      pageGetKeyCopy(pDb->pEnv, pRef, 0, &dummy, pBlob);
      aKey = (u8 *)pBlob->pData;
      nKey = pBlob->nData;
      lsmFsPageRelease(pRef);
    }else{
      aKey = (u8 *)"<indirect>";
      nKey = 11;
    }
  }else{
    aCell += lsmVarintGet32(aCell, &nKey);
    if( rtIsWrite(eType) ) aCell += lsmVarintGet32(aCell, &nVal);
    sortedReadData(pPg, (aCell-aData), nKey+nVal, (void **)&aKey, pBlob);
    aVal = &aKey[nKey];
  }

  if( peType ) *peType = eType;
  if( piPgPtr ) *piPgPtr = iPgPtr;
  if( paKey ) *paKey = aKey;
  if( paVal ) *paVal = aVal;
  if( pnKey ) *pnKey = nKey;
  if( pnVal ) *pnVal = nVal;
}

static int infoAppendBlob(LsmString *pStr, int bHex, u8 *z, int n){
  int iChar;
  for(iChar=0; iChar<n; iChar++){
    if( bHex ){
      lsmStringAppendf(pStr, "%02X", z[iChar]);
    }else{
      lsmStringAppendf(pStr, "%c", isalnum(z[iChar]) ?z[iChar] : '.');
    }
  }
  return LSM_OK;
}

#define INFO_PAGE_DUMP_DATA     0x01
#define INFO_PAGE_DUMP_VALUES   0x02
#define INFO_PAGE_DUMP_HEX      0x04
#define INFO_PAGE_DUMP_INDIRECT 0x08

static int infoPageDump(
  lsm_db *pDb,                    /* Database handle */
  Pgno iPg,                       /* Page number of page to dump */
  int flags,
  char **pzOut                    /* OUT: lsmMalloc'd string */
){
  int rc = LSM_OK;                /* Return code */
  Page *pPg = 0;                  /* Handle for page iPg */
  int i, j;                       /* Loop counters */
  const int perLine = 16;         /* Bytes per line in the raw hex dump */

  int bValues = (flags & INFO_PAGE_DUMP_VALUES);
  int bHex = (flags & INFO_PAGE_DUMP_HEX);
  int bData = (flags & INFO_PAGE_DUMP_DATA);
  int bIndirect = (flags & INFO_PAGE_DUMP_INDIRECT);

  *pzOut = 0;
  if( iPg==0 ) return LSM_ERROR;

  rc = lsmFsDbPageGet(pDb->pFS, iPg, &pPg);
  if( rc==LSM_OK ){
    Blob blob = {0, 0, 0, 0};
    int nKeyWidth = 0;
    LsmString str;
    int nRec;
    int iPtr;
    int flags;
    int iCell;
    u8 *aData; int nData;         /* Page data and size thereof */

    aData = fsPageData(pPg, &nData);
    nRec = pageGetNRec(aData, nData);
    iPtr = pageGetPtr(aData, nData);
    flags = pageGetFlags(aData, nData);

    lsmStringInit(&str, pDb->pEnv);
    lsmStringAppendf(&str, "Page : %d  (%d bytes)\n", iPg, nData);
    lsmStringAppendf(&str, "nRec : %d\n", nRec);
    lsmStringAppendf(&str, "iPtr : %d\n", iPtr);
    lsmStringAppendf(&str, "flags: %04x\n", flags);
    lsmStringAppendf(&str, "\n");

    for(iCell=0; iCell<nRec; iCell++){
      int nKey;
      infoCellDump(pDb, bIndirect, pPg, iCell, 0, 0, 0, &nKey, 0, 0, &blob);
      if( nKey>nKeyWidth ) nKeyWidth = nKey;
    }
    if( bHex ) nKeyWidth = nKeyWidth * 2;

    for(iCell=0; iCell<nRec; iCell++){
      u8 *aKey; int nKey = 0;       /* Key */
      u8 *aVal; int nVal = 0;       /* Value */
      int iPgPtr;
      int eType;
      char cType = '?';
      Pgno iAbsPtr;
      char zFlags[8];

      infoCellDump(pDb, bIndirect, pPg, iCell, &eType, &iPgPtr,
          &aKey, &nKey, &aVal, &nVal, &blob
      );
      iAbsPtr = iPgPtr + ((flags & SEGMENT_BTREE_FLAG) ? 0 : iPtr);

      lsmFlagsToString(eType, zFlags);
      lsmStringAppendf(&str, "%s %d (%s) ", 
          zFlags, iAbsPtr, (rtTopic(eType) ? "sys" : "usr")
      );
      infoAppendBlob(&str, bHex, aKey, nKey); 
      if( nVal>0 && bValues ){
        lsmStringAppendf(&str, "%*s", nKeyWidth - (nKey*(1+bHex)), "");
        lsmStringAppendf(&str, " ");
        infoAppendBlob(&str, bHex, aVal, nVal); 
      }
      lsmStringAppendf(&str, "\n");
    }

    if( bData ){
      lsmStringAppendf(&str, "\n-------------------" 
          "-------------------------------------------------------------\n");
      lsmStringAppendf(&str, "Page %d\n",
          iPg, (iPg-1)*nData, iPg*nData - 1);
      for(i=0; i<nData; i += perLine){
        lsmStringAppendf(&str, "%04x: ", i);
        for(j=0; j<perLine; j++){
          if( i+j>nData ){
            lsmStringAppendf(&str, "   ");
          }else{
            lsmStringAppendf(&str, "%02x ", aData[i+j]);
          }
        }
        lsmStringAppendf(&str, "  ");
        for(j=0; j<perLine; j++){
          if( i+j>nData ){
            lsmStringAppendf(&str, " ");
          }else{
            lsmStringAppendf(&str,"%c", isprint(aData[i+j]) ? aData[i+j] : '.');
          }
        }
        lsmStringAppendf(&str,"\n");
      }
    }

    *pzOut = str.z;
    sortedBlobFree(&blob);
    lsmFsPageRelease(pPg);
  }

  return rc;
}

int lsmInfoPageDump(
  lsm_db *pDb,                    /* Database handle */
  Pgno iPg,                       /* Page number of page to dump */
  int bHex,                       /* True to output key/value in hex form */
  char **pzOut                    /* OUT: lsmMalloc'd string */
){
  int flags = INFO_PAGE_DUMP_DATA | INFO_PAGE_DUMP_VALUES;
  if( bHex ) flags |= INFO_PAGE_DUMP_HEX;
  return infoPageDump(pDb, iPg, flags, pzOut);
}

void sortedDumpSegment(lsm_db *pDb, Segment *pRun, int bVals){
  assert( pDb->xLog );
  if( pRun && pRun->iFirst ){
    int flags = (bVals ? INFO_PAGE_DUMP_VALUES : 0);
    char *zSeg;
    Page *pPg;

    zSeg = segToString(pDb->pEnv, pRun, 0);
    lsmLogMessage(pDb, LSM_OK, "Segment: %s", zSeg);
    lsmFree(pDb->pEnv, zSeg);

    lsmFsDbPageGet(pDb->pFS, pRun->iFirst, &pPg);
    while( pPg ){
      Page *pNext;
      char *z = 0;
      infoPageDump(pDb, lsmFsPageNumber(pPg), flags, &z);
      lsmLogMessage(pDb, LSM_OK, "%s", z);
      lsmFree(pDb->pEnv, z);
#if 0
      sortedDumpPage(pDb, pRun, pPg, bVals);
#endif
      lsmFsDbPageNext(pRun, pPg, 1, &pNext);
      lsmFsPageRelease(pPg);
      pPg = pNext;
    }
  }
}

/*
** Invoke the log callback zero or more times with messages that describe
** the current database structure.
*/
void lsmSortedDumpStructure(
  lsm_db *pDb,                    /* Database handle (used for xLog callback) */
  Snapshot *pSnap,                /* Snapshot to dump */
  int bKeys,                      /* Output the keys from each segment */
  int bVals,                      /* Output the values from each segment */
  const char *zWhy                /* Caption to print near top of dump */
){
  Snapshot *pDump = pSnap;
  Level *pTopLevel;

  assert( pSnap );
  pTopLevel = lsmDbSnapshotLevel(pDump);
  if( pDb->xLog && pTopLevel ){
    Level *pLevel;
    int iLevel = 0;

    lsmLogMessage(pDb, LSM_OK, "Database structure (%s)", zWhy);

    for(pLevel=pTopLevel; pLevel; pLevel=pLevel->pNext){
      char zLeft[1024];
      char zRight[1024];
      int i = 0;

      Segment *aLeft[24];  
      Segment *aRight[24];

      int nLeft = 0;
      int nRight = 0;

      Segment *pSeg = &pLevel->lhs;
      aLeft[nLeft++] = pSeg;

      for(i=0; i<pLevel->nRight; i++){
        aRight[nRight++] = &pLevel->aRhs[i];
      }

      for(i=0; i<nLeft || i<nRight; i++){
        int iPad = 0;
        char zLevel[32];
        zLeft[0] = '\0';
        zRight[0] = '\0';

        if( i<nLeft ){ 
          fileToString(pDb->pEnv, zLeft, sizeof(zLeft), 28, aLeft[i]); 
        }
        if( i<nRight ){ 
          fileToString(pDb->pEnv, zRight, sizeof(zRight), 28, aRight[i]); 
        }

        if( i==0 ){
          sqlite4_snprintf(zLevel, sizeof(zLevel), "L%d: (age=%d)", 
              iLevel, pLevel->iAge
          );
        }else{
          zLevel[0] = '\0';
        }

        if( nRight==0 ){
          iPad = 28 - (strlen(zLeft)/2) ;
        }

        lsmLogMessage(pDb, LSM_OK, "% 7s % *s% -35s %s", 
            zLevel, iPad, "", zLeft, zRight
        );
      }

      iLevel++;
    }

    if( bKeys ){
      for(pLevel=pTopLevel; pLevel; pLevel=pLevel->pNext){
        int i;
        sortedDumpSegment(pDb, &pLevel->lhs, bVals);
        for(i=0; i<pLevel->nRight; i++){
          sortedDumpSegment(pDb, &pLevel->aRhs[i], bVals);
        }
      }
    }
  }
}

void lsmSortedFreeLevel(lsm_env *pEnv, Level *pLevel){
  Level *pNext;
  Level *p;

  for(p=pLevel; p; p=pNext){
    pNext = p->pNext;
    sortedFreeLevel(pEnv, p);
  }
}

void lsmSortedSaveTreeCursors(lsm_db *pDb){
  MultiCursor *pCsr;
  for(pCsr=pDb->pCsr; pCsr; pCsr=pCsr->pNext){
    lsmTreeCursorSave(pCsr->apTreeCsr[0]);
    lsmTreeCursorSave(pCsr->apTreeCsr[1]);
  }
}

#ifdef LSM_DEBUG_EXPENSIVE
static void assertRunInOrder(lsm_db *pDb, Segment *pSeg){
  Page *pPg = 0;
  Blob blob1 = {0, 0, 0, 0};
  Blob blob2 = {0, 0, 0, 0};

  lsmFsDbPageGet(pDb->pFS, pSeg->iFirst, &pPg);
  while( pPg ){
    u8 *aData; int nData;
    Page *pNext;

    aData = lsmFsPageData(pPg, &nData);
    if( 0==(pageGetFlags(aData, nData) & SEGMENT_BTREE_FLAG) ){
      int i;
      int nRec = pageGetNRec(aData, nData);
      for(i=0; i<nRec; i++){
        int iTopic1, iTopic2;
        pageGetKeyCopy(pDb->pEnv, pPg, i, &iTopic1, &blob1);

        if( i==0 && blob2.nData ){
          assert( sortedKeyCompare(
                pDb->xCmp, iTopic2, blob2.pData, blob2.nData,
                iTopic1, blob1.pData, blob1.nData
          )<0 );
        }

        if( i<(nRec-1) ){
          pageGetKeyCopy(pDb->pEnv, pPg, i+1, &iTopic2, &blob2);
          assert( sortedKeyCompare(
                pDb->xCmp, iTopic1, blob1.pData, blob1.nData,
                iTopic2, blob2.pData, blob2.nData
          )<0 );
        }
      }
    }

    lsmFsDbPageNext(pSeg, pPg, 1, &pNext);
    lsmFsPageRelease(pPg);
    pPg = pNext;
  }

  sortedBlobFree(&blob1);
  sortedBlobFree(&blob2);
}
#endif

#ifdef LSM_DEBUG_EXPENSIVE
/*
** This function is only included in the build if LSM_DEBUG_EXPENSIVE is 
** defined. Its only purpose is to evaluate various assert() statements to 
** verify that the database is well formed in certain respects.
**
** More specifically, it checks that the array pOne contains the required 
** pointers to pTwo. Array pTwo must be a main array. pOne may be either a 
** separators array or another main array. If pOne does not contain the 
** correct set of pointers, an assert() statement fails.
*/
static int assertPointersOk(
  lsm_db *pDb,                    /* Database handle */
  Segment *pOne,                  /* Segment containing pointers */
  Segment *pTwo,                  /* Segment containing pointer targets */
  int bRhs                        /* True if pTwo may have been Gobble()d */
){
  int rc = LSM_OK;                /* Error code */
  SegmentPtr ptr1;                /* Iterates through pOne */
  SegmentPtr ptr2;                /* Iterates through pTwo */
  Pgno iPrev;

  assert( pOne && pTwo );

  memset(&ptr1, 0, sizeof(ptr1));
  memset(&ptr2, 0, sizeof(ptr1));
  ptr1.pSeg = pOne;
  ptr2.pSeg = pTwo;
  segmentPtrEndPage(pDb->pFS, &ptr1, 0, &rc);
  segmentPtrEndPage(pDb->pFS, &ptr2, 0, &rc);

  /* Check that the footer pointer of the first page of pOne points to
  ** the first page of pTwo. */
  iPrev = pTwo->iFirst;
  if( ptr1.iPtr!=iPrev && !bRhs ){
    assert( 0 );
  }

  if( rc==LSM_OK && ptr1.nCell>0 ){
    rc = segmentPtrLoadCell(&ptr1, 0);
  }
      
  while( rc==LSM_OK && ptr2.pPg ){
    Pgno iThis;

    /* Advance to the next page of segment pTwo that contains at least
    ** one cell. Break out of the loop if the iterator reaches EOF.  */
    do{
      rc = segmentPtrNextPage(&ptr2, 1);
      assert( rc==LSM_OK );
    }while( rc==LSM_OK && ptr2.pPg && ptr2.nCell==0 );
    if( rc!=LSM_OK || ptr2.pPg==0 ) break;
    iThis = lsmFsPageNumber(ptr2.pPg);

    if( (ptr2.flags & (PGFTR_SKIP_THIS_FLAG|SEGMENT_BTREE_FLAG))==0 ){

      /* Load the first cell in the array pTwo page. */
      rc = segmentPtrLoadCell(&ptr2, 0);

      /* Iterate forwards through pOne, searching for a key that matches the
      ** key ptr2.pKey/nKey. This key should have a pointer to the page that
      ** ptr2 currently points to. */
      while( rc==LSM_OK ){
        int res = rtTopic(ptr1.eType) - rtTopic(ptr2.eType);
        if( res==0 ){
          res = pDb->xCmp(ptr1.pKey, ptr1.nKey, ptr2.pKey, ptr2.nKey);
        }

        if( res<0 ){
          assert( bRhs || ptr1.iPtr+ptr1.iPgPtr==iPrev );
        }else if( res>0 ){
          assert( 0 );
        }else{
          assert( ptr1.iPtr+ptr1.iPgPtr==iThis );
          iPrev = iThis;
          break;
        }

        rc = segmentPtrAdvance(0, &ptr1, 0);
        if( ptr1.pPg==0 ){
          assert( 0 );
        }
      }
    }
  }

  segmentPtrReset(&ptr1);
  segmentPtrReset(&ptr2);
  return LSM_OK;
}

/*
** This function is only included in the build if LSM_DEBUG_EXPENSIVE is 
** defined. Its only purpose is to evaluate various assert() statements to 
** verify that the database is well formed in certain respects.
**
** More specifically, it checks that the b-tree embedded in array pRun
** contains the correct keys. If not, an assert() fails.
*/
static int assertBtreeOk(
  lsm_db *pDb,
  Segment *pSeg
){
  int rc = LSM_OK;                /* Return code */
  if( pSeg->iRoot ){
    Blob blob = {0, 0, 0};        /* Buffer used to cache overflow keys */
    FileSystem *pFS = pDb->pFS;   /* File system to read from */
    Page *pPg = 0;                /* Main run page */
    BtreeCursor *pCsr = 0;        /* Btree cursor */

    rc = btreeCursorNew(pDb, pSeg, &pCsr);
    if( rc==LSM_OK ){
      rc = btreeCursorFirst(pCsr);
    }
    if( rc==LSM_OK ){
      rc = lsmFsDbPageGet(pFS, pSeg->iFirst, &pPg);
    }

    while( rc==LSM_OK ){
      Page *pNext;
      u8 *aData;
      int nData;
      int flags;

      rc = lsmFsDbPageNext(pSeg, pPg, 1, &pNext);
      lsmFsPageRelease(pPg);
      pPg = pNext;
      if( pPg==0 ) break;
      aData = fsPageData(pPg, &nData);
      flags = pageGetFlags(aData, nData);
      if( rc==LSM_OK 
       && 0==((SEGMENT_BTREE_FLAG|PGFTR_SKIP_THIS_FLAG) & flags)
       && 0!=pageGetNRec(aData, nData)
      ){
        u8 *pKey;
        int nKey;
        int iTopic;
        pKey = pageGetKey(pPg, 0, &iTopic, &nKey, &blob);
        assert( nKey==pCsr->nKey && 0==memcmp(pKey, pCsr->pKey, nKey) );
        assert( lsmFsPageNumber(pPg)==pCsr->iPtr );
        rc = btreeCursorNext(pCsr);
      }
    }
    assert( rc!=LSM_OK || pCsr->pKey==0 );

    if( pPg ) lsmFsPageRelease(pPg);

    btreeCursorFree(pCsr);
    sortedBlobFree(&blob);
  }

  return rc;
}
#endif /* ifdef LSM_DEBUG_EXPENSIVE */
