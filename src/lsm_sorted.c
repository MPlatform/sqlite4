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
** SORTED FILE FORMAT:
**
**   A sorted file is divided into pages. The page-size is not stored anywhere
**   within the sorted file itself - it must be known in advance in order to
**   read the file. The maximum allowed page-size is 64KB.
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
**     * Left-hand pointer value (4 bytes).
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
**   Each record in a sorted file is either a WRITE, a DELETE, or a 
**   SEPARATOR. 
**
**   The first byte of the record indicates the type, as follows:
**
**     SORTED_SEPARATOR:  0x01
**     SORTED_WRITE:      0x02
**     SORTED_DELETE:     0x03
**
**   If the sorted file contains pointers, then immediately following the
**   type byte is a pointer to the smallest key in the next file that is larger
**   than the key in the current record. The pointer is encoded as a varint.
**   When added to the 32-bit page number stored in the footer, it is the 
**   page number of the page that contains the smallest key in the next sorted 
**   file that is larger than this key. 
**
**   Next is the number of bytes in the key, encoded as a varint.
**
**   If the record is a SORTED_WRITE, the number of bytes in the value, as
**   a varint, is next.
**
**   Finally, the blob of data containing the key, and for SORTED_WRITE
**   records, the value as well.
**
** SEPARATOR ARRAYS:
**
**   Each time a sorted run that spans more than one page is constructed, a
**   separators array is also constructed. A separators array contains normal
**   pages and b-tree pages. Both types of pages use the same format (as 
**   above).
**
**   The normal pages in the separators array contain a SORTED_SEPARATOR 
**   record with a copy of the first key on each page of the main array
**   except the leftmost. If a main array page contains no keys, then there
**   is no corresponding entry in the separators array.
*/

#ifndef _LSM_INT_H
# include "lsmInt.h"
#endif

/* 
** Record types for user data.
*/
#define SORTED_SEPARATOR 0x01
#define SORTED_WRITE     0x02
#define SORTED_DELETE    0x03

#define SORTED_SYSTEM_DATA 0x10

/* 
** Record types for system data (e.g. free-block list, secondary storage
** management entries). These are the same as the user types with the
** most-significant bit set.
*/
#define SORTED_SYSTEM_SEPARATOR (SORTED_SYSTEM_DATA | SORTED_SEPARATOR)
#define SORTED_SYSTEM_WRITE     (SORTED_SYSTEM_DATA | SORTED_WRITE)
#define SORTED_SYSTEM_DELETE    (SORTED_SYSTEM_DATA | SORTED_DELETE)

/*
** Macros to help decode record types.
*/
#define rtTopic(eType)       ((eType) & 0xF0)
#define rtIsDelete(eType)    (((eType) & 0x0F)==SORTED_DELETE)
#define rtIsSeparator(eType) (((eType) & 0x0F)==SORTED_SEPARATOR)
#define rtIsWrite(eType)     (((eType) & 0x0F)==SORTED_WRITE)




/*
** The following macros are used to access a page footer.
*/
#define SEGMENT_NRECORD_OFFSET(pgsz)        ((pgsz) - 2)
#define SEGMENT_FLAGS_OFFSET(pgsz)          ((pgsz) - 2 - 2)
#define SEGMENT_POINTER_OFFSET(pgsz)        ((pgsz) - 2 - 2 - 4)
#define SEGMENT_CELLPTR_OFFSET(pgsz, iCell) ((pgsz) - 2 - 2 - 4 - 2 - (iCell)*2)

#define SEGMENT_EOF(pgsz, nEntry) SEGMENT_CELLPTR_OFFSET(pgsz, nEntry)

#define SEGMENT_BTREE_FLAG     0x0001
#define PGFTR_SKIP_NEXT_FLAG   0x0002
#define PGFTR_SKIP_THIS_FLAG   0x0004

typedef struct LevelCursor LevelCursor;
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
  Segment *pSeg;                  /* Segment to access */
  SortedRun *pRun;                /* Points to either pSeg->run or pSeg->sep */

  /* Current page. See segmentPtrLoadPage(). */
  Page *pPg;                    /* Current page */
  u16 flags;                    /* Copy of page flags field */
  int nCell;                    /* Number of cells on pPg */
  int iPtr;                     /* Base cascade pointer */

  /* Current cell. See segmentPtrLoadCell() */
  int iCell;                    /* Current record within page pPg */
  int eType;                    /* Type of current record */
  int iPgPtr;                   /* Cascade pointer offset */
  void *pKey; int nKey;         /* Key associated with current record */
  void *pVal; int nVal;         /* Current record value (eType==WRITE only) */

  /* Blobs used to allocate buffers for pKey and pVal as required */
  Blob blob1;
  Blob blob2;
};

/*
** A cursor used to search or iterate within a single sorted file. 
** LevelCursor structures are only used within this file. The rest of the
** system uses MultiCursor objects to search and iterate within the merge
** of a TreeCursor and zero or more LevelCursor objects.
**
** Instances of this structure are manipulated using the following static
** functions. Similar to the 
**
**   levelCursorInit()
**   segmentCursorSeek()
**   segmentCursorAdvance()
**   segmentCursorEnd()
**
**   segmentCursorKey()
**   segmentCursorValue()
**   segmentCursorValid()
*/
struct LevelCursor {
  FileSystem *pFS;                /* File system to read from */
  int (*xCmp)(void *, int, void *, int);         /* Compare function */
  int bIgnoreSeparators;          /* True to ignore SORTED_SEPARATOR records */
  int bIgnoreSystem;              /* True to ignore records for topic!=0 */
  int iCurrentPtr;                /* Current entry in aPtr[] */
  int nPtr;                       /* Size of aPtr[] array */
  SegmentPtr *aPtr;               /* Array of segment pointers */
  Level *pLevel;                  /* Pointer to Level object (if nPtr>1) */
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
  TreeCursor *pTreeCsr;           /* Single tree cursor */

  int flags;                      /* Mask of CURSOR_XXX flags */
  int (*xCmp)(void *, int, void *, int);         /* Compare function */
  int eType;                      /* Cache of current key type */
  Blob key;                       /* Cache of current key (or NULL) */
  int nSegCsr;                    /* Size of aSegCsr[] array */
  LevelCursor *aSegCsr;           /* Array of cursors open on sorted files */
  int nTree;
  int *aTree;

  int *pnHdrLevel;
  void *pSystemVal;
  Snapshot *pSnap;
};

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
** CURSOR_AT_LEVELS
**   This flag is set when sub-cursor CURSOR_DATA_SYSTEM is actually
**   pointing at a free list.
**
** CURSOR_IGNORE_SYSTEM
**   If set, this cursor ignores system keys.
*/
#define CURSOR_IGNORE_DELETE    0x00000001
#define CURSOR_NEW_SYSTEM       0x00000002
#define CURSOR_AT_FREELIST      0x00000004
#define CURSOR_AT_LEVELS        0x00000008
#define CURSOR_IGNORE_SYSTEM    0x00000010

typedef struct MergeWorker MergeWorker;
struct MergeWorker {
  lsm_db *pDb;                    /* Database handle */
  Level *pLevel;                  /* Worker snapshot Level being merged */
  MultiCursor *pCsr;              /* Cursor to read new segment contents from */
  int bFlush;                     /* True if this is an in-memory tree flush */
  Page **apHier;                  /* Separators array b-tree internal nodes */
  int nHier;                      /* Number of entries in apHier[] */
  Page *apPage[2];                /* Current output pages (0 is main run) */
  int nWork;                      /* Number of calls to mergeWorkerNextPage() */
};

#ifdef LSM_DEBUG_EXPENSIVE
static void assertAllPointersOk(int rc, lsm_db *pDb);
static void assertAllBtreesOk(int rc, lsm_db *);
#else
# define assertAllPointersOk(y, z)
# define assertAllBtreesOk(y, z)
#endif

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


static int pageGetNRec(u8 *aData, int nData){
  return (int)lsmGetU16(&aData[SEGMENT_NRECORD_OFFSET(nData)]);
}

static int pageGetPtr(u8 *aData, int nData){
  return (int)lsmGetU32(&aData[SEGMENT_POINTER_OFFSET(nData)]);
}

static int pageGetFlags(u8 *aData, int nData){
  return (int)lsmGetU16(&aData[SEGMENT_FLAGS_OFFSET(nData)]);
}

static u8 *pageGetCell(u8 *aData, int nData, int iCell){
  return &aData[lsmGetU16(&aData[SEGMENT_CELLPTR_OFFSET(nData, iCell)])];
}

/*
** Return the decoded (possibly relative) pointer value stored in cell 
** iCell from page aData/nData.
*/
static int pageGetRecordPtr(u8 *aData, int nData, int iCell){
  int iRet;                       /* Return value */
  u8 *aCell;                      /* Pointer to cell iCell */
  aCell = pageGetCell(aData, nData, iCell);
  lsmVarintGet32(&aCell[1], &iRet);
  return iRet;
}

static void segmentPtrSetPage(SegmentPtr *pPtr, Page *pNext){
  lsmFsPageRelease(pPtr->pPg);
  if( pNext ){
    int nData;
    u8 *aData = lsmFsPageData(pNext, &nData);
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

  assert( pPtr->pSeg==0 
       || pPtr->pRun==&pPtr->pSeg->run 
       || pPtr->pRun==&pPtr->pSeg->sep 
  );
  rc = lsmFsDbPageGet(pFS, pPtr->pRun, iNew, &pPg);
  assert( rc==LSM_OK || pPg==0 );
  segmentPtrSetPage(pPtr, pPg);

  return rc;
}

static int segmentPtrNextPage(
  SegmentPtr *pPtr,              /* Load page into this SegmentPtr object */
  int eDir                       /* +1 for next(), -1 for prev() */
){
  Page *pNext;                   /* New page to load */
  int rc;                        /* Return code */

  assert( eDir==1 || eDir==-1 );
  assert( pPtr->pPg );
  assert( (pPtr->pSeg==0 && eDir>0)
       || pPtr->pRun==&pPtr->pSeg->run 
       || pPtr->pRun==&pPtr->pSeg->sep 
  );

  rc = lsmFsDbPageNext(pPtr->pRun, pPtr->pPg, eDir, &pNext);
  assert( rc==LSM_OK || pNext==0 );
  segmentPtrSetPage(pPtr, pNext);
  return rc;
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

  aData = lsmFsPageData(pPg, &nData);
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

    while( 1 ){
      Page *pNext;
      int flags;

      /* Copy data from pPg into the output buffer. */
      int nCopy = MIN(nRem, iEnd-i);
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
        aData = lsmFsPageData(pPg, &nData);
        flags = lsmGetU16(&aData[SEGMENT_FLAGS_OFFSET(nData)]);
      }while( flags&SEGMENT_BTREE_FLAG );

      iEnd = SEGMENT_EOF(nData, lsmGetU16(&aData[nData-2]));
      assert( iEnd>0 && iEnd<nData );
    }

    lsmFsPageRelease(pPg);
  }

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

  aData = lsmFsPageData(pPg, &nData);
  assert( !(pageGetFlags(aData, nData) & SEGMENT_BTREE_FLAG) );

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
    aData = lsmFsPageData(pPtr->pPg, &nPgsz);
    iOff = lsmGetU16(&aData[SEGMENT_CELLPTR_OFFSET(nPgsz, pPtr->iCell)]);
    pPtr->eType = aData[iOff];
    iOff++;
    iOff += lsmVarintGet32(&aData[iOff], &pPtr->iPgPtr);
    iOff += lsmVarintGet32(&aData[iOff], &pPtr->nKey);
    if( rtIsWrite(pPtr->eType) ){
      iOff += lsmVarintGet32(&aData[iOff], &pPtr->nVal);
    }

    rc = segmentPtrReadData(
        pPtr, iOff, pPtr->nKey, &pPtr->pKey, &pPtr->blob1
    );
    if( rc==LSM_OK && rtIsWrite(pPtr->eType) ){
      rc = segmentPtrReadData(
          pPtr, iOff+pPtr->nKey, pPtr->nVal, &pPtr->pVal, &pPtr->blob2
      );
    }
  }

  return rc;
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

void lsmSortedSplitkey(lsm_db *pDb, Level *pLevel, int *pRc){
  lsm_env *pEnv = pDb->pEnv;      /* Environment handle */
  int rc = *pRc;
  int i;
  Merge *pMerge = pLevel->pMerge;

  for(i=0; rc==LSM_OK && i<pLevel->nRight; i++){
    Page *pPg = 0;
    int iTopic;
    Blob blob = {0, 0, 0, 0};
    SortedRun *pRun = &pLevel->aRhs[i].run;

    assert( pRun->iFirst!=0 );

    rc = lsmFsDbPageGet(pDb->pFS, pRun, pMerge->aInput[i].iPg, &pPg);
    if( rc==LSM_OK ){
      rc = pageGetKeyCopy(pEnv, pPg, pMerge->aInput[i].iCell, &iTopic, &blob);
    }
    if( rc==LSM_OK ){
      int res = -1;
      if( pLevel->pSplitKey ){
        res = sortedKeyCompare(pDb->xCmp,
            iTopic, blob.pData, blob.nData, 
            pLevel->iSplitTopic, pLevel->pSplitKey, pLevel->nSplitKey
        );
      }
      if( res<0 ){
        lsmFree(pEnv, pLevel->pSplitKey);
        pLevel->iSplitTopic = iTopic;
        pLevel->pSplitKey = blob.pData;
        pLevel->nSplitKey = blob.nData;
      }else{
        lsmFree(pEnv, blob.pData);
      }
    }
    lsmFsPageRelease(pPg);
  }

  *pRc = rc;
}

/*
** Initialize a LevelCursor structure. The caller is responsible for
** allocating the memory that the structure itself occupies.
*/
static int levelCursorInit(
  lsm_db *pDb,
  Level *pLevel, 
  int (*xCmp)(void *, int, void *, int),
  LevelCursor *pCsr              /* Cursor structure to initialize */
){
  int rc = LSM_OK;

  memset(pCsr, 0, sizeof(LevelCursor));
  pCsr->pFS = pDb->pFS;
  pCsr->bIgnoreSeparators = 1;
  pCsr->xCmp = xCmp;
  pCsr->pLevel = pLevel;
  pCsr->nPtr = 1 + pLevel->nRight;
  pCsr->aPtr = (SegmentPtr*)lsmMallocZeroRc(pDb->pEnv, 
      sizeof(SegmentPtr)*pCsr->nPtr, &rc
  );

  if( rc==LSM_OK ){
    int i;
    pCsr->aPtr[0].pSeg = &pLevel->lhs;

    for(i=0; i<pLevel->nRight; i++){
      pCsr->aPtr[i+1].pSeg = &pLevel->aRhs[i];
    }
    for(i=0; i<pCsr->nPtr; i++){
      pCsr->aPtr[i].pRun = &pCsr->aPtr[i].pSeg->run;
    }
  }

  return rc;
}

static int levelCursorInitRun(
  lsm_db *pDb,
  SortedRun *pRun, 
  int (*xCmp)(void *, int, void *, int),
  LevelCursor *pCsr              /* Cursor structure to initialize */
){
  int rc = LSM_OK;

  memset(pCsr, 0, sizeof(LevelCursor));
  pCsr->pFS = pDb->pFS;
  pCsr->bIgnoreSeparators = 1;
  pCsr->xCmp = xCmp;
  pCsr->nPtr = 1;
  pCsr->aPtr = (SegmentPtr*)lsmMallocZeroRc(pDb->pEnv, 
      sizeof(SegmentPtr)*pCsr->nPtr, &rc
  );

  if( rc==LSM_OK ){
    pCsr->aPtr[0].pRun = pRun;
  }

  return rc;
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

static void segmentCursorReset(LevelCursor *pCsr){
  int i;
  for(i=0; i<pCsr->nPtr; i++){
    segmentPtrReset(&pCsr->aPtr[i]);
  }
}

/*
** Close the cursor. Release all associated resources. The memory occupied
** by the LevelCursor structure itself is not released. It is assumed to
** be managed by the caller.
*/
static void segmentCursorClose(lsm_env *pEnv, LevelCursor *pCsr){
  segmentCursorReset(pCsr);
  lsmFree(pEnv, pCsr->aPtr);
  memset(pCsr, 0, sizeof(LevelCursor));
}

static int segmentPtrAdvance(
  LevelCursor *pCsr, 
  SegmentPtr *pPtr,
  int bReverse
){
  int eDir = (bReverse ? -1 : 1);
  do {
    int iCell;                    /* Number of new cell in page */

    iCell = pPtr->iCell + eDir;
    assert( pPtr->pPg );
    assert( iCell<=pPtr->nCell && iCell>=-1 );

    if( iCell>=pPtr->nCell || iCell<0 ){
      int rc;
      do {
        rc = segmentPtrNextPage(pPtr, eDir); 
      }while( rc==LSM_OK 
           && pPtr->pPg 
           && (pPtr->nCell==0 || (pPtr->flags & SEGMENT_BTREE_FLAG) ) 
      );
      if( rc!=LSM_OK ) return rc;
      iCell = bReverse ? (pPtr->nCell-1) : 0;
    }
    segmentPtrLoadCell(pPtr, iCell);
  }while( pCsr && pPtr->pPg && (
        (pCsr->bIgnoreSeparators && rtIsSeparator(pPtr->eType))
     || (pCsr->bIgnoreSystem && rtTopic(pPtr->eType)!=0)
  ));


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
    *pRc = lsmFsDbPageEnd(pFS, pPtr->pRun, bLast, &pNew);
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
static void segmentPtrEnd(
  LevelCursor *pCsr,              /* Cursor that owns this segment-pointer */
  SegmentPtr *pPtr,               /* Segment pointer to reposition */
  int bLast,                      /* True for last, false for first */
  int *pRc                        /* IN/OUT error code */
){
  if( *pRc==LSM_OK ){
    int rc = LSM_OK;

    segmentPtrEndPage(pCsr->pFS, pPtr, bLast, &rc);
    while( rc==LSM_OK && pPtr->pPg && pPtr->nCell==0 ){
      rc = segmentPtrNextPage(pPtr, (bLast ? -1 : 1));
    }
    if( pPtr->pPg ){
      segmentPtrLoadCell(pPtr, bLast ? (pPtr->nCell-1) : 0);
    }

    if( rc==LSM_OK && pPtr->pPg && (
          (pCsr->bIgnoreSeparators && rtIsSeparator(pPtr->eType))
       || (pCsr->bIgnoreSystem && rtTopic(pPtr->eType)!=0)
    )){
      rc = segmentPtrAdvance(pCsr, pPtr, bLast);
    }
    *pRc = rc;
  }
}

static void segmentPtrKey(SegmentPtr *pPtr, void **ppKey, int *pnKey){
  assert( pPtr->pPg );
  *ppKey = pPtr->pKey;
  *pnKey = pPtr->nKey;
}

static void segmentCursorKey(LevelCursor *pCsr, void **ppKey, int *pnKey){
  segmentPtrKey(&pCsr->aPtr[pCsr->iCurrentPtr], ppKey, pnKey);
}

static void segmentCursorValue(LevelCursor *pCsr, void **ppVal, int *pnVal){
  SegmentPtr *pPtr = &pCsr->aPtr[pCsr->iCurrentPtr];
  assert( pPtr->pPg );
  *ppVal = pPtr->pVal;
  *pnVal = pPtr->nVal;
}

static void segmentCursorType(LevelCursor *pCsr, int *peType){
  SegmentPtr *pPtr = &pCsr->aPtr[pCsr->iCurrentPtr];
  assert( pPtr->pPg );
  *peType = pPtr->eType;
}

static int segmentCursorValid(LevelCursor *pCsr){
  SegmentPtr *pPtr = &pCsr->aPtr[pCsr->iCurrentPtr];
  return (pPtr->pPg!=0);
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
  LevelCursor *pCsr, 
  SegmentPtr *pPtr, 
  void *pKey, int nKey
){
  lsm_env *pEnv = lsmFsEnv(pCsr->pFS);
  Blob blob = {0, 0, 0};
  int eDir;
  int iTopic = 0;                 /* TODO: Fix me */

  for(eDir=-1; eDir<=1; eDir+=2){
    Page *pTest = pPtr->pPg;

    lsmFsPageRef(pTest);
    while( pTest ){
      Page *pNext;

      int rc = lsmFsDbPageNext(pPtr->pRun, pTest, eDir, &pNext);
      lsmFsPageRelease(pTest);
      pTest = pNext;
      assert( rc==LSM_OK );

      if( pTest ){
        int nData;
        u8 *aData = lsmFsPageData(pTest, &nData);
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
          if( res==0 ) res = pCsr->xCmp(pKey, nKey, pPgKey, nPgKey);
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

int segmentPtrSeek(
  LevelCursor *pCsr,              /* Cursor context */
  SegmentPtr *pPtr,               /* Pointer to seek */
  void *pKey, int nKey,           /* Key to seek to */
  int eSeek,                      /* Search bias - see above */
  int *piPtr                      /* OUT: FC pointer */
){
  int res;                        /* Result of comparison operation */
  int rc = LSM_OK;
  int iMin;
  int iMax;
  int iPtrOut = 0;

  const int iTopic = 0;

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
    res = iLastTopic - iTopic;
    if( res==0 ) res = pCsr->xCmp(pLastKey, nLastKey, pKey, nKey);
    if( res>=0 ) break;

    /* Advance to the next page that contains at least one key. */
    do {
      rc = lsmFsDbPageNext(pPtr->pRun, pPtr->pPg, 1, &pNext);
      if( pNext==0 ) break;
      assert( rc==LSM_OK );
      segmentPtrSetPage(pPtr, pNext);
    }while( (pPtr->nCell==0 || (pPtr->flags & SEGMENT_BTREE_FLAG)) );
    if( pNext==0 ) break;

    /* This should probably be an LSM_CORRUPT error. */
    assert( rc!=LSM_OK || (pPtr->flags & PGFTR_SKIP_THIS_FLAG) );
  }

  iPtrOut = pPtr->iPtr;

  /* Assert that this page is the right page of this segment for the key
  ** that we are searching for. Do this by loading page (iPg-1) and testing
  ** that pKey/nKey is greater than all keys on that page, and then by 
  ** loading (iPg+1) and testing that pKey/nKey is smaller than all
  ** the keys it houses.  */
  assert( assertKeyLocation(pCsr, pPtr, pKey, nKey) );

  assert( pPtr->nCell>0 
       || pPtr->pRun->nSize==1 
       || lsmFsPageNumber(pPtr->pPg)==pPtr->pRun->iLast
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

      segmentPtrLoadCell(pPtr, iTry);
      segmentPtrKey(pPtr, &pKeyT, &nKeyT);
      iTopicT = rtTopic(pPtr->eType);

      res = iTopicT - iTopic;
      if( res==0 ) res = pCsr->xCmp(pKeyT, nKeyT, pKey, nKey);
      if( res<=0 ){
        iPtrOut = pPtr->iPtr + pPtr->iPgPtr;
      }

      if( res==0 || iMin==iMax ){
        break;
      }else if( res>0 ){
        iMax = MAX(iTry-1, iMin);
      }else{
        iMin = iTry+1;
      }
    }

    assert( res==0 || (iMin==iMax && iMin>=0 && iMin<pPtr->nCell) );
    if( res ){
      segmentPtrLoadCell(pPtr, iMin);
    }

    if( (eSeek==0 && (res!=0 || rtIsSeparator(pPtr->eType) )) ){
      segmentPtrReset(pPtr);
    }
    else if( res && eSeek && (res ^ eSeek)<0 ){
      rc = segmentPtrAdvance(pCsr, pPtr, eSeek<0);
    }

    if( rc==LSM_OK && pPtr->pPg && (
          (pCsr->bIgnoreSeparators && rtIsSeparator(pPtr->eType))
       || (pCsr->bIgnoreSystem && rtTopic(pPtr->eType)!=0)
    )){
      rc = segmentPtrAdvance(pCsr, pPtr, eSeek<0);
    }
  }

  *piPtr = iPtrOut;
  return rc;
}

/*
** Helper function for segmentCursorSetCurrent(). See its header comment
** for details.
*/
static int segmentCursorPtrCmp(
  LevelCursor *pCsr,            /* Cursor context */
  int bLargest,                   /* True to identify the larger key */
  SegmentPtr *pLeft,
  SegmentPtr *pRight              
){
  int iRet;
  if( pLeft->pPg==0 ){
    iRet = 1;
  }else if( pRight->pPg==0 ){
    iRet = 0;
  }else{
    int res = pCsr->xCmp(pLeft->pKey, pLeft->nKey, pRight->pKey, pRight->nKey);

    if( res==0 || (res<0 && bLargest==0) || (res>0 && bLargest) ){
      iRet = 0;
    }else{
      iRet = 1;
    }
  }
  return iRet;
}

static void segmentCursorSetCurrent(LevelCursor *pCsr, int bLargest){
  int iBest;
  int i;

  iBest = 0;
  for(i=1; i<pCsr->nPtr; i++){
    int res = segmentCursorPtrCmp(
        pCsr, bLargest, &pCsr->aPtr[iBest], &pCsr->aPtr[i]
    );
    if( res ) iBest = i;
  }

  pCsr->iCurrentPtr = iBest;
}

static int seekInSeparators(
  LevelCursor *pCsr,
  SegmentPtr *pPtr,               /* Segment to seek within */
  void *pKey, int nKey,           /* Key to seek to */
  int *piPtr                      /* OUT: FC pointer */
){
  int rc;
  int iPg;
  Blob blob = {0, 0, 0};
  int iTopic = 0;                 /* TODO: Fix me */
  SortedRun *pSep = &pPtr->pSeg->sep;

  iPg = pSep->iRoot;
  do {
    Page *pPg;
    rc = lsmFsDbPageGet(pCsr->pFS, pSep, iPg, &pPg);
    if( rc==LSM_OK ){
      u8 *aData;                  /* Buffer containing page data */
      int nData;                  /* Size of aData[] in bytes */
      int iMin;
      int iMax;
      int nRec;
      int flags;

      aData = lsmFsPageData(pPg, &nData);
      flags = pageGetFlags(aData, nData);
      if( (flags & SEGMENT_BTREE_FLAG)==0 ){
        lsmFsPageRelease(pPg);
        break;
      }

      iPg = pageGetPtr(aData, nData);
      nRec = pageGetNRec(aData, nData);

      iMin = 0;
      iMax = nRec-1;
      while( iMax>=iMin ){
        Page *pRef = 0;
        int iTry = (iMin+iMax)/2;
        void *pKeyT; int nKeyT;       /* Key for cell iTry */
        int iTopicT;                  /* Topic for key pKeyT/nKeyT */
        int iPtr;                     /* Pointer associated with cell iTry */
        u8 *aCell;                    /* Pointer to cell iTry */
        int res;                      /* (pKey - pKeyT) */
        int eType;

        aCell = pageGetCell(aData, nData, iTry);
        eType = *aCell++;
        aCell += lsmVarintGet32(aCell, &iPtr);
        if( eType==0 ){
          /* If eType==0, then this b-tree cell does not contain a key. 
          ** Instead, it is a reference to another cell in the same separators
          ** array that does contain a key. */
          Pgno iRef;
          aCell += lsmVarintGet32(aCell, &iRef);
          rc = lsmFsDbPageGet(pCsr->pFS, pSep, iRef, &pRef);
          if( rc!=LSM_OK ) break;
          pKeyT = pageGetKey(pRef, 0, &iTopicT, &nKeyT, &blob);
        }else{
          aCell += lsmVarintGet32(aCell, &nKeyT);
          pKeyT = (void *)aCell;
          iTopicT = rtTopic(eType);
        }

        res = iTopic - iTopicT;
        if( res==0 ) res = pCsr->xCmp(pKey, nKey, pKeyT, nKeyT);

        if( res<0 ){
          iPg = iPtr;
          iMax = iTry-1;
        }else{
          iMin = iTry+1;
        }
        lsmFsPageRelease(pRef);
      }
      lsmFsPageRelease(pPg);
    }
  }while( rc==LSM_OK );

  if( rc==LSM_OK ){
    assert( pPtr->pRun==&pPtr->pSeg->run );
    pPtr->pRun = pSep;
    rc = segmentPtrLoadPage(pCsr->pFS, pPtr, iPg);
    if( rc==LSM_OK ){
      rc = segmentPtrSeek(pCsr, pPtr, pKey, nKey, 0, piPtr);
    }
    pPtr->pRun = &pPtr->pSeg->run;
  }

  sortedBlobFree(&blob);
  return rc;
}

static int seekInSegment(
  LevelCursor *pCsr, 
  SegmentPtr *pPtr,
  void *pKey, int nKey,
  int iPg,                        /* Page to search */
  int eSeek,                      /* Search bias - see above */
  int *piPtr                      /* OUT: FC pointer */
){
  int iPtr = iPg;
  int rc = LSM_OK;

  assert( pPtr->pRun==&pPtr->pSeg->run );

  if( segmentHasSeparators(pPtr->pSeg) ){
    rc = seekInSeparators(pCsr, pPtr, pKey, nKey, &iPtr);
  }else if( iPtr==0 ){
    iPtr = pPtr->pSeg->run.iFirst;
  }

  if( rc==LSM_OK ){
    rc = segmentPtrLoadPage(pCsr->pFS, pPtr, iPtr);
  }
  if( rc==LSM_OK ){
    rc = segmentPtrSeek(pCsr, pPtr, pKey, nKey, eSeek, piPtr);
  }
  return rc;
}

/*
** Seek the cursor.
*/
static int segmentCursorSeek(
  LevelCursor *pCsr,              /* Sorted cursor object to seek */
  void *pKey, int nKey,           /* Key to seek to */
  int iPg,                        /* Page to search */
  int eSeek,                      /* Search bias - see above */
  int *piPtr                      /* OUT: FC pointer */
){
  int rc = LSM_OK;

  int iOut = 0;                   /* Pointer to return to caller */
  int res = -1;                   /* Result of xCmp(pKey, split) */

  if( pCsr->nPtr>1 ){ 
    Level *pLevel = pCsr->pLevel;
    assert( pLevel );
    res = 0 - pLevel->iSplitTopic;
    if( res==0 ){
      res = pCsr->xCmp(pKey, nKey, pLevel->pSplitKey, pLevel->nSplitKey);
    }
  }

  if( res<0 ){
    int iPtr = 0;
    if( pCsr->nPtr==1 ) iPtr = iPg;

    rc = seekInSegment(pCsr, &pCsr->aPtr[0], pKey, nKey, iPtr, eSeek, &iOut);
    if( rc==LSM_OK 
     && pCsr->nPtr>1
     && eSeek==LSM_SEEK_GE 
     && pCsr->aPtr[0].pPg==0 
    ){
      res = 0;
    }
  }
  
  if( res>=0 ){
    int iPtr = iPg;
    int i;
    for(i=1; rc==LSM_OK && i<pCsr->nPtr; i++){
      iOut = 0;
      rc = seekInSegment(pCsr, &pCsr->aPtr[i], pKey, nKey, iPtr, eSeek, &iOut);
      iPtr = iOut;
    }

    if( eSeek==LSM_SEEK_LE ){
      segmentPtrEnd(pCsr, &pCsr->aPtr[0], 1, &rc);
    }
  }

  segmentCursorSetCurrent(pCsr, (eSeek<0));
  *piPtr = iOut;
  return rc;
}

/*
** Advance the cursor forwards (bReverse==0) or backwards (bReverse!=0).
*/
static int segmentCursorAdvance(LevelCursor *pCsr, int bReverse){
  int rc;
  int iCurrent = pCsr->iCurrentPtr;
  SegmentPtr *pCurrent;

  pCurrent = &pCsr->aPtr[iCurrent];
  rc = segmentPtrAdvance(pCsr, pCurrent, bReverse);
  
  /* If bReverse is true and the segment pointer just moved is on the rhs
  ** of the level, check if the key it now points to is smaller than the
  ** split-key. If so, set the segment to EOF.
  **
  ** The danger here is that if the current key is smaller than the 
  ** split-key then there may have existed a delete key on a page that
  ** has already been gobbled.
  */
  if( rc==LSM_OK && bReverse && iCurrent>0 && pCurrent->pPg ){
    Level *p = pCsr->pLevel;
    int res = sortedKeyCompare(pCsr->xCmp,
        rtTopic(pCurrent->eType), pCurrent->pKey, pCurrent->nKey,
        p->iSplitTopic, p->pSplitKey, p->nSplitKey
    );
    if( res<0 ){
      segmentPtrReset(pCurrent);
    }
  }

  segmentCursorSetCurrent(pCsr, bReverse);

  if( segmentCursorValid(pCsr)==0 
   && bReverse==0 
   && iCurrent==0 
   && pCsr->nPtr>1
   && pCsr->aPtr[1].pPg==0 
  ){
    Level *p = pCsr->pLevel;
    int i;
    for(i=1; i<pCsr->nPtr; i++){
      SegmentPtr *pPtr = &pCsr->aPtr[i];
      segmentPtrEnd(pCsr, pPtr, 0, &rc);

      /* If the segment-pointer now points to a key that is smaller than
      ** the split-key, advance it until it does not. Again, the danger
      ** is that if the current key is smaller than the split-key then 
      ** there may have existed a delete key on a page that has already 
      ** been gobbled. 
      **
      ** TODO: This might end up taking a long time. Is there some other
      ** way? Apart from searching the entire tree for pSplitkey to set 
      ** this levels segment-pointers?
      */
      while( pPtr->pPg ){
        int res = sortedKeyCompare(pCsr->xCmp,
            rtTopic(pPtr->eType), pPtr->pKey, pPtr->nKey,
            p->iSplitTopic, p->pSplitKey, p->nSplitKey
        );
        if( res>=0 ) break;
        segmentPtrAdvance(pCsr, pPtr, 0);
      }

    }
    segmentCursorSetCurrent(pCsr, bReverse);
  }

  return rc;
}

/*
** Move the cursor to point to either the first element (if bLast==0), or
** the last element (if bLast!=0) in the sorted file.
*/
static int segmentCursorEnd(LevelCursor *pCsr, int bLast){
  int rc = LSM_OK;

  segmentPtrEnd(pCsr, &pCsr->aPtr[0], bLast, &rc);
  if( bLast || pCsr->aPtr[0].pPg==0 ){
    int i;
    for(i=1; i<pCsr->nPtr; i++){
      segmentPtrEnd(pCsr, &pCsr->aPtr[i], bLast, &rc);
    }
  }

  segmentCursorSetCurrent(pCsr, bLast);
  return rc;
}

static void mcursorFreeComponents(MultiCursor *pCsr){
  int i;
  lsm_env *pEnv = pCsr->pDb->pEnv;

  /* Close the tree cursor, if any. */
  lsmTreeCursorDestroy(pCsr->pTreeCsr);

  /* Close the sorted file cursors */
  for(i=0; i<pCsr->nSegCsr; i++){
    segmentCursorClose(pEnv, &pCsr->aSegCsr[i]);
  }

  /* Free allocations */
  lsmFree(pEnv, pCsr->aSegCsr);
  lsmFree(pEnv, pCsr->aTree);
  lsmFree(pEnv, pCsr->pSystemVal);

  /* Zero fields */
  pCsr->nSegCsr = 0;
  pCsr->aSegCsr = 0;
  pCsr->nTree = 0;
  pCsr->aTree = 0;
  pCsr->pSystemVal = 0;
  pCsr->pSnap = 0;
}

void lsmMCursorClose(MultiCursor *pCsr){
  if( pCsr ){
    /* Free the allocation used to cache the current key, if any. */
    sortedBlobFree(&pCsr->key);

    /* Free the component cursors */
    mcursorFreeComponents(pCsr);

    /* Free the cursor structure itself */
    lsmFree(pCsr->pDb->pEnv, pCsr);
  }
}

#define MULTICURSOR_ADDLEVEL_ALL 1
#define MULTICURSOR_ADDLEVEL_RHS 2
#define MULTICURSOR_ADDLEVEL_LHS_SEP 3
#define MULTICURSOR_ADDLEVEL_RHS_SEP 4

/*
** Add segments belonging to level pLevel to the multi-cursor pCsr. The
** third argument must be one of the following:
**
**   MULTICURSOR_ADDLEVEL_ALL
**     Add all segments in the level to the cursor.
**
**   MULTICURSOR_ADDLEVEL_RHS
**     Add only the rhs segments in the level to the cursor.
**
**   MULTICURSOR_ADDLEVEL_LHS_SEP
**     Add only the lhs segment. And iterate through its separators array,
**     not the main run array.
**
**   MULTICURSOR_ADDLEVEL_RHS_SEP
**     Add only the first segment from the rhs. And iterate through its 
**     separators array, not the main run array.
**
** RHS and SEP are only used by cursors created to use as data sources when
** creating new segments (either when flushing the in-memory tree to disk or
** when merging existing runs).
*/
int multiCursorAddLevel(
  MultiCursor *pCsr,              /* Multi-cursor to add segment to */ 
  Level *pLevel,                  /* Level to add to multi-cursor merge */
  int eMode                       /* A MULTICURSOR_ADDLEVEL_*** constant */
){
  int i;
  int nAdd = (eMode==MULTICURSOR_ADDLEVEL_RHS ? pLevel->nRight : 1);

  assert( eMode==MULTICURSOR_ADDLEVEL_ALL
       || eMode==MULTICURSOR_ADDLEVEL_RHS
       || eMode==MULTICURSOR_ADDLEVEL_LHS_SEP
  );

  for(i=0; i<nAdd; i++){
    LevelCursor *pNew;
    lsm_db *pDb = pCsr->pDb;

    /* Grow the pCsr->aSegCsr array if required */
    if( 0==(pCsr->nSegCsr % 16) ){
      int nByte;
      LevelCursor *aNew;
      nByte = sizeof(LevelCursor) * (pCsr->nSegCsr+16);
      aNew = (LevelCursor *)lsmRealloc(pDb->pEnv, pCsr->aSegCsr, nByte);
      if( aNew==0 ) return LSM_NOMEM_BKPT;
      pCsr->aSegCsr = aNew;
    }

    pNew = &pCsr->aSegCsr[pCsr->nSegCsr];

    switch( eMode ){
      case MULTICURSOR_ADDLEVEL_ALL:
        levelCursorInit(pDb, pLevel, pCsr->xCmp, pNew);
        break;

      case MULTICURSOR_ADDLEVEL_RHS:
        levelCursorInitRun(pDb, &pLevel->aRhs[i].run, pCsr->xCmp, pNew);
        break;

      case MULTICURSOR_ADDLEVEL_LHS_SEP:
        levelCursorInitRun(pDb, &pLevel->lhs.sep, pCsr->xCmp, pNew);
        break;
    }
    if( pCsr->flags & CURSOR_IGNORE_SYSTEM ){
      pNew->bIgnoreSystem = 1;
    }
    pCsr->nSegCsr++;
  }

  return LSM_OK;
}


static int multiCursorNew(
  lsm_db *pDb,                    /* Database handle */
  Snapshot *pSnap,                /* Snapshot to use for this cursor */
  int useTree,                    /* If true, search the in-memory tree */
  int bUserOnly,                  /* If true, ignore all system data */
  MultiCursor **ppCsr             /* OUT: Allocated cursor */
){
  int rc = LSM_OK;                /* Return Code */
  MultiCursor *pCsr = *ppCsr;     /* Allocated multi-cursor */

  if( pCsr==0 ){
    pCsr = (MultiCursor *)lsmMallocZeroRc(pDb->pEnv, sizeof(MultiCursor), &rc);
  }

  if( rc==LSM_OK ){
    if( useTree ){
      assert( pDb->pTV );
      rc = lsmTreeCursorNew(pDb->pTV, &pCsr->pTreeCsr);
    }
    pCsr->pDb = pDb;
    pCsr->pSnap = pSnap;
    pCsr->xCmp = pDb->xCmp;
    if( bUserOnly ){
      pCsr->flags |= CURSOR_IGNORE_SYSTEM;
    }
  }
  if( rc!=LSM_OK ){
    lsmMCursorClose(pCsr);
    pCsr = 0;
  }
  *ppCsr = pCsr;
  return rc;
}

static void multiCursorReadSeparators(MultiCursor *pCsr){
  if( pCsr->nSegCsr>0 ){
    pCsr->aSegCsr[pCsr->nSegCsr-1].bIgnoreSeparators = 0;
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
** with (a) the system bit set, and (b) the key "F" and (c) a value blob
** containing the entire serialized free-block list.
*/
static void multiCursorVisitFreelist(MultiCursor *pCsr){
  assert( pCsr );
  pCsr->flags |= CURSOR_NEW_SYSTEM;
}

/*
** Allocate a new cursor to read the database (the in-memory tree and all
** levels). If successful, set *ppCsr to point to the new cursor object
** and return SQLITE_OK. Otherwise, set *ppCsr to NULL and return an
** lsm error code.
**
** If parameter bSystem is true, this is a system cursor. In that case
** the behaviour of this function is modified as follows:
**
**   * the worker snapshot is used instead of the client snapshot, and
**   * the in-memory tree is ignored.
*/
static int multiCursorAllocate(
  lsm_db *pDb,                    /* Database handle */
  int bSystem,                    /* True for a system cursor */
  MultiCursor **ppCsr             /* OUT: Allocated cursor */
){
  int rc = LSM_OK;                /* Return Code */
  MultiCursor *pCsr = *ppCsr;     /* Allocated multi-cursor */
  Level *p;                       /* Level iterator */
  Snapshot *pSnap;                /* Snapshot to use for cursor */

  pSnap = (bSystem ? pDb->pWorker : pDb->pClient);
  assert( pSnap );

  rc = multiCursorNew(pDb, pSnap, !bSystem, !bSystem, &pCsr);
  multiCursorIgnoreDelete(pCsr);
  for(p=lsmDbSnapshotLevel(pSnap); p && rc==LSM_OK; p=p->pNext){
    rc = multiCursorAddLevel(pCsr, p, MULTICURSOR_ADDLEVEL_ALL);
  }

  if( rc!=LSM_OK ){
    lsmMCursorClose(pCsr);
    pCsr = 0;
  }
  *ppCsr = pCsr;
  return rc;
}

/*
** Allocate and return a new database cursor.
*/
int lsmMCursorNew(
  lsm_db *pDb,                    /* Database handle */
  MultiCursor **ppCsr             /* OUT: Allocated cursor */
){
  *ppCsr = 0;
  return multiCursorAllocate(pDb, 0, ppCsr);
}

#define CURSOR_DATA_TREE      0
#define CURSOR_DATA_SYSTEM    1
#define CURSOR_DATA_SEGMENT   2

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
    case CURSOR_DATA_TREE:
      if( lsmTreeCursorValid(pCsr->pTreeCsr) ){
        int nVal;
        void *pVal;

        lsmTreeCursorKey(pCsr->pTreeCsr, &pKey, &nKey);
        lsmTreeCursorValue(pCsr->pTreeCsr, &pVal, &nVal);
        eType = (nVal<0) ? SORTED_DELETE : SORTED_WRITE;
      }
      break;

    case CURSOR_DATA_SYSTEM:
      if( pCsr->flags & CURSOR_AT_FREELIST ){
        pKey = (void *)"FREELIST";
        nKey = 8;
        eType = SORTED_SYSTEM_WRITE;
      }
      else if( pCsr->flags & CURSOR_AT_LEVELS ){
        pKey = (void *)"LEVELS";
        nKey = 6;
        eType = SORTED_SYSTEM_WRITE;
      }
      break;

    default: {
      int iSeg = iKey - CURSOR_DATA_SEGMENT;
      if( iSeg<pCsr->nSegCsr && segmentCursorValid(&pCsr->aSegCsr[iSeg]) ){
        segmentCursorKey(&pCsr->aSegCsr[iSeg], &pKey, &nKey);
        segmentCursorType(&pCsr->aSegCsr[iSeg], &eType);
      }
      break;
    }
  }

  if( peType ) *peType = eType;
  if( pnKey ) *pnKey = nKey;
  if( ppKey ) *ppKey = pKey;
}

static void multiCursorGetVal(
  MultiCursor *pCsr, 
  int iVal, 
  void **ppVal, 
  int *pnVal
){
  if( iVal==CURSOR_DATA_TREE ){
    if( lsmTreeCursorValid(pCsr->pTreeCsr) ){
      lsmTreeCursorValue(pCsr->pTreeCsr, ppVal, pnVal);
    }else{
      *ppVal = 0;
      *pnVal = 0;
    }
  }else if( iVal==CURSOR_DATA_SYSTEM ){
    if( pCsr->flags & CURSOR_AT_FREELIST ){
      int *aVal;
      int nVal;
      assert( pCsr->pSystemVal==0 );
      lsmSnapshotFreelist(pCsr->pSnap, &aVal, &nVal);
      pCsr->pSystemVal = *ppVal = (void *)aVal;
      *pnVal = sizeof(int) * nVal;
    }else if( pCsr->flags & CURSOR_AT_LEVELS ){
      lsmFree(pCsr->pDb->pEnv, pCsr->pSystemVal);
      lsmCheckpointLevels(pCsr->pDb, pCsr->pnHdrLevel, ppVal, pnVal);
      pCsr->pSystemVal = *ppVal;
    }else{
      *ppVal = 0;
      *pnVal = 0;
    }
  }else if( iVal-CURSOR_DATA_SEGMENT<pCsr->nSegCsr 
         && segmentCursorValid(&pCsr->aSegCsr[iVal-CURSOR_DATA_SEGMENT]) 
  ){
    segmentCursorValue(&pCsr->aSegCsr[iVal-CURSOR_DATA_SEGMENT], ppVal, pnVal);
  }else{
    *ppVal = 0;
    *pnVal = 0;
  }
}

int lsmSortedLoadSystem(lsm_db *pDb){
  MultiCursor *pCsr = 0;          /* Cursor used to retreive free-list */
  int rc;                         /* Return Code */

  assert( pDb->pWorker );
  rc = multiCursorAllocate(pDb, 1, &pCsr);
  if( rc==LSM_OK ){
    void *pVal; int nVal;         /* Value read from database */

    rc = lsmMCursorLast(pCsr);
    if( rc==LSM_OK 
     && pCsr->eType==SORTED_SYSTEM_WRITE 
     && pCsr->key.nData==6 
     && 0==memcmp(pCsr->key.pData, "LEVELS", 6)
    ){
      rc = lsmMCursorValue(pCsr, &pVal, &nVal);
      if( rc==LSM_OK ){
        lsmCheckpointLoadLevels(pDb, pVal, nVal);
      }
      if( rc==LSM_OK ){
        rc = lsmMCursorPrev(pCsr);
      }
    }

    if( rc==LSM_OK 
     && pCsr->eType==SORTED_SYSTEM_WRITE 
     && pCsr->key.nData==8 
     && 0==memcmp(pCsr->key.pData, "FREELIST", 8)
    ){
      rc = lsmMCursorValue(pCsr, &pVal, &nVal);
      if( rc==LSM_OK ){
        lsmSnapshotSetFreelist(pDb->pWorker, (int *)pVal, nVal/sizeof(u32));
      }
    }

    lsmMCursorClose(pCsr);
  }
  return rc;
}

static void multiCursorDoCompare(MultiCursor *pCsr, int iOut, int bReverse){
  int i1;
  int i2;
  int iRes;
  void *pKey1; int nKey1; int eType1;
  void *pKey2; int nKey2; int eType2;

  int mul = (bReverse ? -1 : 1);

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

    res = (rtTopic(eType1) - rtTopic(eType2));
    if( res==0 ){
      res = pCsr->xCmp(pKey1, nKey1, pKey2, nKey2);
    }
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

static int multiCursorAllocTree(MultiCursor *pCsr){
  int rc = LSM_OK;
  if( pCsr->aTree==0 ){
    int nByte;                    /* Bytes of space to allocate */
    pCsr->nTree = 2;
    while( pCsr->nTree<(CURSOR_DATA_SEGMENT+pCsr->nSegCsr) ){
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

static int multiCursorEnd(MultiCursor *pCsr, int bLast){
  int rc = LSM_OK;
  int i;

  if( pCsr->pTreeCsr ){
    rc = lsmTreeCursorEnd(pCsr->pTreeCsr, bLast);
  }
  if( pCsr->flags & CURSOR_NEW_SYSTEM ){
    assert( bLast==0 );
    pCsr->flags |= CURSOR_AT_FREELIST;
  }
  for(i=0; rc==LSM_OK && i<pCsr->nSegCsr; i++){
    rc = segmentCursorEnd(&pCsr->aSegCsr[i], bLast);
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
  if( rc==LSM_OK 
   && (pCsr->flags & CURSOR_IGNORE_DELETE) 
   && rtIsDelete(pCsr->eType)
  ){
    if( bLast ){
      rc = lsmMCursorPrev(pCsr);
    }else{
      rc = lsmMCursorNext(pCsr);
    }
  }

  return rc;
}


int lsmMCursorSave(MultiCursor *pCsr){
  int rc = LSM_OK;
  if( pCsr->aTree ){
    if( pCsr->aTree[1]==CURSOR_DATA_TREE ){
      multiCursorCacheKey(pCsr, &rc);
    }
    mcursorFreeComponents(pCsr);
  }
  return rc;
}

int lsmMCursorRestore(lsm_db *pDb, MultiCursor *pCsr){
  int rc;
  rc = multiCursorAllocate(pDb, 1, &pCsr);
  if( rc==LSM_OK && pCsr->key.pData ){
    rc = lsmMCursorSeek(pCsr, pCsr->key.pData, pCsr->key.nData, +1);
  }
  return rc;
}

int lsmMCursorFirst(MultiCursor *pCsr){
  return multiCursorEnd(pCsr, 0);
}

int lsmMCursorLast(MultiCursor *pCsr){
  return multiCursorEnd(pCsr, 1);
}



void lsmMCursorReset(MultiCursor *pCsr){
  int i;
  lsmTreeCursorReset(pCsr->pTreeCsr);
  for(i=0; i<pCsr->nSegCsr; i++){
    segmentCursorReset(&pCsr->aSegCsr[i]);
  }
  pCsr->key.nData = 0;
}

/*
** Seek the cursor.
*/
int lsmMCursorSeek(MultiCursor *pCsr, void *pKey, int nKey, int eSeek){
  int rc = LSM_OK;
  int i;
  int res; 
  int iPtr = 0; 

  assert( (pCsr->flags & CURSOR_NEW_SYSTEM)==0 );
  assert( (pCsr->flags & CURSOR_AT_FREELIST)==0 );
  assert( (pCsr->flags & CURSOR_AT_LEVELS)==0 );
  
  lsmTreeCursorSeek(pCsr->pTreeCsr, pKey, nKey, &res);
  switch( eSeek ){
    case LSM_SEEK_EQ:
      if( res!=0 ){
        lsmTreeCursorReset(pCsr->pTreeCsr);
      }
      break;
    case LSM_SEEK_GE:
      if( res<0 && lsmTreeCursorValid(pCsr->pTreeCsr) ){
        lsmTreeCursorNext(pCsr->pTreeCsr);
      }
      break;
    default:
      assert( eSeek==LSM_SEEK_LE );
      if( res>0 ){
        assert( lsmTreeCursorValid(pCsr->pTreeCsr) );
        lsmTreeCursorPrev(pCsr->pTreeCsr);
      }
      break;
  }

  for(i=0; rc==LSM_OK && i<pCsr->nSegCsr; i++){
    rc = segmentCursorSeek(&pCsr->aSegCsr[i], pKey, nKey, iPtr, eSeek, &iPtr);
  }

  if( rc==LSM_OK ){
    rc = multiCursorAllocTree(pCsr);
  }
  if( rc==LSM_OK ){
    for(i=pCsr->nTree-1; i>0; i--){
      multiCursorDoCompare(pCsr, i, eSeek<0);
    }
  }

  multiCursorCacheKey(pCsr, &rc);
  if( rc==LSM_OK 
   && (pCsr->flags & CURSOR_IGNORE_DELETE) 
   && rtIsDelete(pCsr->eType)
  ){
    switch( eSeek ){
      case LSM_SEEK_EQ:
        lsmMCursorReset(pCsr);
        break;
      case LSM_SEEK_GE:
        rc = lsmMCursorNext(pCsr);
        break;
      default:
        assert( eSeek==LSM_SEEK_LE );
        rc = lsmMCursorPrev(pCsr);
        break;
    }
  }

  return rc;
}

int lsmMCursorValid(MultiCursor *pCsr){
  int res = 0;
  if( pCsr->aTree ){
    int iKey = pCsr->aTree[1];
    if( iKey==CURSOR_DATA_TREE ){
      res = lsmTreeCursorValid(pCsr->pTreeCsr);
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

  /* Check the current key value. If it is not greater than (if bReverse==0)
  ** or less than (if bReverse!=0) the key currently cached in pCsr->key, 
  ** then the cursor has not yet been successfully advanced.  
  */
  multiCursorGetKey(pCsr, pCsr->aTree[1], &eNewType, &pNew, &nNew);
  if( pNew ){
    int res = rtTopic(eNewType) - rtTopic(pCsr->eType);
    if( res==0 ){
      res = pCsr->xCmp(pNew, nNew, pCsr->key.pData, pCsr->key.nData);
    }
    if( (bReverse==0 && res<=0) || (bReverse!=0 && res>=0) ){
      return 0;
    }
  }

  multiCursorCacheKey(pCsr, pRc);
  assert( pCsr->eType==eNewType );

  if( *pRc==LSM_OK 
   && (pCsr->flags & CURSOR_IGNORE_DELETE) 
   && rtIsDelete(eNewType)
   ){
    /* If this cursor is configured to skip deleted keys, and the current
    ** cursor points to a SORTED_DELETE entry, then the cursor has not been 
    ** successfully advanced.  */
    return 0;
  }

  return 1;
}

static int multiCursorAdvance(MultiCursor *pCsr, int bReverse){
  int rc = LSM_OK;                /* Return Code */
  if( lsmMCursorValid(pCsr) ){
    do {
      int iKey = pCsr->aTree[1];
      if( iKey==CURSOR_DATA_TREE ){
        if( bReverse ){
          rc = lsmTreeCursorPrev(pCsr->pTreeCsr);
        }else{
          rc = lsmTreeCursorNext(pCsr->pTreeCsr);
        }
      }else if( iKey==CURSOR_DATA_SYSTEM ){
        assert( pCsr->flags & (CURSOR_AT_FREELIST | CURSOR_AT_LEVELS) );
        assert( pCsr->flags & CURSOR_NEW_SYSTEM );
        assert( bReverse==0 );

        if( pCsr->flags & CURSOR_AT_FREELIST ){
          pCsr->flags &= ~CURSOR_AT_FREELIST;
          pCsr->flags |= CURSOR_AT_LEVELS;
        }else{
          pCsr->flags &= ~CURSOR_AT_LEVELS;
        }
      }else{
        LevelCursor *pLevel = &pCsr->aSegCsr[iKey-CURSOR_DATA_SEGMENT];
        rc = segmentCursorAdvance(pLevel, bReverse);
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
  return multiCursorAdvance(pCsr, 0);
}

int lsmMCursorPrev(MultiCursor *pCsr){
  return multiCursorAdvance(pCsr, 1);
}

int lsmMCursorKey(MultiCursor *pCsr, void **ppKey, int *pnKey){

  if( pCsr->aTree[1]==CURSOR_DATA_TREE ){
    lsmTreeCursorKey(pCsr->pTreeCsr, ppKey, pnKey);
  }else{
    int nKey;

#ifndef NDEBUG
    void *pKey;
    int eType;
    multiCursorGetKey(pCsr, pCsr->aTree[1], &eType, &pKey, &nKey);
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
  return LSM_OK;
}

int lsmMCursorValue(MultiCursor *pCsr, void **ppVal, int *pnVal){
  assert( pCsr->aTree );
  assert( rtIsDelete(pCsr->eType)==0 || !(pCsr->flags & CURSOR_IGNORE_DELETE) );
  multiCursorGetVal(pCsr, pCsr->aTree[1], ppVal, pnVal);
  return LSM_OK;
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
       || eType==SORTED_SEPARATOR 
       || eType==SORTED_SYSTEM_SEPARATOR 
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
static int mergeWorkerMoveHierarchy(MergeWorker *pMW){
  SortedRun *pSep;                /* Separators run being modified */
  lsm_db *pDb = pMW->pDb;         /* Database handle */
  int rc = LSM_OK;                /* Return code */
  int i;
  int iRight = 0;
  int nHier = pMW->nHier;
  Page **apHier = pMW->apHier;

  assert( nHier>0 && pMW->pLevel->pMerge->bHierReadonly );

  pSep = &pMW->pLevel->lhs.sep;

  for(i=0; rc==LSM_OK && i<nHier; i++){
    Page *pNew = 0;
    rc = lsmFsSortedAppend(pDb->pFS, pDb->pWorker, pSep, &pNew);
    assert( rc==LSM_OK );

    if( rc==LSM_OK ){
      u8 *a1; int n1;
      u8 *a2; int n2;

      a1 = lsmFsPageData(pNew, &n1);
      a2 = lsmFsPageData(apHier[i], &n2);
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
        if( iRight ) lsmPutU32(&a1[SEGMENT_POINTER_OFFSET(n1)], iRight);
        lsmFsPageRelease(apHier[i]);
        apHier[i] = pNew;
        iRight = lsmFsPageNumber(pNew);
      }else{
        lsmPutU16(&a1[SEGMENT_FLAGS_OFFSET(n1)], SEGMENT_BTREE_FLAG);
        lsmPutU16(&a1[SEGMENT_NRECORD_OFFSET(n1)], 0);
        lsmPutU32(&a1[SEGMENT_POINTER_OFFSET(n1)], 0);
        i = i - 1;
        lsmFsPageRelease(pNew);
      }
    }
  }

#ifdef LSM_DEBUG
  if( rc==LSM_OK ){
    for(i=0; i<nHier; i++) assert( lsmFsPageWritable(pMW->apHier[i]) );
  }
#endif

  if( rc==LSM_OK ){
    pMW->pLevel->pMerge->bHierReadonly = 0;
  }
  return rc;
}

/*
** Allocate and populate the MergeWorker.apHier[] array.
*/
static int mergeWorkerLoadHierarchy(MergeWorker *pMW){
  int rc = LSM_OK;
  SortedRun *pSep = &pMW->pLevel->lhs.sep;

  if( pMW->apHier==0 && pSep->iRoot!=0 ){
    int bHierReadonly = pMW->pLevel->pMerge->bHierReadonly;
    FileSystem *pFS = pMW->pDb->pFS;
    lsm_env *pEnv = pMW->pDb->pEnv;
    Page **apHier = 0;
    int nHier = 0;
    int iPg = pSep->iRoot;

    do {
      Page *pPg = 0;
      u8 *aData;
      int nData;
      int flags;

      rc = lsmFsDbPageGet(pFS, pSep, iPg, &pPg);
      if( rc!=LSM_OK ) break;

      aData = lsmFsPageData(pPg, &nData);
      flags = pageGetFlags(aData, nData);
      if( flags&SEGMENT_BTREE_FLAG ){
        Page **apNew = (Page **)lsmRealloc(
            pEnv, apHier, sizeof(Page *)*(nHier+1)
        );
        if( apNew==0 ){
          rc = LSM_NOMEM_BKPT;
          break;
        }
        if( bHierReadonly==0 ) lsmFsPageWrite(pPg);
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
      pMW->nHier = nHier;
      pMW->apHier = apHier;
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
** Push the key passed through the pKey/nKey arguments into the b-tree 
** hierarchy. The associated pointer value is iPtr.
**
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
** segWriterMoveHierarchy() operation less complicated (since the pointers 
** that need to be updated are all stored as fixed-size integers within the 
** page footer, not varints in page records).
**
** Records may not span b-tree pages. If this function is called to add a
** record larger than (page-size / 4) bytes, then a pointer to the separators
** array page that contains the main record is added to the b-tree instead.
** In this case the record format is:
**
**         + 0x00 byte (1 byte) 
**         + Absolute pointer value (varint),
**         + Absolute page number of page containing key (varint).
**
** See function seekInSeparators() for the code that traverses b-tree pages.
*/
static int mergeWorkerPushHierarchy(
  MergeWorker *pMW,               /* Merge worker object */
  Pgno iKeyPg,                    /* Page that will contain pKey/nKey */
  int iTopic,                     /* Topic value for this key */
  void *pKey,                     /* Pointer to key buffer */
  int nKey                        /* Size of pKey buffer in bytes */
){
  lsm_db *pDb = pMW->pDb;         /* Database handle */
  int rc;                         /* Return Code */
  int iLevel;                     /* Level of b-tree hierachy to write to */
  int nData;                      /* Size of aData[] in bytes */
  u8 *aData;                      /* Page data for level iLevel */
  int iOff;                       /* Offset on b-tree page to write record to */
  int nRec;                       /* Initial number of records on b-tree page */
  Pgno iPtr;                      /* Pointer value to accompany pKey/nKey */
  int bIndirect;                  /* True to use an indirect record */

  /* If there exists a b-tree hierarchy and it is not loaded into 
  ** memory, load it now.  */
  rc = mergeWorkerLoadHierarchy(pMW);

  /* TODO: What the heck does this do? */
  if( pMW->nHier ){
    aData = lsmFsPageData(pMW->apHier[0], &nData);
    iPtr = lsmGetU32(&aData[SEGMENT_POINTER_OFFSET(nData)]);
  }else{
    iPtr = pMW->pLevel->lhs.sep.iFirst;
  }

  if( pMW->nHier && pMW->pLevel->pMerge->bHierReadonly ){
    rc = mergeWorkerMoveHierarchy(pMW);
    if( rc!=LSM_OK ) goto push_hierarchy_out;
  }

  /* Determine if the indirect format should be used. */
  bIndirect = (nKey*4 > lsmFsPageSize(pMW->pDb->pFS));

  /* The MergeWorker.apHier[] array contains the right-most leaf of the b-tree
  ** hierarchy, the root node, and all nodes that lie on the path between.
  ** apHier[0] is the right-most leaf and apHier[pMW->nHier-1] is the current
  ** root page.
  **
  ** This loop searches for a node with enough space to store the key on,
  ** starting with the leaf and iterating up towards the root. When the loop
  ** exits, the key may be written to apHier[iLevel].
  */
  for(iLevel=0; iLevel<=pMW->nHier; iLevel++){
    int nByte;                    /* Number of free bytes required */
    int iRight;                   /* Right hand pointer from aData[]/nData */

    if( iLevel==pMW->nHier ){
      /* Extend the array and allocate a new root page. */
      Page **aNew;
      aNew = (Page **)lsmRealloc(
          pMW->pDb->pEnv, pMW->apHier, sizeof(Page *)*(pMW->nHier+1)
      );
      if( !aNew ){
        rc = LSM_NOMEM_BKPT;
        goto push_hierarchy_out;
      }
      pMW->apHier = aNew;
    }else{
      int nFree;

      /* If the key will fit on this page, break out of the loop. */
      assert( lsmFsPageWritable(pMW->apHier[iLevel]) );
      aData = lsmFsPageData(pMW->apHier[iLevel], &nData);
      iRight = lsmGetU32(&aData[SEGMENT_POINTER_OFFSET(nData)]);
      if( bIndirect ){
        nByte = 2 + 1 + lsmVarintLen32(iRight) + lsmVarintLen32(iKeyPg);
      }else{
        nByte = 2 + 1 + lsmVarintLen32(iRight) + lsmVarintLen32(nKey) + nKey;
      }
      nRec = pageGetNRec(aData, nData);
      nFree = SEGMENT_EOF(nData, nRec) - mergeWorkerPageOffset(aData, nData);
      if( nByte<=nFree ) break;

      /* Otherwise, it is full. Release it. */
      iPtr = lsmFsPageNumber(pMW->apHier[iLevel]);
      rc = lsmFsPageRelease(pMW->apHier[iLevel]);
    }

    /* Allocate a new page for apHier[iLevel]. */
    pMW->apHier[iLevel] = 0;
    if( rc==LSM_OK ){
      rc = lsmFsSortedAppend(
          pDb->pFS, pDb->pWorker, &pMW->pLevel->lhs.sep, &pMW->apHier[iLevel]
      );
    }
    if( rc!=LSM_OK ) goto push_hierarchy_out;

    aData = lsmFsPageData(pMW->apHier[iLevel], &nData);
    memset(aData, 0, nData);
    lsmPutU16(&aData[SEGMENT_FLAGS_OFFSET(nData)], SEGMENT_BTREE_FLAG);
    lsmPutU16(&aData[SEGMENT_NRECORD_OFFSET(nData)], 0);
    if( iLevel>0 ){
      iRight = lsmFsPageNumber(pMW->apHier[iLevel-1]);
      lsmPutU32(&aData[SEGMENT_POINTER_OFFSET(nData)], iRight);
    }

    if( iLevel==pMW->nHier ){
      pMW->nHier++;
      break;
    }
  }

  /* Write the key into page apHier[iLevel]. */
  aData = lsmFsPageData(pMW->apHier[iLevel], &nData);

  iOff = mergeWorkerPageOffset(aData, nData);

  nRec = pageGetNRec(aData, nData);
  lsmPutU16(&aData[SEGMENT_CELLPTR_OFFSET(nData, nRec)], iOff);
  lsmPutU16(&aData[SEGMENT_NRECORD_OFFSET(nData)], nRec+1);

  if( bIndirect ){
    aData[iOff++] = 0x00;
    iOff += lsmVarintPut32(&aData[iOff], iPtr);
    iOff += lsmVarintPut32(&aData[iOff], iKeyPg);
  }else{
    aData[iOff++] = (u8)(iTopic | SORTED_SEPARATOR);
    iOff += lsmVarintPut32(&aData[iOff], iPtr);
    iOff += lsmVarintPut32(&aData[iOff], nKey);
    memcpy(&aData[iOff], pKey, nKey);
  }

  if( iLevel>0 ){
    int iRight = lsmFsPageNumber(pMW->apHier[iLevel-1]);
    lsmPutU32(&aData[SEGMENT_POINTER_OFFSET(nData)], iRight);
  }

  /* Write the right-hand pointer of the right-most leaf page of the 
  ** b-tree heirarchy. */
  aData = lsmFsPageData(pMW->apHier[0], &nData);
  lsmPutU32(&aData[SEGMENT_POINTER_OFFSET(nData)], iKeyPg);

  /* Ensure that the SortedRun.iRoot field is correct. */
  pMW->pLevel->lhs.sep.iRoot = lsmFsPageNumber(pMW->apHier[pMW->nHier-1]);

push_hierarchy_out:
  return rc;
}

/*
** The merge-worker object passed as the first argument to this function
** was used for an in-memory tree flush. If one was required, the separators 
** array has been assembled in-memory (as a "phantom"). In this case it
** consists of leaf nodes only, there are no b-tree nodes. This function 
** materializes the phantom run (writes it into the db file) and appends
** any required b-tree nodes.
*/
static int mergeWorkerBuildHierarchy(MergeWorker *pMW){
  int rc = LSM_OK;

  assert( pMW->bFlush );
  assert( pMW->pLevel->lhs.sep.iRoot==0 );

  if( pMW->apPage[1] ){
    SortedRun *pRun;              /* Separators run to materialize */
    lsm_db *db = pMW->pDb;
    Blob blob = {0, 0, 0};
    Page *pPg;
    int iLast;

    /* Write the leaf pages into the file. They now have page numbers,
    ** which can be used as pointers in the b-tree hierarchy.  */
    pRun = &pMW->pLevel->lhs.sep;
    rc = lsmFsPhantomMaterialize(db->pFS, db->pWorker, pRun);

    if( rc==LSM_OK ){
      rc = lsmFsDbPageEnd(db->pFS, pRun, 0, &pPg);
    }

    iLast = pRun->iLast;
    while( rc==LSM_OK && lsmFsPageNumber(pPg)!=iLast ){
      Page *pNext = 0;

      rc = lsmFsDbPageNext(pRun, pPg, 1, &pNext);
      lsmFsPageRelease(pPg);
      pPg = pNext;

      if( rc==LSM_OK ){
        u8 *aData;
        int nData;
        aData = lsmFsPageData(pPg, &nData);
        if( pageGetNRec(aData, nData)>0 ){
          u8 *pKey;
          int nKey;
          int iTopic;
          Pgno iPg = lsmFsPageNumber(pPg);

          pKey = pageGetKey(pPg, 0, &iTopic, &nKey, &blob);
          rc = mergeWorkerPushHierarchy(pMW, iPg, iTopic, pKey, nKey);
        }
      }
    }

    if( pMW->nHier>0 ){
      Page *pRoot = pMW->apHier[pMW->nHier-1];
      lsmFsSortedSetRoot(pRun, lsmFsPageNumber(pRoot));
    }else{
      Pgno iRoot = lsmFsFirstPgno(pRun);
      lsmFsSortedSetRoot(pRun, iRoot);
    }

    lsmFsPageRelease(pPg);
    sortedBlobFree(&blob);
  }
  return rc;
}

static int keyszToSkip(FileSystem *pFS, int nKey){
  int nPgsz;                /* Nominal database page size */
  nPgsz = lsmFsPageSize(pFS);
  return MIN(((nKey * 4) / nPgsz), 3);
}

/*
** Advance to the next page of an output run being populated by merge-worker
** pMW. If bSep is true, the separators run output is advanced by one page.
** Otherwise, the main run.
**
** The footer of the new page is initialized to indicate that it contains
** zero records. The flags field is cleared. The page footer pointer field
** is set to iFPtr.
**
** If successful, LSM_OK is returned. Otherwise, an error code.
*/
static int mergeWorkerNextPage(
  MergeWorker *pMW,               /* Merge worker object to append page to */
  int bSep,                       /* True to append to the separators array */
  int iFPtr                       /* Pointer value for footer of new page */
){
  int rc = LSM_OK;                /* Return code */
  Page *pNext = 0;                /* New page appended to run */
  lsm_db *pDb = pMW->pDb;         /* Database handle */
  SortedRun *pRun;                /* Run to append to */

  assert( bSep==0 || bSep==1 );

  pRun = (bSep ? &pMW->pLevel->lhs.sep : &pMW->pLevel->lhs.run);
  rc = lsmFsSortedAppend(pDb->pFS, pDb->pWorker, pRun, &pNext);

  if( rc==LSM_OK ){
    u8 *aData;                    /* Data buffer belonging to page pNext */
    int nData;                    /* Size of aData[] in bytes */

    lsmFsPageRelease(pMW->apPage[bSep]);
    pMW->apPage[bSep] = pNext;
    pMW->pLevel->pMerge->aiOutputOff[bSep] = 0;

    aData = lsmFsPageData(pNext, &nData);
    lsmPutU16(&aData[SEGMENT_NRECORD_OFFSET(nData)], 0);
    lsmPutU16(&aData[SEGMENT_FLAGS_OFFSET(nData)], 0);
    lsmPutU32(&aData[SEGMENT_POINTER_OFFSET(nData)], iFPtr);

    if( bSep==0 ) pMW->nWork++;
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

    assert( lsmFsPageWritable(pMW->apPage[bSep]) );
   
    aData = lsmFsPageData(pMW->apPage[bSep], &nData);
    nRec = pageGetNRec(aData, nData);
    iOff = pMerge->aiOutputOff[bSep];
    nCopy = MIN(nRem, SEGMENT_EOF(nData, nRec) - iOff);

    memcpy(&aData[iOff], &aWrite[nWrite-nRem], nCopy);
    nRem -= nCopy;

    if( nRem>0 ){
      rc = mergeWorkerNextPage(pMW, bSep, iFPtr);
    }else{
      pMerge->aiOutputOff[bSep] = iOff + nCopy;
    }
  }

  return rc;
}


static int mergeWorkerWrite(
  MergeWorker *pMW,               /* Merge worker object to write into */
  int bSep,                       /* True to write to separators array */
  int eType,                      /* One of SORTED_SEPARATOR, WRITE or DELETE */
  void *pKey, int nKey,           /* Key value */
  void *pVal, int nVal,           /* Accompanying value, if any */
  int iPtr,                       /* Absolute value of page pointer, or 0 */
  int *piPtrOut                   /* OUT: Pointer to write to separators */
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
  SortedRun *pRun;                /* Run being written to */
  int flags = 0;                  /* If != 0, flags value for page footer */

  assert( bSep==0 || bSep==1 );
  assert( bSep==0 || rtIsSeparator(eType) );

  pMerge = pMW->pLevel->pMerge;    
  pRun = (bSep ? &pMW->pLevel->lhs.sep : &pMW->pLevel->lhs.run);

  pPg = pMW->apPage[bSep];
  aData = lsmFsPageData(pPg, &nData);
  nRec = pageGetNRec(aData, nData);
  iFPtr = pageGetPtr(aData, nData);

  /* If iPtr is 0, set it to the same value as the absolute pointer 
  ** stored as part of the previous record.  */
  if( iPtr==0 ){
    iPtr = iFPtr;
    if( nRec ) iPtr += pageGetRecordPtr(aData, nData, nRec-1);
  }

  /* Calculate the relative pointer value to write to this record */
  iRPtr = iPtr - iFPtr;
  /* assert( iRPtr>=0 ); */
     
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
  **     4) Value size - 1 varint (SORTED_WRITE only)
  */
  nHdr = 1 + lsmVarintLen32(iRPtr) + lsmVarintLen32(nKey);
  if( rtIsWrite(eType) ) nHdr += lsmVarintLen32(nVal);

  /* If the entire header will not fit on page pPg, or if page pPg is 
  ** marked read-only, advance to the next page of the output run. */
  iOff = pMerge->aiOutputOff[bSep];
  if( iOff<0 || iOff+nHdr > SEGMENT_EOF(nData, nRec+1) ){
    iFPtr = iFPtr + (nRec ? pageGetRecordPtr(aData, nData, nRec-1) : 0);
    iRPtr = iPtr - iFPtr;
    iOff = 0;
    nRec = 0;
    rc = mergeWorkerNextPage(pMW, bSep, iFPtr);
    pPg = pMW->apPage[bSep];
    aData = lsmFsPageData(pPg, &nData);
  }

  /* If this record header will be the first on the page, and the page is 
  ** not the very first in the entire run, special actions may need to be 
  ** taken:
  **
  **   * If currently writing the main run, *piPtrOut should be set to
  **     the current page number. The caller will add a key to the separators
  **     array that points to the current page.
  **
  **   * If currently writing the separators array, push a copy of the key
  **     into the b-tree hierarchy.
  */
  if( rc==LSM_OK && nRec==0 && pRun->iFirst!=pRun->iLast ){
    assert( pMerge->nSkip>=0 );

    if( bSep ){
      if( pMW->bFlush==0 ){
        Pgno iPg = lsmFsPageNumber(pPg);
        rc = mergeWorkerPushHierarchy(pMW, iPg, rtTopic(eType), pKey, nKey);
      }
    }else{
      if( pMerge->nSkip ){
        pMerge->nSkip--;
        flags = PGFTR_SKIP_THIS_FLAG;
      }else{
        *piPtrOut = lsmFsPageNumber(pPg);
        pMerge->nSkip = keyszToSkip(pMW->pDb->pFS, nKey);
      }
      if( pMerge->nSkip ) flags |= PGFTR_SKIP_NEXT_FLAG;
    }
  }

  /* Update the output segment */
  if( rc==LSM_OK ){
    /* Update the page footer. */
    lsmPutU16(&aData[SEGMENT_NRECORD_OFFSET(nData)], nRec+1);
    lsmPutU16(&aData[SEGMENT_CELLPTR_OFFSET(nData, nRec)], iOff);
    if( flags ) lsmPutU16(&aData[SEGMENT_FLAGS_OFFSET(nData)], flags);

    /* Write the entry header into the current page. */
    aData[iOff++] = eType;                                               /* 1 */
    iOff += lsmVarintPut32(&aData[iOff], iRPtr);                         /* 2 */
    iOff += lsmVarintPut32(&aData[iOff], nKey);                          /* 3 */
    if( rtIsWrite(eType) ) iOff += lsmVarintPut32(&aData[iOff], nVal);   /* 4 */
    pMerge->aiOutputOff[bSep] = iOff;

    /* Write the key and data into the segment. */
    assert( iFPtr==pageGetPtr(aData, nData) );
    rc = mergeWorkerData(pMW, bSep, iFPtr+iRPtr, pKey, nKey);
    if( rc==LSM_OK && rtIsWrite(eType) ){
      rc = mergeWorkerData(pMW, bSep, iFPtr+iRPtr, pVal, nVal);
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
static void mergeWorkerShutdown(MergeWorker *pMW){
  int i;                          /* Iterator variable */
  MultiCursor *pCsr = pMW->pCsr;

  /* Unless the merge has finished, save the cursor position in the
  ** Merge.aInput[] array. See function mergeWorkerInit() for the 
  ** code to restore a cursor position based on aInput[].  */
  if( pCsr ){
    Merge *pMerge = pMW->pLevel->pMerge;

    /* pMerge->nInput==0 indicates that this is a FlushTree() operation. */
    assert( pMerge->nInput==0 || pMW->pLevel->nRight>0 );
    assert( pMerge->nInput==0 || pMerge->nInput==pCsr->nSegCsr );

    for(i=0; i<pMerge->nInput; i++){
      SegmentPtr *pPtr = &pCsr->aSegCsr[i].aPtr[0];
      if( pPtr->pPg ){
        pMerge->aInput[i].iPg = lsmFsPageNumber(pPtr->pPg);
        pMerge->aInput[i].iCell = pPtr->iCell;
      }else{
        pMerge->aInput[i].iPg = 0;
        pMerge->aInput[i].iCell = 0;
      }
    }
  }

  lsmMCursorClose(pCsr);
  lsmFsPageRelease(pMW->apPage[0]);
  lsmFsPageRelease(pMW->apPage[1]);
  for(i=0; i<pMW->nHier; i++){
    lsmFsPageRelease(pMW->apHier[i]);
  }
  lsmFree(pMW->pDb->pEnv, pMW->apHier);

  pMW->pCsr = 0;
  pMW->apHier = 0;
  pMW->nHier = 0;
  pMW->apPage[0] = 0;
  pMW->apPage[1] = 0;
}

static int mergeWorkerFirstPage(MergeWorker *pMW){
  int rc;                         /* Return code */
  SortedRun *pRun;                /* Run containing sep. keys to merge in */
  Page *pPg = 0;                  /* First page of run pRun */

  assert( pMW->apPage[0]==0 );

  pRun = pMW->pCsr->aSegCsr[pMW->pCsr->nSegCsr-1].aPtr[0].pRun;
  rc = lsmFsDbPageGet(pMW->pDb->pFS, pRun, pRun->iFirst, &pPg);
  if( rc==LSM_OK ){
    u8 *aData;                    /* Buffer for page pPg */
    int nData;                    /* Size of aData[] in bytes */
    int iFPtr;                    /* Pointer value read from footer of pPg */
    aData = lsmFsPageData(pPg, &nData);
    iFPtr = pageGetPtr(aData, nData);
    lsmFsPageRelease(pPg);
    rc = mergeWorkerNextPage(pMW, 0, iFPtr);
  }

  return rc;
}

static int mergeWorkerStep(MergeWorker *pMW){
  lsm_db *pDb = pMW->pDb;       /* Database handle */
  MultiCursor *pCsr;            /* Cursor to read input data from */
  int rc = LSM_OK;              /* Return code */
  int eType;                    /* SORTED_SEPARATOR, WRITE or DELETE */
  void *pKey; int nKey;         /* Key */
  void *pVal; int nVal;         /* Value */
  Segment *pSeg;                /* Output segment */
  int iPtr = 0;

  pCsr = pMW->pCsr;
  pSeg = &pMW->pLevel->lhs;

  /* Pull the next record out of the source cursor. */
  lsmMCursorKey(pCsr, &pKey, &nKey);
  lsmMCursorValue(pCsr, &pVal, &nVal);
  eType = pCsr->eType;

  /* Figure out if the output record may have a different pointer value
  ** than the previous. This is the case if the current key is identical to
  ** a key that appears in the lowest level run being merged. If so, set 
  ** iPtr to the absolute pointer value. If not, leave iPtr set to zero, 
  ** indicating that the output pointer value should be a copy of the pointer 
  ** value written with the previous key.  */
  if( pCsr->nSegCsr ){
    LevelCursor *pPtrs = &pCsr->aSegCsr[pCsr->nSegCsr-1];
    if( segmentCursorValid(pPtrs)
     && 0==pDb->xCmp(pPtrs->aPtr[0].pKey, pPtrs->aPtr[0].nKey, pKey, nKey)
    ){
      iPtr = pPtrs->aPtr[0].iPtr+pPtrs->aPtr[0].iPgPtr;
    }
  }

  /* If this is a separator key and we know that the output pointer has not
  ** changed, there is no point in writing an output record. Otherwise,
  ** proceed. */
  if( rtIsSeparator(eType)==0 || iPtr!=0 ){
    int iSPtr = 0;                /* Separators require a pointer here */

    if( pMW->apPage[0]==0 ){
      rc = mergeWorkerFirstPage(pMW);
    }

    /* Write the record into the main run. */
    if( rc==LSM_OK ){
      rc = mergeWorkerWrite(pMW, 0, eType, pKey, nKey, pVal, nVal, iPtr,&iSPtr);
    }

    /* If the separators array has not been started, start it now. */
    if( rc==LSM_OK && pMW->apPage[1]==0 ){
      assert( iSPtr==0 );
      assert( pSeg->run.iFirst!=0 );
      rc = mergeWorkerNextPage(pMW, 1, pSeg->run.iFirst);
      pSeg->sep.iRoot = pSeg->sep.iFirst;
    }

    /* If the call to mergeWorkerWrite() above started a new page, then
    ** add a SORTED_SEPARATOR key to the separators run.  */
    if( rc==LSM_OK && iSPtr ){
      int eSType;                 /* Type of record for separators array */

      assert( pMW->apPage[1] && (pSeg->sep.iFirst || pMW->bFlush) );

      /* Figure out how many (if any) keys to skip from this point. */
      pMW->pLevel->pMerge->nSkip = keyszToSkip(pDb->pFS, nKey);

      /* Write the key into the separators array. */
      eSType = rtTopic(eType) | SORTED_SEPARATOR;
      rc = mergeWorkerWrite(pMW, 1, eSType, pKey, nKey, 0, 0, iSPtr, 0);
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
    if( pSeg->run.iFirst ){
      rc = lsmFsSortedFinish(pDb->pFS, pDb->pWorker, &pSeg->run);
    }
    if( rc==LSM_OK && pMW->bFlush ){
      rc = mergeWorkerBuildHierarchy(pMW);
    }
    if( rc==LSM_OK && pSeg->sep.iFirst ){
      rc = lsmFsSortedFinish(pDb->pFS, pDb->pWorker, &pSeg->sep);
    }
    mergeWorkerShutdown(pMW);
  }
  return rc;
}

static int mergeWorkerDone(MergeWorker *pMW){
  return pMW->pCsr==0 || !lsmMCursorValid(pMW->pCsr);
}

static void sortedFreeLevel(lsm_env *pEnv, Level *p){
  if( p ){
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

/*
** Flush the contents of the in-memory tree to a new segment on disk.
** At present, this may occur in two scenarios:
**
**   1. When a transaction has just been committed (by connection pDb), 
**      and the in-memory tree has exceeded the size threshold, or
**
**   2. If the in-memory tree is not empty and the last connection to
**      the database (pDb) is being closed.
**
** In both cases, the connection hold a worker snapshot reference. In
** the first, the connection also holds the in-memory tree write-version.
** In the second, no in-memory tree version reference is held at all.
*/
int lsmSortedFlushTree(
  lsm_db *pDb,                    /* Connection handle */
  int *pnHdrLevel                 /* OUT: Number of levels not stored in LSM */
){
  int rc = LSM_OK;                /* Return Code */
  MultiCursor *pCsr = 0;
  Level *pNext = 0;               /* The current top level */
  Level *pNew;                    /* The new level itself */
  SortedRun *pDel = 0;
  int iLeftPtr = 0;

  assert( pDb->pWorker );
  assert( pDb->pTV==0 || lsmTreeIsWriteVersion(pDb->pTV) );

  lsmDatabaseDirty(pDb->pDatabase);    /* ??? */

  rc = lsmBeginFlush(pDb);

  /* If there is nothing to do, return early. */
  if( lsmTreeSize(pDb->pTV)==0 && lsmDatabaseIsDirty(pDb->pDatabase)==0 ){
    lsmFinishFlush(pDb, 0);
    return LSM_OK;
  }

  /* Allocate the new level structure to write to. */
  pNew = (Level *)lsmMallocZeroRc(pDb->pEnv, sizeof(Level), &rc);

  /* Create a cursor to gather the data required by the new segment. The new
  ** segment contains everything in the tree and pointers to the next segment
  ** in the database (if any).  */
  if( rc==LSM_OK ){
    pNext = lsmDbSnapshotLevel(pDb->pWorker);

    pNew->pNext = pNext;
    lsmDbSnapshotSetLevel(pDb->pWorker, pNew);

    rc = multiCursorNew(pDb, pDb->pWorker, 1, 0, &pCsr);
    if( pNext ){
      assert( pNext->pMerge==0 || pNext->nRight>0 );
      if( pNext->pMerge==0 ){
        if( segmentHasSeparators(&pNext->lhs) ){
          multiCursorAddLevel(pCsr, pNext, MULTICURSOR_ADDLEVEL_LHS_SEP);
          pDel = &pNext->lhs.sep;

          /* This call moves any blocks occupied by separators array pDel to
          ** the pending list. We do this here, even though pDel will be read
          ** while building the new level, so that the blocks will be included
          ** in the "FREELIST" entry visited by the cursor (and written into
          ** the new top level). */
          rc = lsmFsSortedDelete(pDb->pFS, pDb->pWorker, 0, pDel);
        }
        iLeftPtr = pNext->lhs.run.iFirst;
      }
    }else{
      /* The new level will be the only level in the LSM. There is no reason
      ** to write out delete keys in this case.  */
      multiCursorIgnoreDelete(pCsr);
    }
    multiCursorVisitFreelist(pCsr);
    multiCursorReadSeparators(pCsr);
    pCsr->pnHdrLevel = pnHdrLevel;
    lsmFreelistDeltaBegin(pDb);
  }

  if( rc!=LSM_OK ){
    lsmMCursorClose(pCsr);
  }else{
    Merge merge;                  /* Merge object used to create new level */
    MergeWorker mergeworker;      /* MergeWorker object for the same purpose */

    memset(&merge, 0, sizeof(Merge));
    memset(&mergeworker, 0, sizeof(MergeWorker));

    pNew->pMerge = &merge;
    mergeworker.pDb = pDb;
    mergeworker.pLevel = pNew;
    mergeworker.pCsr = pCsr;

    /* Mark the separators array for the new level as a "phantom". */
    mergeworker.bFlush = 1;
    lsmFsSortedPhantom(pDb->pFS, &pNew->lhs.sep);

    /* Allocate the first page of the output segment. */
    rc = mergeWorkerNextPage(&mergeworker, 0, iLeftPtr);

    /* Do the work to create the new merged segment on disk */
    if( rc==LSM_OK ) rc = lsmMCursorFirst(pCsr);
    while( rc==LSM_OK && mergeWorkerDone(&mergeworker)==0 ){
      rc = mergeWorkerStep(&mergeworker);
    }

    lsmFsSortedPhantomFree(pDb->pFS);
    mergeWorkerShutdown(&mergeworker);
    pNew->pMerge = 0;
  }

  /* Link the new level into the top of the tree. Delete the separators
  ** array (if any) that was merged into the new level. */
  if( rc==LSM_OK ){
    if( pDel ){
      /* lsmFsSortedDelete() has already been called on pDel. So all
      ** that is required here is to zero it (so that it is not used by
      ** future LSM searches). */
      memset(pDel, 0, sizeof(SortedRun));
    }
  }else{
    lsmDbSnapshotSetLevel(pDb->pWorker, pNext);
    sortedFreeLevel(pDb->pEnv, pNew);
  }

  if( rc==LSM_OK ){
    sortedInvokeWorkHook(pDb);
  }
#if 0
  lsmSortedDumpStructure(pDb, pDb->pWorker, 0, 0, "tree flush");
#endif

  assertAllBtreesOk(rc, pDb);
  assertAllPointersOk(rc, pDb);
  assert( rc!=LSM_OK || lsmFsIntegrityCheck(pDb) );

  lsmFreelistDeltaEnd(pDb);
  lsmFinishFlush(pDb, rc==LSM_OK);
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
    for(i=0; i<nMerge; i++){
      pNext = p->pNext;
      pNew->aRhs[i] = p->lhs;
      lsmFree(pDb->pEnv, p);
      p = pNext;
    }

    /* Replace the old levels with the new. */
    pTopLevel = lsmDbSnapshotLevel(pDb->pWorker);
    pNew->pNext = p;
    for(pp=&pTopLevel; *pp!=pLevel; pp=&((*pp)->pNext));
    *pp = pNew;
    lsmDbSnapshotSetLevel(pDb->pWorker, pTopLevel);

    /* Determine whether or not the next separators will be linked in */
    if( pNext && pNext->pMerge==0 && segmentHasSeparators(&pNext->lhs) ){
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

static int mergeWorkerLoadOutputPage(MergeWorker *pMW, int bSep){
  int rc = LSM_OK;                /* Return code */
  SortedRun *pRun;                /* Run to load page from */
  Level *pLevel;

  pLevel = pMW->pLevel;
  pRun = (bSep ? &pLevel->lhs.sep : &pLevel->lhs.run);
  if( pRun->iLast ){
    Page *pPg;
    rc = lsmFsDbPageGet(pMW->pDb->pFS, pRun, pRun->iLast, &pPg);

    while( rc==LSM_OK ){
      Page *pNext;
      u8 *aData;
      int nData;
      aData = lsmFsPageData(pPg, &nData);
      if( (pageGetFlags(aData, nData) & SEGMENT_BTREE_FLAG)==0 ) break;
      rc = lsmFsDbPageNext(pRun, pPg, -1, &pNext);
      lsmFsPageRelease(pPg);
      pPg = pNext;
    }

    if( rc==LSM_OK ){
      pMW->apPage[bSep] = pPg;
      if( pLevel->pMerge->aiOutputOff[bSep]>=0 ) rc = lsmFsPageWrite(pPg);
    }
  }
  return rc;
}

static int mergeWorkerInit(
  lsm_db *pDb,                    /* Db connection to do merge work */
  Level *pLevel,                  /* Level to work on merging */
  MergeWorker *pMW                /* Object to initialize */
){
  int rc;                         /* Return code */
  Merge *pMerge = pLevel->pMerge; /* Persistent part of merge state */
  MultiCursor *pCsr = 0;          /* Cursor opened for pMW */

  assert( pDb->pWorker );
  assert( pLevel->pMerge );
  assert( pLevel->nRight>0 );

  memset(pMW, 0, sizeof(MergeWorker));
  pMW->pDb = pDb;
  pMW->pLevel = pLevel;

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
  rc = multiCursorNew(pDb, pDb->pWorker, 0, 0, &pCsr);
  if( rc==LSM_OK ){
    rc = multiCursorAddLevel(pCsr, pLevel, MULTICURSOR_ADDLEVEL_RHS);
  }
  if( rc==LSM_OK && pLevel->pNext ){
    if( pMerge->nInput > pLevel->nRight ){
      Level *pNext = pLevel->pNext;
      rc = multiCursorAddLevel(pCsr, pNext, MULTICURSOR_ADDLEVEL_LHS_SEP);
    }
    multiCursorReadSeparators(pCsr);
  }else{
    multiCursorIgnoreDelete(pCsr);
  }
  assert( pMerge->nInput==pCsr->nSegCsr );
  pMW->pCsr = pCsr;

  /* Load each of the output pages into memory. */
  if( rc==LSM_OK ) rc = mergeWorkerLoadOutputPage(pMW, 0);
  if( rc==LSM_OK ) rc = mergeWorkerLoadOutputPage(pMW, 1);

  /* Position the cursor. */
  if( rc==LSM_OK ){
    if( pMW->apPage[0]==0 ){
      /* The output array is still empty. So position the cursor at the very 
      ** start of the input.  */
      rc = multiCursorEnd(pCsr, 0);
    }else{
      /* The output array is non-empty. Position the cursor based on the
      ** page/cell data saved in the Merge.aInput[] array.  */
      int i;
      for(i=0; rc==LSM_OK && i<pCsr->nSegCsr; i++){
        MergeInput *pInput = &pMerge->aInput[i];
        if( pInput->iPg ){
          SegmentPtr *pPtr;
          assert( pCsr->aSegCsr[i].nPtr==1 );
          assert( pCsr->aSegCsr[i].aPtr[0].pPg==0 );
          pPtr = &pCsr->aSegCsr[i].aPtr[0];
          rc = segmentPtrLoadPage(pDb->pFS, pPtr, pInput->iPg);
          if( rc==LSM_OK && pPtr->nCell>0 ){
            rc = segmentPtrLoadCell(pPtr, pInput->iCell);
          }
        }
      }

      if( rc==LSM_OK ){
        rc = multiCursorSetupTree(pCsr, 0);
      }
    }
  }

  return rc;
}


int sortedWork(lsm_db *pDb, int nWork, int bOptimize, int *pnWrite){
  int rc = LSM_OK;                /* Return Code */
  int nRemaining = nWork;         /* Units of work to do before returning */
  const int eCola = pDb->eCola;   /* Max adjacent segment size ratio */
  Snapshot *pWorker = pDb->pWorker;

  assert( eCola>1 );
  assert( lsmFsIntegrityCheck(pDb) );
  assert( pWorker );
  assertAllPointersOk(rc, pDb);

  if( lsmDbSnapshotLevel(pWorker)==0 ) return LSM_OK;
  lsmDatabaseDirty(pDb->pDatabase);

  while( nRemaining>0 ){
    Level *pLevel;
    const int nTopMerge = 4;
    int i;
    int nMxPage;
    Level *pTopLevel = lsmDbSnapshotLevel(pWorker);

    /* Check if we should start a new merge on the top nTopMerge levels. */
    pLevel = pTopLevel;
    nMxPage = pDb->nTreeLimit / (lsmFsPageSize(pDb->pFS));
    for(i=0; i<nTopMerge; i++){
      if( pLevel==0 
       || pLevel->pMerge 
       || pLevel->lhs.run.nSize>nMxPage 
      ){
        break;
      }
      pLevel = pLevel->pNext;
    }

    if( i==nTopMerge ){
      /* Merge the first nTopMerge levels. */
      rc = sortedMergeSetup(pDb, pTopLevel, nTopMerge, &pLevel);
    }else{
      Level *pSearch = pLevel;

      /* Not starting a new merge. Find an existing merge to work on. */
      while( pLevel && pLevel->pMerge==0 ){
        pLevel = pLevel->pNext;
      }

      /* If no in-progress merge was found, merge together any two adjacent
      ** levels for which level N is larger than level N+1. */
      if( pLevel==0 ){
        pLevel = pSearch;
        while( pLevel && pLevel->pNext ){
          int sz1 = pLevel->lhs.run.nSize;
          int sz2 = pLevel->pNext->lhs.run.nSize;
          if( (sz1*1.1) >= sz2 ){
            rc = sortedMergeSetup(pDb, pLevel, 2, &pLevel);
            break;
          }
          pLevel = pLevel->pNext;
        }
        if( pLevel && !pLevel->pMerge ) pLevel = 0;
      }
    }

    if( pLevel==0 && bOptimize && pTopLevel->pNext ){
      rc = sortedMergeSetup(pDb, pTopLevel, 2, &pLevel);
    }

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
      nRemaining -= mergeworker.nWork;

      /* Check if the merge operation is completely finished. If so, the
      ** Merge object and the right-hand-side of the level can be deleted. 
      **
      ** Otherwise, gobble up (declare eligible for recycling) any pages
      ** from rhs segments for which the content has been completely merged
      ** into the lhs of the level.
      */
      if( rc==LSM_OK ){
        if( mergeWorkerDone(&mergeworker)==0 ){
          int iGobble = mergeworker.pCsr->aTree[1] - CURSOR_DATA_SEGMENT;
          if( iGobble<pLevel->nRight ){
            SegmentPtr *pGobble = &mergeworker.pCsr->aSegCsr[iGobble].aPtr[0];
            if( (pGobble->flags & PGFTR_SKIP_THIS_FLAG)==0 ){
              lsmFsGobble(pWorker, pGobble->pRun, pGobble->pPg);
            }
          }

        }else if( pLevel->lhs.run.iFirst==0 ){
          /* If the new level is completely empty, remove it from the 
          ** database snapshot. This can only happen if all input keys were
          ** annihilated. Since keys are only annihilated if the new level
          ** is the last in the linked list (contains the most ancient of
          ** database content), this guarantees that pLevel->pNext==0.  */ 

          Level *pTop;          /* Top level of worker snapshot */
          Level **pp;           /* Read/write iterator for Level.pNext list */
          assert( pLevel->pNext==0 );
          assert( segmentHasSeparators(&pLevel->lhs)==0 );

          /* Remove the level from the worker snapshot. */
          pTop = lsmDbSnapshotLevel(pWorker);
          for(pp=&pTop; *pp!=pLevel; pp=&((*pp)->pNext));
          *pp = pLevel->pNext;
          lsmDbSnapshotSetLevel(pWorker, pTop);

          /* Free the Level structure. */
          lsmFsSortedDelete(pDb->pFS, pWorker, 1, &pLevel->lhs.run);
          sortedFreeLevel(pDb->pEnv, pLevel);
        }else{
          int i;

          /* Free the separators of the next level, if required. */
          if( pLevel->pMerge->nInput > pLevel->nRight ){
            assert( pLevel->pNext );
            assert( segmentHasSeparators(&pLevel->pNext->lhs) );
            lsmFsSortedDelete(pDb->pFS, pWorker, 1, &pLevel->pNext->lhs.sep);
          }

          /* Free the right-hand-side of pLevel */
          for(i=0; i<pLevel->nRight; i++){
            lsmFsSortedDelete(pDb->pFS, pWorker, 1, &pLevel->aRhs[i].run);
            lsmFsSortedDelete(pDb->pFS, pWorker, 1, &pLevel->aRhs[i].sep);
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
      mergeWorkerShutdown(&mergeworker);
      if( rc==LSM_OK ) sortedInvokeWorkHook(pDb);

#if 0
      lsmSortedDumpStructure(pDb, pDb->pWorker, 0, 0, "work");
#endif

    }
  }

  if( pnWrite ){
    *pnWrite = (nWork - nRemaining);
  }

  assertAllBtreesOk(rc, pDb);
  assertAllPointersOk(rc, pDb);
  assert( rc!=LSM_OK || lsmFsIntegrityCheck(pDb) );
  return rc;
}

typedef struct Metric Metric;
struct Metric {
  double fAvgHeight;
  int nTotalSz;
  int nMinSz;
};

#if 0
static void sortedMeasureDb(lsm_db *pDb, Metric *p){
  Level *pLevel;                  /* Used to iterate through levels */
  assert( pDb->pWorker );

  double fHeight = 0.0;
  int nTotalSz = 0;
  int nMinSz = 0;

  for(pLevel=lsmDbSnapshotLevel(pDb->pWorker); pLevel; pLevel=pLevel->pNext){
    const int nLhsSz = pLevel->lhs.run.nSize;
    int nRhsSz = 0;
    int nSz;
    int i;

    for(i=0; i<pLevel->nRight; i++){
      nRhsSz += pLevel->aRhs[i].run.nSize;
    }

    nSz = nRhsSz + nLhsSz;
    fHeight += (double)nLhsSz/nSz + (double)nRhsSz/nSz * pLevel->nRight;
    nTotalSz += nSz;

    if( nMinSz==0 || nMinSz>nSz ) nMinSz = nSz;
  }

  if( nMinSz ){
    printf("avg-height=%.2f log2(min/total)=%.2f totalsz=%d minsz=%d\n", 
        fHeight, 
        log((double)nTotalSz / nMinSz) / log(2),
        nTotalSz,
        nMinSz
    );
  }
}
#endif

/*
** This function is called once for each page worth of data is written to 
** the in-memory tree. It calls sortedWork() to do roughly enough work
** to prevent the height of the tree from growing indefinitely, even if
** there are no other calls to sortedWork().
*/
int lsmSortedAutoWork(lsm_db *pDb){
  int rc;                         /* Return code */
  const int eCola = pDb->eCola;   /* Max adjacent segment size ratio */
  int nRemaining;                 /* Units of work to do before returning */
  int nDepth;                     /* Current height of tree (longest path) */
  Level *pLevel;                  /* Used to iterate through levels */

  assert( eCola>1 );
  assert( lsmFsIntegrityCheck(pDb) );
  assert( pDb->pWorker );

  /* Determine how many units of work to do before returning. One unit of
  ** work is achieved by writing one page (~4KB) of merged data.  */
  nRemaining = nDepth = 0;
  for(pLevel=lsmDbSnapshotLevel(pDb->pWorker); pLevel; pLevel=pLevel->pNext){
    nDepth += MAX(1, pLevel->nRight); 
    /* nDepth += 1; */
    /* nDepth += pLevel->nRight; */
  }
  nRemaining = nDepth*1;

  rc = sortedWork(pDb, nRemaining, 0, 0);
  return rc;
}

/*
** Perform work to merge database segments together.
*/
int lsm_work(lsm_db *pDb, int flags, int nPage, int *pnWrite){
  int rc = LSM_OK;                /* Return code */

  /* This function may not be called if pDb has an open read or write
  ** transaction. Return LSM_MISUSE if an application attempts this.  */
  if( pDb->nTransOpen || pDb->pCsr ) return LSM_MISUSE_BKPT;

  if( (flags & LSM_WORK_FLUSH) ){
    rc = lsmBeginWriteTrans(pDb);
    if( rc==LSM_OK ){
      rc = lsmFlushToDisk(pDb);
      lsmFinishWriteTrans(pDb, 0);
      lsmFinishReadTrans(pDb);
    }
  }

  /* If the LSM_WORK_CHECKPOINT flag is specified and one is available,
  ** write a checkpoint out to disk.  */
  if( rc==LSM_OK && (flags & LSM_WORK_CHECKPOINT) ){
    rc = lsmCheckpointWrite(pDb);
  }

  if( rc==LSM_OK && nPage>0 ){
    int bOptimize = ((flags & LSM_WORK_OPTIMIZE) ? 1 : 0);
    pDb->pWorker = lsmDbSnapshotWorker(pDb->pDatabase);
    rc = sortedWork(pDb, nPage, bOptimize, pnWrite);
    lsmDbSnapshotRelease(pDb->pWorker);
    pDb->pWorker = 0;
  }else if( pnWrite ){
    *pnWrite = 0;
  }

  return rc;
}

/*
** Return a string representation of the segment passed as the only argument.
** Space for the returned string is allocated using lsmMalloc(), and should
** be freed by the caller using lsmFree().
*/
static char *segToString(lsm_env *pEnv, SortedRun *pRun, int nMin){
  int nSize = lsmFsSortedSize(pRun);
  Pgno iRoot = lsmFsSortedRoot(pRun);
  Pgno iFirst = lsmFsFirstPgno(pRun);
  Pgno iLast = lsmFsLastPgno(pRun);
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
  nPad = MAX(0, nPad);

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
  SortedRun *pRun
){
  int i = 0;
  char *zSeg;

  zSeg = segToString(pEnv, pRun, nMin);
  i += snprintf(&aBuf[i], nBuf-i, "%s", zSeg);
  lsmFree(pEnv, zSeg);

  return i;
}

void sortedDumpPage(lsm_db *pDb, SortedRun *pRun, Page *pPg, int bVals){
  Blob blob = {0, 0, 0};         /* Blob used for keys */
  LsmString s;
  int i;

  int nRec;
  int iPtr;
  int flags;
  u8 *aData;
  int nData;

  aData = lsmFsPageData(pPg, &nData);

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
      aCell += lsmVarintGet32(aCell, &iRef);
      lsmFsDbPageGet(pDb->pFS, pRun, iRef, &pRef);
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

  aData = lsmFsPageData(pPg, &nData);

  aCell = pageGetCell(aData, nData, iCell);
  eType = *aCell++;
  aCell += lsmVarintGet32(aCell, &iPgPtr);

  if( eType==0 ){
    int dummy;
    Pgno iRef;                  /* Page number of referenced page */
    aCell += lsmVarintGet32(aCell, &iRef);
    lsmFsDbPageGet(pDb->pFS, 0, iRef, &pRef);
    pageGetKeyCopy(pDb->pEnv, pRef, 0, &dummy, pBlob);
    aKey = (u8 *)pBlob->pData;
    nKey = pBlob->nData;
    lsmFsPageRelease(pRef);
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



int lsmInfoPageDump(lsm_db *pDb, Pgno iPg, char **pzOut){
  int rc = LSM_OK;                /* Return code */
  Snapshot *pWorker;              /* Worker snapshot */
  Snapshot *pRelease = 0;         /* Snapshot to release */
  Page *pPg = 0;                  /* Handle for page iPg */

  *pzOut = 0;
  if( iPg==0 ) return LSM_ERROR;

  /* Obtain the worker snapshot */
  pWorker = pDb->pWorker;
  if( !pWorker ){
    pRelease = pWorker = lsmDbSnapshotWorker(pDb->pDatabase);
  }

  rc = lsmFsDbPageGet(pDb->pFS, 0, iPg, &pPg);
  if( rc==LSM_OK ){
    Blob blob = {0, 0, 0, 0};
    int nKeyWidth = 0;
    LsmString str;
    int nRec;
    int iPtr;
    int flags;
    int iCell;
    u8 *aData; int nData;         /* Page data and size thereof */

    aData = lsmFsPageData(pPg, &nData);
    nRec = pageGetNRec(aData, nData);
    iPtr = pageGetPtr(aData, nData);
    flags = pageGetFlags(aData, nData);

    lsmStringInit(&str, pDb->pEnv);
    lsmStringAppendf(&str, "Page : %d\n", iPg);
    lsmStringAppendf(&str, "nRec : %d\n", nRec);
    lsmStringAppendf(&str, "iPtr : %d\n", iPtr);
    lsmStringAppendf(&str, "flags: %04x\n", flags);
    lsmStringAppendf(&str, "\n");

    for(iCell=0; iCell<nRec; iCell++){
      int nKey;
      infoCellDump(pDb, pPg, iCell, 0, 0, 0, &nKey, 0, 0, &blob);
      if( nKey>nKeyWidth ) nKeyWidth = nKey;
    }

    for(iCell=0; iCell<nRec; iCell++){
      u8 *aKey; int nKey = 0;       /* Key */
      u8 *aVal; int nVal = 0;       /* Value */
      int iChar;
      int iPgPtr;
      int eType;
      char cType = '?';

      infoCellDump(pDb, pPg, iCell, &eType, &iPgPtr,
          &aKey, &nKey, &aVal, &nVal, &blob
      );

      if( rtIsDelete(eType) ) cType = 'D';
      if( rtIsWrite(eType) ) cType = 'W';
      if( rtIsSeparator(eType) ) cType = 'S';
      lsmStringAppendf(&str, "%c %d (%s) ", 
          cType, (iPtr+iPgPtr), (rtTopic(eType) ? "sys" : "usr")
      );
      for(iChar=0; iChar<nKey; iChar++){
        lsmStringAppendf(&str, "%c", isalnum(aKey[iChar]) ? aKey[iChar] : '.');
      }
      lsmStringAppendf(&str, "%*s", nKeyWidth - nKey, "");
      if( nVal>0 ){
        lsmStringAppendf(&str, " ");
        for(iChar=0; iChar<nVal; iChar++){
          lsmStringAppendf(&str, "%c", isalnum(aVal[iChar])?aVal[iChar]:'.');
        }
      }
      lsmStringAppendf(&str, "\n");
    }


    *pzOut = str.z;
    sortedBlobFree(&blob);
    lsmFsPageRelease(pPg);
  }

  lsmDbSnapshotRelease(pRelease);
  return rc;
}

void sortedDumpSegment(lsm_db *pDb, SortedRun *pRun, int bVals){
  assert( pDb->xLog );
  if( pRun ){
    char *zSeg;
    Page *pPg;

    zSeg = segToString(pDb->pEnv, pRun, 0);
    lsmLogMessage(pDb, LSM_OK, "Segment: %s", zSeg);
    lsmFree(pDb->pEnv, zSeg);

    lsmFsDbPageEnd(pDb->pFS, pRun, 0, &pPg);
    while( pPg ){
      Page *pNext;
      sortedDumpPage(pDb, pRun, pPg, bVals);
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

  if( pDump==0 ){
    assert( pDb->pWorker==0 );
    pDump = lsmDbSnapshotWorker(pDb->pDatabase);
  }

  pTopLevel = lsmDbSnapshotLevel(pDump);
  if( pDb->xLog && pTopLevel ){
    Level *pLevel;
    int iLevel = 0;

    lsmLogMessage(pDb, LSM_OK, "Database structure (%s)", zWhy);

    for(pLevel=pTopLevel; pLevel; pLevel=pLevel->pNext){
      char zLeft[1024];
      char zRight[1024];
      int i = 0;

      SortedRun *aLeft[24];  
      SortedRun *aRight[24];

      int nLeft = 0;
      int nRight = 0;

      Segment *pSeg = &pLevel->lhs;
      if( segmentHasSeparators(pSeg) ){
        aLeft[nLeft++] = &pSeg->sep;
      }
      aLeft[nLeft++] = &pSeg->run;

      for(i=0; i<pLevel->nRight; i++){
        if( segmentHasSeparators(&pLevel->aRhs[i]) ){
          aRight[nRight++] = &pLevel->aRhs[i].sep;
        }
        aRight[nRight++] = &pLevel->aRhs[i].run;
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
          snprintf(zLevel, sizeof(zLevel), "L%d:", iLevel);
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

#if 0
    lsmLogMessage(pDb, LSM_OK, "Block map", zWhy);
    for(pLevel=pDb->pLevel; pLevel; pLevel=pLevel->pNext){
      int iRhs;
      lsmFsDumpBlockmap(pDb, pLevel->lhs.pSep);
      lsmFsDumpBlockmap(pDb, pLevel->lhs.pRun);
      for(iRhs=0; iRhs<pLevel->nRight; iRhs++){
        lsmFsDumpBlockmap(pDb, pLevel->aRhs[iRhs].pSep);
        lsmFsDumpBlockmap(pDb, pLevel->aRhs[iRhs].pRun);
      }
    }
    lsmFsDumpBlocklists(pDb);
#endif

    if( bKeys ){
      for(pLevel=pTopLevel; pLevel; pLevel=pLevel->pNext){
        int i;
        sortedDumpSegment(pDb, &pLevel->lhs.sep, 0);
        sortedDumpSegment(pDb, &pLevel->lhs.run, bVals);
        for(i=0; i<pLevel->nRight; i++){
          if( pLevel->aRhs[i].sep.iFirst>0 ){
            sortedDumpSegment(pDb, &pLevel->aRhs[i].sep, 0);
          }
          sortedDumpSegment(pDb, &pLevel->aRhs[i].run, bVals);
        }
      }
    }
  }

  if( pSnap==0 ){
    lsmDbSnapshotRelease(pDump);
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

int lsmSortedFlushDb(lsm_db *pDb){
  int rc = LSM_OK;
  Level *p;

  assert( pDb->pWorker );
  for(p=lsmDbSnapshotLevel(pDb->pWorker); p && rc==LSM_OK; p=p->pNext){
    Merge *pMerge = p->pMerge;
    if( pMerge ){
      pMerge->aiOutputOff[0] = -1;
      pMerge->aiOutputOff[1] = -1;
      pMerge->bHierReadonly = 1;
    }
  }

  return LSM_OK;
}

void lsmSortedFixTreeVersions(lsm_db *pDb){
  lsm_cursor *pCsr;
  for(pCsr=pDb->pCsr; pCsr; pCsr=pCsr->pNext){
    lsmTreeFixVersion(pCsr->pMC->pTreeCsr, pDb->pTV);
  }
}

void lsmSortedSaveTreeCursors(lsm_db *pDb){
  lsm_cursor *pCsr;
  for(pCsr=pDb->pCsr; pCsr; pCsr=pCsr->pNext){
    lsmTreeCursorSave(pCsr->pMC->pTreeCsr);
  }
}


#ifdef LSM_DEBUG_EXPENSIVE

/*
** Argument iPg is a page number within a separators run. Assert() that for
** each key K on on the page, (pKey1 >= K > pKey2) is true. 
**
** Also, if page iPg is a BTREE page, call this function recursively to
** check that the keys on each child page fall into the expected range.
*/
static void assertBtreeRanges(
  lsm_db *pDb, 
  SortedRun *pRun, 
  Pgno iPg,                       /* Database page to load */
  void *pKey1, int nKey1,         /* All keys must be >= than this */
  void *pKey2, int nKey2          /* And < than this */
){
  Blob blob = {0, 0, 0};
  u8 *aData;
  int nData;
  Page *pPg;
  int rc;
  int i;
  int nRec;
  int flags;

  int iPrevTopic = 0;             /* Previous topic value */
  u8 *aPrev = 0;                  /* Buffer pointing to previous key */
  int nPrev = 0;                  /* Size of aPrev[] in bytes */

  rc = lsmFsDbPageGet(pDb->pFS, pRun, iPg, &pPg);
  assert( rc==LSM_OK );
  aData = lsmFsPageData(pPg, &nData);

  nRec = pageGetNRec(aData, nData);
  flags = pageGetFlags(aData, nData);

  for(i=0; i<nRec; i++){
    u8 *aKey;
    int nKey;
    int iTopic;
    int iPtr;

    if( flags & SEGMENT_BTREE_FLAG ){
      aKey = pageGetCell(aData, nData, i);
      aKey += lsmVarintGet32(aKey, &iPtr);
      aKey += lsmVarintGet32(aKey, &nKey);
    }else{
      aKey = pageGetKey(pPg, i, &iTopic, &nKey, &blob);
    }

    assert( pKey1==0 || pDb->xCmp(aKey, nKey, pKey1, nKey1)>=0 );
    assert( pKey2==0 || pDb->xCmp(aKey, nKey, pKey2, nKey2)<0 );

    if( flags&SEGMENT_BTREE_FLAG ){
      assertBtreeRanges(pDb, pRun, iPtr, aPrev, nPrev, aKey, nKey);
    }
    aPrev = aKey;
    nPrev = nKey;
  }

  if( flags&SEGMENT_BTREE_FLAG ){
    int iRight = pageGetPtr(aData, nData);
    assertBtreeRanges(pDb, pRun, iRight, aPrev, nPrev, 0, 0);
  }

  lsmFsPageRelease(pPg);
  sortedBlobFree(&blob);
}

/*
** Check that the array pOne contains the required pointers to pTwo.
** Array pTwo must be a main array. pOne may be either a separators array
** or another main array. 
**
** If an error is encountered, *pzErr is set to point to a buffer containing
** a nul-terminated error message and this function returns immediately. The
** caller should eventually call lsmFree(*pzErr) to free the allocated
** error message buffer.
*/
static void assertPointersOk(
  lsm_db *pDb,                    /* Database handle */
  SortedRun *pOne,                /* Run containing pointers */
  SortedRun *pTwo,                /* Run containing pointer targets */
  int bRhs,                       /* True if pTwo may have been Gobble()d */
  char **pzErr
){
  int rc = LSM_OK;                /* Error code */
  SegmentPtr ptr1;                /* Iterates through pOne */
  SegmentPtr ptr2;                /* Iterates through pTwo */
  Pgno iPrev;

  assert( pOne && pTwo );

  memset(&ptr1, 0, sizeof(ptr1));
  memset(&ptr2, 0, sizeof(ptr1));
  ptr1.pRun = pOne;
  ptr2.pRun = pTwo;
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

    if( (ptr2.flags & PGFTR_SKIP_THIS_FLAG)==0 ){

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
          iPrev = lsmFsPageNumber(ptr2.pPg);
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
}

static int countBtreeKeys(FileSystem *pFS, SortedRun *pRun, Pgno iPg){
#if 0
  int rc;
  Page *pPg;
  u8 *aData;
  int nData;
  int flags;
  int nRet;

  rc = lsmFsDbPageGet(pFs, pRun, iPg, &pPg);
  assert( rc==LSM_OK );
  aData = lsmFsPageData(pPg, &nData);
  flags = pageGetFlags(aData, nData);

  if( flags & SEGMENT_BTREE_FLAG ){
    Pgno iRight;
    int nRec;
    int i;

    iRight = pageGetPtr(aData, nData);
    nRec = pageGetNRec(aData, nData);

    nRet = nRec;
    nRet += countBtreeKeys(pFS, pRun, iRight);
    for(i=0; i<nRec; i++){
      Pgno iPtr;
      u8 *aCell = pageGetCell(aData, nData, i);
      aCell += lsmVarintGet32(aCell, &iPtr);
      if( iPtr==0 ){
        lsmVarintGet32(aCell, &iPtr);
      }
      nRet += countBtreeKeys(pFS, pRun, iPtr);
    }
  }else{
    nRet = 0;
  }

  lsmFsPageRelease(pPg);
  return nRet;
#endif
  return 0;
}

static void assertBtreeSize(FileSystem *pFS, SortedRun *pRun, Pgno iRoot){
#if 0
  int nRun = 0;
  int nKey = 0;
  int rc;

  Page *pPg;

  rc = lsmFsDbPageEnd(pFS, pRun, 0, &pPg);
  assert( rc==LSM_OK );
  while( pPg ){
    Page *pNext = 0;
    u8 *aData;
    int nData;
    int flags;
    int nRec;

    aData = lsmFsPageData(pPg, &nData);
    flags = pageGetFlags(aData, nData);
    nRec = pageGetNRec(aData, nData);

    if( (flags & SEGMENT_BTREE_FLAG)==0 && nRec ){
      nRun++;
    }

    rc = lsmFsDbPageNext(pPg, 1, &pNext);
    assert( rc==LSM_OK );
    lsmFsPageRelease(pPg);
    pPg = pNext;
  }

  nKey = countBtreeKeys(pFS, pRun, iRoot);
  assert( nRun==1+nKey );
#endif
}

static void assertAllBtreesOk(int rc, lsm_db *pDb){
#if 0
  if( rc==LSM_OK ){
    Level *p;
    for(p=pDb->pLevel; p; p=p->pNext){
      SortedRun *pSep = p->lhs.pSep;
      Pgno iRoot = lsmFsSortedRoot(pSep);
      if( pSep && iRoot ){
        assertBtreeRanges(pDb, pSep, iRoot, 0, 0, 0, 0);
        assertBtreeSize(pDb->pFS, pSep, iRoot);
      }
    }
  }
#endif
}

/*
** This function is only useful for debugging. 
*/
static void assertAllPointersOk(int rc, lsm_db *pDb){
  assert( rc!=LSM_OK || pDb->pWorker );
  if( rc==LSM_OK ){
    Level *p;
    for(p=lsmDbSnapshotLevel(pDb->pWorker); p; p=p->pNext){
      int i;

      if( segmentHasSeparators(&p->lhs) ){
        assertPointersOk(pDb, &p->lhs.sep, &p->lhs.run, 0, 0);
      }
      for(i=0; i<p->nRight; i++){
        if( segmentHasSeparators(&p->aRhs[i]) ){
          assertPointersOk(pDb, &p->aRhs[i].sep, &p->aRhs[i].run, 1, 0);
        }
      }

    }
  }
}

#endif /* ifdef LSM_DEBUG_EXPENSIVE */
