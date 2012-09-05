/*
** 2011-09-11
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
** This file contains code to read and write checkpoints.
**
** A checkpoint represents the database layout at a single point in time.
** It includes a log offset. When an existing database is opened, the
** current state is determined by reading the newest checkpoint and updating
** it with all committed transactions from the log that follow the specified
** offset.
*/
#include "lsmInt.h"

/*
** CHECKPOINT BLOB FORMAT:
**
** A checkpoint blob is a series of unsigned 32-bit integers stored in
** big-endian byte order. As follows:
**
**   Checkpoint header (see the CKPT_HDR_XXX #defines):
**
**     1. The checkpoint id MSW.
**     2. The checkpoint id LSW.
**     3. The number of integer values in the entire checkpoint, including 
**        the two checksum values.
**     4. The total number of blocks in the database.
**     5. The block size.
**     6. The number of levels.
**     7. The nominal database page size.
**     8. Flag indicating if there exists a FREELIST record in the database.
**
**   Log pointer:
**
**     4 integers (2 for a 64-bit offset and 2 for a 64-bit checksum). See 
**     ckptExportLog() and ckptImportLog().
**
**   Append points:
**
**     4 integers. See ckptExportAppendlist().
**
**   For each level in the database, a level record. Formatted as follows:
**
**     0. Age of the level.
**     1. The number of right-hand segments (nRight, possibly 0),
**     2. Segment record for left-hand segment (4 integers defined below),
**     3. Segment record for each right-hand segment (4 integers defined below),
**     4. If nRight>0, The number of segments involved in the merge
**     5. if nRight>0, Current nSkip value (see Merge structure defn.),
**     6. For each segment in the merge:
**        5a. Page number of next cell to read during merge
**        5b. Cell number of next cell to read during merge
**     7. Page containing current split-key.
**     8. Cell within page containing current split-key.
**
**   The freelist. 
**
**     1. Number of free-list entries stored in checkpoint header.
**     2. For each entry:
**        2a. Block number of free block.
**        2b. MSW of associated checkpoint id.
**        2c. LSW of associated checkpoint id.
**
**   If the overflow flag is set, then extra free-list entries may be stored
**   in the FREELIST record. The FREELIST record contains 3 32-bit integers
**   per entry, in the same format as above (without the "number of entries"
**   field).
**
**   The checksum:
**
**     1. Checksum value 1.
**     2. Checksum value 2.
**
** In the above, a segment record is:
**
**     1. First page of array,
**     2. Last page of array,
**     3. Root page of array (or 0),
**     4. Size of array in pages,
*/

/*
** LARGE NUMBERS OF LEVEL RECORDS:
**
** A limit on the number of rhs segments that may be present in the database
** file. Defining this limit ensures that all level records fit within
** the 4096 byte limit for checkpoint blobs.
**
** The number of right-hand-side segments in a database is counted as 
** follows:
**
**   * For each level in the database not undergoing a merge, add 1.
**
**   * For each level in the database that is undergoing a merge, add 
**     the number of segments on the rhs of the level.
**
** A level record not undergoing a merge is 6 integers. A level record 
** with nRhs rhs segments and (nRhs+1) input segments (i.e. including the 
** separators from the next level) is (6*nRhs+12) integers. The maximum
** per right-hand-side level is therefore 12 integers. So the maximum
** size of all level records in a checkpoint is 12*40=480 integers.
*/
#define LSM_MAX_RHS_SEGMENTS 40

/*
** LARGE NUMBERS OF FREELIST ENTRIES:
**
** There is also a limit (LSM_MAX_FREELIST_ENTRIES - defined in lsmInt.h)
** on the number of free-list entries stored in a checkpoint. Since each 
** free-list entry consists of 3 integers, the maximum free-list size is 
** 3*100=300 integers. Combined with the limit on rhs segments defined
** above, this ensures that a checkpoint always fits within a 4096 byte
** meta page.
**
** If the database contains more than 100 free blocks, the "overflow" flag
** in the checkpoint header is set and the remainder are stored in the
** system FREELIST entry in the LSM (along with user data). The value
** accompanying the FREELIST key in the LSM is, like a checkpoint, an array
** of 32-bit big-endian integers. As follows:
**
**     For each entry:
**       a. Block number of free block.
**       b. MSW of associated checkpoint id.
**       c. LSW of associated checkpoint id.
**
** The number of entries is not required - it is implied by the size of the
** value blob containing the integer array.
**
** Note that the limit defined by LSM_MAX_FREELIST_ENTRIES is a hard limit.
** The actual value used may be configured using LSM_CONFIG_MAX_FREELIST.
*/

/*
** The argument to this macro must be of type u32. On a little-endian
** architecture, it returns the u32 value that results from interpreting
** the 4 bytes as a big-endian value. On a big-endian architecture, it
** returns the value that would be produced by intepreting the 4 bytes
** of the input value as a little-endian integer.
*/
#define BYTESWAP32(x) ( \
   (((x)&0x000000FF)<<24) + (((x)&0x0000FF00)<<8)  \
 + (((x)&0x00FF0000)>>8)  + (((x)&0xFF000000)>>24) \
)

static const int one = 1;
#define LSM_LITTLE_ENDIAN (*(u8 *)(&one))

/* Sizes, in integers, of various parts of the checkpoint. */
#define CKPT_HDR_SIZE         8
#define CKPT_LOGPTR_SIZE      4
#define CKPT_SEGMENT_SIZE     4
#define CKPT_CKSUM_SIZE       2
#define CKPT_APPENDLIST_SIZE  LSM_APPLIST_SZ

/* A #define to describe each integer in the checkpoint header. */
#define CKPT_HDR_ID_MSW   0
#define CKPT_HDR_ID_LSW   1
#define CKPT_HDR_NCKPT    2
#define CKPT_HDR_NBLOCK   3
#define CKPT_HDR_BLKSZ    4
#define CKPT_HDR_NLEVEL   5
#define CKPT_HDR_PGSZ     6
#define CKPT_HDR_OVFL     7

#define CKPT_HDR_LO_MSW     8
#define CKPT_HDR_LO_LSW     9
#define CKPT_HDR_LO_CKSUM1 10
#define CKPT_HDR_LO_CKSUM2 11

typedef struct CkptBuffer CkptBuffer;

/*
** Dynamic buffer used to accumulate data for a checkpoint.
*/
struct CkptBuffer {
  lsm_env *pEnv;
  int nAlloc;
  u32 *aCkpt;
};

/*
** Calculate the checksum of the checkpoint specified by arguments aCkpt and
** nCkpt. Store the checksum in *piCksum1 and *piCksum2 before returning.
**
** The value of the nCkpt parameter includes the two checksum values at
** the end of the checkpoint. They are not used as inputs to the checksum 
** calculation. The checksum is based on the array of (nCkpt-2) integers
** at aCkpt[].
*/
static void ckptChecksum(u32 *aCkpt, u32 nCkpt, u32 *piCksum1, u32 *piCksum2){
  int i;
  u32 cksum1 = 1;
  u32 cksum2 = 2;

  if( nCkpt % 2 ){
    cksum1 += aCkpt[nCkpt-3] & 0x0000FFFF;
    cksum2 += aCkpt[nCkpt-3] & 0xFFFF0000;
  }

  for(i=0; (i+3)<nCkpt; i+=2){
    cksum1 += cksum2 + aCkpt[i];
    cksum2 += cksum1 + aCkpt[i+1];
  }

  *piCksum1 = cksum1;
  *piCksum2 = cksum2;
}

/*
** Set integer iIdx of the checkpoint accumulating in buffer *p to iVal.
*/
static void ckptSetValue(CkptBuffer *p, int iIdx, u32 iVal, int *pRc){
  if( *pRc ) return;
  if( iIdx>=p->nAlloc ){
    int nNew = LSM_MAX(8, iIdx*2);
    p->aCkpt = (u32 *)lsmReallocOrFree(p->pEnv, p->aCkpt, nNew*sizeof(u32));
    if( !p->aCkpt ){
      *pRc = LSM_NOMEM_BKPT;
      return;
    }
    p->nAlloc = nNew;
  }
  p->aCkpt[iIdx] = iVal;
}

/*
** Argument aInt points to an array nInt elements in size. Switch the 
** endian-ness of each element of the array.
*/
static void ckptChangeEndianness(u32 *aInt, int nInt){
  if( LSM_LITTLE_ENDIAN ){
    int i;
    for(i=0; i<nInt; i++) aInt[i] = BYTESWAP32(aInt[i]);
  }
}

/*
** Object *p contains a checkpoint in native byte-order. The checkpoint is
** nCkpt integers in size, not including any checksum. This function sets
** the two checksum elements of the checkpoint accordingly.
*/
static void ckptAddChecksum(CkptBuffer *p, int nCkpt, int *pRc){
  if( *pRc==LSM_OK ){
    u32 aCksum[2] = {0, 0};
    ckptChecksum(p->aCkpt, nCkpt+2, &aCksum[0], &aCksum[1]);
    ckptSetValue(p, nCkpt, aCksum[0], pRc);
    ckptSetValue(p, nCkpt+1, aCksum[1], pRc);
  }
}

/*
** Append a 6-value segment record corresponding to pSeg to the checkpoint 
** buffer passed as the third argument.
*/
static void ckptExportSegment(
  Segment *pSeg, 
  CkptBuffer *p, 
  int *piOut, 
  int *pRc
){
  int iOut = *piOut;

  ckptSetValue(p, iOut++, pSeg->iFirst, pRc);
  ckptSetValue(p, iOut++, pSeg->iLast, pRc);
  ckptSetValue(p, iOut++, pSeg->iRoot, pRc);
  ckptSetValue(p, iOut++, pSeg->nSize, pRc);

  *piOut = iOut;
}

static void ckptExportLevel(
  Level *pLevel,                  /* Level object to serialize */
  CkptBuffer *p,                  /* Append new level record to this ckpt */
  int *piOut,                     /* IN/OUT: Size of checkpoint so far */
  int *pRc                        /* IN/OUT: Error code */
){
  int iOut = *piOut;
  Merge *pMerge;

  pMerge = pLevel->pMerge;
  ckptSetValue(p, iOut++, pLevel->iAge, pRc);
  ckptSetValue(p, iOut++, pLevel->nRight, pRc);
  ckptExportSegment(&pLevel->lhs, p, &iOut, pRc);

  assert( (pLevel->nRight>0)==(pMerge!=0) );
  if( pMerge ){
    int i;
    for(i=0; i<pLevel->nRight; i++){
      ckptExportSegment(&pLevel->aRhs[i], p, &iOut, pRc);
    }
    assert( pMerge->nInput==pLevel->nRight 
         || pMerge->nInput==pLevel->nRight+1 
    );
    ckptSetValue(p, iOut++, pMerge->nInput, pRc);
    ckptSetValue(p, iOut++, pMerge->nSkip, pRc);
    for(i=0; i<pMerge->nInput; i++){
      ckptSetValue(p, iOut++, pMerge->aInput[i].iPg, pRc);
      ckptSetValue(p, iOut++, pMerge->aInput[i].iCell, pRc);
    }
    ckptSetValue(p, iOut++, pMerge->splitkey.iPg, pRc);
    ckptSetValue(p, iOut++, pMerge->splitkey.iCell, pRc);
  }

  *piOut = iOut;
}

/*
** Populate the log offset fields of the checkpoint buffer. 4 values.
*/
static void ckptExportLog(
  lsm_db *pDb, 
  int bFlush,
  CkptBuffer *p, 
  int *piOut, 
  int *pRc
){
  int iOut = *piOut;

  assert( iOut==CKPT_HDR_LO_MSW );

  if( bFlush ){
    DbLog *pLog = &pDb->treehdr.log;
    i64 iOff = pLog->aRegion[2].iEnd;
    ckptSetValue(p, iOut++, (iOff >> 32) & 0xFFFFFFFF, pRc);
    ckptSetValue(p, iOut++, (iOff & 0xFFFFFFFF), pRc);
    ckptSetValue(p, iOut++, pLog->cksum0, pRc);
    ckptSetValue(p, iOut++, pLog->cksum1, pRc);
  }else{
    for(; iOut<=CKPT_HDR_LO_CKSUM2; iOut++){
      ckptSetValue(p, iOut, pDb->pShmhdr->aWorker[iOut], pRc);
    }
  }

  *piOut = iOut;
}

static void ckptExportAppendlist(
  lsm_db *db,                     /* Database connection */
  CkptBuffer *p,                  /* Checkpoint buffer to write to */
  int *piOut,                     /* IN/OUT: Offset within checkpoint buffer */
  int *pRc                        /* IN/OUT: Error code */
){
  int i;
  int iOut = *piOut;
  u32 *aiAppend = db->pWorker->aiAppend;

  for(i=0; i<CKPT_APPENDLIST_SIZE; i++){
    ckptSetValue(p, iOut++, aiAppend[i], pRc);
  }
  *piOut = iOut;
};

static int ckptExportSnapshot( 
  lsm_db *pDb,                    /* Connection handle */
  int nOvfl,                      /* Number of free-list entries in LSM */
  int bLog,                       /* True to update log-offset fields */
  i64 iId,                        /* Checkpoint id */
  int bCksum,                     /* If true, include checksums */
  void **ppCkpt,                  /* OUT: Buffer containing checkpoint */
  int *pnCkpt                     /* OUT: Size of checkpoint in bytes */
){
  int rc = LSM_OK;                /* Return Code */
  FileSystem *pFS = pDb->pFS;     /* File system object */
  Snapshot *pSnap = pDb->pWorker; /* Worker snapshot */
  int nLevel = 0;                 /* Number of levels in checkpoint */
  int iLevel;                     /* Used to count out nLevel levels */
  int iOut = 0;                   /* Current offset in aCkpt[] */
  Level *pLevel;                  /* Level iterator */
  int i;                          /* Iterator used while serializing freelist */
  CkptBuffer ckpt;
  int nFree;
 
  nFree = pSnap->freelist.nEntry;
  if( nOvfl>=0 ){
    nFree -=  nOvfl;
  }else{
    nOvfl = pDb->pShmhdr->aWorker[CKPT_HDR_OVFL];
  }

  /* Initialize the output buffer */
  memset(&ckpt, 0, sizeof(CkptBuffer));
  ckpt.pEnv = pDb->pEnv;
  iOut = CKPT_HDR_SIZE;

  /* Write the log offset into the checkpoint. */
  ckptExportLog(pDb, bLog, &ckpt, &iOut, &rc);

  /* Write the append-point list */
  ckptExportAppendlist(pDb, &ckpt, &iOut, &rc);

  /* Figure out how many levels will be written to the checkpoint. */
  for(pLevel=lsmDbSnapshotLevel(pSnap); pLevel; pLevel=pLevel->pNext) nLevel++;

  /* Serialize nLevel levels. */
  iLevel = 0;
  for(pLevel=lsmDbSnapshotLevel(pSnap); iLevel<nLevel; pLevel=pLevel->pNext){
    ckptExportLevel(pLevel, &ckpt, &iOut, &rc);
    iLevel++;
  }

  /* Write the freelist */
  if( rc==LSM_OK ){
    ckptSetValue(&ckpt, iOut++, nFree, &rc);
    for(i=0; i<nFree; i++){
      FreelistEntry *p = &pSnap->freelist.aEntry[i];
      ckptSetValue(&ckpt, iOut++, p->iBlk, &rc);
      ckptSetValue(&ckpt, iOut++, (p->iId >> 32) & 0xFFFFFFFF, &rc);
      ckptSetValue(&ckpt, iOut++, p->iId & 0xFFFFFFFF, &rc);
    }
  }

  /* Write the checkpoint header */
  assert( iId>=0 );
  ckptSetValue(&ckpt, CKPT_HDR_ID_MSW, (u32)(iId>>32), &rc);
  ckptSetValue(&ckpt, CKPT_HDR_ID_LSW, (u32)(iId&0xFFFFFFFF), &rc);
  ckptSetValue(&ckpt, CKPT_HDR_NCKPT, iOut+2, &rc);
  ckptSetValue(&ckpt, CKPT_HDR_NBLOCK, pSnap->nBlock, &rc);
  ckptSetValue(&ckpt, CKPT_HDR_BLKSZ, lsmFsBlockSize(pFS), &rc);
  ckptSetValue(&ckpt, CKPT_HDR_NLEVEL, nLevel, &rc);
  ckptSetValue(&ckpt, CKPT_HDR_PGSZ, lsmFsPageSize(pFS), &rc);
  ckptSetValue(&ckpt, CKPT_HDR_OVFL, nOvfl, &rc);

  if( bCksum ){
    ckptAddChecksum(&ckpt, iOut, &rc);
  }else{
    ckptSetValue(&ckpt, iOut, 0, &rc);
    ckptSetValue(&ckpt, iOut+1, 0, &rc);
  }
  iOut += 2;
  assert( iOut<=1024 );

#if 0
  lsmLogMessage(pDb, rc, 
      "ckptExportSnapshot(): id=%d freelist: %d/%d", (int)iId, nFree, nOvfl
  );
#endif

  *ppCkpt = (void *)ckpt.aCkpt;
  if( pnCkpt ) *pnCkpt = sizeof(u32)*iOut;
  return rc;
}


/*
** Helper function for ckptImport().
*/
static void ckptNewSegment(
  u32 *aIn,
  int *piIn,
  Segment *pSegment               /* Populate this structure */
){
  int iIn = *piIn;

  assert( pSegment->iFirst==0 && pSegment->iLast==0 );
  assert( pSegment->nSize==0 && pSegment->iRoot==0 );
  pSegment->iFirst = aIn[iIn++];
  pSegment->iLast = aIn[iIn++];
  pSegment->iRoot = aIn[iIn++];
  pSegment->nSize = aIn[iIn++];

  *piIn = iIn;
}

static int ckptSetupMerge(lsm_db *pDb, u32 *aInt, int *piIn, Level *pLevel){
  Merge *pMerge;                  /* Allocated Merge object */
  int nInput;                     /* Number of input segments in merge */
  int iIn = *piIn;                /* Next value to read from aInt[] */
  int i;                          /* Iterator variable */
  int nByte;                      /* Number of bytes to allocate */

  /* Allocate the Merge object. If malloc() fails, return LSM_NOMEM. */
  nInput = (int)aInt[iIn++];
  nByte = sizeof(Merge) + sizeof(MergeInput) * nInput;
  pMerge = (Merge *)lsmMallocZero(pDb->pEnv, nByte);
  if( !pMerge ) return LSM_NOMEM_BKPT;
  pLevel->pMerge = pMerge;

  /* Populate the Merge object. */
  pMerge->aInput = (MergeInput *)&pMerge[1];
  pMerge->nInput = nInput;
  pMerge->iOutputOff = -1;
  pMerge->bHierReadonly = 1;
  pMerge->nSkip = (int)aInt[iIn++];
  for(i=0; i<nInput; i++){
    pMerge->aInput[i].iPg = (Pgno)aInt[iIn++];
    pMerge->aInput[i].iCell = (int)aInt[iIn++];
  }
  pMerge->splitkey.iPg = (Pgno)aInt[iIn++];
  pMerge->splitkey.iCell = (int)aInt[iIn++];

  /* Set *piIn and return LSM_OK. */
  *piIn = iIn;
  return LSM_OK;
}


static int ckptLoadLevels(
  lsm_db *pDb,
  u32 *aIn, 
  int *piIn, 
  int nLevel,
  Level **ppLevel
){
  int i;
  int rc = LSM_OK;
  Level *pRet = 0;
  Level **ppNext;
  int iIn = *piIn;

  ppNext = &pRet;
  for(i=0; rc==LSM_OK && i<nLevel; i++){
    int iRight;
    Level *pLevel;

    /* Allocate space for the Level structure and Level.apRight[] array */
    pLevel = (Level *)lsmMallocZeroRc(pDb->pEnv, sizeof(Level), &rc);
    if( rc==LSM_OK ){
      pLevel->iAge = aIn[iIn++];
      pLevel->nRight = aIn[iIn++];
      if( pLevel->nRight ){
        int nByte = sizeof(Segment) * pLevel->nRight;
        pLevel->aRhs = (Segment *)lsmMallocZeroRc(pDb->pEnv, nByte, &rc);
      }
      if( rc==LSM_OK ){
        *ppNext = pLevel;
        ppNext = &pLevel->pNext;

        /* Allocate the main segment */
        ckptNewSegment(aIn, &iIn, &pLevel->lhs);

        /* Allocate each of the right-hand segments, if any */
        for(iRight=0; iRight<pLevel->nRight; iRight++){
          ckptNewSegment(aIn, &iIn, &pLevel->aRhs[iRight]);
        }

        /* Set up the Merge object, if required */
        if( pLevel->nRight>0 ){
          rc = ckptSetupMerge(pDb, aIn, &iIn, pLevel);
        }
      }
    }
  }

  if( rc!=LSM_OK ){
    /* An OOM must have occurred. Free any level structures allocated and
    ** return the error to the caller. */
    lsmSortedFreeLevel(pDb->pEnv, pRet);
    pRet = 0;
  }

  *ppLevel = pRet;
  *piIn = iIn;
  return rc;
}


int lsmCheckpointLoadLevels(lsm_db *pDb, void *pVal, int nVal){
  int rc = LSM_OK;
  if( nVal>0 ){
    u32 *aIn;

    aIn = lsmMallocRc(pDb->pEnv, nVal, &rc);
    if( aIn ){
      Level *pLevel = 0;
      Level *pParent;

      int nIn;
      int nLevel;
      int iIn = 1;
      memcpy(aIn, pVal, nVal);
      nIn = nVal / sizeof(u32);

      ckptChangeEndianness(aIn, nIn);
      nLevel = aIn[0];
      rc = ckptLoadLevels(pDb, aIn, &iIn, nLevel, &pLevel);
      lsmFree(pDb->pEnv, aIn);
      assert( rc==LSM_OK || pLevel==0 );
      if( rc==LSM_OK ){
        pParent = lsmDbSnapshotLevel(pDb->pWorker);
        assert( pParent );
        while( pParent->pNext ) pParent = pParent->pNext;
        pParent->pNext = pLevel;
      }
    }
  }

  return rc;
}

/*
** Return the data for the LEVELS record.
**
** The size of the checkpoint that can be stored in the database header
** must not exceed 1024 32-bit integers. Normally, it does not. However,
** if it does, part of the checkpoint must be stored in the LSM. This
** routine returns that part.
*/
int lsmCheckpointLevels(
  lsm_db *pDb,                    /* Database handle */
  int nLevel,                     /* Number of levels to write to blob */
  void **paVal,                   /* OUT: Pointer to LEVELS blob */
  int *pnVal                      /* OUT: Size of LEVELS blob in bytes */
){
  Level *p;                       /* Used to iterate through levels */
  int nAll= 0;
  int rc;
  int i;
  int iOut;
  CkptBuffer ckpt;
  assert( nLevel>0 );

  for(p=lsmDbSnapshotLevel(pDb->pWorker); p; p=p->pNext) nAll++;

  assert( nAll>nLevel );
  nAll -= nLevel;
  for(p=lsmDbSnapshotLevel(pDb->pWorker); p && nAll>0; p=p->pNext) nAll--;

  memset(&ckpt, 0, sizeof(CkptBuffer));
  ckpt.pEnv = pDb->pEnv;

  ckptSetValue(&ckpt, 0, nLevel, &rc);
  iOut = 1;
  for(i=0; rc==LSM_OK && i<nLevel; i++){
    ckptExportLevel(p, &ckpt, &iOut, &rc);
    p = p->pNext;
  }
  assert( rc!=LSM_OK || p==0 );

  if( rc==LSM_OK ){
    ckptChangeEndianness(ckpt.aCkpt, iOut);
    *paVal = (void *)ckpt.aCkpt;
    *pnVal = iOut * sizeof(u32);
  }else{
    *pnVal = 0;
    *paVal = 0;
  }

  return rc;
}

/*
** The worker lock must be held to call this function.
**
** The function serializes and returns the data that should be stored as
** the FREELIST system record.
*/
int lsmCheckpointOverflow(
  lsm_db *pDb,                    /* Database handle (must hold worker lock) */
  void **ppVal,                   /* OUT: lsmMalloc'd buffer */
  int *pnVal,                     /* OUT: Size of *ppVal in bytes */
  int *pnOvfl                     /* OUT: Number of freelist entries in buf */
){
  int rc = LSM_OK;
  int nRet;
  Snapshot *p = pDb->pWorker;

  assert( lsmShmAssertWorker(pDb) );
  assert( pnOvfl && ppVal && pnVal );
  assert( pDb->nMaxFreelist>=2 && pDb->nMaxFreelist<=LSM_MAX_FREELIST_ENTRIES );

  if( p->nFreelistOvfl ){
    rc = lsmCheckpointOverflowLoad(pDb, &p->freelist);
    if( rc!=LSM_OK ) return rc;
    p->nFreelistOvfl = 0;
  }

  if( p->freelist.nEntry<=pDb->nMaxFreelist ){
    nRet = 0;
    *pnVal = 0;
    *ppVal = 0;
  }else{
    int i;                        /* Iterator variable */
    int iOut = 0;                 /* Current size of blob in ckpt */
    CkptBuffer ckpt;              /* Used to build FREELIST blob */

    nRet = (p->freelist.nEntry - pDb->nMaxFreelist);

    memset(&ckpt, 0, sizeof(CkptBuffer));
    ckpt.pEnv = pDb->pEnv;
    for(i=p->freelist.nEntry-nRet; rc==LSM_OK && i<p->freelist.nEntry; i++){
      FreelistEntry *pEntry = &p->freelist.aEntry[i];
      ckptSetValue(&ckpt, iOut++, pEntry->iBlk, &rc);
      ckptSetValue(&ckpt, iOut++, (pEntry->iId >> 32) & 0xFFFFFFFF, &rc);
      ckptSetValue(&ckpt, iOut++, pEntry->iId & 0xFFFFFFFF, &rc);
    }
    ckptChangeEndianness(ckpt.aCkpt, iOut);

    *ppVal = ckpt.aCkpt;
    *pnVal = iOut*sizeof(u32);
  }

  *pnOvfl = nRet;
  return rc;
}

/*
** The connection must be the worker in order to call this function.
**
** True is returned if there are currently too many free-list entries
** in-memory to store in a checkpoint. Before calling lsmCheckpointSaveWorker()
** to save the current worker snapshot, a new top-level LSM segment must
** be created so that some of them can be written to the LSM. 
*/
int lsmCheckpointOverflowRequired(lsm_db *pDb){
  assert( lsmShmAssertWorker(pDb) );
  return (pDb->pWorker->freelist.nEntry > pDb->nMaxFreelist);
}

/*
** Connection pDb must be the worker to call this function.
**
** Load the FREELIST record from the database. Decode it and append the
** results to list pFreelist.
*/
int lsmCheckpointOverflowLoad(
  lsm_db *pDb,
  Freelist *pFreelist
){
  int rc;
  int nVal = 0;
  void *pVal = 0;
  assert( lsmShmAssertWorker(pDb) );

  /* Load the blob of data from the LSM. If that is successful (and the
  ** blob is greater than zero bytes in size), decode the contents and
  ** merge them into the current contents of *pFreelist.  */
  rc = lsmSortedLoadFreelist(pDb, &pVal, &nVal);
  if( pVal ){
    u32 *aFree = (u32 *)pVal;
    int nFree = nVal / sizeof(int);
    ckptChangeEndianness(aFree, nFree);
    if( (nFree % 3) ){
      rc = LSM_CORRUPT_BKPT;
    }else{
      int iNew = 0;               /* Offset of next element in aFree[] */
      int iOld = 0;               /* Next element in freelist fl */
      Freelist fl = *pFreelist;   /* Original contents of *pFreelist */

      memset(pFreelist, 0, sizeof(Freelist));
      while( rc==LSM_OK && (iNew<nFree || iOld<fl.nEntry) ){
        int iBlk;
        i64 iId;

        if( iOld>=fl.nEntry ){
          iBlk = aFree[iNew];
          iId = ((i64)(aFree[iNew+1])<<32) + (i64)aFree[iNew+2];
          iNew += 3;
        }else if( iNew>=nFree ){
          iBlk = fl.aEntry[iOld].iBlk;
          iId = fl.aEntry[iOld].iId;
          iOld += 1;
        }else{
          iId = ((i64)(aFree[iNew+1])<<32) + (i64)aFree[iNew+2];
          if( iId<fl.aEntry[iOld].iId ){
            iBlk = aFree[iNew];
            iNew += 3;
          }else{
            iBlk = fl.aEntry[iOld].iBlk;
            iId = fl.aEntry[iOld].iId;
            iOld += 1;
          }
        }

        rc = lsmFreelistAppend(pDb->pEnv, pFreelist, iBlk, iId);
      }
      lsmFree(pDb->pEnv, fl.aEntry);

#ifdef LSM_DEBUG
      if( rc==LSM_OK ){
        int i;
        for(i=1; rc==LSM_OK && i<pFreelist->nEntry; i++){
          assert( pFreelist->aEntry[i].iId >= pFreelist->aEntry[i-1].iId );
        }
        assert( pFreelist->nEntry==(fl.nEntry + nFree/3) );
      }
#endif
    }

    lsmFree(pDb->pEnv, pVal);
  }

  return rc;
}

/*
** Read the checkpoint id from meta-page pPg.
*/
static i64 ckptLoadId(MetaPage *pPg){
  i64 ret = 0;
  if( pPg ){
    int nData;
    u8 *aData = lsmFsMetaPageData(pPg, &nData);
    ret = (((i64)lsmGetU32(&aData[CKPT_HDR_ID_MSW*4])) << 32) + 
          ((i64)lsmGetU32(&aData[CKPT_HDR_ID_LSW*4]));
  }
  return ret;
}

/*
** Return true if the buffer passed as an argument contains a valid
** checkpoint.
*/
static int ckptChecksumOk(u32 *aCkpt){
  u32 nCkpt = aCkpt[CKPT_HDR_NCKPT];
  u32 cksum1;
  u32 cksum2;

  if( nCkpt<CKPT_HDR_NCKPT || nCkpt>(LSM_META_PAGE_SIZE)/sizeof(u32) ) return 0;
  ckptChecksum(aCkpt, nCkpt, &cksum1, &cksum2);
  return (cksum1==aCkpt[nCkpt-2] && cksum2==aCkpt[nCkpt-1]);
}

/*
** Attempt to load a checkpoint from meta page iMeta.
**
** This function is a no-op if *pRc is set to any value other than LSM_OK
** when it is called. If an error occurs, *pRc is set to an LSM error code
** before returning.
**
** If no error occurs and the checkpoint is successfully loaded, copy it to
** ShmHeader.aClient[] and ShmHeader.aWorker[], and set ShmHeader.iMetaPage 
** to indicate its origin. In this case return 1. Or, if the checkpoint 
** cannot be loaded (because the checksum does not compute), return 0.
*/
static int ckptTryLoad(lsm_db *pDb, MetaPage *pPg, u32 iMeta, int *pRc){
  int bLoaded = 0;                /* Return value */
  if( *pRc==LSM_OK ){
    int rc = LSM_OK;              /* Error code */
    u32 *aCkpt = 0;               /* Pointer to buffer containing checkpoint */
    u32 nCkpt;                    /* Number of elements in aCkpt[] */
    int nData;                    /* Bytes of data in aData[] */
    u8 *aData;                    /* Meta page data */
   
    aData = lsmFsMetaPageData(pPg, &nData);
    nCkpt = (u32)lsmGetU32(&aData[CKPT_HDR_NCKPT*sizeof(u32)]);
    if( nCkpt<=nData/sizeof(u32) && nCkpt>CKPT_HDR_NCKPT ){
      aCkpt = (u32 *)lsmMallocRc(pDb->pEnv, nCkpt*sizeof(u32), &rc);
    }
    if( aCkpt ){
      memcpy(aCkpt, aData, nCkpt*sizeof(u32));
      ckptChangeEndianness(aCkpt, nCkpt);
      if( ckptChecksumOk(aCkpt) ){
        ShmHeader *pShm = pDb->pShmhdr;
        memcpy(pShm->aClient, aCkpt, nCkpt*sizeof(u32));
        memcpy(pShm->aWorker, aCkpt, nCkpt*sizeof(u32));
        memcpy(pDb->aSnapshot, aCkpt, nCkpt*sizeof(u32));
        pShm->iMetaPage = iMeta;
        bLoaded = 1;
      }
    }

    lsmFree(pDb->pEnv, aCkpt);
    *pRc = rc;
  }
  return bLoaded;
}

/*
** Initialize the shared-memory header with an empty snapshot. This function
** is called when no valid snapshot can be found in the database header.
*/
static void ckptLoadEmpty(lsm_db *pDb){
  u32 aCkpt[] = {
    0,                  /* CKPT_HDR_ID_MSW */
    10,                 /* CKPT_HDR_ID_LSW */
    0,                  /* CKPT_HDR_NCKPT */
    0,                  /* CKPT_HDR_NBLOCK */
    0,                  /* CKPT_HDR_BLKSZ */
    0,                  /* CKPT_HDR_NLEVEL */
    0,                  /* CKPT_HDR_PGSZ */
    0,                  /* CKPT_HDR_OVFL */
    0, 0, 1234, 5678,   /* The log pointer and initial checksum */
    0, 0, 0, 0,         /* The append list */
    0,                  /* The free block list */
    0, 0                /* Space for checksum values */
  };
  u32 nCkpt = array_size(aCkpt);
  ShmHeader *pShm = pDb->pShmhdr;

  aCkpt[CKPT_HDR_NCKPT] = nCkpt;
  aCkpt[CKPT_HDR_BLKSZ] = pDb->nDfltBlksz;
  aCkpt[CKPT_HDR_PGSZ] = pDb->nDfltPgsz;
  ckptChecksum(aCkpt, array_size(aCkpt), &aCkpt[nCkpt-2], &aCkpt[nCkpt-1]);

  memcpy(pShm->aClient, aCkpt, nCkpt*sizeof(u32));
  memcpy(pShm->aWorker, aCkpt, nCkpt*sizeof(u32));
  memcpy(pDb->aSnapshot, aCkpt, nCkpt*sizeof(u32));
}

/*
** This function is called as part of database recovery to initialize the
** ShmHeader.aClient[] and ShmHeader.aWorker[] snapshots.
*/
int lsmCheckpointRecover(lsm_db *pDb){
  int rc = LSM_OK;                /* Return Code */
  i64 iId1;                       /* Id of checkpoint on meta-page 1 */
  i64 iId2;                       /* Id of checkpoint on meta-page 2 */
  int bLoaded = 0;                /* True once checkpoint has been loaded */
  int cmp;                        /* True if (iId2>iId1) */
  MetaPage *apPg[2] = {0, 0};     /* Meta-pages 1 and 2 */

  rc = lsmFsMetaPageGet(pDb->pFS, 0, 1, &apPg[0]);
  if( rc==LSM_OK ) rc = lsmFsMetaPageGet(pDb->pFS, 0, 2, &apPg[1]);

  iId1 = ckptLoadId(apPg[0]);
  iId2 = ckptLoadId(apPg[1]);
  cmp = (iId2 > iId1);
  bLoaded = ckptTryLoad(pDb, apPg[cmp?1:0], (cmp?2:1), &rc);
  if( bLoaded==0 ){
    bLoaded = ckptTryLoad(pDb, apPg[cmp?0:1], (cmp?1:2), &rc);
  }

  /* The database does not contain a valid checkpoint. Initialize the shared
  ** memory header with an empty checkpoint.  */
  if( bLoaded==0 ){
    ckptLoadEmpty(pDb);
  }

  lsmFsMetaPageRelease(apPg[0]);
  lsmFsMetaPageRelease(apPg[1]);

  return rc;
}

/* 
** Store the snapshot in pDb->aSnapshot[] in meta-page iMeta.
*/
int lsmCheckpointStore(lsm_db *pDb, int iMeta){
  MetaPage *pPg = 0;
  int rc;

  assert( iMeta==1 || iMeta==2 );
  rc = lsmFsMetaPageGet(pDb->pFS, 1, iMeta, &pPg);
  if( rc==LSM_OK ){
    u8 *aData;
    int nData;
    int nCkpt;

    nCkpt = (int)pDb->aSnapshot[CKPT_HDR_NCKPT];
    aData = lsmFsMetaPageData(pPg, &nData);
    memcpy(aData, pDb->aSnapshot, nCkpt*sizeof(u32));
    ckptChangeEndianness((u32 *)aData, nCkpt);
    rc = lsmFsMetaPageRelease(pPg);
  }
      
  return rc;
}

/*
** Copy the current client snapshot from shared-memory to pDb->aSnapshot[].
*/
int lsmCheckpointLoad(lsm_db *pDb){
  while( 1 ){
    int rc;
    int nInt;
    ShmHeader *pShm = pDb->pShmhdr;

    nInt = pShm->aClient[CKPT_HDR_NCKPT];
    memcpy(pDb->aSnapshot, pShm->aClient, nInt*sizeof(u32));
    if( ckptChecksumOk(pDb->aSnapshot) ) return LSM_OK;

    rc = lsmShmLock(pDb, LSM_LOCK_WORKER, LSM_LOCK_EXCL, 0);
    if( rc==LSM_BUSY ){
      usleep(50);
    }else{
      if( rc==LSM_OK ){
        if( ckptChecksumOk(pShm->aClient)==0 ){
          nInt = pShm->aWorker[CKPT_HDR_NCKPT];
          memcpy(pShm->aClient, pShm->aWorker, nInt*sizeof(u32));
        }
        nInt = pShm->aClient[CKPT_HDR_NCKPT];
        memcpy(pDb->aSnapshot, &pShm->aClient, nInt*sizeof(u32));
        lsmShmLock(pDb, LSM_LOCK_WORKER, LSM_LOCK_UNLOCK, 0);

        if( ckptChecksumOk(pDb->aSnapshot)==0 ){
          rc = LSM_CORRUPT_BKPT;
        }
      }
      return rc;
    }
  }
}

int lsmCheckpointLoadWorker(lsm_db *pDb){
  int rc;
  ShmHeader *pShm = pDb->pShmhdr;

  /* Must be holding the WORKER lock to do this */
  assert( lsmShmAssertLock(pDb, LSM_LOCK_WORKER, LSM_LOCK_EXCL) );

  if( ckptChecksumOk(pShm->aWorker)==0 ){
    int nInt = (int)pShm->aClient[CKPT_HDR_NCKPT];
    memcpy(pShm->aWorker, pShm->aClient, nInt*sizeof(u32));
    if( ckptChecksumOk(pShm->aWorker)==0 ) return LSM_CORRUPT_BKPT;
  }

  rc = lsmCheckpointDeserialize(pDb, 1, pShm->aWorker, &pDb->pWorker);
  assert( rc!=LSM_OK || lsmFsIntegrityCheck(pDb) );
  return rc;
}

int lsmCheckpointDeserialize(
  lsm_db *pDb, 
  int bInclFreelist,              /* If true, deserialize free-list */
  u32 *aCkpt, 
  Snapshot **ppSnap
){
  int rc = LSM_OK;
  Snapshot *pNew;

  pNew = (Snapshot *)lsmMallocZeroRc(pDb->pEnv, sizeof(Snapshot), &rc);
  if( rc==LSM_OK ){
    int nFree;
    int nCopy;
    int nLevel = (int)aCkpt[CKPT_HDR_NLEVEL];
    int iIn = CKPT_HDR_SIZE + CKPT_APPENDLIST_SIZE + CKPT_LOGPTR_SIZE;

    pNew->iId = lsmCheckpointId(aCkpt, 0);
    pNew->nBlock = aCkpt[CKPT_HDR_NBLOCK];
    rc = ckptLoadLevels(pDb, aCkpt, &iIn, nLevel, &pNew->pLevel);

    /* Make a copy of the append-list */
    nCopy = sizeof(u32) * LSM_APPLIST_SZ;
    memcpy(pNew->aiAppend, &aCkpt[CKPT_HDR_SIZE+CKPT_LOGPTR_SIZE], nCopy);

    /* Copy the free-list */
    if( bInclFreelist ){
      pNew->nFreelistOvfl = aCkpt[CKPT_HDR_OVFL];
      nFree = aCkpt[iIn++];
      if( nFree ){
        pNew->freelist.aEntry = (FreelistEntry *)lsmMallocZeroRc(
            pDb->pEnv, sizeof(FreelistEntry)*nFree, &rc
        );
        if( rc==LSM_OK ){
          int i;
          for(i=0; i<nFree; i++){
            FreelistEntry *p = &pNew->freelist.aEntry[i];
            p->iBlk = aCkpt[iIn++];
            p->iId = ((i64)(aCkpt[iIn])<<32) + aCkpt[iIn+1];
            iIn += 2;
          }
          pNew->freelist.nEntry = pNew->freelist.nAlloc = nFree;
        }
      }
    }
  }

  if( rc!=LSM_OK ){
    lsmFreeSnapshot(pDb->pEnv, pNew);
    pNew = 0;
  }

  *ppSnap = pNew;
  return rc;
}

/*
** Connection pDb must be the worker connection in order to call this
** function. It returns true if the database already contains the maximum
** number of levels or false otherwise.
**
** This is used when flushing the in-memory tree to disk. If the database
** is already full, then the caller should invoke lsm_work() or similar
** until it is not full before creating a new level by flushing the in-memory
** tree to disk. Limiting the number of levels in the database ensures that
** the records describing them always fit within the checkpoint blob.
*/
int lsmDatabaseFull(lsm_db *pDb){
  Level *p;
  int nRhs = 0;

  assert( lsmShmAssertLock(pDb, LSM_LOCK_WORKER, LSM_LOCK_EXCL) );
  assert( pDb->pWorker );

  for(p=pDb->pWorker->pLevel; p; p=p->pNext){
    nRhs += (p->nRight ? p->nRight : 1);
  }

  return (nRhs >= LSM_MAX_RHS_SEGMENTS);
}

/*
** The connection passed as the only argument is currently the worker
** connection. Some work has been performed on the database by the connection,
** but no new snapshot has been written into shared memory.
**
** This function updates the shared-memory worker and client snapshots with
** the new snapshot produced by the work performed by pDb.
**
** If successful, LSM_OK is returned. Otherwise, if an error occurs, an LSM
** error code is returned.
*/
int lsmCheckpointSaveWorker(lsm_db *pDb, int bFlush, int nOvfl){
  Snapshot *pSnap = pDb->pWorker;
  ShmHeader *pShm = pDb->pShmhdr;
  void *p = 0;
  int n = 0;
  int rc;

  rc = ckptExportSnapshot(pDb, nOvfl, bFlush, pSnap->iId+1, 1, &p, &n);
  if( rc!=LSM_OK ) return rc;
  assert( ckptChecksumOk((u32 *)p) );

  assert( n<=LSM_META_PAGE_SIZE );
  memcpy(pShm->aWorker, p, n);
  lsmShmBarrier(pDb);
  memcpy(pShm->aClient, p, n);
  lsmFree(pDb->pEnv, p);

  return LSM_OK;
}

int lsmCheckpointSynced(lsm_db *pDb, i64 *piId){
  int rc = LSM_OK;
  const int nAttempt = 3;
  int i;
  for(i=0; i<nAttempt; i++){
    MetaPage *pPg;
    u32 iMeta;

    iMeta = pDb->pShmhdr->iMetaPage;
    rc = lsmFsMetaPageGet(pDb->pFS, 0, iMeta, &pPg);
    if( rc==LSM_OK ){
      int nCkpt;
      int nData;
      u8 *aData; 

      aData = lsmFsMetaPageData(pPg, &nData);
      assert( nData==LSM_META_PAGE_SIZE );
      nCkpt = lsmGetU32(&aData[CKPT_HDR_NCKPT*sizeof(u32)]);

      if( nCkpt<(LSM_META_PAGE_SIZE/sizeof(u32)) ){
        u32 *aCopy = lsmMallocRc(pDb->pEnv, sizeof(u32) * nCkpt, &rc);
        if( aCopy ){
          memcpy(aCopy, aData, nCkpt*sizeof(u32));
          ckptChangeEndianness(aCopy, nCkpt);
          if( ckptChecksumOk(aCopy) ){
            *piId = lsmCheckpointId(aCopy, 0);
          }
          lsmFree(pDb->pEnv, aCopy);
        }
      }
      lsmFsMetaPageRelease(pPg);
    }
    if( rc!=LSM_OK || pDb->pShmhdr->iMetaPage==iMeta ) break;
  }

  return (rc==LSM_OK && i==3) ? LSM_BUSY : LSM_OK;
}

/*
** Return the checkpoint-id of the checkpoint array passed as the first
** argument to this function. If the second argument is true, then assume
** that the checkpoint is made up of 32-bit big-endian integers. If it
** is false, assume that the integers are in machine byte order.
*/
i64 lsmCheckpointId(u32 *aCkpt, int bDisk){
  i64 iId;
  if( bDisk ){
    u8 *aData = (u8 *)aCkpt;
    iId = (((i64)lsmGetU32(&aData[CKPT_HDR_ID_MSW*4])) << 32);
    iId += ((i64)lsmGetU32(&aData[CKPT_HDR_ID_LSW*4]));
  }else{
    iId = ((i64)aCkpt[CKPT_HDR_ID_MSW] << 32) + (i64)aCkpt[CKPT_HDR_ID_LSW];
  }
  return iId;
}

i64 lsmCheckpointLogOffset(u32 *aCkpt){
  return ((i64)aCkpt[CKPT_HDR_LO_MSW] << 32) + (i64)aCkpt[CKPT_HDR_LO_LSW];
}

int lsmCheckpointPgsz(u32 *aCkpt){ return (int)aCkpt[CKPT_HDR_PGSZ]; }

int lsmCheckpointBlksz(u32 *aCkpt){ return (int)aCkpt[CKPT_HDR_BLKSZ]; }

void lsmCheckpointLogoffset(
  u32 *aCkpt,
  DbLog *pLog
){ 
  u32 iOffMSB = aCkpt[CKPT_HDR_LO_MSW];
  u32 iOffLSB = aCkpt[CKPT_HDR_LO_LSW];
  pLog->aRegion[2].iStart = (((i64)iOffMSB) << 32) + ((i64)iOffLSB);
  pLog->cksum0 = aCkpt[CKPT_HDR_LO_CKSUM1];
  pLog->cksum1 = aCkpt[CKPT_HDR_LO_CKSUM2];
}

void lsmCheckpointZeroLogoffset(lsm_db *pDb){
  u32 nCkpt;

  nCkpt = pDb->aSnapshot[CKPT_HDR_NCKPT];
  assert( nCkpt>CKPT_HDR_NCKPT );
  assert( nCkpt==pDb->pShmhdr->aClient[CKPT_HDR_NCKPT] );
  assert( 0==memcmp(pDb->aSnapshot, pDb->pShmhdr->aClient, nCkpt*sizeof(u32)) );
  assert( 0==memcmp(pDb->aSnapshot, pDb->pShmhdr->aWorker, nCkpt*sizeof(u32)) );

  pDb->aSnapshot[CKPT_HDR_LO_MSW] = 0;
  pDb->aSnapshot[CKPT_HDR_LO_LSW] = 0;
  ckptChecksum(pDb->aSnapshot, nCkpt, 
      &pDb->aSnapshot[nCkpt-2], &pDb->aSnapshot[nCkpt-1]
  );

  memcpy(pDb->pShmhdr->aClient, pDb->aSnapshot, nCkpt*sizeof(u32));
  memcpy(pDb->pShmhdr->aWorker, pDb->aSnapshot, nCkpt*sizeof(u32));
}

