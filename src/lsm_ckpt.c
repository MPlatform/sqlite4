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
**     8. Flag indicating if overflow records are used. If true, the top-level
**        segment contains LEVELS and FREELIST entries. 
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
** OVERSIZED CHECKPOINT BLOBS:
**
** There are two slots allocated for checkpoints at the start of each
** database file. Each are 4096 bytes in size, so may accommodate
** checkpoints that consist of up to 1024 32-bit integers. Normally,
** this is enough.
**
** However, if a database contains a sufficiently large number of levels,
** a checkpoint may exceed 1024 integers in size. In most circumstances this 
** is an undesirable scenario, as a database with so many levels will be 
** slow to query. If this does happen, then only the uppermost (more recent)
** levels are stored in the checkpoint blob itself. The remainder are stored
** in an LSM record with the system key "LEVELS". The payload of the entry
** is a series of 32-bit big-endian integers, as follows:
**
**    1. Number of levels (store in the LEVELS record, not total).
**    2. For each level, a "level record" (as desribed above).
**
** There is no checksum in the LEVELS record.
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

int lsmCheckpointExport( 
  lsm_db *pDb,                    /* Connection handle */
  int nLsmLevel,                  /* Number of levels to store in LSM */
  int bOvfl,                      /* True if free list is stored in LSM */
  int bLog,                       /* True to update log-offset fields */
  i64 iId,                        /* Checkpoint id */
  int bCksum,                     /* If true, include checksums */
  void **ppCkpt,                  /* OUT: Buffer containing checkpoint */
  int *pnCkpt                     /* OUT: Size of checkpoint in bytes */
){
  int rc = LSM_OK;                /* Return Code */
  FileSystem *pFS = pDb->pFS;     /* File system object */
  Snapshot *pSnap = pDb->pWorker; /* Worker snapshot */
  int nAll = 0;                   /* Number of levels in db */
  int nHdrLevel = 0;              /* Number of levels in checkpoint */
  int iLevel;                     /* Used to count out nHdrLevel levels */
  int iOut = 0;                   /* Current offset in aCkpt[] */
  Level *pLevel;                  /* Level iterator */
  int i;                          /* Iterator used while serializing freelist */
  CkptBuffer ckpt;

  assert( bOvfl || nLsmLevel==0 );
  
  /* Initialize the output buffer */
  memset(&ckpt, 0, sizeof(CkptBuffer));
  ckpt.pEnv = pDb->pEnv;
  iOut = CKPT_HDR_SIZE;

  /* Write the log offset into the checkpoint. */
  ckptExportLog(pDb, bLog, &ckpt, &iOut, &rc);

  /* Write the append-point list */
  ckptExportAppendlist(pDb, &ckpt, &iOut, &rc);

  /* Figure out how many levels will be written to the checkpoint. */
  for(pLevel=lsmDbSnapshotLevel(pSnap); pLevel; pLevel=pLevel->pNext) nAll++;
  nHdrLevel = nAll - nLsmLevel;
  assert( nHdrLevel>0 );

  /* Serialize nHdrLevel levels. */
  iLevel = 0;
  for(pLevel=lsmDbSnapshotLevel(pSnap); iLevel<nHdrLevel; pLevel=pLevel->pNext){
    ckptExportLevel(pLevel, &ckpt, &iOut, &rc);
    iLevel++;
  }

  /* Write the freelist delta (if bOvfl is true) or else the entire free-list
  ** (if bOvfl is false).  */
  assert( bOvfl==0 );             /* TODO: Fix this */
  if( rc==LSM_OK ){
    int nFree = pSnap->freelist.nEntry;
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
  ckptSetValue(&ckpt, CKPT_HDR_NLEVEL, nHdrLevel, &rc);
  ckptSetValue(&ckpt, CKPT_HDR_PGSZ, lsmFsPageSize(pFS), &rc);
  ckptSetValue(&ckpt, CKPT_HDR_OVFL, bOvfl, &rc);

  if( bCksum ){
    ckptAddChecksum(&ckpt, iOut, &rc);
  }else{
    ckptSetValue(&ckpt, iOut, 0, &rc);
    ckptSetValue(&ckpt, iOut+1, 0, &rc);
  }
  iOut += 2;
  assert( iOut<=1024 );

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
** The function is used to determine if the FREELIST and LEVELS overflow
** records may be required if a new top level segment is written and a
** serialized checkpoint blob created. 
**
** If the checkpoint will definitely fit in a single meta page, 0 is 
** returned and *pnLsmLevel is set to 0. In this case the caller need not
** bother creating FREELIST and LEVELS records. 
**
** Or, if it is likely that the overflow records will be required, non-zero
** is returned.
*/
int lsmCheckpointOverflow(
  lsm_db *pDb,                    /* Database handle (must hold worker lock) */
  int *pnLsmLevel                 /* OUT: Number of levels to store in LSM */
){
  Level *p;                       /* Used to iterate through levels */
  int nFree;                      /* Free integers remaining in db header */
  int nList;                      /* Size of freelist in integers */
  int nLevel = 0;                 /* Number of levels stored in LEVELS */
 
  /* Number of free integers - 1024 less those used by the checkpoint header,
  ** less the 4 used for the log-pointer, less the 3 used for the free-list 
  ** delta and the 2 used for the checkpoint checksum. Value nFree is 
  ** therefore the total number of integers available to store the database 
  ** levels and freelist.  */
  nFree = 1024 - CKPT_HDR_SIZE - CKPT_LOGPTR_SIZE - CKPT_CKSUM_SIZE;
  nFree -= CKPT_APPENDLIST_SIZE;

  /* Allow space for the free-list  with LSM_CKPT_MIN_FREELIST entries */
  nFree--;
  nFree -= LSM_CKPT_MIN_FREELIST * 3;

  /* Allow space for the new level that may be created */
  nFree -= (2 + CKPT_SEGMENT_SIZE);

  /* Each level record not currently undergoing a merge consumes 2 + 4
  ** integers. Each level that is undergoing a merge consumes 2 + 4 +
  ** (nRhs * 4) + 1 + 1 + (nMerge * 2) + 2, where nRhs is the number of levels
  ** used as input to the merge and nMerge is the total number of segments
  ** (same as the number of levels, possibly plus 1 separators array). 
  **
  ** The calculation in the following block may overestimate the number
  ** of integers required by a single level by 2 (as it assumes 
  ** that nMerge==nRhs+1).  */
  for(p=lsmDbSnapshotLevel(pDb->pWorker); p; p=p->pNext){
    int nThis;                    /* Number of integers required by level p */
    if( p->pMerge ){
      nThis = 2 + (1 + p->nRight) * (2 + CKPT_SEGMENT_SIZE) + 1 + 1 + 2;
    }else{
      nThis = 2 + CKPT_SEGMENT_SIZE;
    }
    if( nFree<nThis ) break;
    nFree -= nThis;
  }

  /* Count the levels that will not fit in the checkpoint record. */
  while( p ){
    nLevel++;
    p = p->pNext;
  }
  *pnLsmLevel = nLevel;

  /* Set nList to the number of values required to store the free-list 
  ** completely in the checkpoint.  */
  nList = 1 + 3*pDb->pWorker->freelist.nEntry;
  nFree -= nList;

  return (nLevel>0 || nFree<0);
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

  if( nCkpt<CKPT_HDR_NCKPT ) return 0;
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

    rc = lsmShmLock(pDb, LSM_LOCK_WORKER, LSM_LOCK_EXCL);
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
        lsmShmLock(pDb, LSM_LOCK_WORKER, LSM_LOCK_UNLOCK);

        if( ckptChecksumOk(pDb->aSnapshot)==0 ){
          rc = LSM_CORRUPT_BKPT;
        }
      }
      return rc;
    }
  }
}

int lsmCheckpointLoadWorker(lsm_db *pDb){
  ShmHeader *pShm = pDb->pShmhdr;

  /* Must be holding the WORKER lock to do this */
  assert( lsmShmAssertLock(pDb, LSM_LOCK_WORKER, LSM_LOCK_EXCL) );

  if( ckptChecksumOk(pShm->aWorker)==0 ){
    int nInt = (int)pShm->aClient[CKPT_HDR_NCKPT];
    memcpy(pShm->aWorker, pShm->aClient, nInt*sizeof(u32));
    if( ckptChecksumOk(pShm->aWorker)==0 ) return LSM_CORRUPT_BKPT;
  }
  return lsmCheckpointDeserialize(pDb, pShm->aWorker, &pDb->pWorker);
}

int lsmCheckpointDeserialize(
  lsm_db *pDb, 
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
    nFree = aCkpt[iIn++];
    if( nFree ){
      pNew->freelist.aEntry = (FreelistEntry *)lsmMallocRc(
          pDb->pEnv, sizeof(FreelistEntry)*nFree, &rc
      );
      if( rc==LSM_OK ){
        int i;
        for(i=0; i<nFree; i++){
          pNew->freelist.aEntry[i].iBlk = aCkpt[iIn++];
          pNew->freelist.aEntry[i].iId = ((i64)(aCkpt[iIn])<<32) + aCkpt[iIn+1];
          iIn += 2;
        }
        pNew->freelist.nEntry = pNew->freelist.nAlloc = nFree;
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
int lsmCheckpointSaveWorker(lsm_db *pDb, int bFlush){
  Snapshot *pSnap = pDb->pWorker;
  ShmHeader *pShm = pDb->pShmhdr;
  void *p = 0;
  int n = 0;
  int rc;

  rc = lsmCheckpointExport(pDb, 0, 0, bFlush, pSnap->iId+1, 1, &p, &n);
  if( rc!=LSM_OK ) return rc;
  assert( ckptChecksumOk((u32 *)p) );

  assert( n<=LSM_META_PAGE_SIZE );
  memcpy(pShm->aWorker, p, n);
  lsmShmBarrier(pDb);
  memcpy(pShm->aClient, p, n);
  lsmFree(pDb->pEnv, p);

  return LSM_OK;
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

