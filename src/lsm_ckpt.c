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
**     4. Log file page to begin recovery at.
**     5. Log file salt 1.
**     6. Log file salt 2.
**     7. The total number of blocks in the database.
**     8. The block size.
**     9. The number of levels.
**     10. The nominal database page size.
**
**   For each level in the database, a level record. Formatted as follows:
**
**     1. The number of right-hand segments (possibly 0),
**     2. Segment record for left-hand segment (6 integers),
**     3. Segment record for each right-hand segment (6 integers),
**     4. If nRight>0, The number of segments involved in the merge,
**     5. Current nSkip value (see Merge structure defn.),
**     6. For each segment in the merge:
**        5a. Page number of next cell to read during merge
**        5b. Cell number of next cell to read during merge
**
**   A list of all append-points in the database file:
**
**     1. The number (possibly 0) of append points in the file.
**     2. The page number of each append point.
**
**   The freelist delta. Currently consists of (this will change):
**
**     1. The size to truncate the free list to after it is loaded.
**     2. First refree block (or 0),
**     3. Second refree block (or 0),
**
**   The checksum:
**
**     1. Checksum value 1.
**     2. Checksum value 2.
**
** In the above, a segment record is:
**
**     1. First page of main array,
**     2. Last page of main array,
**     3. Size of main array in pages,
**     4. First page of separators array (or 0),
**     5. Last page of separators array (or 0),
**     6. Root page of separators array (or 0).
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

/* Total number of 32-bit integers in the checkpoint header. */
#define CKPT_HDRSIZE      10

/* A #define to describe each integer in the checkpoint header. */
#define CKPT_HDR_ID_MSW   0
#define CKPT_HDR_ID_LSW   1
#define CKPT_HDR_NCKPT    2
#define CKPT_HDR_LOGPGNO  3
#define CKPT_HDR_LOGSALT1 4
#define CKPT_HDR_LOGSALT2 5
#define CKPT_HDR_NBLOCK   6
#define CKPT_HDR_BLKSZ    7
#define CKPT_HDR_NLEVEL   8
#define CKPT_HDR_PGSZ     9

/*
** Generate or extend an 8 byte checksum based on the data in array aByte[]
** and the initial values of aIn[0] and aIn[1] (or initial values of 0 and 
** 0 if aIn==NULL).
**
** The checksum is written back into aOut[] before returning.
*/
void lsmChecksumBytes(
  const u8 *a,     /* Content to be checksummed */
  int nByte,       /* Bytes of content in a[] */
  const u32 *aIn,  /* Initial checksum value input */
  u32 *aOut        /* OUT: Final checksum value output */
){
  u32 s1, s2;
  u32 *aData = (u32 *)a;
  u32 *aEnd = (u32 *)&a[nByte & ~0x00000007];

  u32 aExtra[2] = {0, 0};
  memcpy(aExtra, &a[nByte & ~0x00000007], nByte & 0x00000007);

  if( aIn ){
    s1 = aIn[0];
    s2 = aIn[1];
  }else{
    s1 = s2 = 0;
  }

  if( LSM_LITTLE_ENDIAN ){
    /* little-endian */
    s1 += aExtra[0] + s2;
    s2 += aExtra[1] + s1;
    while( aData<aEnd ){
      s1 += *aData++ + s2;
      s2 += *aData++ + s1;
    }
  }else{
    /* big-endian */
    s1 += BYTESWAP32(aExtra[0]) + s2;
    s2 += BYTESWAP32(aExtra[1]) + s1;
    while( aData<aEnd ){
      s1 += BYTESWAP32(aData[0]) + s2;
      s2 += BYTESWAP32(aData[1]) + s1;
      aData += 2;
    }
  }

  aOut[0] = s1;
  aOut[1] = s2;
}

typedef struct CkptBuffer CkptBuffer;
struct CkptBuffer {
  int nAlloc;
  u32 *aCkpt;
};

static void ckptSetValue(CkptBuffer *p, int iIdx, u32 iVal, int *pRc){
  if( iIdx>=p->nAlloc ){
    int nNew = MAX(8, iIdx*2);
    p->aCkpt = (u32 *)lsmReallocOrFree(0, p->aCkpt, nNew*sizeof(u32));
    if( !p->aCkpt ){
      *pRc = LSM_NOMEM_BKPT;
      return;
    }
    p->nAlloc = nNew;
  }
  p->aCkpt[iIdx] = iVal;
}

static void ckptAddChecksum(CkptBuffer *p, int nCkpt, int *pRc){
  if( *pRc==LSM_OK ){
    u32 aCksum[2] = {0, 0};
    if( LSM_LITTLE_ENDIAN ){
      int i;
      for(i=0; i<nCkpt; i++) p->aCkpt[i] = BYTESWAP32(p->aCkpt[i]);
    }
    lsmChecksumBytes((u8 *)p->aCkpt, sizeof(u32)*nCkpt, 0, aCksum);
    if( LSM_LITTLE_ENDIAN ){
      aCksum[0] = BYTESWAP32(aCksum[0]);
      aCksum[1] = BYTESWAP32(aCksum[1]);
    }
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

  ckptSetValue(p, iOut++, pSeg->run.iFirst, pRc);
  ckptSetValue(p, iOut++, pSeg->run.iLast, pRc);
  ckptSetValue(p, iOut++, pSeg->run.nSize, pRc);
  if( segmentHasSeparators(pSeg) ){
    ckptSetValue(p, iOut++, pSeg->sep.iFirst, pRc);
    ckptSetValue(p, iOut++, pSeg->sep.iLast, pRc);
    ckptSetValue(p, iOut++, pSeg->sep.iRoot, pRc);
  }else{
    ckptSetValue(p, iOut++, 0, pRc);
    ckptSetValue(p, iOut++, 0, pRc);
    ckptSetValue(p, iOut++, 0, pRc);
  }

  *piOut = iOut;
}

int lsmCheckpointExport( 
  lsm_db *pDb,                    /* Connection handle */
  i64 iId,                        /* Checkpoint id */
  int bCksum,                     /* If true, include checksums */
  void **ppCkpt,                  /* OUT: Buffer containing checkpoint */
  int *pnCkpt                     /* OUT: Size of checkpoint in bytes */
){
  int rc = LSM_OK;                /* Return Code */
  FileSystem *pFS = pDb->pFS;     /* File system object */
  Snapshot *pSnap = pDb->pWorker; /* Worker snapshot */
  u32 nLevel = 0;                 /* Number of levels in db file */
  int iOut = 0;                   /* Current offset in aCkpt[] */
  Level *pNext = 0;               /* Used to help iterate through levels */
  Level *pTopLevel;               /* Top level of database snapshot */
  IList *pAppend;                 /* Pointer to append-point list */
  int i;                          /* Iterator used for several purposes */
  u32 aDelta[LSM_FREELIST_DELTA_SIZE];

  u32 iSalt1;
  u32 iSalt2;

  CkptBuffer ckpt;
  memset(&ckpt, 0, sizeof(CkptBuffer));
  iOut = CKPT_HDRSIZE;

  pTopLevel = lsmDbSnapshotLevel(pSnap);
  while( pNext!=pTopLevel ){
    Level *pLevel;                /* This level */
    Merge *pMerge;

    for(pLevel=pTopLevel; pLevel->pNext!=pNext; pLevel=pLevel->pNext);
    pMerge = pLevel->pMerge;
    assert( (pLevel->nRight>0)==(pMerge!=0) );

    ckptSetValue(&ckpt, iOut++, pLevel->nRight, &rc);
    ckptExportSegment(&pLevel->lhs, &ckpt, &iOut, &rc);
    if( pMerge ){

      for(i=0; i<pLevel->nRight; i++){
        ckptExportSegment(&pLevel->aRhs[i], &ckpt, &iOut, &rc);
      }

      assert( pMerge->nInput==pLevel->nRight 
           || pMerge->nInput==pLevel->nRight+1 
      );

      ckptSetValue(&ckpt, iOut++, pMerge->nInput, &rc);
      ckptSetValue(&ckpt, iOut++, pMerge->nSkip, &rc);
      for(i=0; i<pMerge->nInput; i++){
        ckptSetValue(&ckpt, iOut++, pMerge->aInput[i].iPg, &rc);
        ckptSetValue(&ckpt, iOut++, pMerge->aInput[i].iCell, &rc);
      }
    }

    pNext = pLevel;
    nLevel++;
  }

  /* Export the append-point list */
  pAppend = lsmSnapshotList(pSnap, LSM_APPEND_LIST);
  ckptSetValue(&ckpt, iOut++, pAppend->n, &rc);
  for(i=0; i<pAppend->n; i++){
    ckptSetValue(&ckpt, iOut++, pAppend->a[i], &rc);
  }

  /* Write the freelist delta */
  lsmFreelistDelta(pDb, aDelta);
  for(i=0; i<LSM_FREELIST_DELTA_SIZE; i++){
    ckptSetValue(&ckpt, iOut++, aDelta[i], &rc);
  }

  /* Write the checkpoint header */
  assert( iId>=0 );
  lsmSnapshotGetSalt(pSnap, &iSalt1, &iSalt2);
  ckptSetValue(&ckpt, CKPT_HDR_ID_MSW, (u32)(iId>>32), &rc);
  ckptSetValue(&ckpt, CKPT_HDR_ID_LSW, (u32)(iId&0xFFFFFFFF), &rc);
  ckptSetValue(&ckpt, CKPT_HDR_NCKPT, iOut+2, &rc);
  ckptSetValue(&ckpt, CKPT_HDR_LOGPGNO, lsmSnapshotGetLogpgno(pSnap), &rc);
  ckptSetValue(&ckpt, CKPT_HDR_LOGSALT1, iSalt1, &rc);
  ckptSetValue(&ckpt, CKPT_HDR_LOGSALT2, iSalt2, &rc);
  ckptSetValue(&ckpt, CKPT_HDR_NBLOCK, lsmSnapshotGetNBlock(pSnap), &rc);
  ckptSetValue(&ckpt, CKPT_HDR_BLKSZ, lsmFsBlockSize(pFS), &rc);
  ckptSetValue(&ckpt, CKPT_HDR_NLEVEL, nLevel, &rc);
  ckptSetValue(&ckpt, CKPT_HDR_PGSZ, lsmFsPageSize(pFS), &rc);

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

  assert( pSegment->run.iFirst==0 && pSegment->run.iLast==0 );
  assert( pSegment->run.nSize==0 && pSegment->run.iRoot==0 );
  assert( pSegment->sep.iFirst==0 && pSegment->sep.iLast==0 );
  assert( pSegment->sep.nSize==0 && pSegment->sep.iRoot==0 );

  pSegment->run.iFirst = aIn[iIn++];
  pSegment->run.iLast = aIn[iIn++];
  pSegment->run.nSize = aIn[iIn++];
  pSegment->sep.iFirst = aIn[iIn++];
  pSegment->sep.iLast = aIn[iIn++];
  pSegment->sep.iRoot = aIn[iIn++];
  if( pSegment->sep.iFirst ) pSegment->sep.nSize = 1;

  *piIn = iIn;
}

static int ckptSetupMerge(u32 *aInt, int *piIn, Level *pLevel){
  Merge *pMerge;                  /* Allocated Merge object */
  int nInput;                     /* Number of input segments in merge */
  int iIn = *piIn;                /* Next value to read from aInt[] */
  int i;                          /* Iterator variable */

  /* Allocate the Merge object. If malloc() fails, return LSM_NOMEM. */
  nInput = (int)aInt[iIn++];
  pMerge = (Merge *)lsmMallocZero(0, sizeof(Merge) + sizeof(MergeInput) * nInput);
  if( !pMerge ) return LSM_NOMEM_BKPT;
  pLevel->pMerge = pMerge;

  /* Populate the Merge object. */
  pMerge->aInput = (MergeInput *)&pMerge[1];
  pMerge->nInput = nInput;
  pMerge->aiOutputOff[0] = -1;
  pMerge->aiOutputOff[1] = -1;
  pMerge->nSkip = (int)aInt[iIn++];
  for(i=0; i<nInput; i++){
    pMerge->aInput[i].iPg = (Pgno)aInt[iIn++];
    pMerge->aInput[i].iCell = (int)aInt[iIn++];
  }

  /* Set *piIn and return LSM_OK. */
  *piIn = iIn;
  return LSM_OK;
}

int ckptImport(lsm_db *pDb, void *pCkpt, int nInt, int *pRc){
  int ret = 0;
  Snapshot *pSnap = pDb->pWorker;
  FileSystem *pFS = pDb->pFS;
  u32 cksum[2] = {0, 0};
  u32 *aInt = (u32 *)pCkpt;

  lsmChecksumBytes((u8 *)aInt, sizeof(u32)*(nInt-2), 0, cksum);
  if( LSM_LITTLE_ENDIAN ){
    int i;
    for(i=0; i<nInt; i++) aInt[i] = BYTESWAP32(aInt[i]);
  }

  if( aInt[nInt-2]==cksum[0] && aInt[nInt-1]==cksum[1] ){
    int rc = LSM_OK;
    int i;
    int nLevel;
    int iIn = CKPT_HDRSIZE;
    i64 iId;
    u32 *aDelta;

    /* Read header fields */
    iId = ((i64)aInt[CKPT_HDR_ID_MSW] << 32) + (i64)aInt[CKPT_HDR_ID_LSW];
    lsmSnapshotSetCkptid(pSnap, iId);
    nLevel = (int)aInt[CKPT_HDR_NLEVEL];
    lsmSnapshotSetNBlock(pSnap, (int)aInt[CKPT_HDR_NBLOCK]);
    lsmSnapshotSetLogpgno(pSnap, aInt[CKPT_HDR_LOGPGNO]);
    lsmSnapshotSetSalt(pSnap, aInt[CKPT_HDR_LOGSALT1], aInt[CKPT_HDR_LOGSALT2]);
    lsmFsSetPageSize(pFS, (int)aInt[CKPT_HDR_PGSZ]);
    lsmFsSetBlockSize(pFS, (int)aInt[CKPT_HDR_BLKSZ]);

    /* Import each level. This loop runs once for each db level. */
    for(i=0; rc==LSM_OK && i<nLevel; i++){
      int iRight;
      Level *pLevel;

      /* Allocate space for the Level structure and Level.apRight[] array */
      pLevel = (Level *)lsmMallocZeroRc(pDb->pEnv, sizeof(Level), &rc);
      if( rc==LSM_OK ){
        pLevel->nRight = aInt[iIn++];
        if( pLevel->nRight ){
          int nByte = sizeof(Segment) * pLevel->nRight;
          pLevel->aRhs = (Segment *)lsmMallocZeroRc(pDb->pEnv, nByte, &rc);
        }

        pLevel->pNext = lsmDbSnapshotLevel(pSnap);
        lsmDbSnapshotSetLevel(pSnap, pLevel);
      }

      if( rc==LSM_OK ){
        /* Allocate the main segment */
        ckptNewSegment(aInt, &iIn, &pLevel->lhs);

        /* Allocate each of the right-hand segments, if any */
        for(iRight=0; iRight<pLevel->nRight; iRight++){
          ckptNewSegment(aInt, &iIn, &pLevel->aRhs[iRight]);
        }

        /* Set up the Merge object, if required */
        if( pLevel->nRight>0 ){
          ckptSetupMerge(aInt, &iIn, pLevel);
        }
      }
    }

    /* Import the append list */
    if( rc==LSM_OK ){
      int nApp = aInt[iIn++];
      IList *pAppend = lsmSnapshotList(pSnap, LSM_APPEND_LIST);
      rc = lsmIListSet(pAppend, (int *)&aInt[iIn], nApp);
      iIn += nApp;
    }

    /* Import the freelist delta */
    aDelta = lsmFreelistDeltaPtr(pDb);
    for(i=0; i<LSM_FREELIST_DELTA_SIZE; i++){
      aDelta[i] = aInt[iIn++];
    }

    ret = 1;
  }
 
  assert( *pRc!=LSM_OK || lsmFsIntegrityCheck(pDb) );
  return ret;
}


/*
** If *pRc is not LSM_OK when this function is called, it is a no-op. 
** 
** Otherwise, it attempts to read the id and size of the checkpoint stored in
** slot iSlot of the database header. If an error occurs during processing, 
** *pRc is set to an error code before returning. The returned value is 
** always zero in this case.
**
** Or, if no error occurs, set *pnInt to the total number of integer values
** in the checkpoint and return the checkpoint id.
*/
static i64 ckptReadId(
  lsm_db *pDb,                    /* Connection handle */
  int iSlot,                      /* Slot to read from (1 or 2) */
  int *pnInt,                     /* OUT: Size of slot checkpoint in ints */
  int *pRc                        /* IN/OUT: Error code */
){
  i64 iId = 0;                    /* Checkpoint id (return value) */

  assert( iSlot==1 || iSlot==2 );
  if( *pRc==LSM_OK ){
    Page *pPg;                    /* Meta page for slot iSlot */
    *pRc = lsmFsMetaPageGet(pDb->pFS, iSlot, &pPg);
    if( *pRc==LSM_OK ){
      u8 *aData = lsmFsPageData(pPg, 0);

      iId = (i64)lsmGetU32(&aData[CKPT_HDR_ID_MSW*4]) << 32;
      iId += (i64)lsmGetU32(&aData[CKPT_HDR_ID_LSW*4]);
      *pnInt = (int)lsmGetU32(&aData[CKPT_HDR_NCKPT*4]);

      lsmFsPageRelease(pPg);
    }
  }
  return iId;
}

/*
** Attempt to load the checkpoint from slot iSlot. Return true if the
** attempt is successful.
*/
static int ckptTryRead(
  lsm_db *pDb, 
  int iSlot, 
  int nCkpt, 
  int *pRc
){
  int ret = 0;
  assert( iSlot==1 || iSlot==2 );
  if( *pRc==LSM_OK 
   && nCkpt>=CKPT_HDRSIZE
   && nCkpt<65536 
  ){
    u32 *aCkpt;
    aCkpt = (u32 *)lsmMallocZeroRc(pDb->pEnv, sizeof(u32)*nCkpt, pRc);
    if( aCkpt ){
      int rc = LSM_OK;
      int iPg;
      int nRem;
      u8 *aRem;

      /* Read the checkpoint data. */
      nRem = sizeof(u32) * nCkpt;
      aRem = (u8 *)aCkpt;
      iPg = iSlot;
      while( rc==LSM_OK && nRem ){
        Page *pPg;
        rc = lsmFsMetaPageGet(pDb->pFS, iPg, &pPg);
        if( rc==LSM_OK ){
          int nCopy;
          int nData;
          u8 *aData = lsmFsPageData(pPg, &nData);

          nCopy = MIN(nRem, nData);
          memcpy(aRem, aData, nCopy);
          aRem += nCopy;
          nRem -= nCopy;
          lsmFsPageRelease(pPg);
        }
        iPg += 2;
      }

      *pRc = rc;
      ret = ckptImport(pDb, aCkpt, nCkpt, &rc);
      lsmFree(pDb->pEnv, aCkpt);
    }
  }

  return ret;
}


int lsmCheckpointRead(lsm_db *pDb){
  int rc = LSM_OK;                /* Return Code */

  i64 iId1;
  i64 iId2;
  int nInt1;
  int nInt2;
  int bLoaded = 0;

  iId1 = ckptReadId(pDb, 1, &nInt1, &rc);
  iId2 = ckptReadId(pDb, 2, &nInt2, &rc);

  if( iId1>=iId2 ){
    bLoaded = ckptTryRead(pDb, 1, nInt1, &rc);
    if( bLoaded==0 ){
      bLoaded = ckptTryRead(pDb, 2, nInt2, &rc);
    }
  }else{
    bLoaded = ckptTryRead(pDb, 2, nInt2, &rc);
    if( bLoaded==0 ){
      bLoaded = ckptTryRead(pDb, 1, nInt1, &rc);
    }
  }

  return rc;
}
