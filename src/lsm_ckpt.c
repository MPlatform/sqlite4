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
**
**   Log pointer:
**
**     4 integers. See ckptExportLog() and ckptImportLog().
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

/* Total number of 32-bit integers in the checkpoint header. */
#define CKPT_HDRSIZE      7

/* A #define to describe each integer in the checkpoint header. */
#define CKPT_HDR_ID_MSW   0
#define CKPT_HDR_ID_LSW   1
#define CKPT_HDR_NCKPT    2
#define CKPT_HDR_NBLOCK   3
#define CKPT_HDR_BLKSZ    4
#define CKPT_HDR_NLEVEL   5
#define CKPT_HDR_PGSZ     6

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
  lsm_env *pEnv;
  int nAlloc;
  u32 *aCkpt;
};

static void ckptSetValue(CkptBuffer *p, int iIdx, u32 iVal, int *pRc){
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

static void ckptChangeEndianness(u32 *a, int n){
  if( LSM_LITTLE_ENDIAN ){
    int i;
    for(i=0; i<n; i++) a[i] = BYTESWAP32(a[i]);
  }
}

static void ckptAddChecksum(CkptBuffer *p, int nCkpt, int *pRc){
  if( *pRc==LSM_OK ){
    u32 aCksum[2] = {0, 0};
    ckptChangeEndianness(p->aCkpt, nCkpt);
    lsmChecksumBytes((u8 *)p->aCkpt, sizeof(u32)*nCkpt, 0, aCksum);
    ckptChangeEndianness(aCksum, 2);
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
  Level *pLevel,
  CkptBuffer *p,
  int *piOut,
  int *pRc
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
  }

  *piOut = iOut;
}

/*
** Write the current log offset into the checkpoint buffer. 4 values.
*/
static void ckptExportLog(DbLog *pLog, CkptBuffer *p, int *piOut, int *pRc){
  int iOut = *piOut;
  i64 iOff = pLog->aRegion[2].iEnd;

  ckptSetValue(p, iOut++, (iOff >> 32) & 0xFFFFFFFF, pRc);
  ckptSetValue(p, iOut++, (iOff & 0xFFFFFFFF), pRc);
  ckptSetValue(p, iOut++, pLog->cksum0, pRc);
  ckptSetValue(p, iOut++, pLog->cksum1, pRc);

  *piOut = iOut;
}

/*
** Import a log offset.
*/
static void ckptImportLog(u32 *aIn, int *piIn, DbLog *pLog){
  int iIn = *piIn;

  /* TODO: Look at this again after updating lsmLogRecover() */
  pLog->aRegion[2].iStart = (((i64)aIn[iIn]) << 32) + (i64)aIn[iIn+1];
  pLog->cksum0 = aIn[iIn+2];
  pLog->cksum1 = aIn[iIn+3];

  *piIn = iIn+4;
}

lsm_i64 lsmCheckpointLogOffset(void *pExport){
  u8 *aIn = (u8 *)pExport;
  u32 i1;
  u32 i2;
  i1 = lsmGetU32(&aIn[CKPT_HDRSIZE*4]);
  i2 = lsmGetU32(&aIn[CKPT_HDRSIZE*4+4]);
  return (((i64)i1) << 32) + (i64)i2;
}


int lsmCheckpointExport( 
  lsm_db *pDb,                    /* Connection handle */
  int nHdrLevel,                  /* Number of levels to store in checkpoint */
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
  int i;                          /* Iterator used for several purposes */
  Level *pLevel;                  /* Level iterator */
  u32 aDelta[LSM_FREELIST_DELTA_SIZE];

  CkptBuffer ckpt;
  memset(&ckpt, 0, sizeof(CkptBuffer));
  ckpt.pEnv = pDb->pEnv;
  iOut = CKPT_HDRSIZE;

  /* Write the current log offset */
  ckptExportLog(lsmDatabaseLog(pDb), &ckpt, &iOut, &rc);

  /* Export up to nHdrLevel levels. Or, if nHdrLevel==0, export all levels. */
  for(pLevel=lsmDbSnapshotLevel(pSnap); 
      pLevel && (nHdrLevel==0 || nLevel<nHdrLevel); 
      pLevel=pLevel->pNext
  ){
    ckptExportLevel(pLevel, &ckpt, &iOut, &rc);
    nLevel++;
  }

  /* Write the freelist delta */
  lsmFreelistDelta(pDb, aDelta);
  for(i=0; i<LSM_FREELIST_DELTA_SIZE; i++){
    ckptSetValue(&ckpt, iOut++, aDelta[i], &rc);
  }

  /* Write the checkpoint header */
  assert( iId>=0 );
  ckptSetValue(&ckpt, CKPT_HDR_ID_MSW, (u32)(iId>>32), &rc);
  ckptSetValue(&ckpt, CKPT_HDR_ID_LSW, (u32)(iId&0xFFFFFFFF), &rc);
  ckptSetValue(&ckpt, CKPT_HDR_NCKPT, iOut+2, &rc);
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

static int ckptImport(lsm_db *pDb, void *pCkpt, int nInt, int *pRc){
  int ret = 0;
  if( *pRc==LSM_OK ){
    Snapshot *pSnap = pDb->pWorker;
    u32 cksum[2] = {0, 0};
    u32 *aInt = (u32 *)pCkpt;

    lsmChecksumBytes((u8 *)aInt, sizeof(u32)*(nInt-2), 0, cksum);
    if( LSM_LITTLE_ENDIAN ){
      int i;
      for(i=0; i<nInt; i++) aInt[i] = BYTESWAP32(aInt[i]);
    }

    if( aInt[nInt-2]==cksum[0] && aInt[nInt-1]==cksum[1] ){
      int i;
      int nLevel;
      int iIn = CKPT_HDRSIZE;
      i64 iId;
      u32 *aDelta;

      Level *pTopLevel = 0;

      /* Read header fields */
      iId = ((i64)aInt[CKPT_HDR_ID_MSW] << 32) + (i64)aInt[CKPT_HDR_ID_LSW];
      lsmSnapshotSetCkptid(pSnap, iId);
      nLevel = (int)aInt[CKPT_HDR_NLEVEL];
      lsmSnapshotSetNBlock(pSnap, (int)aInt[CKPT_HDR_NBLOCK]);
      lsmDbSetPagesize(pDb,(int)aInt[CKPT_HDR_PGSZ],(int)aInt[CKPT_HDR_BLKSZ]);

      /* Import log offset */
      ckptImportLog(aInt, &iIn, lsmDatabaseLog(pDb));

      /* Import all levels stored in the checkpoint. */
      *pRc = ckptLoadLevels(pDb, aInt, &iIn, nLevel, &pTopLevel);
      lsmDbSnapshotSetLevel(pSnap, pTopLevel);

      /* Import the freelist delta */
      aDelta = lsmFreelistDeltaPtr(pDb);
      for(i=0; i<LSM_FREELIST_DELTA_SIZE; i++){
        aDelta[i] = aInt[iIn++];
      }

      ret = 1;
    }

    assert( *pRc!=LSM_OK || lsmFsIntegrityCheck(pDb) );
  }
  return ret;
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
    MetaPage *pPg;                    /* Meta page for slot iSlot */
    *pRc = lsmFsMetaPageGet(pDb->pFS, 0, iSlot, &pPg);
    if( *pRc==LSM_OK ){
      u8 *aData = lsmFsMetaPageData(pPg, 0);

      iId = (i64)lsmGetU32(&aData[CKPT_HDR_ID_MSW*4]) << 32;
      iId += (i64)lsmGetU32(&aData[CKPT_HDR_ID_LSW*4]);
      *pnInt = (int)lsmGetU32(&aData[CKPT_HDR_NCKPT*4]);

      lsmFsMetaPageRelease(pPg);
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
        MetaPage *pPg;
        rc = lsmFsMetaPageGet(pDb->pFS, 0, iPg, &pPg);
        if( rc==LSM_OK ){
          int nCopy;
          int nData;
          u8 *aData = lsmFsMetaPageData(pPg, &nData);

          nCopy = LSM_MIN(nRem, nData);
          memcpy(aRem, aData, nCopy);
          aRem += nCopy;
          nRem -= nCopy;
          lsmFsMetaPageRelease(pPg);
        }
        iPg += 2;
      }

      ret = ckptImport(pDb, aCkpt, nCkpt, &rc);
      lsmFree(pDb->pEnv, aCkpt);
      *pRc = rc;
    }
  }

  return ret;
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
  int *pnHdrLevel,                /* OUT: Levels to write to db header */
  void **paVal,                   /* OUT: Pointer to LEVELS blob */
  int *pnVal                      /* OUT: Size of LEVELS blob in bytes */
){
  int rc = LSM_OK;                /* Return code */
  const int SEGMENT_SIZE = 4;     /* Size of a checkpoint segment record */
  Level *p;                       /* Used to iterate through levels */
  int nFree;                      /* Free integers remaining in db header */
  int nHdr = 0;                   /* Number of levels stored in db header */
  int nLevels = 0;                /* Number of levels stored in LEVELS */
 
  /* Number of free integers - 1024 less those used by the checkpoint header,
  ** less the 4 used for the log-pointer, less the 3 used for the 
  ** free-list delta and the 2 used for the checkpoint checksum.  */
  nFree = 1024 - CKPT_HDRSIZE - 4 - 3 - 2;

  /* Each level record not currently undergoing a merge consumes 2 + 6
  ** integers. Each level that is undergoing a merge consumes 2 + 6 +
  ** (nRhs * 6) + 1 + 1 + (nMerge * 2), where nRhs is the number of levels
  ** used as input to the merge and nMerge is the total number of segments
  ** (same as the number of levels, possibly plus 1 separators array). 
  **
  ** The calculation in the following block may overestimate the number
  ** of integers required by a single level by 2 (as it assumes 
  ** that nMerge==nRhs+1).
  */
  for(p=lsmDbSnapshotLevel(pDb->pWorker); p; p=p->pNext){
    int nThis;                    /* Number of integers required by level p */
    if( p->pMerge ){
      nThis = 2 + (1 + p->nRight) * (2 + SEGMENT_SIZE) + 1 + 1;
    }else{
      nThis = 2 + SEGMENT_SIZE;
    }
    if( nFree<nThis ) break;
    nFree -= nThis;
    nHdr++;
  }

  if( p ){
    int iOut;
    CkptBuffer ckpt;

    memset(&ckpt, 0, sizeof(CkptBuffer));
    ckpt.pEnv = pDb->pEnv;
    iOut = 1;
    do{
      nLevels++;
      ckptExportLevel(p, &ckpt, &iOut, &rc);
    } while( 0!=(p = p->pNext) );
    ckptSetValue(&ckpt, 0, (u32)nLevels, &rc);

    ckptChangeEndianness(ckpt.aCkpt, iOut);
    *paVal = (void *)ckpt.aCkpt;
    *pnVal = iOut * sizeof(u32);
  }else{
    *pnVal = 0;
    *paVal = 0;
  }

  *pnHdrLevel = nHdr;
  return rc;
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
