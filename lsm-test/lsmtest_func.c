
#include "lsmtest.h"

/*
** An instance of this type is used as an iterator to visit each sorted run
** within a checkpoint integer array. See functions:
**
**     segmentIterInit()
**     segmentIterNext()
*/
typedef struct SegmentIter SegmentIter;
typedef struct Seg Seg;
typedef struct Block Block;

struct Seg {
  int iFirst;
  int iLast;
};

struct Block {
  int nUsed;
  int iLast;
};

struct SegmentIter {
  int iFirst;                     /* First page of current sorted run */
  int iLast;                      /* Last page of current sorted run */

  int *aCkpt;
  Seg *aSeg;
  int nSeg;
  int iSeg;

  int nBlock;                     /* Blocks in file */
  int nBlksz;                     /* Block size */
  int nPgsz;                      /* Nominal page size */

  int nFree;                      /* Size of aFree[] */
  int *aFree;                     /* Array of free blocks */
};

/* Copied from src/checkpoint.c */
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

static void segmentIterInit(int *aCkpt, SegmentIter *pIter){
  int nLevel;
  int iOff;
  int i;
  int iSeg;

  memset(pIter, 0, sizeof(SegmentIter));
  pIter->aCkpt = aCkpt;

  pIter->nBlock = aCkpt[CKPT_HDR_NBLOCK];
  pIter->nBlksz = aCkpt[CKPT_HDR_BLKSZ];
  nLevel = aCkpt[CKPT_HDR_NLEVEL];
  pIter->nPgsz = aCkpt[CKPT_HDR_PGSZ];
  assert( (pIter->nBlksz % pIter->nPgsz)==0 );

  /* Allocate space for segments */
  pIter->aSeg = malloc(128 * sizeof(SegmentIter));

  /* Populate pIter->aSeg[] */
  iOff = 10;
  iSeg = 0;
  for(i=0; i<nLevel; i++){
    int nRight = aCkpt[iOff];

    pIter->aSeg[iSeg].iFirst = aCkpt[iOff+1];
    pIter->aSeg[iSeg].iLast = aCkpt[iOff+2];
    iSeg++;
    if( aCkpt[iOff+4] ){
      pIter->aSeg[iSeg].iFirst = aCkpt[iOff+4];
      pIter->aSeg[iSeg].iLast = aCkpt[iOff+5];
      iSeg++;
    }
    iOff += 7;

    if( nRight ){
      int j;
      int nMerge;
      for(j=0; j<nRight; j++){
        pIter->aSeg[iSeg].iFirst = aCkpt[iOff+0];
        pIter->aSeg[iSeg].iLast = aCkpt[iOff+1];
        iSeg++;
        if( aCkpt[iOff+3] ){
          pIter->aSeg[iSeg].iFirst = aCkpt[iOff+3];
          pIter->aSeg[iSeg].iLast = aCkpt[iOff+4];
          iSeg++;
        }
        iOff +=6;
      }

      nMerge = aCkpt[iOff++];
      iOff++;                   /* Merge.nSkip */
      iOff += nMerge*2;
      assert( iOff>=0 && iOff<=10000 );
    }
  }
  pIter->nSeg = iSeg;

  if( pIter->nSeg ){
    pIter->iFirst = pIter->aSeg[0].iFirst;
    pIter->iLast = pIter->aSeg[0].iLast;
  }

  pIter->nFree = aCkpt[iOff++];
  pIter->aFree = &aCkpt[iOff];
}

static int segmentIterEof(SegmentIter *pIter){
  return (pIter->iSeg>=pIter->nSeg);
}

static void segmentIterNext(SegmentIter *pIter){
  pIter->iSeg++;
  if( segmentIterEof(pIter)==0 ){
    pIter->iFirst = pIter->aSeg[pIter->iSeg].iFirst;
    pIter->iLast = pIter->aSeg[pIter->iSeg].iLast;
  }
}

static void segmentIterFinish(SegmentIter *pIter){
  free(pIter->aSeg);
  memset(pIter, 0, sizeof(SegmentIter));
}

int pageToBlockPage(SegmentIter *pIter, int iPg, int *piBlockPg){
  const int nPagePerBlock = pIter->nBlksz / pIter->nPgsz;
  *piBlockPg = (iPg-1) % nPagePerBlock;
  return (iPg / nPagePerBlock) + 1;
}

static void dumpBlockMap(lsm_db *pDb, int bData){
  lsmSortedDumpStructure(pDb, 0, bData, "show");
}

static void xLog(void *pCtx, int rc, const char *z){
  unused_parameter(pCtx);
  unused_parameter(rc);
  fprintf(stderr, "%s\n", z);
  fflush(stderr);
}


static int dumpCheckpoint(lsm_db *pDb){
  int *aCkpt;
  int rc;

  rc = lsm_info(pDb, LSM_INFO_CKPT, &aCkpt);
  if( rc==LSM_OK ){
    SegmentIter iter;
    int i;
    int nCkpt = aCkpt[1];

    int nTotal;                   /* Total number of pages in db file */
    int nTotalUsed;               /* Total number of pages used for content */

    printf("Checkpoint:");
    for(i=0; i<nCkpt; i++){
      printf(" %d", aCkpt[i]);
    }
    printf("\n");

    segmentIterInit(aCkpt, &iter);
    if( iter.nBlock ){
      Block *aBlock;
      const int nPagePerBlock = (iter.nBlksz / iter.nPgsz);
      int nFullBlk = 0;           /* Number of full blocks in file */
      int nFreeBlk = 0;           /* Number of free blocks in file */
      int nPartialBlk = 0;        /* Number of partially free in file */

      aBlock = (Block *)malloc((iter.nBlock+1) * sizeof(Block));
      memset(aBlock, 0, (iter.nBlock+1) * sizeof(Block));

      while( segmentIterEof(&iter)==0 ){
        int iBlk1;
        int iBlk2;
        int iPg1;
        int iPg2;

        iBlk1 = pageToBlockPage(&iter, iter.iFirst, &iPg1);
        iBlk2 = pageToBlockPage(&iter, iter.iLast, &iPg2);

        if( iBlk1!=iBlk2 ){
          aBlock[iBlk1].nUsed += (nPagePerBlock - iPg1);
          aBlock[iBlk1].iLast = nPagePerBlock-1;
          aBlock[iBlk2].nUsed += (iPg2 + 1);
          aBlock[iBlk2].iLast = MAX(aBlock[iBlk2].iLast, iPg2);
        }else{
          aBlock[iBlk2].nUsed += (iPg2 - iPg1 + 1);
          aBlock[iBlk2].iLast = MAX(aBlock[iBlk2].iLast, iPg2);
        }

        segmentIterNext(&iter);
      }

      /* Set all blocks that were not dealt with above, and are not on the
       ** free-block list, to full. */
      for(i=1; i<=iter.nBlock; i++){
        if( aBlock[i].iLast==0 ){
          aBlock[i].nUsed = nPagePerBlock;
          aBlock[i].iLast = nPagePerBlock-1;
        }
      }
      for(i=0; i<iter.nFree; i++){
        int iBlk = iter.aFree[i];
        aBlock[iBlk].nUsed = 0;
        aBlock[iBlk].iLast = 0;
      }

      /* Figure out the total ratio of used to free pages in the file */
      nTotalUsed = 0;
      nTotal = iter.nBlock * nPagePerBlock;
      for(i=1; i<=iter.nBlock; i++){
        nTotalUsed += aBlock[i].nUsed;

        if( i==1 
         && aBlock[i].nUsed==(nPagePerBlock - (8192 / iter.nPgsz))
        ){
          nFullBlk++;
        }
        else if( aBlock[i].nUsed==nPagePerBlock ) nFullBlk++;
        else if( aBlock[i].nUsed==0 ) nFreeBlk++;
        else nPartialBlk++;
      }

      printf("Blocks: %d full, %d partial, %d free\n", 
          nFullBlk, nPartialBlk, nFreeBlk);
      printf("Composite blocks:\n");
      for(i=1; i<=iter.nBlock; i++){
        if( aBlock[i].nUsed<nPagePerBlock && aBlock[i].nUsed ){
          Block *p = &aBlock[i];
          printf("    Block %d: %d used %d freed %d available\n", i,
              p->nUsed, 
              p->iLast - p->nUsed + 1, 
              nPagePerBlock - p->iLast - 1
              );
        }
      }
      printf("Page usage: %d of %d in use (%.1f%%)\n",
          nTotalUsed, nTotal, 100.0 * nTotalUsed / (double) nTotal
      );
      printf("\n");

      free(aBlock);
    }

    segmentIterFinish(&iter);
    lsm_free(lsm_get_env(pDb), aCkpt);
  }

  return rc;
}

int do_work(int nArg, char **azArg){
  struct Option {
    const char *zName;
  } aOpt [] = {
    { "-optimize" },
    { "-npage" },
    { 0 }
  };

  lsm_db *pDb;
  int rc;
  int i;
  const char *zDb;
  int flags = LSM_WORK_CHECKPOINT;
  int nWork = (1<<30);

  if( nArg==0 ) goto usage;
  zDb = azArg[nArg-1];
  for(i=0; i<(nArg-1); i++){
    int iSel;
    rc = testArgSelect(aOpt, "option", azArg[i], &iSel);
    if( rc ) return rc;
    switch( iSel ){
      case 0:
        flags |= LSM_WORK_OPTIMIZE;
        break;
      case 1:
        i++;
        if( i==(nArg-1) ) goto usage;
        nWork = atoi(azArg[i]);
        break;
    }
  }

  rc = lsm_new(0, &pDb);
  if( rc!=LSM_OK ){
    testPrintError("lsm_open(): rc=%d\n", rc);
  }else{
    rc = lsm_open(pDb, zDb);
    if( rc!=LSM_OK ){
      testPrintError("lsm_open(): rc=%d\n", rc);
    }else{
      rc = lsm_work(pDb, flags, nWork, 0);
      if( rc!=LSM_OK ){
        testPrintError("lsm_work(): rc=%d\n", rc);
      }
    }
  }

  lsm_close(pDb);
  return rc;

 usage:
  testPrintUsage("?-optimize? ?-n N? DATABASE");
  return -1;
}


/*
**   lsmtest show checkpoint DATABASE
*/
int do_show(int nArg, char **azArg){
  lsm_db *pDb;
  int rc;
  int i;
  int bData = 0;
  int bCheckpoint = 0;
  int bList = 0;
  int bStructure = 1;
  const char *zDb;

  struct Option {
    const char *zName;
  } aOpt [] = { 
    { "data" },                   /* 0 */
    { "checkpoint" },             /* 1 */
    { "list" },                   /* 2 */
    { 0 } 
  };

  if( nArg==0 ){
    testPrintUsage("?data? ?checkpoint? ?list? DATABASE");
    return -1;
  }

  zDb = azArg[nArg-1];
  for(i=0; i<nArg-1; i++){
    int iSel;
    rc = testArgSelect(aOpt, "option", azArg[0], &iSel);
    if( rc!=0 ) return rc;
    switch( iSel ){
      case 0: bData = 1; break;
      case 1: bCheckpoint = 1; break;
      case 2: bList = 1; break;
    }
  }

  rc = lsm_new(0, &pDb);
  if( rc!=LSM_OK ){
    testPrintError("lsm_new(): rc=%d\n", rc);
  }else{
    rc = lsm_open(pDb, zDb);
    if( rc!=LSM_OK ){
      testPrintError("lsm_open(): rc=%d\n", rc);
    }
  }

  if( rc==LSM_OK ){
    if( bList ){
      char *zList = 0;
      rc = lsm_info(pDb, LSM_INFO_DB_STRUCTURE, &zList);
      assert( rc==LSM_OK );
      printf("List: %s\n", zList);
      fflush(stdout);
      lsm_free(lsm_get_env(pDb), zList);
    }
    if( bCheckpoint ){
      dumpCheckpoint(pDb);
    }
    if( bStructure ){
      char *z = 0;
      lsm_info(pDb, LSM_INFO_DB_STRUCTURE, &z);
      if( z ){
        printf("%s\n", z);
        lsm_free(lsm_get_env(pDb), z);
      }
#if 1
      if( bData ){
        lsm_config_log(pDb, xLog, 0);
        dumpBlockMap(pDb, bData);
      }
#endif
    }
  }

  lsm_close(pDb);
  return 0;
}



