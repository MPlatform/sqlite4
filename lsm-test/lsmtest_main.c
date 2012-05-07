
#include "lsmtest.h"
#include "stdio.h"
#include "assert.h"
#include "string.h"
#include "stdlib.h"

#include <sqlite3.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>


void test_failed(){ return; }

#define testSetError(rc) testSetErrorFunc(rc, pRc, __FILE__, __LINE__)
static void testSetErrorFunc(int rc, int *pRc, const char *zFile, int iLine){
  if( rc ){
    *pRc = rc;
    printf("FAILED (%s:%d) rc=%d ", zFile, iLine, rc);
    test_failed();
  }
}

static int lsm_memcmp(u8 *a, u8 *b, int c){
  int i;
  for(i=0; i<c; i++){
    if( a[i]!=b[i] ) return a[i] - b[i];
  }
  return 0;
}

/*
** A test utility function.
*/
void testFetch(
  TestDb *pDb,                    /* Database handle */
  void *pKey, int nKey,           /* Key to query database for */
  void *pVal, int nVal,           /* Expected value */
  int *pRc                        /* IN/OUT: Error code */
){
  if( *pRc==0 ){
    void *pDbVal;
    int nDbVal;
    int rc;
    rc = tdb_fetch(pDb, pKey, nKey, &pDbVal, &nDbVal);
    testSetError(rc);
    if( rc==0 && (nVal!=nDbVal || (nVal>0 && lsm_memcmp(pVal, pDbVal, nVal))) ){
      testSetError(1);
    }
  }
}

void testWrite(
  TestDb *pDb,                    /* Database handle */
  void *pKey, int nKey,           /* Key to query database for */
  void *pVal, int nVal,           /* Value to write */
  int *pRc                        /* IN/OUT: Error code */
){
  if( *pRc==0 ){
    int rc;
    rc = tdb_write(pDb, pKey, nKey, pVal, nVal);
    testSetError(rc);
  }
}
void testDelete(
  TestDb *pDb,                    /* Database handle */
  void *pKey, int nKey,           /* Key to query database for */
  int *pRc                        /* IN/OUT: Error code */
){
  if( *pRc==0 ){
    int rc;
    rc = tdb_delete(pDb, pKey, nKey);
    testSetError(rc);
  }
}

void testBegin(TestDb *pDb, int iTrans, int *pRc){
  if( *pRc==0 ){
    int rc;
    rc = tdb_begin(pDb, iTrans);
    testSetError(rc);
  }
}
void testCommit(TestDb *pDb, int iTrans, int *pRc){
  if( *pRc==0 ){
    int rc;
    rc = tdb_commit(pDb, iTrans);
    testSetError(rc);
  }
}
static void testRollback(TestDb *pDb, int iTrans, int *pRc){
  if( *pRc==0 ){
    int rc;
    rc = tdb_rollback(pDb, iTrans);
    testSetError(rc);
  }
}

void testWriteStr(
  TestDb *pDb,                    /* Database handle */
  const char *zKey,               /* Key to query database for */
  const char *zVal,               /* Value to write */
  int *pRc                        /* IN/OUT: Error code */
){
  int nVal = (zVal ? strlen(zVal) : 0);
  testWrite(pDb, (void *)zKey, strlen(zKey), (void *)zVal, nVal, pRc);
}

static void testDeleteStr(TestDb *pDb, const char *zKey, int *pRc){
  testDelete(pDb, (void *)zKey, strlen(zKey), pRc);
}

void testFetchStr(
  TestDb *pDb,                    /* Database handle */
  const char *zKey,               /* Key to query database for */
  const char *zVal,               /* Value to write */
  int *pRc                        /* IN/OUT: Error code */
){
  int nVal = (zVal ? strlen(zVal) : 0);
  testFetch(pDb, (void *)zKey, strlen(zKey), (void *)zVal, nVal, pRc);
}

static void testFetchCompare(
  TestDb *pDb1, 
  TestDb *pDb2, 
  void *pKey, int nKey, 
  int *pRc
){
  int rc;
  void *pDbVal1;
  void *pDbVal2;
  int nDbVal1;
  int nDbVal2;

  rc = tdb_fetch(pDb1, pKey, nKey, &pDbVal1, &nDbVal1);
  testSetError(rc);

  rc = tdb_fetch(pDb2, pKey, nKey, &pDbVal2, &nDbVal2);
  testSetError(rc);

  if( *pRc==0 
   && (nDbVal1!=nDbVal2 || (nDbVal1>0 && memcmp(pDbVal1, pDbVal2, nDbVal1)))
  ){
    testSetError(1);
  }
}

typedef struct ScanResult ScanResult;
struct ScanResult {
  TestDb *pDb;

  int nRow;
  u32 cksum1;
  u32 cksum2;
  void *pKey1; int nKey1;
  void *pKey2; int nKey2;

  int bReverse;
  int nPrevKey;
  u8 aPrevKey[256];
};

static int keyCompare(void *pKey1, int nKey1, void *pKey2, int nKey2){
  int res;
  res = memcmp(pKey1, pKey2, MIN(nKey1, nKey2));
  if( res==0 ){
    res = nKey1 - nKey2;
  }
  return res;
}

static int test_scan_debug = 0;

static void scanCompareCb(
  void *pCtx, 
  void *pKey, int nKey,
  void *pVal, int nVal
){
  ScanResult *p = (ScanResult *)pCtx;
  u8 *aKey = (u8 *)pKey;
  u8 *aVal = (u8 *)pVal;
  int i;

  if( test_scan_debug ) printf("%s ", (char *)pKey);

#if 0
  /* Check tdb_fetch() matches */
  int rc = 0;
  testFetch(p->pDb, pKey, nKey, pVal, nVal, &rc);
  assert( rc==0 );
#endif

  /* Update the checksum data */
  p->nRow++;
  for(i=0; i<nKey; i++){
    p->cksum1 += ((int)aKey[i] << (i&0x0F));
    p->cksum2 += p->cksum1;
  }
  for(i=0; i<nVal; i++){
    p->cksum1 += ((int)aVal[i] << (i&0x0F));
    p->cksum2 += p->cksum1;
  }

  /* Check that the delivered row is not out of order. */
  if( nKey<(int)sizeof(p->aPrevKey) ){
    if( p->nPrevKey ){
      int res = keyCompare(p->aPrevKey, p->nPrevKey, pKey, nKey);
      if( (res<0 && p->bReverse) || (res>0 && p->bReverse==0) ){
        testPrintError("Returned key out of order at %s:%d\n", 
            __FILE__, __LINE__
        );
      }
    }

    p->nPrevKey = nKey;
    memcpy(p->aPrevKey, pKey, MIN(p->nPrevKey, nKey));
  }

  /* Check that the delivered row is within range. */
  if( p->pKey1 && (
      (memcmp(p->pKey1, pKey, MIN(p->nKey1, nKey))>0)
   || (memcmp(p->pKey1, pKey, MIN(p->nKey1, nKey))==0 && p->nKey1>nKey)
  )){
    testPrintError("Returned key too small at %s:%d\n", __FILE__, __LINE__);
  }
  if( p->pKey2 && (
      (memcmp(p->pKey2, pKey, MIN(p->nKey2, nKey))<0)
   || (memcmp(p->pKey2, pKey, MIN(p->nKey2, nKey))==0 && p->nKey2<nKey)
  )){
    testPrintError("Returned key too large at %s:%d\n", __FILE__, __LINE__);
  }

}

/*
** Scan the contents of the two databases. Check that they match.
*/
void testScanCompare(
  TestDb *pDb1,                   /* Control (trusted) database */
  TestDb *pDb2,                   /* Database being tested */
  int bReverse,
  void *pKey1, int nKey1, 
  void *pKey2, int nKey2, 
  int *pRc
){
  if( *pRc==0 ){
    ScanResult res1;
    ScanResult res2;
    void *pRes1 = (void *)&res1;
    void *pRes2 = (void *)&res2;

    memset(&res1, 0, sizeof(ScanResult));
    memset(&res2, 0, sizeof(ScanResult));

    res1.pDb = pDb1;
    res1.nKey1 = nKey1; res1.pKey1 = pKey1;
    res1.nKey2 = nKey2; res1.pKey2 = pKey2;
    res1.bReverse = bReverse;
    res2.pDb = pDb2;
    res2.nKey1 = nKey1; res2.pKey1 = pKey1;
    res2.nKey2 = nKey2; res2.pKey2 = pKey2;
    res2.bReverse = bReverse;

    tdb_scan(pDb1, pRes1, bReverse, pKey1, nKey1, pKey2, nKey2, scanCompareCb);
if( test_scan_debug ) printf("\n");
    tdb_scan(pDb2, pRes2, bReverse, pKey1, nKey1, pKey2, nKey2, scanCompareCb);
if( test_scan_debug ) printf("\n");

    if( res1.nRow!=res2.nRow 
     || res1.cksum1!=res2.cksum1 
     || res1.cksum2!=res2.cksum2
    ){
      printf("expected: %d %X %X\n", res1.nRow, res1.cksum1, res1.cksum2);
      printf("got:      %d %X %X\n", res2.nRow, res2.cksum1, res2.cksum2);
      testSetError(1);
      *pRc = 1;
    }
  }
}

void testClose(TestDb **ppDb){
  tdb_close(*ppDb);
  *ppDb = 0;
}

TestDb *testOpen(const char *zSystem, int bClear, int *pRc){
  TestDb *pDb = 0;
  if( *pRc==0 ){
    int rc;
    rc = tdb_open(zSystem, 0, bClear, &pDb);
    if( rc!=0 ){
      testSetError(rc);
      *pRc = rc;
    }
  }
  return pDb;
}

void testReopen(TestDb **ppDb, int *pRc){
  if( *pRc==0 ){
    const char *zLib;
    zLib = tdb_library_name(*ppDb);
    testClose(ppDb);
    *pRc = tdb_open(zLib, 0, 0, ppDb);
  }
}


static void testSystemSelect(const char *zSys, int *piSel, int *pRc){
  if( *pRc==0 ){
    struct SysName { const char *zName; } *aName;
    int nSys;
    int i;

    for(nSys=0; tdb_system_name(nSys); nSys++);
    aName = malloc(sizeof(struct SysName) * (nSys+1));
    for(i=0; i<=nSys; i++){
      aName[i].zName = tdb_system_name(i);
    }

    *pRc = testArgSelect(aName, "db", zSys, piSel);
    free(aName);
  }
}

char *testMallocVPrintf(const char *zFormat, va_list ap){
  int nByte;
  va_list copy;
  char *zRet;

  va_copy(copy, ap);
  nByte = vsnprintf(0, 0, zFormat, copy);
  va_end(copy);

  assert( nByte>=0 );
  zRet = (char *)malloc(nByte+1);
  vsnprintf(zRet, nByte+1, zFormat, ap);
  return zRet;
}

char *testMallocPrintf(const char *zFormat, ...){
  va_list ap;
  char *zRet;

  va_start(ap, zFormat);
  zRet = testMallocVPrintf(zFormat, ap);
  va_end(ap);

  return zRet;
}


/*
** A wrapper around malloc(3).
**
** This function should be used for all allocations made by test procedures.
** It has the following properties:
**
**   * Test code may assume that allocations may not fail.
**   * Returned memory is always zeroed.
**
** Allocations made using testMalloc() should be freed using testFree().
*/
void *testMalloc(int n){
  void *pRet = malloc(n);
  memset(pRet, 0, n);
  return pRet;
}

/*
** Free an allocation made by an earlier call to testMalloc().
*/
void testFree(void *p){
  free(p);
}

/*
** String zPattern contains a glob pattern. Return true if zStr matches 
** the pattern, or false if it does not.
*/
int testGlobMatch(const char *zPattern, const char *zStr){
  int i = 0;
  int j = 0;

  while( zPattern[i] ){
    char p = zPattern[i];

    if( p=='*' || p=='%' ){
      do {
        if( testGlobMatch(&zPattern[i+1], &zStr[j]) ) return 1;
      }while( zStr[j++] );
      return 0;
    }

    if( zStr[j]==0 || (p!='?' && p!=zStr[j]) ){
      /* Match failed. */
      return 0;
    }

    j++;
    i++;
  }

  return (zPattern[i]==0 && zStr[j]==0);
}

/* 
** End of test utilities 
**************************************************************************/

int do_test(int nArg, char **azArg){
  int j;
  int nFail = 0;
  const char *zPattern = 0;

  if( nArg>1 ){
    testPrintError("Usage: test ?PATTERN?\n");
    return 1;
  }
  if( nArg==1 ){
    zPattern = azArg[0];
  }

  for(j=0; tdb_system_name(j); j++){
    int rc = LSM_OK;

    test_data_3(tdb_system_name(j), zPattern, &rc);
    test_rollback(tdb_system_name(j), zPattern, &rc);
    test_mc(tdb_system_name(j), zPattern, &rc);
    test_mt(tdb_system_name(j), zPattern, &rc);

    if( rc ) nFail++;
  }

  return (nFail!=0);
}

int do_speed_tests(int nArg, char **azArg){

  struct DbSystem {
    const char *zLibrary;
    const char *zColor;
  } aSys[] = {
    { "sqlite3",      "black" },
    { "leveldb",      "blue" },
    { "lsm",          "red" },
    { "lsm_mt2",      "orange" },
    { "lsm_mt3",      "purple" },
    { "kyotocabinet", "green" },
    {0, 0}
  };

  int i;
  int j;
  int rc;
  int nSleep = 0;                 /* ms of rest allowed between INSERT tests */
  int nRow = 0;                   /* Number of rows to insert into database */
  int nStep;                      /* Measure INSERT time after this many rows */
  int nSelStep;                   /* Measure SELECT time after this many rows */
  int nSelTest;                   /* Number of SELECTs to run for timing */
  int doReadTest = 1;
  int doWriteTest = 1;

  int *aTime;                     /* INSERT timing data */
  int *aWrite;                    /* Writes per nStep inserts */
  int *aSelTime;                  /* SELECT timing data */
  int isFirst = 1;
  int bSleep = 0;

  u32 sys_mask = 0;

  for(i=0; i<nArg; i++){
    struct Opt { 
      const char *zOpt; 
      int isSwitch;
    } aOpt[] = {
      { "sqlite3" , 0},
      { "leveldb" , 0},
      { "lsm" , 0},
      { "lsm_mt2" , 0},
      { "lsm_mt3" , 0},
      { "kyotocabinet" , 0},
      { "-rows" , 1},
      { "-sleep" , 2},
      { "-testmode" , 3},
      { 0, 0}
    };
    int iSel;

    rc = testArgSelect(aOpt, "argument", azArg[i], &iSel);
    if( rc ) return rc;

    if( aOpt[iSel].isSwitch ){
      i++;
      if( i>=nArg ){
        testPrintError("option %s requires an argument\n", aOpt[iSel].zOpt);
        return 1;
      }
      if( aOpt[iSel].isSwitch==1 ){
        nRow = atoi(azArg[i]);
      }
      if( aOpt[iSel].isSwitch==2 ){
        nSleep = atoi(azArg[i]);
      }
      if( aOpt[iSel].isSwitch==3 ){
        struct Mode {
          const char *zMode;
          int doReadTest;
          int doWriteTest;
        } aMode[] = {{"ro", 1, 0} , {"rw", 1, 1}, {"wo", 0, 1}, {0, 0, 0}};
        int iMode;
        rc = testArgSelect(aMode, "option", azArg[i], &iMode);
        if( rc ) return rc;
        doReadTest = aMode[iMode].doReadTest;
        doWriteTest = aMode[iMode].doWriteTest;
      }
    }else{
      /* A db name */
      rc = testArgSelect(aOpt, "system", azArg[i], &iSel);
      if( rc ) return rc;
      sys_mask |= (1<<iSel);
    }
  }

  if( sys_mask==0 ) sys_mask = (1 << 0) | (1 << 1) | (1 << 2) | (1 << 3);
  nRow = MAX(nRow, 100000);
  nStep = nRow/100;
  nSelStep = nRow/10;
  nSelTest = (nSelStep > 100000) ? 100000 : nSelStep;

  aTime = malloc(sizeof(int) * ArraySize(aSys) * nRow/nStep);
  aWrite = malloc(sizeof(int) * nRow/nStep);
  aSelTime = malloc(sizeof(int) * ArraySize(aSys) * nRow/nSelStep);

  /* This loop collects the INSERT speed data. */
  if( doWriteTest ){
    for(j=0; aSys[j].zLibrary; j++){
      TestDb *pDb;                  /* Database being tested */
      lsm_db *pLsm;
  
      if( ((1<<j)&sys_mask)==0 ) continue;
      if( bSleep && nSleep ) sqlite3_sleep(nSleep);
      bSleep = 1;

      rc = tdb_open(aSys[j].zLibrary, 0, 1, &pDb);
      if( rc ) return rc;
      pLsm = tdb_lsm(pDb);
  
      testTimeInit();
      for(i=0; i<nRow; i+=nStep){
        int iStep;
        int nWrite1, nWrite2;
        if( pLsm ) lsm_info(pLsm, LSM_INFO_NWRITE, &nWrite1);
        for(iStep=0; iStep<nStep; iStep++){
          u32 aKey[4];                  /* 16-byte key */
          u32 aVal[25];                 /* 100 byte value */
          testPrngArray(i+iStep, aKey, ArraySize(aKey));
          testPrngArray(i+iStep, aVal, ArraySize(aVal));
          rc = tdb_write(pDb, aKey, sizeof(aKey), aVal, sizeof(aVal));
        }
        aTime[(j*nRow+i)/nStep] = testTimeGet();
        if( pLsm ) lsm_info(pLsm, LSM_INFO_NWRITE, &nWrite2);
        aWrite[i/nStep] = nWrite2 - nWrite1;
      }

      tdb_close(pDb);
    }
  }

  /* This loop collects the SELECT speed data. */
  if( doReadTest ){
    for(j=0; aSys[j].zLibrary; j++){
      TestDb *pDb;                  /* Database being tested */

      if( ((1<<j)&sys_mask)==0 ) continue;
      if( bSleep && nSleep ) sqlite3_sleep(nSleep);
      bSleep = 1;

      if( doWriteTest ){
        rc = tdb_open(aSys[j].zLibrary, 0, 1, &pDb);
        if( rc ) return rc;
    
        for(i=0; i<nRow; i+=nSelStep){
          int iStep;
          int iSel;
          for(iStep=0; iStep<nSelStep; iStep++){
            u32 aKey[4];                  /* 16-byte key */
            u32 aVal[25];                 /* 100 byte value */
            testPrngArray(i+iStep, aKey, ArraySize(aKey));
            testPrngArray(i+iStep, aVal, ArraySize(aVal));
            rc = tdb_write(pDb, aKey, sizeof(aKey), aVal, sizeof(aVal));
          }
    
          testTimeInit();
          for(iSel=0; iSel<nSelTest; iSel++){
            void *pDummy;
            int nDummy;
            u32 iKey;
            u32 aKey[4];                  /* 16-byte key */
    
            iKey = testPrngValue(iSel) % (i+nSelStep);
            testPrngArray(iKey, aKey, ArraySize(aKey));
            rc = tdb_fetch(pDb, aKey, sizeof(aKey), &pDummy, &nDummy);
          }
          aSelTime[(j*nRow+i)/nSelStep] = testTimeGet();
          tdb_fetch(pDb, 0, 0, 0, 0);
        }
      }else{
        int t;
        int iSel;
        rc = tdb_open(aSys[j].zLibrary, 0, 0, &pDb);
        testTimeInit();
        for(iSel=0; rc==LSM_OK && iSel<nSelTest; iSel++){
          void *pDummy;
          int nDummy;
          u32 iKey;
          u32 aKey[4];                  /* 16-byte key */
    
          iKey = testPrngValue(iSel) % nRow;
          testPrngArray(iKey, aKey, ArraySize(aKey));
          rc = tdb_fetch(pDb, aKey, sizeof(aKey), &pDummy, &nDummy);

#ifndef NDEBUG
          u32 aVal[25];                 /* 100 byte value */
          testPrngArray(iKey, aVal, ArraySize(aVal));
          assert( nDummy==100 && memcmp(aVal, pDummy, 100)==0 );
#endif
        }
        if( rc!=LSM_OK ) return rc;

        t = testTimeGet();
        tdb_fetch(pDb, 0, 0, 0, 0);

        printf("%s: %d selects/second\n", 
            aSys[j].zLibrary, (int)((double)nSelTest*1000.0/t)
        );
      }

      tdb_close(pDb);
    }
  }

  if( doWriteTest ){
    printf("set xlabel \"Rows Inserted\"\n");
    printf("set ylabel \"Inserts per second\"\n");
    if( doReadTest ){
      printf("set y2label \"Selects per second\"\n");
    }else if( sys_mask==(1<<2) ){
      printf("set y2label \"Page writes per %d inserts\"\n", nStep);
    }
    printf("set yrange [0:*]\n");
    printf("set y2range [0:*]\n");
    printf("set xrange [%d:*]\n", MAX(nStep, nRow/20) );
    printf("set ytics nomirror\n");
    printf("set y2tics nomirror\n");
    printf("set key box lw 0.01\n");
    printf("plot ");
  
    for(j=0; aSys[j].zLibrary; j++){
      if( (1<<j)&sys_mask ){
        const char *zLib = aSys[j].zLibrary;
        printf("%s\"-\" ti \"%s INSERT\" with lines lc rgb \"%s\" ", 
            (isFirst?"":", "), zLib, aSys[j].zColor
        );
        if( doReadTest ){
          printf(", \"-\" ti \"%s SELECT\" "
                 "axis x1y2 with points lw 3 lc rgb \"%s\""
              , zLib, aSys[j].zColor
          );
        }
        isFirst = 0;
      }
    }

    assert( strcmp(aSys[2].zLibrary, "lsm")==0 );
    if( sys_mask==(1<<2) && !doReadTest ){
      printf(", \"-\" ti \"lsm pages written\" "
        "axis x1y2 with boxes lw 1 lc rgb \"grey\""
      );
    }
  
    printf("\n");
  
    for(j=0; aSys[j].zLibrary; j++){
      if( ((1<<j)&sys_mask)==0 ) continue;
      printf("# Rows    Inserts per second\n");
      for(i=0; i<nRow; i+=nStep){
        int iTime = aTime[(j*nRow+i)/nStep];
        int ips = (int)((i+nStep)*1000.0 / (double)iTime);
        printf("%d %d\n", i+nStep, ips);
      }
      printf("end\n");
  
      if( doReadTest ){
        printf("# Rows    Selects per second\n");
        for(i=0; i<nRow; i+=nSelStep){
          int sps = (int)(nSelTest*1000.0/(double)aSelTime[(j*nRow+i)/nSelStep]);
          printf("%d %d\n", i+nSelStep, sps);
        }
        printf("end\n");
      }else if( sys_mask==(1<<2) ){
        for(i=0; i<(nRow/nStep); i++){
          printf("%d %f\n", i*nStep, (double)aWrite[i] / (double)nStep);
        }
        printf("end\n");
      }
    }
  
    printf("pause -1\n");
  }

  free(aTime);
  free(aSelTime);
  free(aWrite);
  return 0;
}

/*
** Usage: lsmtest random ?N?
**
** This command prints a sequence of zero or more numbers from the PRNG
** system to stdout. If the "N" argument is missing, values the first 10
** values (i=0, i=1, ... i=9) are printed. Otherwise, the first N.
**
** This was added to verify that the PRNG values do not change between
** runs of the lsmtest program.
*/
int do_random_tests(int nArg, char **azArg){
  int i;
  int nRand;
  if( nArg==0 ){
    nRand = 10;
  }else if( nArg==1 ){
    nRand = atoi(azArg[0]);
  }else{
    testPrintError("Usage: random ?N?\n");
    return -1;
  }
  for(i=0; i<nRand; i++){
    printf("0x%x\n", testPrngValue(i));
  }
  return 0;
}

static int testFormatSize(char *aBuf, int nBuf, i64 nByte){
  int res;
  if( nByte<(1<<10) ){
    res = snprintf(aBuf, nBuf, "%d byte", (int)nByte);
  }else if( nByte<(1<<20) ){
    res = snprintf(aBuf, nBuf, "%dK", (int)(nByte/(1<<10)));
  }else{
    res = snprintf(aBuf, nBuf, "%dM", (int)(nByte/(1<<20)));
  }
  return res;
}

static i64 testReadSize(char *z){
  int n = strlen(z);
  char c = z[n-1];
  i64 nMul = 1;

  switch( c ){
    case 'g': case 'G':
      nMul = (1<<30);
      break;

    case 'm': case 'M':
      nMul = (1<<20);
      break;

    case 'k': case 'K':
      nMul = (1<<10);
      break;

    default:
      nMul = 1;
  }

  return nMul * (i64)atoi(z);
} 

/*
** Usage: lsmtest writespeed FILESIZE BLOCKSIZE SYNCSIZE
*/
static int do_writer_test(int nArg, char **azArg){
  int nBlock;
  int nSize;
  int i;
  int fd;
  int ms;
  char aFilesize[32];
  char aBlockSize[32];

  char *aBlock;
  int *aOrder;
  int nSync;

  i64 filesize;
  i64 blocksize;
  i64 syncsize;

  /* How long to sleep before running a trial (in ms). */
  const int nSleep = 10000;

  if( nArg!=3 ){
    testPrintUsage("FILESIZE BLOCKSIZE SYNCSIZE");
    return -1;
  }

  filesize = testReadSize(azArg[0]);
  blocksize = testReadSize(azArg[1]);
  syncsize = testReadSize(azArg[2]);

  nBlock = (int)(filesize / blocksize);
  nSize = (int)blocksize;
  nSync = (int)(syncsize / blocksize);

  aBlock = (char *)malloc(nSize);
  aOrder = (int *)malloc(nBlock * sizeof(int));
  for(i=0; i<nBlock; i++) aOrder[i] = i;
  for(i=0; i<(nBlock*25); i++){
    int tmp;
    u32 a = testPrngValue(i);
    u32 b = testPrngValue(a);
    a = a % nBlock;
    b = b % nBlock;
    tmp = aOrder[a];
    aOrder[a] = aOrder[b];
    aOrder[b] = tmp;
  }

  testFormatSize(aFilesize, sizeof(aFilesize), (i64)nBlock * (i64)nSize);
  testFormatSize(aBlockSize, sizeof(aFilesize), nSize);

  printf("Testing writing a %s file using %s blocks. ", aFilesize, aBlockSize);
  if( nSync==1 ){
    printf("Sync after each block.\n");
  }else{
    printf("Sync after each %d blocks.\n", nSync);
  }

  printf("Preparing file... ");
  fflush(stdout);
  unlink("writer.out");
  fd = open("writer.out", O_RDWR|O_CREAT, 0664);
  if( fd<0 ){
    testPrintError("open(): %d - %s\n", errno, strerror(errno));
    return -1;
  }
  testTimeInit();
  for(i=0; i<nBlock; i++){
    memset(aBlock, i&0xFF, nSize);
    write(fd, aBlock, nSize);
  }
  fsync(fd);
  printf("ok (%d ms)\n", testTimeGet());

  for(i=0; i<5; i++){
    int j;

    sqlite3_sleep(nSleep);
    printf("Now writing sequentially...  ");
    fflush(stdout);

    lseek(fd, 0, SEEK_SET);
    testTimeInit();
    for(j=0; j<nBlock; j++){
      if( ((j+1)%nSync)==0 ) fsync(fd);
      memset(aBlock, j&0xFF, nSize);
      write(fd, aBlock, nSize);
    }
    fsync(fd);
    ms = testTimeGet();
    printf("%d ms\n", ms);
    sqlite3_sleep(nSleep);
    printf("Now in an arbitrary order... ");

    fflush(stdout);
    testTimeInit();
    for(j=0; j<nBlock; j++){
      if( ((j+1)%nSync)==0 ) fsync(fd);
      memset(aBlock, j&0xFF, nSize);
      lseek(fd, aOrder[j]*nSize, SEEK_SET);
      write(fd, aBlock, nSize);
    }
    fsync(fd);
    ms = testTimeGet();
    printf("%d ms\n", ms);
  }

  close(fd);
  free(aBlock);
  free(aOrder);

  return 0;
}

static void do_insert_work_hook(lsm_db *db, void *p){
  char *z = 0;
  lsm_info(db, LSM_INFO_STRUCTLIST, &z);
  if( z ){
    printf("%s\n", z);
    fflush(stdout);
    lsm_free(lsm_get_env(db), z);
  }

  unused_parameter(p);
}


static int do_insert(int nArg, char **azArg){
  const char *zDb = "lsm";
  TestDb *pDb = 0;
  int i;
  int rc;
  const int nRow = 10000000;

  DatasourceDefn defn = { TEST_DATASOURCE_RANDOM, 8, 15, 80, 150 };
  Datasource *pData = 0;

  if( nArg>1 ){
    testPrintError("Usage: insert ?DATABASE?\n");
    return 1;
  }
  if( nArg==1 ){
    zDb = azArg[0];
  }

  rc = tdb_open(zDb, 0, 1, &pDb);
  if( rc!=0 ){
    testPrintError("Error opening db \"%s\": %d\n", zDb, rc);
    return rc;
  }
  pData = testDatasourceNew(&defn);
  tdb_lsm_config_work_hook(pDb, do_insert_work_hook, 0);

  for(i=0; i<nRow; i++){
    void *pKey; int nKey;         /* Database key to insert */
    void *pVal; int nVal;         /* Database value to insert */

    testDatasourceEntry(pData, i, &pKey, &nKey, &pVal, &nVal);
    tdb_write(pDb, pKey, nKey, pVal, nVal);
  }
  
  testDatasourceFree(pData);
  tdb_close(pDb);

  return 0;
}

static int st_do_show(int a, char **b)      { return do_show(a, b); }
static int st_do_work(int a, char **b)      { return do_work(a, b); }
static int st_do_io(int a, char **b)        { return do_io(a, b); }
static int st_do_crash_test(int a, char **b){ return do_crash_test(a, b); }


#ifdef __linux__
#include <sys/time.h>
#include <sys/resource.h>

static void lsmtest_rusage_report(void){
  int res;
  struct rusage r;
  memset(&r, sizeof(r), 0);

  res = getrusage(RUSAGE_SELF, &r);
  assert( res==0 );

  printf("getrusage: { ru_maxrss %d ru_oublock %d }\n", 
      (int)r.ru_maxrss, (int)r.ru_oublock
  );
}
#else
static void lsmtest_rusage_report(void){
  /* no-op */
}
#endif

int main(int argc, char **argv){
  struct TestFunc {
    const char *zName;
    int (*xFunc)(int, char **);
  } aTest[] = {
    {"speed",       do_speed_tests},
    {"random",      do_random_tests},
    {"writespeed",  do_writer_test},
    {"show",        st_do_show},
    {"work",        st_do_work},
    {"io",          st_do_io},

    {"insert",      do_insert},

    {"crash",       st_do_crash_test},
    {"test",        do_test},
    {0, 0}
  };
  int rc;                         /* Return Code */
  int iFunc;                      /* Index into aTest[] */

  int nLeakAlloc;                 /* Allocations leaked by lsm */
  int nLeakByte;                  /* Bytes leaked by lsm */

#ifdef LSM_DEBUG_MEM
  FILE *pReport = 0;              /* lsm malloc() report file */
#endif

#ifndef NDEBUG
  testMallocInstall(tdb_lsm_env());
#endif

  if( argc<2 ){
    testPrintError("Usage: %s sub-command ?args...?\n", argv[0]);
    return -1;
  }

  /* Initialize error reporting */
  testErrorInit(argc, argv);

  /* Initialize PRNG system */
  testPrngInit();

  rc = testArgSelect(aTest, "sub-command", argv[1], &iFunc);
  if( rc==0 ){
    rc = aTest[iFunc].xFunc(argc-2, &argv[2]);
  }

#ifdef LSM_DEBUG_MEM
  pReport = fopen("malloc.txt", "w");
  testMallocCheck(&nLeakAlloc, &nLeakByte, pReport);
  fclose(pReport);
#else
  testMallocCheck(&nLeakAlloc, &nLeakByte, 0);
#endif
  if( nLeakAlloc ){
    testPrintError("Leaked %d bytes in %d allocations\n", nLeakByte,nLeakAlloc);
    if( rc==0 ) rc = -1;
  }

  lsmtest_rusage_report();
  return rc;
}

