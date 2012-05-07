
/*
** This file contains tests related to recovery following application 
** and system crashes (power failures) while writing to the database.
*/

#include "lsmtest.h"

/*
** Structure used by testCksumDatabase() to accumulate checksum values in.
*/
typedef struct Cksum Cksum;
struct Cksum {
  int nRow;
  int cksum1;
  int cksum2;
};

/*
** tdb_scan() callback used by testCksumDatabase()
*/
static void scanCksumDb(
  void *pCtx, 
  void *pKey, int nKey,
  void *pVal, int nVal
){
  Cksum *p = (Cksum *)pCtx;
  int i;

  p->nRow++;
  for(i=0; i<nKey; i++){
    p->cksum1 += ((u8 *)pKey)[i];
    p->cksum2 += p->cksum1;
  }
  for(i=0; i<nVal; i++){
    p->cksum1 += ((u8 *)pVal)[i];
    p->cksum2 += p->cksum1;
  }
}

static void scanCountDb(
  void *pCtx, 
  void *pKey, int nKey,
  void *pVal, int nVal
){
  Cksum *p = (Cksum *)pCtx;
  p->nRow++;

  unused_parameter(pKey);
  unused_parameter(nKey);
  unused_parameter(pVal);
  unused_parameter(nVal);
}


/*
** Iterate through the entire contents of database pDb. Write a checksum
** string based on the db contents into buffer zOut before returning. A
** checksum string is at most 29 (TEST_CKSUM_BYTES) bytes in size:
**
**    * 32-bit integer (10 bytes)
**    * 1 space        (1 byte)
**    * 32-bit hex     (8 bytes)
**    * 1 space        (1 byte)
**    * 32-bit hex     (8 bytes)
**    * nul-terminator (1 byte)
**
** The number of entries in the database is returned.
*/
int testCksumDatabase(
  TestDb *pDb,                    /* Database handle */
  char *zOut                      /* Buffer to write checksum to */
){
  Cksum cksum;
  memset(&cksum, 0, sizeof(Cksum));
  tdb_scan(pDb, (void *)&cksum, 0, 0, 0, 0, 0, scanCksumDb);
  sprintf(zOut, "%d %x %x", 
      cksum.nRow, (u32)cksum.cksum1, (u32)cksum.cksum2
  );
  assert( strlen(zOut)<TEST_CKSUM_BYTES );
  return cksum.nRow;
}

static int cksumDatasource(
  Datasource *pData, 
  int nEntry,
  char *zOut
){
  TestDb *pDb;
  int i;
  int ret;
  int rc = 0;

  tdb_open("lsm", "tmpdb.lsm", 1, &pDb);
  for(i=0; i<nEntry; i++){
    void *pKey; int nKey;
    void *pVal; int nVal;
    testDatasourceEntry(pData, i, &pKey, &nKey, &pVal, &nVal);
    testWrite(pDb, pKey, nKey, pVal, nVal, &rc);
    assert( rc==0 );
    tdb_write(pDb, pKey, nKey, pVal, nVal);
  }

  ret = testCksumDatabase(pDb, zOut);
  tdb_close(pDb);
  return ret;
}

int testCountDatabase(TestDb *pDb){
  Cksum cksum;
  memset(&cksum, 0, sizeof(Cksum));
  tdb_scan(pDb, (void *)&cksum, 0, 0, 0, 0, 0, scanCountDb);
  return cksum.nRow;
}

/*
** This function is a no-op if *pRc is not 0 when it is called.
**
** Otherwise, the two nul-terminated strings z1 and z1 are compared. If
** they are the same, the function returns without doing anything. Otherwise,
** an error message is printed, *pRc is set to 1 and the test_failed()
** function called.
*/
void testCompareStr(const char *z1, const char *z2, int *pRc){
  if( *pRc==0 ){
    if( strcmp(z1, z2) ){
      testPrintError("testCompareStr: \"%s\" != \"%s\"\n", z1, z2);
      *pRc = 1;
      test_failed();
    }
  }
}

/*
** This function is a no-op if *pRc is not 0 when it is called.
**
** Otherwise, the two integers i1 and i2 are compared. If they are equal,
** the function returns without doing anything. Otherwise, an error message 
** is printed, *pRc is set to 1 and the test_failed() function called.
*/
void testCompareInt(int i1, int i2, int *pRc){
  if( *pRc==0 && i1!=i2 ){
    testPrintError("testCompareInt: %d != %d\n", i1, i2);
    *pRc = 1;
    test_failed();
  }
}

/*
** The database handle passed as the only argument must be an LSM database.
** This function simulates an application crash by configuring the VFS
** wrapper to return LSM_IOERR for all subsequent IO calls, then closing 
** the database handle.
*/
void testApplicationCrash(TestDb *pDb){
  assert( tdb_lsm(pDb) );
  tdb_lsm_application_crash(pDb);
  tdb_close(pDb);
}

/*
** The database handle passed as the only argument must be an LSM database.
** This function simulates a system crash by:
*/
void testSystemCrash(TestDb *pDb){
  assert( tdb_lsm(pDb) );
  tdb_lsm_system_crash(pDb);
  tdb_close(pDb);
}

void testCaseStart(int *pRc, char *zFmt, ...){
  va_list ap;
  va_start(ap, zFmt);
  vprintf(zFmt, ap);
  printf(" ...");
  va_end(ap);
  *pRc = 0;
  fflush(stdout);
}

/*
** This function is a no-op if *pRc is non-zero when it is called. Zero
** is returned in this case.
**
** Otherwise, the zFmt (a printf style format string) and following arguments 
** are used to create a test case name. If zPattern is NULL or a glob pattern
** that matches the test case name, 1 is returned and the test case started.
** Otherwise, zero is returned and the test case does not start.
*/
int testCaseBegin(int *pRc, const char *zPattern, const char *zFmt, ...){
  int res = 0;
  if( *pRc==0 ){
    char *zTest;
    va_list ap;

    va_start(ap, zFmt);
    zTest = testMallocVPrintf(zFmt, ap);
    va_end(ap);
    if( zPattern==0 || testGlobMatch(zPattern, zTest) ){
      printf("%s ...", zTest);
      res = 1;
    }
    testFree(zTest);
    fflush(stdout);
  }

  return res;
}

void testCaseDot(){
  printf(".");
  fflush(stdout);
}

void testCaseFinish(int rc){
  if( rc==0 ){
    printf("Ok\n");
  }else{
    printf("FAILED\n");
  }
  fflush(stdout);
}

void testCaseSkip(){
  printf("Skipped\n");
}

static Datasource *getDatasource(int iTest){

  DatasourceDefn aDefn[2] = {
    { TEST_DATASOURCE_SEQUENCE, 0, 0, 50, 100 }, 
    { TEST_DATASOURCE_RANDOM, 10, 15, 50, 100 }, 
  };

  assert( iTest>=0 && iTest<ArraySize(aDefn) );
  return testDatasourceNew(&aDefn[iTest]);
}

static int crash_test1(
  const char *zTitle,
  const char *zSystem,            /* Database system to test */
  int bSystemCrash,               /* True for a system crash, false for app */
  int eSyncMode                   /* Either LSM_SAFETY_NORMAL or FULL */
){
  const int nTrial = 1000;
  const int nRow = 10;
  int iTest;

  int bLossy;                     /* True if a crash may cause data loss */

  assert( eSyncMode==LSM_SAFETY_NORMAL || eSyncMode==LSM_SAFETY_FULL );
  bLossy = (bSystemCrash && eSyncMode==LSM_SAFETY_NORMAL );

  for(iTest=0; 1; iTest++){
    TestDb *pDb;
    int rc;
    int i, j;

    char zCksum1[TEST_CKSUM_BYTES];
    int nCksumRow = -1;

    Datasource *pData;            /* Test data */
    pData = getDatasource(iTest);
    if( !pData ) break;

    testCaseStart(&rc, "crash.%s.%d", zTitle, iTest);

    pDb = 0;
    rc = tdb_open(zSystem, 0, 1, &pDb);
    if( rc==0 ) tdb_lsm_safety(pDb, eSyncMode);
    if( rc==0 && bSystemCrash ) tdb_lsm_prepare_system_crash(pDb);

    for(i=0; rc==LSM_OK && i<nTrial; i++){
      int nCurrent;               /* Current number of rows in db */
      int nRequired;              /* Desired number of rows */
     
      nCurrent = testCountDatabase(pDb);
      nRequired = (i + 1) * nRow;

      /* Write some more records to the database. */
      for(j=nCurrent; j<nRequired; j++){
        void *pKey; int nKey;
        void *pVal; int nVal;
        testDatasourceEntry(pData, j, &pKey, &nKey, &pVal, &nVal);
        testWrite(pDb, pKey, nKey, pVal, nVal, &rc);
      }

      if( rc==LSM_OK ){
        int nDbRow;
        char zCksum2[TEST_CKSUM_BYTES];

        if( bLossy==0 ) testCksumDatabase(pDb, zCksum1);
        
        if( bSystemCrash ){
          testSystemCrash(pDb);
        }else{
          testApplicationCrash(pDb);
        }

        pDb = 0;
        rc = tdb_open(zSystem, 0, 0, &pDb);
        if( rc==0 ) tdb_lsm_safety(pDb, eSyncMode);
        if( rc==0 && bSystemCrash ) tdb_lsm_prepare_system_crash(pDb);

        nDbRow = testCksumDatabase(pDb, zCksum2);
        if( bLossy && nDbRow!=nCksumRow ){
          nCksumRow = cksumDatasource(pData, nDbRow, zCksum1);
          assert( nCksumRow==nDbRow );
        }
        testCompareStr(zCksum1, zCksum2, &rc);
      }
    }
    tdb_close(pDb);
    testCaseFinish(rc);
    testDatasourceFree(pData);
  }

  return LSM_OK;
}

static int crash_test2(
  const char *zTitle,
  const char *zSystem,
  int bSystemCrash,               /* True for a system crash, false for app */
  int eSyncMode                   /* Either LSM_SAFETY_NORMAL or FULL */
){
  const int nTrial = 100;
  const int nRow = 10;
  int iTest;

  /* This test is for system-crashes only in full-sync mode only. */
  assert( bSystemCrash && eSyncMode==LSM_SAFETY_FULL );

  for(iTest=0; 1; iTest++){
    TestDb *pDb;
    int rc;
    int i;

    char zCksum1[TEST_CKSUM_BYTES];
    char zCksum2[TEST_CKSUM_BYTES];
    int nCksumRow = -1;

    Datasource *pData;            /* Test data */
    pData = getDatasource(iTest);
    if( !pData ) break;

    testCaseStart(&rc, "crash.%s.%d", zTitle, iTest);
    tdb_open(zSystem, 0, 1, &pDb);
    tdb_close(pDb);

    for(i=0; rc==LSM_OK && i<nTrial; i++){
      int iSync;                  /* fsync() to crash on */
     
      tdb_open(zSystem, 0, 0, &pDb);
      tdb_lsm_safety(pDb, LSM_SAFETY_FULL);

      for(iSync=1; iSync<=(nRow+4); iSync++){
        int nCurrent;               /* Current number of rows in db */
        int testrc = 0;
        int nDbRow;
        int j;

        tdb_lsm_prepare_sync_crash(pDb, iSync);
        nCurrent = testCountDatabase(pDb);

        for(j=0; j<nRow && testrc==0; j++){
          void *pKey; int nKey;
          void *pVal; int nVal;
          testDatasourceEntry(pData, nCurrent+j, &pKey, &nKey, &pVal, &nVal);
          testrc = tdb_write(pDb, pKey, nKey, pVal, nVal);
        }

        /* Close and reopen the database */
        tdb_close(pDb);
        tdb_open(zSystem, 0, 0, &pDb);
        tdb_lsm_safety(pDb, LSM_SAFETY_FULL);

        /* Check that we have not mysteriously lost synced data. */
        nDbRow = testCountDatabase(pDb);
        testCompareInt((nDbRow>=(nCurrent+j-1)), 1, &rc);

        /* Check that the database contents match the size. */
        if( nDbRow!=nCksumRow ){
          nCksumRow = cksumDatasource(pData, nDbRow, zCksum1);
          assert( nCksumRow==nDbRow );
        }
        nDbRow = testCksumDatabase(pDb, zCksum2);
        testCompareStr(zCksum1, zCksum2, &rc);
      }

      tdb_close(pDb);
    }
    testCaseFinish(rc);
    testDatasourceFree(pData);
  }

  return LSM_OK;
}


int do_crash_test(int nArg, char **azArg){
  struct Test {
    const char *zTest;
    const char *zSystem;
    int bSystemCrash;
    int eSafety;
    int (*x)(const char *, const char *, int bSysCrash, int eSyncMode);
  } aTest [] = {
    { "app.normal.lomem.1", "lsm_lomem", 0, LSM_SAFETY_NORMAL, crash_test1 },
    { "app.full.lomem.1",   "lsm_lomem", 0, LSM_SAFETY_FULL  , crash_test1 },
    { "app.normal.lsm.1",   "lsm",       0, LSM_SAFETY_NORMAL, crash_test1 },
    { "app.full.lsm.1",     "lsm",       0, LSM_SAFETY_FULL  , crash_test1 },
    { "sys.normal.lomem.1", "lsm_lomem", 1, LSM_SAFETY_NORMAL, crash_test1 },
    { "sys.full.lomem.1",   "lsm_lomem", 1, LSM_SAFETY_FULL  , crash_test1 },
    { "sys.full.lomem.2",   "lsm_lomem", 1, LSM_SAFETY_FULL  , crash_test2 },
    { 0, 0, 0, 0, 0 }
  };
  int iSel = -1;
  int i;
  int rc = 0;

  if( nArg>0 ){
    rc = testArgSelect(aTest, "test", azArg[0], &iSel);
  }

  for(i=0; rc==0 && i<(ArraySize(aTest)-1); i++){
    if( i==iSel || iSel<0 ){
      struct Test *p = &aTest[i];
      rc = p->x(p->zTest, p->zSystem, p->bSystemCrash, p->eSafety);
    }
  }

  return rc;
}

