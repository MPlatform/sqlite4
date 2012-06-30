
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

/*
** tdb_scan() callback used by testCountDatabase()
*/
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
      printf("%-50s ...", zTest);
      res = 1;
    }
    testFree(zTest);
    fflush(stdout);
  }

  return res;
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


/* Above this point are reusable test routines. Not clear that they
** should really be in this file.
*************************************************************************/

/*
** The database handle passed as the only argument must be an LSM database.
** This function simulates a system crash by:
*/
void testSystemCrash(TestDb *pDb){
  assert( tdb_lsm(pDb) );
  tdb_lsm_system_crash(pDb);
  tdb_close(pDb);
}

static Datasource *getDatasource(int iTest){
  DatasourceDefn aDefn[2] = {
    { TEST_DATASOURCE_SEQUENCE, 0, 0, 50, 100 }, 
    { TEST_DATASOURCE_RANDOM, 10, 15, 50, 100 }, 
  };

  if( iTest>=ArraySize(aDefn) ) return 0;
  return testDatasourceNew(&aDefn[iTest]);
}

static int crash_test1(
  const char *zPattern,
  const char *zTitle,
  const char *zSystem,            /* Database system to test */
  int eSyncMode                   /* Either LSM_SAFETY_NORMAL or FULL */
){
  const int nTrial = 1000;
  const int nRow = 10;
  int iDatasource;
  Datasource *pData = 0;          /* Test data */
  int bLossy;                     /* True if a crash may cause data loss */

  /* The crash event is "lossy" - implying that data may be lost following
  ** recovery - if the safety-mode is set to NORMAL.  */
  assert( eSyncMode==LSM_SAFETY_NORMAL || eSyncMode==LSM_SAFETY_FULL );
  bLossy = eSyncMode==LSM_SAFETY_NORMAL;

  /* Function getDatasource() provides a couple of different datasources.
  ** The following loop runs the test case once with each datasource.  */
  for(iDatasource=0; (pData = getDatasource(iDatasource)); iDatasource++){
    int iDot = 0;                 /* testCaseProgress() context */
    TestDb *pDb;
    int rc = LSM_OK;
    int i, j;

    char zCksum1[TEST_CKSUM_BYTES];
    int nCksumRow = -1;

    if( 0==testCaseBegin(&rc, zPattern, "crash.%s.%d", zTitle, iDatasource) ){
      continue;
    }

    /* Open and configure the LSM database connection. */
    pDb = 0;
    rc = tdb_open(zSystem, 0, 1, &pDb);
    if( rc==0 ) tdb_lsm_safety(pDb, eSyncMode);
    if( rc==0 ) tdb_lsm_prepare_system_crash(pDb);

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

        nCksumRow = testCksumDatabase(pDb, zCksum1);
        
        /* Do the crash. */
        testSystemCrash(pDb);

        /* Re-open (and re-configure) the LSM connection. */
        pDb = 0;
        rc = tdb_open(zSystem, 0, 0, &pDb);
        if( rc==0 ) tdb_lsm_safety(pDb, eSyncMode);
        if( rc==0 ) tdb_lsm_prepare_system_crash(pDb);

        /* Generate a checksum of the database. If the simulated crash is
        ** not supposed to be lossy, then this checksum should match the
        ** checksum generated above. 
        **
        ** Otherwise, if the crash was lossy, check how many rows are in
        ** the database following recovery. The new checksum should reflect 
        ** the fact that the database now contains key-value pairs 
        ** 0..(nDbRow-1) from datasource pData, inclusive.
        */
        nDbRow = testCksumDatabase(pDb, zCksum2);
        if( bLossy && nDbRow!=nCksumRow ){
          nCksumRow = cksumDatasource(pData, nDbRow, zCksum1);
          assert( nCksumRow==nDbRow );
        }

        /* Check that the checksum matches whatever it is that it is 
        ** supposed to match (see above).  */
        testCompareStr(zCksum1, zCksum2, &rc);
      }

      testCaseProgress(i, nTrial, testCaseNDot(), &iDot);
    }

    tdb_close(pDb);
    testCaseFinish(rc);
    testDatasourceFree(pData);
  }

  return LSM_OK;
}

#if 0
static int crash_test2(
  const char *zTitle,
  const char *zSystem,
  int eSyncMode                   /* Either LSM_SAFETY_NORMAL or FULL */
){
  const int nTrial = 100;
  const int nRow = 10;
  int iTest;

  /* This test is for system-crashes only in full-sync mode only. */
  assert( eSyncMode==LSM_SAFETY_FULL );

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
#endif

/*
** This test verifies that if a system crash occurs when checkpointing
** the database, data is not lost (assuming that any writes not synced
** to the db have been synced into the log file).
*/
static void crash_test3(int *pRc){
  const char *DBNAME = "testdb.lsm";
  const int nIter = 100;
  const DatasourceDefn defn = {TEST_DATASOURCE_RANDOM, 12, 16, 1000, 1000};

  int i;
  Datasource *pData;
  CksumDb *pCksumDb;
  TestDb *pDb;

  /* Allocate datasource. And calculate the expected checksums. */
  pData = testDatasourceNew(&defn);
  pCksumDb = testCksumArrayNew(pData, 15, 10);

  /* Setup the initial database. Save it using testSaveLsmdb(). */
  pDb = testOpen("lsm", 1, pRc);
  testWriteDatasourceRange(pDb, pData, 0, 100, pRc);
  testClose(&pDb);
  testSaveLsmdb(DBNAME);

  for(i=0; i<nIter && *pRc==0; i++){
    int iOpen;
    testRestoreLsmdb(DBNAME);
    for(iOpen=0; iOpen<5; iOpen++){
      char zCksum[TEST_CKSUM_BYTES];

      /* Open the database. Insert 10 more records. */
      pDb = testOpen("lsm", 0, pRc);
      testWriteDatasourceRange(pDb, pData, 100+iOpen*10, 10, pRc);

      /* Schedule a crash simulation then close the db. */
      tdb_lsm_prepare_sync_crash(pDb, 1 + (i%2));
      tdb_close(pDb);

      /* Open the database and check that the crash did not cause any
      ** data loss.  */
      pDb = testOpen("lsm", 0, pRc);
      testCksumDatabase(pDb, zCksum);
      testCompareStr(zCksum, testCksumArrayGet(pCksumDb, 110 + iOpen*10), pRc);
      testClose(&pDb);
    }
  }

  testDatasourceFree(pData);
}

void do_crash_test(const char *zPattern, int *pRc){
  struct Test {
    const char *zTest;
    const char *zSystem;
    int eSafety;
    int (*x)(const char *, const char *, const char *, int eSyncMode);
  } aTest [] = {
    { "sys.full.lomem.1",   "lsm_lomem", LSM_SAFETY_FULL  , crash_test1 },
    { "sys.normal.lomem.1", "lsm_lomem", LSM_SAFETY_NORMAL, crash_test1 },
  };
  int i;

#if 0
  for(i=0; *pRc==LSM_OK && i<ArraySize(aTest); i++){
    struct Test *p = &aTest[i];
    *pRc = p->x(zPattern, p->zTest, p->zSystem, p->eSafety);
  }
#endif

  if( testCaseBegin(pRc, zPattern, "%s", "crashtest3") ){
    crash_test3(pRc);
    testCaseFinish(*pRc);
  }
}



