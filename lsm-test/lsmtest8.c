
/*
** This file contains test cases to verify that "live-recovery" following
** a mid-transaction failure of a writer process.
*/

#include "lsmtest.h"

typedef struct SetupStep SetupStep;
struct SetupStep {
  int workflags;                  /* Flags to pass to lsm_work() */
  int iInsStart;                  /* First key-value from ds to insert */
  int nIns;                       /* Number of rows to insert */
  int iDelStart;                  /* First key from ds to delete */
  int nDel;                       /* Number of rows to delete */
};

static void doSetupStep(
  TestDb *pDb, 
  Datasource *pData, 
  const SetupStep *pStep, 
  int *pRc
){
  testWriteDatasourceRange(pDb, pData, pStep->iInsStart, pStep->nIns, pRc);
  testDeleteDatasourceRange(pDb, pData, pStep->iDelStart, pStep->nDel, pRc);
  if( *pRc==0 ){
    lsm_db *db = tdb_lsm(pDb);
    *pRc = lsm_work(db, pStep->workflags, 0, 0);
  }
}

static void doSetupStepArray(
  TestDb *pDb, 
  Datasource *pData, 
  const SetupStep *aStep, 
  int nStep
){
  int i;
  for(i=0; i<nStep; i++){
    int rc = 0;
    doSetupStep(pDb, pData, &aStep[i], &rc);
    assert( rc==0 );
  }
}

static void setupDatabase1(TestDb *pDb, Datasource **ppData){
  const SetupStep aStep[] = {
    { 0,                                  1,     2000, 0, 0 },
    { LSM_WORK_CHECKPOINT|LSM_WORK_FLUSH, 0,     0, 0, 0 },
    { 0,                                  10001, 1000, 0, 0 },
  };
  const DatasourceDefn defn = {TEST_DATASOURCE_RANDOM, 12, 16, 100, 500};
  Datasource *pData;

  pData = testDatasourceNew(&defn);
  doSetupStepArray(pDb, pData, aStep, ArraySize(aStep));
  if( ppData ){
    *ppData = pData;
  }else{
    testDatasourceFree(pData);
  }
}

/*
** This function makes a copy of the three files associated with LSM 
** database zDb (i.e. if zDb is "test.db", it makes copies of "test.db",
** "test.db-log" and "test.db-shm").
**
** It then opens a new database connection to the copy with the xLock() call
** instrumented so that it appears that some other process already connected
** to the db (holding a shared lock on DMS2). This prevents recovery from
** running. Then:
**
**    1) Check that the checksum of the database is zCksum. 
**    2) Write a few keys to the database. Then delete the same keys. 
**    3) Check that the checksum is zCksum.
**    4) Flush the db to disk and run a checkpoint. 
**    5) Check once more that the checksum is still zCksum.
*/
static void doLiveRecovery(const char *zDb, const char *zCksum, int *pRc){
  const DatasourceDefn defn = {TEST_DATASOURCE_RANDOM, 20, 25, 100, 500};
  Datasource *pData;
  const char *zCopy = "testcopy.lsm";
  char zCksum2[TEST_CKSUM_BYTES];
  TestDb *pDb = 0;
  int rc;

  pData = testDatasourceNew(&defn);

  testCopyLsmdb(zDb, zCopy);
  rc = tdb_lsm_open("test_no_recovery=1", zCopy, 0, &pDb);
  if( rc==0 ){
    lsm_db *db;
    testCksumDatabase(pDb, zCksum2);
    testCompareStr(zCksum, zCksum2, &rc);

    testWriteDatasourceRange(pDb, pData, 1, 10, &rc);
    testDeleteDatasourceRange(pDb, pData, 1, 10, &rc);

    if( rc==0 ){
      db = tdb_lsm(pDb);
      rc = lsm_work(db, LSM_WORK_FLUSH|LSM_WORK_CHECKPOINT, 0, 0);
    }

    testCksumDatabase(pDb, zCksum2);
    testCompareStr(zCksum, zCksum2, &rc);
  }

  testDatasourceFree(pData);
  testClose(&pDb);
  *pRc = rc;
}

static void doWriterCrash1(int *pRc){
  const int nWrite = 2000;
  const int iWriteStart = 20000;
  int rc = 0;
  TestDb *pDb = 0;
  Datasource *pData = 0;

  pDb = testOpen("lsm", 1, &rc);
  if( rc==0 ){
    int iDot = 0;
    char zCksum[TEST_CKSUM_BYTES];
    int i;
    setupDatabase1(pDb, &pData);
    testCksumDatabase(pDb, zCksum);
    testBegin(pDb, 2, &rc);
    for(i=0; rc==0 && i<nWrite; i++){
      testCaseProgress(i, nWrite, testCaseNDot(), &iDot);
      testWriteDatasourceRange(pDb, pData, iWriteStart+i, 1, &rc);
      doLiveRecovery("testdb.lsm", zCksum, &rc);
    }
  }
  testCommit(pDb, 0, &rc);
  testClose(&pDb);
  testDatasourceFree(pData);
  *pRc = rc;
}

void do_writer_crash_test(const char *zPattern, int *pRc){
  if( testCaseBegin(pRc, zPattern, "writercrash1.lsm") ){
    doWriterCrash1(pRc);
    testCaseFinish(*pRc);
  }
}


