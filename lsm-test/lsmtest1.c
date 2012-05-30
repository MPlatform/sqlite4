
#include "lsmtest.h"

#define DATA_SEQUENTIAL TEST_DATASOURCE_SEQUENCE
#define DATA_RANDOM     TEST_DATASOURCE_RANDOM

typedef struct Datatest Datatest;
typedef struct MinMax MinMax;

struct MinMax {
  int nMin;
  int nMax;
};

/*
** An instance of the following structure contains parameters used to
** customize the test function in this file. Test procedure:
**
**   1. Create a data-source based on the "datasource definition" vars.
**
**   2. Insert nRow key value pairs into the database.
**
**   3. Delete all keys from the database. Deletes are done in the same 
**      order as the inserts.
**
** During steps 2 and 3 above, after each Datatest.nVerify inserts or
** deletes, the following:
**
**   a. Run Datasource.nTest key lookups and check the results are as expected.
**
**   b. If Datasource.bTestScan is true, run a handful (8) of range
**      queries (scanning forwards and backwards). Check that the results
**      are as expected.
**
**   c. Close and reopen the database. Then run (a) and (b) again.
*/
struct Datatest {
  /* Datasource definition */
  DatasourceDefn defn;

  /* Test procedure parameters */
  int nRow;                       /* Number of rows to insert then delete */
  int nVerify;                    /* How often to verify the db contents */
  int nTest;                      /* Number of keys to test (0==all) */
  int bTestScan;                  /* True to do scan tests */
};

static char *getName(const char *zSystem, Datatest *pTest){
  char *zRet;
  char *zData;
  zData = testDatasourceName(&pTest->defn);
  zRet = testMallocPrintf("data.%s.%s.%d.%d", 
      zSystem, zData, pTest->nRow, pTest->nVerify
  );
  testFree(zData);
  return zRet;
}

static void startTestCase(int *pRc, const char *zSystem, Datatest *pTest){
  char *zName;
  zName = getName(zSystem, pTest);
  testCaseStart(pRc, "% -*s", 50, zName);
  free(zName);
}

static int testControlDb(TestDb **ppDb){
#ifdef HAVE_KYOTOCABINET
  return tdb_open("kyotocabinet", "tmp.db", 1, ppDb);
#else
  return tdb_open("sqlite3", "tmp.db", 1, ppDb);
#endif
}

/*
** This function is called to test that the contents of database pDb
** are as expected. In this case, expected is defined as containing
** key-value pairs iFirst through iLast, inclusive, from data source 
** pData. In other words, a loop like the following could be used to
** construct a database with identical contents from scratch.
**
**   for(i=iFirst; i<=iLast; i++){
**     testDatasourceEntry(pData, i, &pKey, &nKey, &pVal, &nVal);
**     // insert (pKey, nKey) -> (pVal, nVal) into database
**   }
**
** The key domain consists of keys 0 to (nRow-1), inclusive, from
** data source pData. For both scan and lookup tests, keys are selected
** pseudo-randomly from within this set.
**
** This function runs nLookupTest lookup tests and nScanTest scan tests.
**
** A lookup test consists of selecting a key from the domain and querying
** pDb for it. The test fails if the presence of the key and, if present,
** the associated value do not match the expectations defined above.
**
** A scan test involves selecting a key from the domain and running
** the following queries:
**
**   1. Scan all keys equal to or greater than the key, in ascending order.
**   2. Scan all keys equal to or smaller than the key, in descending order.
**
** Additionally, if nLookupTest is greater than zero, the following are
** run once:
**
**   1. Scan all keys in the db, in ascending order.
**   2. Scan all keys in the db, in descending order.
**
** As you would assume, the test fails if the returned values do not match
** expectations.
*/
void testDbContents(
  TestDb *pDb,                    /* Database handle being tested */
  Datasource *pData,              /* pDb contains data from here */
  int nRow,                       /* Size of key domain */
  int iFirst,                     /* Index of first key from pData in pDb */
  int iLast,                      /* Index of last key from pData in pDb */
  int nLookupTest,                /* Number of lookup tests to run */
  int nScanTest,                  /* Number of scan tests to run */
  int *pRc                        /* IN/OUT: Error code */
){
  int j;
  int rc = *pRc;

  if( rc==0 && nScanTest ){
    TestDb *pDb2 = 0;

    /* Open a control db (i.e. one that we assume works) */
    rc = testControlDb(&pDb2);

    for(j=iFirst; rc==0 && j<=iLast; j++){
      void *pKey; int nKey;         /* Database key to insert */
      void *pVal; int nVal;         /* Database value to insert */
      testDatasourceEntry(pData, j, &pKey, &nKey, &pVal, &nVal);
      rc = tdb_write(pDb2, pKey, nKey, pVal, nVal);
    }

    if( rc==0 ){
      int iKey1;
      int iKey2;
      void *pKey1; int nKey1;       /* Start key */
      void *pKey2; int nKey2;       /* Final key */

      iKey1 = testPrngValue((iFirst<<8) + (iLast<<16)) % nRow;
      iKey2 = testPrngValue((iLast<<8) + (iFirst<<16)) % nRow;
      testDatasourceEntry(pData, iKey1, &pKey2, &nKey1, 0, 0);
      pKey1 = testMalloc(nKey1+1);
      memcpy(pKey1, pKey2, nKey1+1);
      testDatasourceEntry(pData, iKey2, &pKey2, &nKey2, 0, 0);

      testScanCompare(pDb2, pDb, 0, 0, 0,         0, 0,         &rc);
      testScanCompare(pDb2, pDb, 0, 0, 0,         pKey2, nKey2, &rc);
      testScanCompare(pDb2, pDb, 0, pKey1, nKey1, 0, 0,         &rc);
      testScanCompare(pDb2, pDb, 0, pKey1, nKey1, pKey2, nKey2, &rc);
      testScanCompare(pDb2, pDb, 1, 0, 0,         0, 0,         &rc);
      testScanCompare(pDb2, pDb, 1, 0, 0,         pKey2, nKey2, &rc);
      testScanCompare(pDb2, pDb, 1, pKey1, nKey1, 0, 0,         &rc);
      testScanCompare(pDb2, pDb, 1, pKey1, nKey1, pKey2, nKey2, &rc);

      testFree(pKey1);
    }
    tdb_close(pDb2);
  }

  /* Test some lookups. */
  for(j=0; rc==0 && j<nLookupTest; j++){
    int iKey;                     /* Datasource key to test */
    void *pKey; int nKey;         /* Database key to query for */
    void *pVal; int nVal;         /* Expected result of query */

    if( nLookupTest>=nRow ){
      iKey = j;
    }else{
      iKey = testPrngValue(j + (iFirst<<8) + (iLast<<16)) % nRow;
    }

    testDatasourceEntry(pData, iKey, &pKey, &nKey, &pVal, &nVal);
    if( iFirst>iKey || iKey>iLast ){
      pVal = 0;
      nVal = -1;
    }

    testFetch(pDb, pKey, nKey, pVal, nVal, &rc);
  }

  *pRc = rc;
}

void testCaseProgress(int i, int n, int nDot, int *piDot){
  int iDot = *piDot;
  while( iDot < ( ((nDot*2+1) * i) / (n*2) ) ){
    testCaseDot();
    iDot++;
  }
  *piDot = iDot;
}

static void printScanCb(
    void *pCtx, void *pKey, int nKey, void *pVal, int nVal
){
  printf("%s\n", (char *)pKey);
  fflush(stdout);
}

static void doDataTest(
  const char *zSystem,            /* Database system to test */
  Datatest *p,                    /* Structure containing test parameters */
  int *pRc                        /* OUT: Error code */
){
  int i;
  int iDot;
  int rc = LSM_OK;
  Datasource *pData;
  TestDb *pDb;

  /* Start the test case, open a database and allocate the datasource. */
  startTestCase(&rc, zSystem, p);
  pDb = testOpen(zSystem, 1, &rc);
  pData = testDatasourceNew(&p->defn);

  i = 0;
  iDot = 0;
  while( rc==LSM_OK && i<p->nRow ){

    /* Insert some data */
    testWriteDatasourceRange(pDb, pData, i, p->nVerify, &rc);
    i += p->nVerify;

    /* Check that the db content is correct. */
    testDbContents(pDb, pData, p->nRow, 0, i-1, p->nTest, p->bTestScan, &rc);

    /* Close and reopen the database. */
    testReopen(&pDb, &rc);

    /* Check that the db content is still correct. */
    testDbContents(pDb, pData, p->nRow, 0, i-1, p->nTest, p->bTestScan, &rc);

    /* Update the progress dots... */
    testCaseProgress(i, p->nRow, 10, &iDot);
  }

  i = 0;
  iDot = 0;
  while( rc==LSM_OK && i<p->nRow ){

    /* Delete some entries */
    testDeleteDatasourceRange(pDb, pData, i, p->nVerify, &rc);
    i += p->nVerify;

    /* Check that the db content is correct. */
    testDbContents(pDb, pData, p->nRow, i, p->nRow-1,p->nTest,p->bTestScan,&rc);

    /* Close and reopen the database. */
    testReopen(&pDb, &rc);

    /* Check that the db content is still correct. */
    testDbContents(pDb, pData, p->nRow, i, p->nRow-1,p->nTest,p->bTestScan,&rc);

    /* Update the progress dots... */
    testCaseProgress(i, p->nRow, 10, &iDot);
  }

  /* Free the datasource, close the database and finish the test case. */
  testDatasourceFree(pData);
  tdb_close(pDb);
  testCaseFinish(rc);
  *pRc = rc;
}


void test_data_3(
  const char *zSystem,            /* Database system name */
  const char *zPattern,           /* Run test cases that match this pattern */
  int *pRc                        /* IN/OUT: Error code */
){
  Datatest aTest[] = {
    { {DATA_RANDOM,     20,25,     100,200},       1000,  250, 1000, 1},
    { {DATA_RANDOM,     8,10,      100,200},       1000,  250, 1000, 1},
    { {DATA_RANDOM,     8,10,      10,20},         1000,  250, 1000, 1},
    { {DATA_RANDOM,     8,10,      1000,2000},     1000,  250, 1000, 1},
    { {DATA_RANDOM,     8,100,     10000,20000},    100,   25,  100, 1},
    { {DATA_RANDOM,     80,100,    10,20},         1000,  250, 1000, 1},
    { {DATA_RANDOM,     5000,6000, 10,20},          100,   25,  100, 1},
    { {DATA_SEQUENTIAL, 5,10,      10,20},         1000,  250, 1000, 1},
    { {DATA_SEQUENTIAL, 5,10,      100,200},       1000,  250, 1000, 1},
    { {DATA_SEQUENTIAL, 5,10,      1000,2000},     1000,  250, 1000, 1},
    { {DATA_SEQUENTIAL, 5,100,     10000,20000},    100,   25,  100, 1},
    { {DATA_RANDOM,     10,10,     100,100},     100000, 1000,  100, 0},
    { {DATA_SEQUENTIAL, 10,10,     100,100},     100000, 1000,  100, 0},
  };

  int i;

  for(i=0; *pRc==LSM_OK && i<ArraySize(aTest); i++){
    int bRun = 1;

    if( zPattern ){
      char *zName = getName(zSystem, &aTest[i]);
      bRun = testGlobMatch(zPattern, zName);
      free(zName);
    }

    if( bRun ){
      doDataTest(zSystem, &aTest[i], pRc);
    }
  }
}

