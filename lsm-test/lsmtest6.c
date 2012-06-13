
#include "lsmtest.h"

typedef struct OomTest OomTest;
struct OomTest {
  lsm_env *pEnv;
  int iNext;                      /* Next value to pass to testMallocOom() */
  int nFail;                      /* Number of OOM events injected */
  int rc;                         /* Test case error code */
};

static void testOomStart(OomTest *p){
  memset(p, 0, sizeof(OomTest));
  p->iNext = 1;
  p->nFail = 1;
  p->pEnv = tdb_lsm_env();
}

static void xOomHook(OomTest *p){
  p->nFail++;
}

static int testOomContinue(OomTest *p){
  if( p->rc!=0 || (p->iNext>1 && p->nFail==0) ){
    return 0;
  }
  p->nFail = 0;
  testMallocOom(p->pEnv, p->iNext, 0, (void (*)(void*))xOomHook, (void *)p);
  return 1;
}

static void testOomEnable(OomTest *p, int bEnable){
  testMallocOomEnable(p->pEnv, bEnable);
}

static void testOomNext(OomTest *p){
  p->iNext++;
}

static int testOomHit(OomTest *p){
  return (p->nFail>0);
}

static int testOomFinish(OomTest *p){
  return p->rc;
}

static void testOomAssert(OomTest *p, int bVal){
  if( bVal==0 ){
    test_failed();
    p->rc = 1;
  }
}

/*
** Test that the error code matches the state of the OomTest object passed
** as the first argument. Specifically, check that rc is LSM_NOMEM if an 
** OOM error has already been injected, or LSM_OK if not.
*/
static void testOomAssertRc(OomTest *p, int rc){
  testOomAssert(p, rc==LSM_OK || rc==LSM_NOMEM);
  testOomAssert(p, testOomHit(p)==(rc==LSM_NOMEM));
}

static void testOomOpen(
  OomTest *pOom,
  const char *zName,
  lsm_db **ppDb,
  int *pRc
){
  if( *pRc==LSM_OK ){
    int rc;
    rc = lsm_new(tdb_lsm_env(), ppDb);
    if( rc==LSM_OK ) rc = lsm_open(*ppDb, zName);
    testOomAssertRc(pOom, rc);
    *pRc = rc;
  }
}

static void testOomFetchStr(
  OomTest *pOom,
  lsm_db *pDb,
  const char *zKey,
  const char *zVal,
  int *pRc
){
  testOomAssertRc(pOom, *pRc);
  if( *pRc==LSM_OK ){
    lsm_cursor *pCsr;
    int rc;
    int nKey = strlen(zKey);
    int nVal = strlen(zVal);

    rc = lsm_csr_open(pDb, &pCsr);
    if( rc==LSM_OK ) rc = lsm_csr_seek(pCsr, (void *)zKey, nKey, 0);
    testOomAssertRc(pOom, rc);

    if( rc==LSM_OK ){
      void *p; int n;
      testOomAssert(pOom, lsm_csr_valid(pCsr));

      rc = lsm_csr_key(pCsr, &p, &n);
      testOomAssertRc(pOom, rc);
      testOomAssert(pOom, rc!=LSM_OK || (n==nKey && memcmp(zKey, p, nKey)==0) );
    }

    if( rc==LSM_OK ){
      void *p; int n;
      testOomAssert(pOom, lsm_csr_valid(pCsr));

      rc = lsm_csr_value(pCsr, &p, &n);
      testOomAssertRc(pOom, rc);
      testOomAssert(pOom, rc!=LSM_OK || (n==nVal && memcmp(zVal, p, nVal)==0) );
    }

    lsm_csr_close(pCsr);
    *pRc = rc;
  }
}

static void testOomWriteStr(
  OomTest *pOom,
  lsm_db *pDb,
  const char *zKey,
  const char *zVal,
  int *pRc
){
  testOomAssertRc(pOom, *pRc);
  if( *pRc==LSM_OK ){
    int rc;
    int nKey = strlen(zKey);
    int nVal = strlen(zVal);

    rc = lsm_write(pDb, (void *)zKey, nKey, (void *)zVal, nVal);
    testOomAssertRc(pOom, rc);

    *pRc = rc;
  }
}

#define LSMTEST6_TESTDB "testdb.lsm" 

#include <unistd.h>

static void testDeleteTestdb(const char *zFile){
  char *zLog = testMallocPrintf("%s-log", zFile);
  unlink(zFile);
  unlink(zLog);
  testFree(zLog);
}

static void testSaveTestdb(const char *zFile){
  char *zLog = testMallocPrintf("%s-log", zFile);
  char *zFileSave = testMallocPrintf("%s-save", zFile);
  char *zLogSave = testMallocPrintf("%s-log-save", zFile);

  unlink(zFileSave);
  unlink(zLogSave);
  link(zFile, zFileSave);
  link(zLog, zLogSave);

  testFree(zLog); testFree(zFileSave); testFree(zLogSave);
}

static void testRestoreTestdb(const char *zFile){
  char *zLog = testMallocPrintf("%s-log", zFile);
  char *zFileSave = testMallocPrintf("%s-save", zFile);
  char *zLogSave = testMallocPrintf("%s-log-save", zFile);

  unlink(zFile);
  unlink(zLog);
  link(zFileSave, zFile);
  link(zLogSave, zLog);

  testFree(zLog); testFree(zFileSave); testFree(zLogSave);
}


static void setup_delete_db(){
  testDeleteTestdb(LSMTEST6_TESTDB);
}

static int lsmWriteStr(lsm_db *pDb, const char *zKey, const char *zVal){
  int nKey = strlen(zKey);
  int nVal = strlen(zVal);
  return lsm_write(pDb, (void *)zKey, nKey, (void *)zVal, nVal);
}

static void setup_populate_db(){
  const char *azStr[] = {
    "one",   "one",
    "two",   "four",
    "three", "nine",
    "four",  "sixteen",
    "five",  "twentyfive",
    "six",   "thirtysix",
    "seven", "fourtynine",
    "eight", "sixtyfour",
  };
  int rc;
  int ii;
  lsm_db *pDb;

  testDeleteTestdb(LSMTEST6_TESTDB);

  rc = lsm_new(tdb_lsm_env(), &pDb);
  if( rc==LSM_OK ) rc = lsm_open(pDb, LSMTEST6_TESTDB);

  for(ii=0; rc==LSM_OK && ii<ArraySize(azStr); ii+=2){
    rc = lsmWriteStr(pDb, azStr[ii], azStr[ii+1]);
  }
  lsm_close(pDb);

  assert( rc==LSM_OK );
}

static void simple_oom_1(OomTest *pOom){
  int rc;
  lsm_db *pDb;

  rc = lsm_new(tdb_lsm_env(), &pDb);
  testOomAssertRc(pOom, rc);

  lsm_close(pDb);
}

static void simple_oom_2(OomTest *pOom){
  int rc;
  lsm_db *pDb;

  rc = lsm_new(tdb_lsm_env(), &pDb);
  if( rc==LSM_OK ){
    rc = lsm_open(pDb, "testdb.lsm");
  }
  testOomAssertRc(pOom, rc);

  lsm_close(pDb);
}

static void simple_oom_3(OomTest *pOom){
  int rc = LSM_OK;
  lsm_db *pDb;

  testOomOpen(pOom, LSMTEST6_TESTDB, &pDb, &rc);

  testOomFetchStr(pOom, pDb, "four",  "sixteen",    &rc);
  testOomFetchStr(pOom, pDb, "seven", "fourtynine", &rc);
  testOomFetchStr(pOom, pDb, "one",   "one",        &rc);
  testOomFetchStr(pOom, pDb, "eight", "sixtyfour",  &rc);

  lsm_close(pDb);
}

static void simple_oom_4(OomTest *pOom){
  int rc = LSM_OK;
  lsm_db *pDb;

  testDeleteTestdb(LSMTEST6_TESTDB);
  testOomOpen(pOom, LSMTEST6_TESTDB, &pDb, &rc);

  testOomWriteStr(pOom, pDb, "123", "onetwothree", &rc);
  testOomWriteStr(pOom, pDb, "456", "fourfivesix", &rc);
  testOomWriteStr(pOom, pDb, "789", "seveneightnine", &rc);
  testOomWriteStr(pOom, pDb, "123", "teneleventwelve", &rc);
  testOomWriteStr(pOom, pDb, "456", "fourteenfifteensixteen", &rc);

  lsm_close(pDb);
}

static void do_test_oom1(const char *zPattern, int *pRc){
  struct SimpleOom {
    const char *zName;
    void (*xSetup)(void);
    void (*xFunc)(OomTest *);
  } aSimple[] = {
    { "oom1.lsm.1", setup_delete_db,   simple_oom_1 },
    { "oom1.lsm.2", setup_delete_db,   simple_oom_2 },
    { "oom1.lsm.3", setup_populate_db, simple_oom_3 },
    { "oom1.lsm.4", setup_delete_db,   simple_oom_4 },
  };
  int i;

  for(i=0; i<ArraySize(aSimple); i++){
    if( *pRc==0 && testCaseBegin(pRc, zPattern, "%s", aSimple[i].zName) ){
      OomTest t;

      if( aSimple[i].xSetup ){
        aSimple[i].xSetup();
      }

      for(testOomStart(&t); testOomContinue(&t); testOomNext(&t)){
        aSimple[i].xFunc(&t);
      }

      printf("(%d injections).", t.iNext-2);
      testCaseFinish( (*pRc = testOomFinish(&t)) );
      testMallocOom(tdb_lsm_env(), 0, 0, 0, 0);
    }
  }
}



void test_oom(
  const char *zPattern,           /* Run test cases that match this pattern */
  int *pRc                        /* IN/OUT: Error code */
){
  do_test_oom1(zPattern, pRc);
}


