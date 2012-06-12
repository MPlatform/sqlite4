
#include "lsmtest.h"

typedef struct OomTest OomTest;
struct OomTest {
  lsm_env *pEnv;
  int iNext;                      /* Next value to pass to testOomStart() */
  int nFail;                      /* Next value to pass to testOomStart() */
  int rc;                         /* Return code */
};

static void testOomStart(OomTest *p){
  memset(p, 0, sizeof(OomTest));
  p->iNext = 1;
  p->pEnv = tdb_lsm_env();
}

static void xOomHook(void *pCtx){
  OomTest *p = (OomTest *)pCtx;
  p->nFail++;
}

static int testOomContinue(OomTest *p){
  if( p->rc!=0 || (p->iNext>1 && p->nFail==0) ){
    return 0;
  }
  p->nFail = 0;
  testMallocOom(p->pEnv, p->iNext, 0, xOomHook, (void *)p);
  return 1;
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


static void simple_oom_1(OomTest *pOom){
  int rc;
  int bHit;
  lsm_db *pDb;

  rc = lsm_new(tdb_lsm_env(), &pDb);

  bHit = testOomHit(pOom);
  testOomAssert(pOom, (bHit==0 || (rc==LSM_NOMEM && pDb==0)));
  testOomAssert(pOom, (bHit==1 || (rc==LSM_OK    && pDb!=0)));

  lsm_close(pDb);
}

static void simple_oom_2(OomTest *pOom){
  int rc;
  int bHit;
  lsm_db *pDb;

  rc = lsm_new(tdb_lsm_env(), &pDb);
  if( rc==LSM_OK ){
    rc = lsm_open(pDb, "testdb.lsm");
  }

  bHit = testOomHit(pOom);
  testOomAssert(pOom, (bHit==0 || (rc==LSM_NOMEM)));
  testOomAssert(pOom, (bHit==1 || (rc==LSM_OK   )));

  lsm_close(pDb);
}


static void do_test_oom1(const char *zPattern, int *pRc) {
  struct SimpleOom {
    const char *zName;
    int (*xFunc)(OomTest *);
  } aSimple[] = {
    { "oom1.lsm.1", simple_oom_1 },
    { "oom1.lsm.2", simple_oom_2 }
  };
  int i;

  for(i=0; i<ArraySize(aSimple); i++){
    if( *pRc==0 && testCaseBegin(pRc, zPattern, "%s", aSimple[i].zName) ){
      OomTest t;
      for(testOomStart(&t); testOomContinue(&t); testOomNext(&t)){
        aSimple[i].xFunc(&t);
      }
      testCaseFinish( (*pRc = testOomFinish(&t)) );
    }
  }
}



void test_oom(
  const char *zPattern,           /* Run test cases that match this pattern */
  int *pRc                        /* IN/OUT: Error code */
){
  do_test_oom1(zPattern, pRc);
}




