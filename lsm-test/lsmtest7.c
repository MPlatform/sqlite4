

#include "lsmtest.h"


/*
** Test that the rules for when lsm_csr_next() and lsm_csr_prev() are
** enforced. Specifically:
**
**   * Both functions always return LSM_MISUSE if the cursor is at EOF
**     when they are called.
**
**   * lsm_csr_next() may only be used after lsm_csr_seek(LSM_SEEK_GE) or 
**     lsm_csr_first(). 
**
**   * lsm_csr_prev() may only be used after lsm_csr_seek(LSM_SEEK_LE) or 
**     lsm_csr_last().
*/
static void do_test_api1_lsm(lsm_db *pDb, int *pRc){
  int ret;
  lsm_cursor *pCsr;
  lsm_cursor *pCsr2;
  int nKey;
  void *pKey;

  ret = lsm_csr_open(pDb, &pCsr);
  testCompareInt(LSM_OK, ret, pRc);

  ret = lsm_csr_next(pCsr);
  testCompareInt(LSM_MISUSE, ret, pRc);
  ret = lsm_csr_prev(pCsr);
  testCompareInt(LSM_MISUSE, ret, pRc);

  ret = lsm_csr_seek(pCsr, "jjj", 3, LSM_SEEK_GE);
  testCompareInt(LSM_OK, ret, pRc);
  ret = lsm_csr_next(pCsr);
  testCompareInt(LSM_OK, ret, pRc);
  ret = lsm_csr_prev(pCsr);
  testCompareInt(LSM_MISUSE, ret, pRc);

  ret = lsm_csr_seek(pCsr, "jjj", 3, LSM_SEEK_LE);
  testCompareInt(LSM_OK, ret, pRc);
  ret = lsm_csr_next(pCsr);
  testCompareInt(LSM_MISUSE, ret, pRc);
  ret = lsm_csr_prev(pCsr);
  testCompareInt(LSM_OK, ret, pRc);

  ret = lsm_csr_seek(pCsr, "jjj", 3, LSM_SEEK_LEFAST);
  testCompareInt(LSM_OK, ret, pRc);
  ret = lsm_csr_next(pCsr);
  testCompareInt(LSM_MISUSE, ret, pRc);
  ret = lsm_csr_prev(pCsr);
  testCompareInt(LSM_MISUSE, ret, pRc);

  ret = lsm_csr_key(pCsr, &pKey, &nKey);
  testCompareInt(LSM_OK, ret, pRc);

  ret = lsm_csr_open(pDb, &pCsr2);
  testCompareInt(LSM_OK, ret, pRc);

  ret = lsm_csr_seek(pCsr2, pKey, nKey, LSM_SEEK_EQ);
  testCompareInt(LSM_OK, ret, pRc);
  testCompareInt(1, lsm_csr_valid(pCsr2), pRc);
  ret = lsm_csr_next(pCsr2);
  testCompareInt(LSM_MISUSE, ret, pRc);
  ret = lsm_csr_prev(pCsr2);
  testCompareInt(LSM_MISUSE, ret, pRc);

  lsm_csr_close(pCsr2);

  ret = lsm_csr_first(pCsr);
  testCompareInt(LSM_OK, ret, pRc);
  ret = lsm_csr_next(pCsr);
  testCompareInt(LSM_OK, ret, pRc);
  ret = lsm_csr_prev(pCsr);
  testCompareInt(LSM_MISUSE, ret, pRc);

  ret = lsm_csr_last(pCsr);
  testCompareInt(LSM_OK, ret, pRc);
  ret = lsm_csr_prev(pCsr);
  testCompareInt(LSM_OK, ret, pRc);
  ret = lsm_csr_next(pCsr);
  testCompareInt(LSM_MISUSE, ret, pRc);

  ret = lsm_csr_first(pCsr);
  while( lsm_csr_valid(pCsr) ){
    ret = lsm_csr_next(pCsr);
    testCompareInt(LSM_OK, ret, pRc);
  }
  ret = lsm_csr_next(pCsr);
  testCompareInt(LSM_OK, ret, pRc);
  ret = lsm_csr_prev(pCsr);
  testCompareInt(LSM_MISUSE, ret, pRc);

  ret = lsm_csr_last(pCsr);
  while( lsm_csr_valid(pCsr) ){
    ret = lsm_csr_prev(pCsr);
    testCompareInt(LSM_OK, ret, pRc);
  }
  ret = lsm_csr_prev(pCsr);
  testCompareInt(LSM_OK, ret, pRc);
  ret = lsm_csr_next(pCsr);
  testCompareInt(LSM_MISUSE, ret, pRc);

  lsm_csr_close(pCsr);
}

static void do_test_api1(const char *zPattern, int *pRc){
  if( testCaseBegin(pRc, zPattern, "api1.lsm") ){
    const DatasourceDefn defn = { TEST_DATASOURCE_RANDOM, 10, 15, 200, 250 };
    Datasource *pData;
    TestDb *pDb;
    int rc = 0;

    pDb = testOpen("lsm_lomem", 1, &rc);
    pData = testDatasourceNew(&defn);
    testWriteDatasourceRange(pDb, pData, 0, 1000, pRc);

    do_test_api1_lsm(tdb_lsm(pDb), pRc);

    testDatasourceFree(pData);
    testClose(&pDb);

    testCaseFinish(*pRc);
  }
}

void test_api(
  const char *zPattern,           /* Run test cases that match this pattern */
  int *pRc                        /* IN/OUT: Error code */
){
  do_test_api1(zPattern, pRc);
}

