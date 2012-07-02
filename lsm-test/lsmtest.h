
#ifndef __WRAPPER_INT_H_
#define __WRAPPER_INT_H_

#include "lsmtest_tdb.h"
#include "sqlite3.h"
#include "lsm.h"

void lsmSortedDumpStructure(lsm_db *pDb, void *, int, const char *);

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef unsigned int  u32;
typedef unsigned char u8;
typedef long long int i64;
typedef unsigned long long int u64;


#define ArraySize(x) ((int)(sizeof(x) / sizeof((x)[0])))

#define MIN(x,y) ((x)<(y) ? (x) : (y))
#define MAX(x,y) ((x)>(y) ? (x) : (y))

#define unused_parameter(x) (void)(x)

#define TESTDB_DEFAULT_PAGE_SIZE   4096
#define TESTDB_DEFAULT_CACHE_SIZE  2048

/*
** Ideally, these should be in wrapper.c. But they are here instead so that 
** they can be used by the C++ database wrappers in wrapper2.cc.
*/
typedef struct DatabaseMethods DatabaseMethods;
struct TestDb {
  DatabaseMethods const *pMethods;          /* Database methods */
  const char *zLibrary;                     /* Library name for tdb_open() */
};
struct DatabaseMethods {
  int (*xClose)(TestDb *);
  int (*xWrite)(TestDb *, void *, int , void *, int);
  int (*xDelete)(TestDb *, void *, int);
  int (*xFetch)(TestDb *, void *, int, void **, int *);
  int (*xScan)(TestDb *, void *, int, void *, int, void *, int,
    void (*)(void *, void *, int , void *, int)
  );
  int (*xBegin)(TestDb *, int);
  int (*xCommit)(TestDb *, int);
  int (*xRollback)(TestDb *, int);
};

/* 
** Functions in wrapper2.cc (a C++ source file). wrapper2.cc contains the
** wrapper for Kyoto Cabinet. Kyoto cabinet has a C API, but
** the primary interface is the C++ API.
*/
int test_kc_open(const char *zFilename, int bClear, TestDb **ppDb);
int test_kc_close(TestDb *);
int test_kc_write(TestDb *, void *, int , void *, int);
int test_kc_delete(TestDb *, void *, int);
int test_kc_fetch(TestDb *, void *, int, void **, int *);
int test_kc_scan(TestDb *, void *, int, void *, int, void *, int,
  void (*)(void *, void *, int , void *, int)
);

/* 
** Functions in wrapper3.c. This file contains the tdb wrapper for lsm.
** The wrapper for lsm is a bit more involved than the others, as it 
** includes code for a couple of different lsm configurations, and for
** various types of fault injection and robustness testing.
*/
int test_lsm_open(const char *zFilename, int bClear, TestDb **ppDb);
int test_lsm_lomem_open(const char *zFilename, int bClear, TestDb **ppDb);
int test_lsm_small_open(const char *zFilename, int bClear, TestDb **ppDb);
int test_lsm_mt2(const char *zFilename, int bClear, TestDb **ppDb);
int test_lsm_mt3(const char *zFilename, int bClear, TestDb **ppDb);

/* Functions in testutil.c. */
int  testPrngInit(void);
u32  testPrngValue(u32 iVal);
void testPrngArray(u32 iVal, u32 *aOut, int nOut);
void testPrngString(u32 iVal, char *aOut, int nOut);

void testErrorInit(int argc, char **);
void testPrintError(const char *zFormat, ...);
void testPrintUsage(const char *zArgs);
void testTimeInit(void);
int  testTimeGet(void);

/* Functions in testmem.c. */
void testMallocInstall(lsm_env *pEnv);
void testMallocUninstall(lsm_env *pEnv);
void testMallocCheck(lsm_env *pEnv, int *, int *, FILE *);
void testMallocOom(lsm_env *pEnv, int, int, void(*)(void*), void *);
void testMallocOomEnable(lsm_env *pEnv, int);

/* lsmtest.c */
TestDb *testOpen(const char *zSystem, int, int *pRc);
void testReopen(TestDb **ppDb, int *pRc);
void testClose(TestDb **ppDb);

void testFetch(TestDb *, void *, int, void *, int, int *);
void testWrite(TestDb *, void *, int, void *, int, int *);
void testDelete(TestDb *, void *, int, int *);
void testWriteStr(TestDb *, const char *, const char *zVal, int *pRc);
void testFetchStr(TestDb *, const char *, const char *, int *pRc);

void testBegin(TestDb *pDb, int iTrans, int *pRc);
void testCommit(TestDb *pDb, int iTrans, int *pRc);

void test_failed(void);

char *testMallocPrintf(const char *zFormat, ...);
char *testMallocVPrintf(const char *zFormat, va_list ap);
int testGlobMatch(const char *zPattern, const char *zStr);

void testScanCompare(TestDb *, TestDb *, int, void *, int, void *, int, int *);

void *testMalloc(int);
void *testRealloc(void *, int);
void testFree(void *);

/* testio.c */
int testVfsConfigureDb(TestDb *pDb);

/* testfunc.c */
int do_show(int nArg, char **azArg);
int do_work(int nArg, char **azArg);

/* testio.c */
int do_io(int nArg, char **azArg);

/* lsmtest2.c */
void do_crash_test(const char *zPattern, int *pRc);
int do_rollback_test(int nArg, char **azArg);

/* test3.c */
void test_rollback(const char *zSystem, const char *zPattern, int *pRc);

/* test4.c */
void test_mc(const char *zSystem, const char *zPattern, int *pRc);

/* test5.c */
void test_mt(const char *zSystem, const char *zPattern, int *pRc);

/* lsmtest6.c */
void test_oom(const char *zPattern, int *pRc);
void testDeleteLsmdb(const char *zFile);

void testSaveLsmdb(const char *zFile);
void testRestoreLsmdb(const char *zFile);

/*************************************************************************
** Interface to functionality in test_datasource.c.
*/
typedef struct Datasource Datasource;
typedef struct DatasourceDefn DatasourceDefn;

struct DatasourceDefn {
  int eType;                      /* A TEST_DATASOURCE_* value */
  int nMinKey;                    /* Minimum key size */
  int nMaxKey;                    /* Maximum key size */
  int nMinVal;                    /* Minimum value size */
  int nMaxVal;                    /* Maximum value size */
};

#define TEST_DATASOURCE_RANDOM    1
#define TEST_DATASOURCE_SEQUENCE  2

char *testDatasourceName(const DatasourceDefn *);
Datasource *testDatasourceNew(const DatasourceDefn *);
void testDatasourceFree(Datasource *);
void testDatasourceEntry(Datasource *, int, void **, int *, void **, int *);
/* End of test_datasource.c interface.
*************************************************************************/

void testWriteDatasource(TestDb *, Datasource *, int, int *);
void testWriteDatasourceRange(TestDb *, Datasource *, int, int, int *);
void testDeleteDatasource(TestDb *, Datasource *, int, int *);
void testDeleteDatasourceRange(TestDb *, Datasource *, int, int, int *);


/* test1.c */
void test_data_1(TestDb *pDb, int *pRc);
void test_data_2(TestDb *pDb, int *pRc);
void test_data_3(const char *, const char *, int *pRc);
void testDbContents(TestDb *, Datasource *, int, int, int, int, int, int *);
void testCaseProgress(int, int, int, int *);
int testCaseNDot(void);

typedef struct CksumDb CksumDb;
CksumDb *testCksumArrayNew(Datasource *, int, int, int);
char *testCksumArrayGet(CksumDb *, int);
void testCksumArrayFree(CksumDb *);
void testCaseStart(int *pRc, char *zFmt, ...);
void testCaseFinish(int rc);
void testCaseSkip(void);
int testCaseBegin(int *, const char *, const char *, ...);

#define TEST_CKSUM_BYTES 29
int testCksumDatabase(TestDb *pDb, char *zOut);
int testCountDatabase(TestDb *pDb);
void testCompareInt(int, int, int *);
void testCompareStr(const char *z1, const char *z2, int *pRc);


/*
** Similar to the Tcl_GetIndexFromObjStruct() Tcl library function.
*/
#define testArgSelect(w,x,y,z) testArgSelectX(w,x,sizeof(w[0]),y,z)
int testArgSelectX(void *, const char *, int, const char *, int *);

#ifdef __cplusplus
}  /* End of the 'extern "C"' block */
#endif

#endif
