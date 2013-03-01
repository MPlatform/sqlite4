/*
** 2013 March 1
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains C code for a program that links against SQLite
** versions 3 and 4. It contains a few simple performance test routines
** that can be run against either database system.
*/

#include "sqlite4.h"
#include "sqlite3.h"
#include "lsm.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#define SQLITE3_DB_FILE "test.db3"
#define SQLITE4_DB_FILE "test.db4"

#include "lsmtest_util.c"

/*
** Unlink database zDb and its supporting files (wal, shm, journal, and log).
** This function works with both lsm and sqlite3 databases.
*/
static int unlink_db(const char *zDb){
  int i;
  const char *azExt[] = { "", "-shm", "-wal", "-journal", "-log", 0 };

  for(i=0; azExt[i]; i++){
    char *zFile = sqlite4_mprintf(0, "%s%s", zDb, azExt[i]);
    unlink(zFile);
    sqlite4_free(0, zFile);
  }

  return 0;
}

static char *create_schema_sql(int nIdx){
  char *zSchema;
  int i;

  zSchema = sqlite4_mprintf(0, "CREATE TABLE t1(k PRIMARY KEY,");
  for(i=0; i<nIdx; i++){
    zSchema = sqlite4_mprintf(0, "%z c%d BLOB,", zSchema, i);
  }
  zSchema = sqlite4_mprintf(0, "%z v BLOB);", zSchema);

  for(i=0; i<nIdx; i++){
    zSchema = sqlite4_mprintf(
      0, "%z\nCREATE INDEX i%d ON t1 (c%d);", zSchema, i, i
    );
  }

  return zSchema;
}

static char *create_insert_sql(int nIdx){
  char *zInsert;
  int i;

  zInsert = sqlite4_mprintf(0, "INSERT INTO t1 VALUES(rblob(:1, 8, 20),");
  for(i=0; i<nIdx; i++){
    zInsert = sqlite4_mprintf(0, "%z rblob((:1<<%d)+:1, 8, 20),", zInsert, i);
  }
  zInsert = sqlite4_mprintf(0, "%z rblob((:1<<%d)+:1, 100, 150));", zInsert, i);

  return zInsert;
}

static char *create_select_sql(int iIdx){
  char *zSql;
  if( iIdx==0 ){
    zSql = sqlite4_mprintf(0, "SELECT * FROM t1 WHERE k = rblob(:1, 8, 20)");
  }else{
    int iCol = iIdx-1;
    zSql = sqlite4_mprintf(0, 
        "SELECT * FROM t1 WHERE c%d = rblob((:1<<%d)+:1, 8, 20)", iCol, iCol
    );
  }
  return zSql;
}

static int do_explode(const char *zLine, int rc, int iLine){
  if( rc ){
    fprintf(stderr, "ERROR: \"%s\" at line %d failed. rc=%d\n", 
        zLine, iLine, rc
    );
    exit(-1);
  }
  return 0;
}
#define EXPLODE(rc) do_explode(#rc, rc, __LINE__)


/*************************************************************************
** Implementations of the rblob(nMin, nMax) function. One for src4 and
** one for sqlite3.
*/

/* src4 implementation */
static void rblobFunc4(sqlite4_context *ctx, int nArg, sqlite4_value **apArg){
  unsigned char aBlob[1000];
  static unsigned int iCall = 0;

  int iSeed = sqlite4_value_int(apArg[0]);
  int nMin = sqlite4_value_int(apArg[1]);
  int nMax = sqlite4_value_int(apArg[2]);
  int nByte;

  nByte = testPrngValue(iSeed + 1000000) & 0x7FFFFFFF;
  nByte = (nByte % (nMax+1-nMin)) + nMin;
  assert( nByte>=nMin && nByte<=nMax );
  if( nByte>sizeof(aBlob) ) nByte = sizeof(aBlob);
  testPrngArray(iSeed, (unsigned int *)aBlob, (nByte+3)/4);

  sqlite4_result_blob(ctx, aBlob, nByte, SQLITE4_TRANSIENT, 0);
}
static void install_rblob_function4(sqlite4 *db){
  testPrngInit();
  sqlite4_create_function(db, "rblob", 3, SQLITE4_UTF8, 0, rblobFunc4, 0, 0);
}

/* sqlite3 implementation */
static void rblobFunc3(sqlite3_context *ctx, int nArg, sqlite3_value **apArg){
  unsigned char aBlob[1000];
  static unsigned int iCall = 0;

  int iSeed = sqlite3_value_int(apArg[0]);
  int nMin = sqlite3_value_int(apArg[1]);
  int nMax = sqlite3_value_int(apArg[2]);
  int nByte;

  nByte = testPrngValue(iSeed + 1000000) & 0x7FFFFFFF;
  nByte = (nByte % (nMax+1-nMin)) + nMin;
  assert( nByte>=nMin && nByte<=nMax );
  if( nByte>sizeof(aBlob) ) nByte = sizeof(aBlob);
  testPrngArray(iSeed, (unsigned int *)aBlob, (nByte+3)/4);

  sqlite3_result_blob(ctx, aBlob, nByte, SQLITE_TRANSIENT);
}
static void install_rblob_function3(sqlite3 *db){
  testPrngInit();
  sqlite3_create_function(db, "rblob", 3, SQLITE_UTF8, 0, rblobFunc3, 0, 0);
}
/*
** End of rblob() implementations.
*************************************************************************/

/*************************************************************************
** Integer query functions for sqlite3 and src4.
*/
static int integer_query4(sqlite4 *db, const char *zSql){
  int iRet;
  sqlite4_stmt *pStmt;

  EXPLODE( sqlite4_prepare(db, zSql, -1, &pStmt, 0) );
  EXPLODE( SQLITE_ROW!=sqlite4_step(pStmt) );
  iRet = sqlite4_column_int(pStmt, 0);
  EXPLODE( sqlite4_finalize(pStmt) );

  return iRet;
}
static int integer_query3(sqlite3 *db, const char *zSql){
  int iRet;
  sqlite3_stmt *pStmt;

  EXPLODE( sqlite3_prepare(db, zSql, -1, &pStmt, 0) );
  EXPLODE( SQLITE_ROW!=sqlite3_step(pStmt) );
  iRet = sqlite3_column_int(pStmt, 0);
  EXPLODE( sqlite3_finalize(pStmt) );

  return iRet;
}
/*
** End of integer query implementations.
*************************************************************************/

static int do_insert1_test4(
  int nRow,                       /* Number of rows to insert in total */
  int nRowPerTrans,               /* Number of rows per transaction */
  int nIdx,                       /* Number of aux indexes (aside from PK) */
  int iSync                       /* PRAGMA synchronous value (0, 1 or 2) */
){
  char *zCreateTbl;               /* Create table statement */
  char *zInsert;                  /* INSERT statement */
  sqlite4_stmt *pInsert;          /* Compiled INSERT statement */
  sqlite4 *db = 0;                /* Database handle */
  int i;                          /* Counter to count nRow rows */
  int nMs;                        /* Test time in ms */

  lsm_db *pLsm;

  unlink_db(SQLITE4_DB_FILE);
  EXPLODE(  sqlite4_open(0, SQLITE4_DB_FILE, &db)  );
  sqlite4_kvstore_control(db, "main", SQLITE4_KVCTRL_LSM_HANDLE, &pLsm);
  i = iSync;
  lsm_config(pLsm, LSM_CONFIG_SAFETY, &i);
  assert( i==iSync );

  install_rblob_function4(db);

  zCreateTbl = create_schema_sql(nIdx);
  zInsert = create_insert_sql(nIdx);

  /* Create the db schema and prepare the INSERT statement */
  EXPLODE(  sqlite4_exec(db, zCreateTbl, 0, 0, 0)  );
  EXPLODE(  sqlite4_prepare(db, zInsert, -1, &pInsert, 0)  );

  /* Run the test */
  testTimeInit();
  for(i=0; i<nRow; i++){
    if( (i % nRowPerTrans)==0 ){
      if( i!=0 ) EXPLODE(  sqlite4_exec(db, "COMMIT", 0, 0, 0)  );
      EXPLODE(  sqlite4_exec(db, "BEGIN", 0, 0, 0)  );
    }
    sqlite4_bind_int(pInsert, 1, i);
    sqlite4_step(pInsert);
    EXPLODE(  sqlite4_reset(pInsert)  );
  }
  EXPLODE(  sqlite4_exec(db, "COMMIT", 0, 0, 0)  );

  /* Free all the stuff allocated above */
  sqlite4_finalize(pInsert);
  sqlite4_free(0, zCreateTbl);
  sqlite4_free(0, zInsert);
  sqlite4_close(db);
  nMs = testTimeGet();

  /* Print out the time taken by the test */
  printf("%.3f seconds\n", (double)nMs / 1000.0);
  return 0;
}
static int do_insert1_test3(
  int nRow,                       /* Number of rows to insert in total */
  int nRowPerTrans,               /* Number of rows per transaction */
  int nIdx,                       /* Number of aux indexes (aside from PK) */
  int iSync                       /* PRAGMA synchronous value (0, 1 or 2) */
){
  char *zCreateTbl;               /* Create table statement */
  char *zInsert;                  /* INSERT statement */
  char *zSync;                    /* "PRAGMA synchronous=" statement */
  sqlite3_stmt *pInsert;          /* Compiled INSERT statement */
  sqlite3 *db = 0;                /* Database handle */
  int i;                          /* Counter to count nRow rows */
  int nMs;                        /* Test time in ms */

  unlink_db(SQLITE3_DB_FILE);
  EXPLODE( sqlite3_open(SQLITE3_DB_FILE, &db) );
  EXPLODE( sqlite3_exec(db, "PRAGMA journal_mode=WAL", 0, 0, 0) );
  zSync = sqlite4_mprintf(0, "PRAGMA synchronous=%d", iSync);
  EXPLODE( sqlite3_exec(db, zSync, 0, 0, 0) );
  sqlite4_free(0, zSync);

  install_rblob_function3(db);

  zCreateTbl = create_schema_sql(nIdx);
  zInsert = create_insert_sql(nIdx);

  /* Create the db schema and prepare the INSERT statement */
  EXPLODE(  sqlite3_exec(db, zCreateTbl, 0, 0, 0)  );
  EXPLODE(  sqlite3_prepare(db, zInsert, -1, &pInsert, 0)  );

  /* Run the test */
  testTimeInit();
  for(i=0; i<nRow; i++){
    if( (i % nRowPerTrans)==0 ){
      if( i!=0 ) EXPLODE(  sqlite3_exec(db, "COMMIT", 0, 0, 0)  );
      EXPLODE(  sqlite3_exec(db, "BEGIN", 0, 0, 0)  );
    }
    sqlite3_bind_int(pInsert, 1, i);
    sqlite3_step(pInsert);
    EXPLODE(  sqlite3_reset(pInsert)  );
  }
  EXPLODE(  sqlite3_exec(db, "COMMIT", 0, 0, 0)  );

  /* Finalize the statement and close the db. */
  sqlite3_finalize(pInsert);
  sqlite3_close(db);
  nMs = testTimeGet();

  /* Free the stuff allocated above */
  sqlite4_free(0, zCreateTbl);
  sqlite4_free(0, zInsert);

  /* Print out the time taken by the test */
  printf("%.3f seconds\n", (double)nMs / 1000.0);
  return 0;
}

static int do_insert1(int argc, char **argv){
  struct Insert1Arg {
    const char *zArg;
    int nMin;
    int nMax;
  } aArg[] = { 
    {"-db",           3,    4}, 
    {"-rows",         1,    10000000}, 
    {"-rowspertrans", 1,    10000000}, 
    {"-indexes",      0,    20}, 
    {"-sync",         0,    2}, 
    {0,0,0}
  };
  int i;

  int iDb = 4;                    /* SQLite 3 or 4 */
  int nRow = 50000;               /* Total rows: 50000 */
  int nRowPerTrans = 10;          /* Total rows each transaction: 50000 */
  int nIdx = 3;                   /* Number of auxilliary indexes */
  int iSync = 1;                  /* PRAGMA synchronous setting */

  for(i=0; i<argc; i++){
    int iSel;
    int iVal;
    int rc;

    rc = testArgSelectX(aArg, "argument", sizeof(aArg[0]), argv[i], &iSel);
    if( rc!=0 ) return -1;
    if( i==argc-1 ){
      fprintf(stderr, "option %s requires an argument\n", aArg[iSel].zArg);
      return -1;
    }
    iVal = atoi(argv[++i]);
    if( iVal<aArg[iSel].nMin || iVal>aArg[iSel].nMax ){
      fprintf(stderr, "option %s out of range (%d..%d)\n", 
          aArg[iSel].zArg, aArg[iSel].nMin, aArg[iSel].nMax 
      );
      return -1;
    }

    switch( iSel ){
      case 0: iDb = iVal;          break;
      case 1: nRow = iVal;         break;
      case 2: nRowPerTrans = iVal; break;
      case 3: nIdx = iVal;         break;
      case 4: iSync = iVal;        break;
    }
  }

  printf("insert1: db=%d rows=%d rowspertrans=%d indexes=%d sync=%d ... ", 
      iDb, nRow, nRowPerTrans, nIdx, iSync
  );
  fflush(stdout);
  if( iDb==3 ){
    do_insert1_test3(nRow, nRowPerTrans, nIdx, iSync);
  }else{
    do_insert1_test4(nRow, nRowPerTrans, nIdx, iSync);
  }

  return 0;
}

static int do_select1_test4(
  int nRow,                       /* Number of rows to read in total */
  int nRowPerTrans,               /* Number of rows per transaction */
  int iIdx
){
  int nMs = 0;
  sqlite4_stmt *pSelect = 0;
  char *zSelect;
  sqlite4 *db;
  int i;
  int nTblRow;

  EXPLODE( sqlite4_open(0, SQLITE4_DB_FILE, &db) );
  install_rblob_function4(db);

  nTblRow = integer_query4(db, "SELECT count(*) FROM t1");

  /* Create the db schema and prepare the INSERT statement */
  zSelect = create_select_sql(iIdx);
  EXPLODE(  sqlite4_prepare(db, zSelect, -1, &pSelect, 0)  );

  testTimeInit();
  for(i=0; i<nRow; i++){
    if( (i % nRowPerTrans)==0 ){
      if( i!=0 ) EXPLODE(  sqlite4_exec(db, "COMMIT", 0, 0, 0)  );
      EXPLODE(  sqlite4_exec(db, "BEGIN", 0, 0, 0)  );
    }
    sqlite4_bind_int(pSelect, 1, (i*211)%nTblRow);
    EXPLODE(  SQLITE_ROW!=sqlite4_step(pSelect)  );
    EXPLODE(  sqlite4_reset(pSelect)  );
  }
  EXPLODE(  sqlite4_exec(db, "COMMIT", 0, 0, 0)  );
  nMs = testTimeGet();

  sqlite4_finalize(pSelect);
  sqlite4_close(db);
  sqlite4_free(0, zSelect);

  printf("%.3f seconds\n", (double)nMs / 1000.0);
  return 0;
}
static int do_select1_test3(
  int nRow,                       /* Number of rows to read in total */
  int nRowPerTrans,               /* Number of rows per transaction */
  int iIdx
){
  int nMs = 0;
  sqlite3_stmt *pSelect = 0;
  char *zSelect;
  sqlite3 *db;
  int i;
  int nTblRow;

  EXPLODE( sqlite3_open(SQLITE3_DB_FILE, &db) );
  install_rblob_function3(db);

  nTblRow = integer_query3(db, "SELECT count(*) FROM t1");

  /* Create the db schema and prepare the INSERT statement */
  zSelect = create_select_sql(iIdx);
  EXPLODE(  sqlite3_prepare(db, zSelect, -1, &pSelect, 0)  );

  testTimeInit();
  for(i=0; i<nRow; i++){
    if( (i % nRowPerTrans)==0 ){
      if( i!=0 ) EXPLODE(  sqlite3_exec(db, "COMMIT", 0, 0, 0)  );
      EXPLODE(  sqlite3_exec(db, "BEGIN", 0, 0, 0)  );
    }
    sqlite3_bind_int(pSelect, 1, (i*211)%nTblRow);
    EXPLODE(  SQLITE_ROW!=sqlite3_step(pSelect)  );
    EXPLODE(  sqlite3_reset(pSelect)  );
  }
  EXPLODE(  sqlite3_exec(db, "COMMIT", 0, 0, 0)  );
  nMs = testTimeGet();

  sqlite3_finalize(pSelect);
  sqlite3_close(db);
  sqlite4_free(0, zSelect);

  printf("%.3f seconds\n", (double)nMs / 1000.0);
  return 0;
}

static int do_select1(int argc, char **argv){
  struct Insert1Arg {
    const char *zArg;
    int nMin;
    int nMax;
  } aArg[] = {
    {"-db",           3,    4}, 
    {"-rows",         1,    10000000}, 
    {"-rowspertrans", 1,    10000000}, 
    {"-index",        0,    21}, 
    {0,0,0}
  };
  int i;

  int iDb = 4;                    /* SQLite 3 or 4 */
  int nRow = 50000;               /* Total rows: 50000 */
  int nRowPerTrans = 10;          /* Total rows each transaction: 50000 */
  int iIdx = 0;

  for(i=0; i<argc; i++){
    int iSel;
    int iVal;
    int rc;

    rc = testArgSelectX(aArg, "argument", sizeof(aArg[0]), argv[i], &iSel);
    if( rc!=0 ) return -1;
    if( i==argc-1 ){
      fprintf(stderr, "option %s requires an argument\n", aArg[iSel].zArg);
      return -1;
    }
    iVal = atoi(argv[++i]);
    if( iVal<aArg[iSel].nMin || iVal>aArg[iSel].nMax ){
      fprintf(stderr, "option %s out of range (%d..%d)\n", 
          aArg[iSel].zArg, aArg[iSel].nMin, aArg[iSel].nMax 
      );
      return -1;
    }

    switch( iSel ){
      case 0: iDb = iVal;          break;
      case 1: nRow = iVal;         break;
      case 2: nRowPerTrans = iVal; break;
      case 3: iIdx = iVal;         break;
    }
  }

  printf("select1: db=%d rows=%d rowspertrans=%d index=%d ... ", 
      iDb, nRow, nRowPerTrans, iIdx
  );
  fflush(stdout);
  if( iDb==3 ){
    do_select1_test3(nRow, nRowPerTrans, iIdx);
  }else{
    do_select1_test4(nRow, nRowPerTrans, iIdx);
  }

  return 0;
}

int main(int argc, char **argv){
  struct SqltestArg {
    const char *zPrg;
    int (*xPrg)(int, char **);
  } aArg[] = { 
    {"select", do_select1},
    {"insert", do_insert1},
    {0, 0}
  };
  int iSel;
  int rc;

  if( argc<2 ){
    fprintf(stderr, "Usage: %s sub-program...\n", argv[0]);
    return -1;
  }

  rc = testArgSelectX(aArg, "sub-program", sizeof(aArg[0]), argv[1], &iSel);
  if( rc!=0 ) return -1;

  aArg[iSel].xPrg(argc-2, argv+2);
  return 0;
}
