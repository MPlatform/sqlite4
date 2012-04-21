/*
** 2008 March 19
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Code for testing all sorts of SQLite interfaces.  This code
** implements new SQL functions used by the test scripts.
*/
#include "sqlite4.h"
#include "tcl.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>


/*
** Allocate nByte bytes of space using sqlite4_malloc(). If the
** allocation fails, call sqlite4_result_error_nomem() to notify
** the database handle that malloc() has failed.
*/
static void *testContextMalloc(sqlite4_context *context, int nByte){
  char *z = sqlite4_malloc(nByte);
  if( !z && nByte>0 ){
    sqlite4_result_error_nomem(context);
  }
  return z;
}

/*
** This function generates a string of random characters.  Used for
** generating test data.
*/
static void randStr(sqlite4_context *context, int argc, sqlite4_value **argv){
  static const unsigned char zSrc[] = 
     "abcdefghijklmnopqrstuvwxyz"
     "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
     "0123456789"
     ".-!,:*^+=_|?/<> ";
  int iMin, iMax, n, r, i;
  unsigned char zBuf[1000];

  /* It used to be possible to call randstr() with any number of arguments,
  ** but now it is registered with SQLite as requiring exactly 2.
  */
  assert(argc==2);

  iMin = sqlite4_value_int(argv[0]);
  if( iMin<0 ) iMin = 0;
  if( iMin>=sizeof(zBuf) ) iMin = sizeof(zBuf)-1;
  iMax = sqlite4_value_int(argv[1]);
  if( iMax<iMin ) iMax = iMin;
  if( iMax>=sizeof(zBuf) ) iMax = sizeof(zBuf)-1;
  n = iMin;
  if( iMax>iMin ){
    sqlite4_randomness(sizeof(r), &r);
    r &= 0x7fffffff;
    n += r%(iMax + 1 - iMin);
  }
  assert( n<sizeof(zBuf) );
  sqlite4_randomness(n, zBuf);
  for(i=0; i<n; i++){
    zBuf[i] = zSrc[zBuf[i]%(sizeof(zSrc)-1)];
  }
  zBuf[n] = 0;
  sqlite4_result_text(context, (char*)zBuf, n, SQLITE_TRANSIENT);
}

/*
** The following two SQL functions are used to test returning a text
** result with a destructor. Function 'test_destructor' takes one argument
** and returns the same argument interpreted as TEXT. A destructor is
** passed with the sqlite4_result_text() call.
**
** SQL function 'test_destructor_count' returns the number of outstanding 
** allocations made by 'test_destructor';
**
** WARNING: Not threadsafe.
*/
static int test_destructor_count_var = 0;
static void destructor(void *p){
  char *zVal = (char *)p;
  assert(zVal);
  zVal--;
  sqlite4_free(zVal);
  test_destructor_count_var--;
}
static void test_destructor(
  sqlite4_context *pCtx, 
  int nArg,
  sqlite4_value **argv
){
  char *zVal;
  int len;
  
  test_destructor_count_var++;
  assert( nArg==1 );
  if( sqlite4_value_type(argv[0])==SQLITE_NULL ) return;
  len = sqlite4_value_bytes(argv[0]); 
  zVal = testContextMalloc(pCtx, len+3);
  if( !zVal ){
    return;
  }
  zVal[len+1] = 0;
  zVal[len+2] = 0;
  zVal++;
  memcpy(zVal, sqlite4_value_text(argv[0]), len);
  sqlite4_result_text(pCtx, zVal, -1, destructor);
}
#ifndef SQLITE_OMIT_UTF16
static void test_destructor16(
  sqlite4_context *pCtx, 
  int nArg,
  sqlite4_value **argv
){
  char *zVal;
  int len;
  
  test_destructor_count_var++;
  assert( nArg==1 );
  if( sqlite4_value_type(argv[0])==SQLITE_NULL ) return;
  len = sqlite4_value_bytes16(argv[0]); 
  zVal = testContextMalloc(pCtx, len+3);
  if( !zVal ){
    return;
  }
  zVal[len+1] = 0;
  zVal[len+2] = 0;
  zVal++;
  memcpy(zVal, sqlite4_value_text16(argv[0]), len);
  sqlite4_result_text16(pCtx, zVal, -1, destructor);
}
#endif
static void test_destructor_count(
  sqlite4_context *pCtx, 
  int nArg,
  sqlite4_value **argv
){
  sqlite4_result_int(pCtx, test_destructor_count_var);
}

/*
** The following aggregate function, test_agg_errmsg16(), takes zero 
** arguments. It returns the text value returned by the sqlite4_errmsg16()
** API function.
*/
#ifndef SQLITE_OMIT_BUILTIN_TEST
void sqlite4BeginBenignMalloc(void);
void sqlite4EndBenignMalloc(void);
#else
  #define sqlite4BeginBenignMalloc()
  #define sqlite4EndBenignMalloc()
#endif
static void test_agg_errmsg16_step(sqlite4_context *a, int b,sqlite4_value **c){
}
static void test_agg_errmsg16_final(sqlite4_context *ctx){
#ifndef SQLITE_OMIT_UTF16
  const void *z;
  sqlite4 * db = sqlite4_context_db_handle(ctx);
  sqlite4_aggregate_context(ctx, 2048);
  sqlite4BeginBenignMalloc();
  z = sqlite4_errmsg16(db);
  sqlite4EndBenignMalloc();
  sqlite4_result_text16(ctx, z, -1, SQLITE_TRANSIENT);
#endif
}

/*
** Routines for testing the sqlite4_get_auxdata() and sqlite4_set_auxdata()
** interface.
**
** The test_auxdata() SQL function attempts to register each of its arguments
** as auxiliary data.  If there are no prior registrations of aux data for
** that argument (meaning the argument is not a constant or this is its first
** call) then the result for that argument is 0.  If there is a prior
** registration, the result for that argument is 1.  The overall result
** is the individual argument results separated by spaces.
*/
static void free_test_auxdata(void *p) {sqlite4_free(p);}
static void test_auxdata(
  sqlite4_context *pCtx, 
  int nArg,
  sqlite4_value **argv
){
  int i;
  char *zRet = testContextMalloc(pCtx, nArg*2);
  if( !zRet ) return;
  memset(zRet, 0, nArg*2);
  for(i=0; i<nArg; i++){
    char const *z = (char*)sqlite4_value_text(argv[i]);
    if( z ){
      int n;
      char *zAux = sqlite4_get_auxdata(pCtx, i);
      if( zAux ){
        zRet[i*2] = '1';
        assert( strcmp(zAux,z)==0 );
      }else {
        zRet[i*2] = '0';
      }
      n = strlen(z) + 1;
      zAux = testContextMalloc(pCtx, n);
      if( zAux ){
        memcpy(zAux, z, n);
        sqlite4_set_auxdata(pCtx, i, zAux, free_test_auxdata);
      }
      zRet[i*2+1] = ' ';
    }
  }
  sqlite4_result_text(pCtx, zRet, 2*nArg-1, free_test_auxdata);
}

/*
** A function to test error reporting from user functions. This function
** returns a copy of its first argument as the error message.  If the
** second argument exists, it becomes the error code.
*/
static void test_error(
  sqlite4_context *pCtx, 
  int nArg,
  sqlite4_value **argv
){
  sqlite4_result_error(pCtx, (char*)sqlite4_value_text(argv[0]), -1);
  if( nArg==2 ){
    sqlite4_result_error_code(pCtx, sqlite4_value_int(argv[1]));
  }
}

/*
** Implementation of the counter(X) function.  If X is an integer
** constant, then the first invocation will return X.  The second X+1.
** and so forth.  Can be used (for example) to provide a sequence number
** in a result set.
*/
static void counterFunc(
  sqlite4_context *pCtx,   /* Function context */
  int nArg,                /* Number of function arguments */
  sqlite4_value **argv     /* Values for all function arguments */
){
  int *pCounter = (int*)sqlite4_get_auxdata(pCtx, 0);
  if( pCounter==0 ){
    pCounter = sqlite4_malloc( sizeof(*pCounter) );
    if( pCounter==0 ){
      sqlite4_result_error_nomem(pCtx);
      return;
    }
    *pCounter = sqlite4_value_int(argv[0]);
    sqlite4_set_auxdata(pCtx, 0, pCounter, sqlite4_free);
  }else{
    ++*pCounter;
  }
  sqlite4_result_int(pCtx, *pCounter);
}


/*
** This function takes two arguments.  It performance UTF-8/16 type
** conversions on the first argument then returns a copy of the second
** argument.
**
** This function is used in cases such as the following:
**
**      SELECT test_isolation(x,x) FROM t1;
**
** We want to verify that the type conversions that occur on the
** first argument do not invalidate the second argument.
*/
static void test_isolation(
  sqlite4_context *pCtx, 
  int nArg,
  sqlite4_value **argv
){
#ifndef SQLITE_OMIT_UTF16
  sqlite4_value_text16(argv[0]);
  sqlite4_value_text(argv[0]);
  sqlite4_value_text16(argv[0]);
  sqlite4_value_text(argv[0]);
#endif
  sqlite4_result_value(pCtx, argv[1]);
}

/*
** Invoke an SQL statement recursively.  The function result is the 
** first column of the first row of the result set.
*/
static void test_eval(
  sqlite4_context *pCtx, 
  int nArg,
  sqlite4_value **argv
){
  sqlite4_stmt *pStmt;
  int rc;
  sqlite4 *db = sqlite4_context_db_handle(pCtx);
  const char *zSql;

  zSql = (char*)sqlite4_value_text(argv[0]);
  rc = sqlite4_prepare(db, zSql, -1, &pStmt, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite4_step(pStmt);
    if( rc==SQLITE_ROW ){
      sqlite4_result_value(pCtx, sqlite4_column_value(pStmt, 0));
    }
    rc = sqlite4_finalize(pStmt);
  }
  if( rc ){
    char *zErr;
    assert( pStmt==0 );
    zErr = sqlite4_mprintf("sqlite4_prepare() error: %s",sqlite4_errmsg(db));
    sqlite4_result_text(pCtx, zErr, -1, sqlite4_free);
    sqlite4_result_error_code(pCtx, rc);
  }
}


/*
** convert one character from hex to binary
*/
static int testHexChar(char c){
  if( c>='0' && c<='9' ){
    return c - '0';
  }else if( c>='a' && c<='f' ){
    return c - 'a' + 10;
  }else if( c>='A' && c<='F' ){
    return c - 'A' + 10;
  }
  return 0;
}

/*
** Convert hex to binary.
*/
static void testHexToBin(const char *zIn, char *zOut){
  while( zIn[0] && zIn[1] ){
    *(zOut++) = (testHexChar(zIn[0])<<4) + testHexChar(zIn[1]);
    zIn += 2;
  }
}

/*
**      hex_to_utf16be(HEX)
**
** Convert the input string from HEX into binary.  Then return the
** result using sqlite4_result_text16le().
*/
#ifndef SQLITE_OMIT_UTF16
static void testHexToUtf16be(
  sqlite4_context *pCtx, 
  int nArg,
  sqlite4_value **argv
){
  int n;
  const char *zIn;
  char *zOut;
  assert( nArg==1 );
  n = sqlite4_value_bytes(argv[0]);
  zIn = (const char*)sqlite4_value_text(argv[0]);
  zOut = sqlite4_malloc( n/2 );
  if( zOut==0 ){
    sqlite4_result_error_nomem(pCtx);
  }else{
    testHexToBin(zIn, zOut);
    sqlite4_result_text16be(pCtx, zOut, n/2, sqlite4_free);
  }
}
#endif

/*
**      hex_to_utf8(HEX)
**
** Convert the input string from HEX into binary.  Then return the
** result using sqlite4_result_text16le().
*/
static void testHexToUtf8(
  sqlite4_context *pCtx, 
  int nArg,
  sqlite4_value **argv
){
  int n;
  const char *zIn;
  char *zOut;
  assert( nArg==1 );
  n = sqlite4_value_bytes(argv[0]);
  zIn = (const char*)sqlite4_value_text(argv[0]);
  zOut = sqlite4_malloc( n/2 );
  if( zOut==0 ){
    sqlite4_result_error_nomem(pCtx);
  }else{
    testHexToBin(zIn, zOut);
    sqlite4_result_text(pCtx, zOut, n/2, sqlite4_free);
  }
}

/*
**      hex_to_utf16le(HEX)
**
** Convert the input string from HEX into binary.  Then return the
** result using sqlite4_result_text16le().
*/
#ifndef SQLITE_OMIT_UTF16
static void testHexToUtf16le(
  sqlite4_context *pCtx, 
  int nArg,
  sqlite4_value **argv
){
  int n;
  const char *zIn;
  char *zOut;
  assert( nArg==1 );
  n = sqlite4_value_bytes(argv[0]);
  zIn = (const char*)sqlite4_value_text(argv[0]);
  zOut = sqlite4_malloc( n/2 );
  if( zOut==0 ){
    sqlite4_result_error_nomem(pCtx);
  }else{
    testHexToBin(zIn, zOut);
    sqlite4_result_text16le(pCtx, zOut, n/2, sqlite4_free);
  }
}
#endif

static int registerTestFunctions(sqlite4 *db){
  static const struct {
     char *zName;
     signed char nArg;
     unsigned char eTextRep; /* 1: UTF-16.  0: UTF-8 */
     void (*xFunc)(sqlite4_context*,int,sqlite4_value **);
  } aFuncs[] = {
    { "randstr",               2, SQLITE_UTF8, randStr    },
    { "test_destructor",       1, SQLITE_UTF8, test_destructor},
#ifndef SQLITE_OMIT_UTF16
    { "test_destructor16",     1, SQLITE_UTF8, test_destructor16},
    { "hex_to_utf16be",        1, SQLITE_UTF8, testHexToUtf16be},
    { "hex_to_utf16le",        1, SQLITE_UTF8, testHexToUtf16le},
#endif
    { "hex_to_utf8",           1, SQLITE_UTF8, testHexToUtf8},
    { "test_destructor_count", 0, SQLITE_UTF8, test_destructor_count},
    { "test_auxdata",         -1, SQLITE_UTF8, test_auxdata},
    { "test_error",            1, SQLITE_UTF8, test_error},
    { "test_error",            2, SQLITE_UTF8, test_error},
    { "test_eval",             1, SQLITE_UTF8, test_eval},
    { "test_isolation",        2, SQLITE_UTF8, test_isolation},
    { "test_counter",          1, SQLITE_UTF8, counterFunc},
  };
  int i;

  for(i=0; i<sizeof(aFuncs)/sizeof(aFuncs[0]); i++){
    sqlite4_create_function(db, aFuncs[i].zName, aFuncs[i].nArg,
        aFuncs[i].eTextRep, 0, aFuncs[i].xFunc, 0, 0);
  }

  sqlite4_create_function(db, "test_agg_errmsg16", 0, SQLITE_ANY, 0, 0, 
      test_agg_errmsg16_step, test_agg_errmsg16_final);
      
  return SQLITE_OK;
}

/*
** TCLCMD:  autoinstall_test_functions
**
** Invoke this TCL command to use sqlite4_auto_extension() to cause
** the standard set of test functions to be loaded into each new
** database connection.
*/
static int autoinstall_test_funcs(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  extern int Md5_Register(sqlite4*);
  int rc = sqlite4_auto_extension((void*)registerTestFunctions);
  if( rc==SQLITE_OK ){
    rc = sqlite4_auto_extension((void*)Md5_Register);
  }
  Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
  return TCL_OK;
}

/*
** A bogus step function and finalizer function.
*/
static void tStep(sqlite4_context *a, int b, sqlite4_value **c){}
static void tFinal(sqlite4_context *a){}


/*
** tclcmd:  abuse_create_function
**
** Make various calls to sqlite4_create_function that do not have valid
** parameters.  Verify that the error condition is detected and reported.
*/
static int abuse_create_function(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  extern int getDbPointer(Tcl_Interp*, const char*, sqlite4**);
  sqlite4 *db;
  int rc;
  int mxArg;

  if( getDbPointer(interp, Tcl_GetString(objv[1]), &db) ) return TCL_ERROR;

  rc = sqlite4_create_function(db, "tx", 1, SQLITE_UTF8, 0, tStep,tStep,tFinal);
  if( rc!=SQLITE_MISUSE ) goto abuse_err;

  rc = sqlite4_create_function(db, "tx", 1, SQLITE_UTF8, 0, tStep, tStep, 0);
  if( rc!=SQLITE_MISUSE ) goto abuse_err;

  rc = sqlite4_create_function(db, "tx", 1, SQLITE_UTF8, 0, tStep, 0, tFinal);
  if( rc!=SQLITE_MISUSE) goto abuse_err;

  rc = sqlite4_create_function(db, "tx", 1, SQLITE_UTF8, 0, 0, 0, tFinal);
  if( rc!=SQLITE_MISUSE ) goto abuse_err;

  rc = sqlite4_create_function(db, "tx", 1, SQLITE_UTF8, 0, 0, tStep, 0);
  if( rc!=SQLITE_MISUSE ) goto abuse_err;

  rc = sqlite4_create_function(db, "tx", -2, SQLITE_UTF8, 0, tStep, 0, 0);
  if( rc!=SQLITE_MISUSE ) goto abuse_err;

  rc = sqlite4_create_function(db, "tx", 128, SQLITE_UTF8, 0, tStep, 0, 0);
  if( rc!=SQLITE_MISUSE ) goto abuse_err;

  rc = sqlite4_create_function(db, "funcxx"
       "_123456789_123456789_123456789_123456789_123456789"
       "_123456789_123456789_123456789_123456789_123456789"
       "_123456789_123456789_123456789_123456789_123456789"
       "_123456789_123456789_123456789_123456789_123456789"
       "_123456789_123456789_123456789_123456789_123456789",
       1, SQLITE_UTF8, 0, tStep, 0, 0);
  if( rc!=SQLITE_MISUSE ) goto abuse_err;

  /* This last function registration should actually work.  Generate
  ** a no-op function (that always returns NULL) and which has the
  ** maximum-length function name and the maximum number of parameters.
  */
  sqlite4_limit(db, SQLITE_LIMIT_FUNCTION_ARG, 10000);
  mxArg = sqlite4_limit(db, SQLITE_LIMIT_FUNCTION_ARG, -1);
  rc = sqlite4_create_function(db, "nullx"
       "_123456789_123456789_123456789_123456789_123456789"
       "_123456789_123456789_123456789_123456789_123456789"
       "_123456789_123456789_123456789_123456789_123456789"
       "_123456789_123456789_123456789_123456789_123456789"
       "_123456789_123456789_123456789_123456789_123456789",
       mxArg, SQLITE_UTF8, 0, tStep, 0, 0);
  if( rc!=SQLITE_OK ) goto abuse_err;
                                
  return TCL_OK;

abuse_err:
  Tcl_AppendResult(interp, "sqlite4_create_function abused test failed", 
                   (char*)0);
  return TCL_ERROR;
}

/*
** Register commands with the TCL interpreter.
*/
int Sqlitetest_func_Init(Tcl_Interp *interp){
  static struct {
     char *zName;
     Tcl_ObjCmdProc *xProc;
  } aObjCmd[] = {
     { "autoinstall_test_functions",    autoinstall_test_funcs },
     { "abuse_create_function",         abuse_create_function  },
  };
  int i;
  extern int Md5_Register(sqlite4*);

  for(i=0; i<sizeof(aObjCmd)/sizeof(aObjCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aObjCmd[i].zName, aObjCmd[i].xProc, 0, 0);
  }
  sqlite4_initialize();
  sqlite4_auto_extension((void*)registerTestFunctions);
  sqlite4_auto_extension((void*)Md5_Register);
  return TCL_OK;
}
