/*
** 2012 May 21
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
*/

#include <tcl.h>
#include "lsm.h"
#include "sqlite4.h"
#include <assert.h>
#include <string.h>

extern int getDbPointer(Tcl_Interp *interp, const char *zA, sqlite4 **ppDb);
extern const char *sqlite4TestErrorName(int);

/*************************************************************************
*/
#define ENCRYPTION_XOR_MASK 0x23b2bbb6
static int testCompressEncBound(void *pCtx, int nSrc){
  return nSrc;
}
static int testCompressEncCompress(
  void *pCtx, 
  char *pOut, int *pnOut, 
  const char *pIn, int nIn
){
  int i;
  unsigned int *aIn = (unsigned int *)pOut;
  unsigned int *aOut = (unsigned int *)pIn;

  assert( (nIn%4)==0 );
  for(i=0; i<(nIn/4); i++){
    aOut[i] = (aIn[i] ^ ENCRYPTION_XOR_MASK);
  }
  *pnOut = nIn;

  return LSM_OK;
}
static int testCompressEncUncompress(
  void *pCtx, 
  char *pOut, int *pnOut, 
  const char *pIn, int nIn
){
  return testCompressEncCompress(pCtx, pOut, pnOut, pIn, nIn);
}
static void testCompressEncFree(void *pCtx){
  /* no-op */
}
/* 
** End of compression routines "encrypt".
*************************************************************************/

/*************************************************************************
*/
static int testCompressRleBound(void *pCtx, int nSrc){
  return nSrc*2;
}
static int testCompressRleCompress(
  void *pCtx, 
  char *pOut, int *pnOut, 
  const char *pIn, int nIn
){
  int iOut = 0;
  int i;
  char c;
  int n;

  c = pIn[0];
  n = 1;
  for(i=1; i<nIn; i++){
    if( pIn[i]==c && n<127 ){
      n++;
    }else{
      pOut[iOut++] = c;
      pOut[iOut++] = (char)n;
      c = pIn[i];
      n = 1;
    }
  }

  pOut[iOut++] = c;
  pOut[iOut++] = (char)n;
  *pnOut = iOut;

  return LSM_OK;
}
static int testCompressRleUncompress(
  void *pCtx, 
  char *pOut, int *pnOut, 
  const char *pIn, int nIn
){
  int i;
  int iOut = 0;

  for(i=0; i<nIn; i+=2){
    int iRep;
    char c = pIn[i];
    int n = (int)(pIn[i+1]);

    for(iRep=0; iRep<n; iRep++){
      pOut[iOut++] = c;
    }
  }

  *pnOut = iOut;
  return LSM_OK;
}
static void testCompressRleFree(void *pCtx){
}
/* 
** End of compression routines "rle".
*************************************************************************/

/*************************************************************************
*/
static int testCompressNoopBound(void *pCtx, int nSrc){
  return nSrc;
}
static int testCompressNoopCompress(
  void *pCtx, 
  char *pOut, int *pnOut, 
  const char *pIn, int nIn
){
  *pnOut = nIn;
  memcpy(pOut, pIn, nIn);
  return LSM_OK;
}
static int testCompressNoopUncompress(
  void *pCtx, 
  char *pOut, int *pnOut, 
  const char *pIn, int nIn
){
  *pnOut = nIn;
  memcpy(pOut, pIn, nIn);
  return LSM_OK;
}
static void testCompressNoopFree(void *pCtx){
}
/* 
** End of compression routines "noop".
*************************************************************************/

static int testConfigureSetCompression(
  Tcl_Interp *interp, 
  lsm_db *db, 
  Tcl_Obj *pCmp,
  unsigned int iId
){
  struct CompressionScheme {
    const char *zName;
    lsm_compress cmp;
  } aCmp[] = {
    { "encrypt", { 0, 43, 
        testCompressEncBound, testCompressEncCompress,
        testCompressEncUncompress, testCompressEncFree
    } },
    { "rle", { 0, 44, 
        testCompressRleBound, testCompressRleCompress,
        testCompressRleUncompress, testCompressRleFree
    } },
    { "noop", { 0, 45, 
        testCompressNoopBound, testCompressNoopCompress,
        testCompressNoopUncompress, testCompressNoopFree
    } },
    { 0, {0, 0, 0, 0, 0, 0} }
  };
  int iOpt;
  int rc;

  if( interp ){
    rc = Tcl_GetIndexFromObjStruct(
        interp, pCmp, aCmp, sizeof(aCmp[0]), "scheme", 0, &iOpt
        );
    if( rc!=TCL_OK ) return rc;
  }else{
    int nOpt = sizeof(aCmp)/sizeof(aCmp[0]);
    for(iOpt=0; iOpt<nOpt; iOpt++){
      if( iId==aCmp[iOpt].cmp.iId ) break;
    }
    if( iOpt==nOpt ) return 0;
  }

  rc = lsm_config(db, LSM_CONFIG_SET_COMPRESSION, &aCmp[iOpt].cmp);
  return rc;
}

static int testCompressFactory(void *pCtx, lsm_db *db, unsigned int iId){
  return testConfigureSetCompression(0, db, 0, iId);
}

static int testConfigureSetFactory(
  Tcl_Interp *interp, 
  lsm_db *db, 
  Tcl_Obj *pArg
){
  lsm_compress_factory aFactory[2] = {
    { 0, 0, 0 },
    { 0, testCompressFactory, 0 },
  };
  int bArg = 0;
  int rc;

  rc = Tcl_GetBooleanFromObj(interp, pArg, &bArg);
  if( rc!=TCL_OK ) return rc;
  assert( bArg==1 || bArg==0 );

  rc = lsm_config(db, LSM_CONFIG_SET_COMPRESSION_FACTORY, &aFactory[bArg]);
  return rc;
}

/*
** Array apObj[] is an array of nObj Tcl objects intended to be transformed
** into lsm_config() calls on database db.
**
** Each pair of objects in the array is treated as a key/value pair used
** as arguments to a single lsm_config() call. If there are an even number
** of objects in the array, then the interpreter result is set to the output
** value of the final lsm_config() call. Or, if there are an odd number of
** objects in the array, the final object is treated as the key for a 
** read-only call to lsm_config(), the return value of which is used as
** the interpreter result. For example, the following:
**
**   { safety 1 mmap 0 use_log }
**
** Results in a sequence of calls similar to:
**
**   iVal = 1;  lsm_config(db, LSM_CONFIG_SAFETY,  &iVal);
**   iVal = 0;  lsm_config(db, LSM_CONFIG_MMAP,    &iVal);
**   iVal = -1; lsm_config(db, LSM_CONFIG_USE_LOG, &iVal);
**   Tcl_SetObjResult(interp, Tcl_NewIntObj(iVal));
*/
static int testConfigureLsm(
  Tcl_Interp *interp, 
  lsm_db *db, 
  int nObj,
  Tcl_Obj *const* apObj
){
  struct Lsmconfig {
    const char *zOpt;
    int eOpt;
    int bInteger;
  } aConfig[] = {
    { "autoflush",               LSM_CONFIG_AUTOFLUSH,               1 },
    { "page_size",               LSM_CONFIG_PAGE_SIZE,               1 },
    { "block_size",              LSM_CONFIG_BLOCK_SIZE,              1 },
    { "safety",                  LSM_CONFIG_SAFETY,                  1 },
    { "autowork",                LSM_CONFIG_AUTOWORK,                1 },
    { "autocheckpoint",          LSM_CONFIG_AUTOCHECKPOINT,          1 },
    { "mmap",                    LSM_CONFIG_MMAP,                    1 },
    { "use_log",                 LSM_CONFIG_USE_LOG,                 1 },
    { "automerge",               LSM_CONFIG_AUTOMERGE,               1 },
    { "max_freelist",            LSM_CONFIG_MAX_FREELIST,            1 },
    { "multi_proc",              LSM_CONFIG_MULTIPLE_PROCESSES,      1 },
    { "set_compression",         LSM_CONFIG_SET_COMPRESSION,         0 },
    { "set_compression_factory", LSM_CONFIG_SET_COMPRESSION_FACTORY, 0 },
    { "readonly",                LSM_CONFIG_READONLY,                1 },
    { 0, 0, 0 }
  };
  int i;
  int rc = TCL_OK;

  for(i=0; rc==TCL_OK && i<nObj; i+=2){
    int iOpt;
    rc = Tcl_GetIndexFromObjStruct(
        interp, apObj[i], aConfig, sizeof(aConfig[0]), "option", 0, &iOpt
    );
    if( rc==TCL_OK ){
      if( i==(nObj-1) ){
        Tcl_ResetResult(interp);
        if( aConfig[iOpt].bInteger ){
          int iVal = -1;
          lsm_config(db, aConfig[iOpt].eOpt, &iVal);
          Tcl_SetObjResult(interp, Tcl_NewIntObj(iVal));
        }
      }else{
        if( aConfig[iOpt].eOpt==LSM_CONFIG_SET_COMPRESSION ){
          rc = testConfigureSetCompression(interp, db, apObj[i+1], 0);
        }
        else if( aConfig[iOpt].eOpt==LSM_CONFIG_SET_COMPRESSION_FACTORY ){
          rc = testConfigureSetFactory(interp, db, apObj[i+1]);
        }
        else {
          int iVal;
          rc = Tcl_GetIntFromObj(interp, apObj[i+1], &iVal);
          if( rc==TCL_OK ){
            lsm_config(db, aConfig[iOpt].eOpt, &iVal);
          }
          Tcl_SetObjResult(interp, Tcl_NewIntObj(iVal));
        }
      }
    }
  }

  return rc;
}

/*
** TCLCMD:    sqlite4_lsm_config DB DBNAME PARAM ...
*/
static int test_sqlite4_lsm_config(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){

  const char *zDb;                /* objv[1] as a string */
  const char *zName;              /* objv[2] as a string */

  int rc;
  sqlite4 *db;
  lsm_db *pLsm;

  /* Process arguments. Return early if there is a problem. */
  if( objc!=4 && objc!=5 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB DBNAME PARAM ?VALUE?");
    return TCL_ERROR;
  }
  zDb = Tcl_GetString(objv[1]);
  zName = Tcl_GetString(objv[2]);

  rc = getDbPointer(interp, zDb, &db);
  if( rc==TCL_OK ){
    rc = sqlite4_kvstore_control(db, zName, SQLITE4_KVCTRL_LSM_HANDLE, &pLsm);
    if( rc!=SQLITE4_OK ){
      Tcl_SetResult(interp, (char *)sqlite4TestErrorName(rc), TCL_STATIC);
      rc = TCL_ERROR;
    }
  }

  if( rc==SQLITE4_OK ){
    testConfigureLsm(interp, pLsm, objc-3, &objv[3]);
  }
  return rc;
}

/*
** TCLCMD:    sqlite4_lsm_info DB DBNAME PARAM
*/
static int test_sqlite4_lsm_info(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  struct Switch {
    const char *zSwitch;
    int iVal;
  } aParam[] = {
    { "db-structure",    LSM_INFO_DB_STRUCTURE }, 
    { "log-structure",   LSM_INFO_LOG_STRUCTURE }, 
    { 0, 0 }
  };

  const char *zDb;
  const char *zName;
  int iParam;

  int rc;
  sqlite4 *db;
  lsm_db *pLsm;

  /* Process arguments. Return early if there is a problem. */
  if( objc!=4 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB DBNAME PARAM");
    return TCL_ERROR;
  }
  zDb = Tcl_GetString(objv[1]);
  zName = Tcl_GetString(objv[2]);
  rc = Tcl_GetIndexFromObjStruct(
      interp, objv[3], aParam, sizeof(aParam[0]), "param", 0, &iParam
  );
  if( rc!=TCL_OK ) return rc;
  if( rc==TCL_OK ){
    iParam = aParam[iParam].iVal;
    rc = getDbPointer(interp, zDb, &db);
  }
  if( rc!=TCL_OK ) return rc;

  rc = sqlite4_kvstore_control(db, zName, SQLITE4_KVCTRL_LSM_HANDLE, &pLsm);
  if( rc==SQLITE4_OK ){
    char *zList;
    lsm_info(pLsm, iParam, &zList);
    Tcl_SetResult(interp, zList, TCL_VOLATILE);
    lsm_free(lsm_get_env(pLsm), zList);
  }

  if( rc!=SQLITE4_OK ){
    Tcl_SetResult(interp, (char *)sqlite4TestErrorName(rc), TCL_STATIC);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** TCLCMD:    sqlite4_lsm_work DB DBNAME ?-nmerge N? ?-npage N?
*/
static int test_sqlite4_lsm_work(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  struct Switch {
    const char *zSwitch;
  } aSwitch[] = {
    { "-nmerge" },
    { "-npage" },
    { 0 }
  };

  int nMerge = 1;
  int nPage = 0;
  const char *zDb;
  const char *zName;
  int i;
  int rc;
  sqlite4 *db;
  lsm_db *pLsm;
  int nWork;

  if( objc!=3 && objc!=5 && objc!=7 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB DBNAME ?-nmerge N? ?-npage N?");
    return TCL_ERROR;
  }
  zDb = Tcl_GetString(objv[1]);
  zName = Tcl_GetString(objv[2]);

  for(i=3; i<objc; i+=2){
    int iIdx;
    int iVal;
    rc = Tcl_GetIndexFromObjStruct(
        interp, objv[i], aSwitch, sizeof(aSwitch[0]), "switch", 0, &iIdx
        );
    if( rc!=TCL_OK ) return rc;
    rc = Tcl_GetIntFromObj(interp, objv[i+1], &iVal);
    if( rc!=TCL_OK ) return rc;
    if( iIdx==0 ) nMerge = iVal;
    if( iIdx==1 ) nPage = iVal;
  }

  rc = getDbPointer(interp, zDb, &db);
  if( rc!=TCL_OK ) return rc;

  rc = sqlite4_kvstore_control(db, zName, SQLITE4_KVCTRL_LSM_HANDLE, &pLsm);
  if( rc==SQLITE4_OK ){
    rc = lsm_work(pLsm, nMerge, nPage, &nWork);
  }
  if( rc!=SQLITE4_OK ){
    Tcl_SetResult(interp, (char *)sqlite4TestErrorName(rc), TCL_STATIC);
    return TCL_ERROR;
  }

  Tcl_SetObjResult(interp, Tcl_NewIntObj(nWork));
  return TCL_OK;
}

/*
** TCLCMD:    sqlite4_lsm_checkpoint DB DBNAME 
*/
static int test_sqlite4_lsm_checkpoint(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  const char *zDb;
  const char *zName;
  int rc;
  sqlite4 *db;
  lsm_db *pLsm;

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB DBNAME");
    return TCL_ERROR;
  }
  zDb = Tcl_GetString(objv[1]);
  zName = Tcl_GetString(objv[2]);

  rc = getDbPointer(interp, zDb, &db);
  if( rc!=TCL_OK ) return rc;

  rc = sqlite4_kvstore_control(db, zName, SQLITE4_KVCTRL_LSM_HANDLE, &pLsm);
  if( rc==SQLITE4_OK ){
    rc = lsm_checkpoint(pLsm, 0);
  }
  if( rc!=SQLITE4_OK ){
    Tcl_SetResult(interp, (char *)sqlite4TestErrorName(rc), TCL_STATIC);
    return TCL_ERROR;
  }

  Tcl_ResetResult(interp);
  return TCL_OK;
}


typedef struct TclLsmCursor TclLsmCursor;
typedef struct TclLsm TclLsm;

struct TclLsm {
  lsm_db *db;
};

struct TclLsmCursor {
  lsm_cursor *csr;
};

static int test_lsm_error(Tcl_Interp *interp, const char *zApi, int rc){
  char zMsg[64];
  if( rc==LSM_OK ){
    return TCL_OK;
  }

  sprintf(zMsg, "error in %s() - %d", zApi, rc);
  Tcl_ResetResult(interp);
  Tcl_AppendResult(interp, zMsg, 0);
  return TCL_ERROR;
}

static void test_lsm_cursor_del(void *ctx){
  TclLsmCursor *pCsr = (TclLsmCursor *)ctx;
  if( pCsr ){
    lsm_csr_close(pCsr->csr);
    ckfree((char *)pCsr);
  }
}

static void test_lsm_del(void *ctx){
  TclLsm *p = (TclLsm *)ctx;
  if( p ){
    lsm_close(p->db);
    ckfree((char *)p);
  }
}

static int testInfoLsm(Tcl_Interp *interp, lsm_db *db, Tcl_Obj *pObj){
  struct Lsminfo {
    const char *zOpt;
    int eOpt;
  } aInfo[] = {
    { "compression_id",          LSM_INFO_COMPRESSION_ID },
    { 0, 0 }
  };
  int rc;
  int iOpt;

  rc = Tcl_GetIndexFromObjStruct(
      interp, pObj, aInfo, sizeof(aInfo[0]), "option", 0, &iOpt
  );
  if( rc==LSM_OK ){
    switch( aInfo[iOpt].eOpt ){
      case LSM_INFO_COMPRESSION_ID: {
        unsigned int iCmpId = 0;
        rc = lsm_info(db, LSM_INFO_COMPRESSION_ID, &iCmpId);
        if( rc==LSM_OK ){
          Tcl_SetObjResult(interp, Tcl_NewWideIntObj((Tcl_WideInt)iCmpId));
        }else{
          test_lsm_error(interp, "lsm_info", rc);
        }
        break;
      }
    }
  }

  return rc;
}

/*
** Usage: CSR sub-command ...
*/
static int test_lsm_cursor_cmd(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  struct Subcmd {
    const char *zCmd;
    int nArg;
    const char *zUsage;
  } aCmd[] = {
    /* 0 */ {"close",      0, ""},
    /* 1 */ {"seek",       2, "KEY SEEK-TYPE"},
    /* 2 */ {"first",      0, ""},
    /* 3 */ {"last",       0, ""},
    /* 4 */ {"next",       0, ""},
    /* 5 */ {"prev",       0, ""},
    /* 6 */ {"key",        0, ""},
    /* 7 */ {"value",      0, ""},
    /* 8 */ {"valid",      0, ""},
    {0, 0, 0}
  };
  int iCmd;
  int rc;
  TclLsmCursor *pCsr = (TclLsmCursor *)clientData;

  rc = Tcl_GetIndexFromObjStruct(
      interp, objv[1], aCmd, sizeof(aCmd[0]), "sub-command", 0, &iCmd
  );
  if( rc!=TCL_OK ) return rc;
  if( aCmd[iCmd].nArg>=0 && objc!=(2 + aCmd[iCmd].nArg) ){
    Tcl_WrongNumArgs(interp, 2, objv, aCmd[iCmd].zUsage);
    return TCL_ERROR;
  }

  switch( iCmd ){

    case 0: assert( 0==strcmp(aCmd[0].zCmd, "close") ); {
      Tcl_DeleteCommand(interp, Tcl_GetStringFromObj(objv[0], 0));
      return TCL_OK;
    }

    case 1: assert( 0==strcmp(aCmd[1].zCmd, "seek") ); {
      struct Seekbias {
        const char *zBias;
        int eBias;
      } aBias[] = {
        {"eq",     LSM_SEEK_EQ},
        {"le",     LSM_SEEK_LE},
        {"lefast", LSM_SEEK_LEFAST},
        {"ge",     LSM_SEEK_GE},
        {0, 0}
      };
      int iBias;
      const char *zKey; int nKey;
      zKey = Tcl_GetStringFromObj(objv[2], &nKey);

      rc = Tcl_GetIndexFromObjStruct(
          interp, objv[3], aBias, sizeof(aBias[0]), "bias", 0, &iBias
      );
      if( rc!=TCL_OK ) return rc;

      rc = lsm_csr_seek(pCsr->csr, zKey, nKey, aBias[iBias].eBias);
      return test_lsm_error(interp, "lsm_seek", rc);
    }

    case 2: 
    case 3: 
    case 4: 
    case 5: {
      const char *zApi;

      assert( 0==strcmp(aCmd[2].zCmd, "first") );
      assert( 0==strcmp(aCmd[3].zCmd, "last") );
      assert( 0==strcmp(aCmd[4].zCmd, "next") );
      assert( 0==strcmp(aCmd[5].zCmd, "prev") );

      switch( iCmd ){
        case 2: rc = lsm_csr_first(pCsr->csr); zApi = "lsm_csr_first"; break;
        case 3: rc = lsm_csr_last(pCsr->csr);  zApi = "lsm_csr_last";  break;
        case 4: rc = lsm_csr_next(pCsr->csr);  zApi = "lsm_csr_next";  break;
        case 5: rc = lsm_csr_prev(pCsr->csr);  zApi = "lsm_csr_prev";  break;
      }

      return test_lsm_error(interp, zApi, rc);
    }

    case 6: assert( 0==strcmp(aCmd[6].zCmd, "key") ); {
      const void *pKey; int nKey;
      rc = lsm_csr_key(pCsr->csr, &pKey, &nKey);
      if( rc!=LSM_OK ) test_lsm_error(interp, "lsm_csr_key", rc);

      Tcl_SetObjResult(interp, Tcl_NewStringObj((const char *)pKey, nKey));
      return TCL_OK;
    }

    case 7: assert( 0==strcmp(aCmd[7].zCmd, "value") ); {
      const void *pVal; int nVal;
      rc = lsm_csr_value(pCsr->csr, &pVal, &nVal);
      if( rc!=LSM_OK ) test_lsm_error(interp, "lsm_csr_value", rc);

      Tcl_SetObjResult(interp, Tcl_NewStringObj((const char *)pVal, nVal));
      return TCL_OK;
    }

    case 8: assert( 0==strcmp(aCmd[8].zCmd, "valid") ); {
      int bValid = lsm_csr_valid(pCsr->csr);
      Tcl_SetObjResult(interp, Tcl_NewBooleanObj(bValid));
      return TCL_OK;
    }
  }

  Tcl_AppendResult(interp, "internal error", 0);
  return TCL_ERROR;
}

/*
** Usage: DB sub-command ...
*/
static int test_lsm_cmd(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  struct Subcmd {
    const char *zCmd;
    int nArg;
    const char *zUsage;
  } aCmd[] = {
    /*  0 */ {"close",        0, ""},
    /*  1 */ {"write",        2, "KEY VALUE"},
    /*  2 */ {"delete",       1, "KEY"},
    /*  3 */ {"delete_range", 2, "START-KEY END-KEY"},
    /*  4 */ {"begin",        1, "LEVEL"},
    /*  5 */ {"commit",       1, "LEVEL"},
    /*  6 */ {"rollback",     1, "LEVEL"},
    /*  7 */ {"csr_open",     1, "CSR"},
    /*  8 */ {"work",        -1, "?NMERGE? NPAGE"},
    /*  9 */ {"flush",        0, ""},
    /* 10 */ {"config",       1, "LIST"},
    /* 11 */ {"checkpoint",   0, ""},
    /* 12 */ {"info",         1, "OPTION"},
    {0, 0, 0}
  };
  int iCmd;
  int rc;
  TclLsm *p = (TclLsm *)clientData;

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUB-COMMAND ...");
    return TCL_ERROR;
  }

  rc = Tcl_GetIndexFromObjStruct(
      interp, objv[1], aCmd, sizeof(aCmd[0]), "sub-command", 0, &iCmd
  );
  if( rc!=TCL_OK ) return rc;
  if( aCmd[iCmd].nArg>=0 && objc!=(2 + aCmd[iCmd].nArg) ){
    Tcl_WrongNumArgs(interp, 2, objv, aCmd[iCmd].zUsage);
    return TCL_ERROR;
  }

  switch( iCmd ){

    case 0: assert( 0==strcmp(aCmd[0].zCmd, "close") ); {
      Tcl_DeleteCommand(interp, Tcl_GetStringFromObj(objv[0], 0));
      return TCL_OK;
    }

    case 1: assert( 0==strcmp(aCmd[1].zCmd, "write") ); {
      const char *zKey; int nKey;
      const char *zVal; int nVal;

      zKey = Tcl_GetStringFromObj(objv[2], &nKey);
      zVal = Tcl_GetStringFromObj(objv[3], &nVal);

      rc = lsm_insert(p->db, zKey, nKey, zVal, nVal);
      return test_lsm_error(interp, "lsm_insert", rc);
    }

    case 2: assert( 0==strcmp(aCmd[2].zCmd, "delete") ); {
      const char *zKey; int nKey;

      zKey = Tcl_GetStringFromObj(objv[2], &nKey);

      rc = lsm_delete(p->db, zKey, nKey);
      return test_lsm_error(interp, "lsm_delete", rc);
    }

    case 3: assert( 0==strcmp(aCmd[3].zCmd, "delete_range") ); {
      const char *zKey1; int nKey1;
      const char *zKey2; int nKey2;

      zKey1 = Tcl_GetStringFromObj(objv[2], &nKey1);
      zKey2 = Tcl_GetStringFromObj(objv[3], &nKey2);

      rc = lsm_delete_range(p->db, zKey1, nKey1, zKey2, nKey2);
      return test_lsm_error(interp, "lsm_delete_range", rc);
    }

    case 4: 
    case 5: 
    case 6: {
      const char *zApi;
      int iLevel;

      rc = Tcl_GetIntFromObj(interp, objv[2], &iLevel);
      if( rc!=TCL_OK ) return rc;

      assert( 0==strcmp(aCmd[4].zCmd, "begin") );
      assert( 0==strcmp(aCmd[5].zCmd, "commit") );
      assert( 0==strcmp(aCmd[6].zCmd, "rollback") );
      switch( iCmd ){
        case 4: rc = lsm_begin(p->db, iLevel); zApi = "lsm_begin"; break;
        case 5: rc = lsm_commit(p->db, iLevel); zApi = "lsm_commit"; break;
        case 6: rc = lsm_rollback(p->db, iLevel); zApi = "lsm_rollback"; break;
      }

      return test_lsm_error(interp, zApi, rc);
    }

    case 7: assert( 0==strcmp(aCmd[7].zCmd, "csr_open") ); {
      const char *zCsr = Tcl_GetString(objv[2]);
      TclLsmCursor *pCsr;

      pCsr = (TclLsmCursor *)ckalloc(sizeof(TclLsmCursor));
      rc = lsm_csr_open(p->db, &pCsr->csr);
      if( rc!=LSM_OK ){
        test_lsm_cursor_del(pCsr);
        return test_lsm_error(interp, "lsm_csr_open", rc);
      }

      Tcl_CreateObjCommand(
          interp, zCsr, test_lsm_cursor_cmd, 
          (ClientData)pCsr, test_lsm_cursor_del
      );
      Tcl_SetObjResult(interp, objv[2]);
      return TCL_OK;
    }

    case 8: assert( 0==strcmp(aCmd[8].zCmd, "work") ); {
      int nWork = 0;
      int nMerge = 1;
      int nWrite = 0;

      if( objc==3 ){
        rc = Tcl_GetIntFromObj(interp, objv[2], &nWork);
      }else if( objc==4 ){
        rc = Tcl_GetIntFromObj(interp, objv[2], &nMerge);
        if( rc!=TCL_OK ) return rc;
        rc = Tcl_GetIntFromObj(interp, objv[3], &nWork);
      }else{
        Tcl_WrongNumArgs(interp, 2, objv, "?NMERGE? NWRITE");
        return TCL_ERROR;
      }
      if( rc!=TCL_OK ) return rc;

      rc = lsm_work(p->db, nMerge, nWork, &nWrite);
      if( rc!=LSM_OK ) return test_lsm_error(interp, "lsm_work", rc);
      Tcl_SetObjResult(interp, Tcl_NewIntObj(nWrite));
      return TCL_OK;
    }

    case 9: assert( 0==strcmp(aCmd[9].zCmd, "flush") ); {
      rc = lsm_flush(p->db);
      return test_lsm_error(interp, "lsm_flush", rc);
    }

    case 10: assert( 0==strcmp(aCmd[10].zCmd, "config") ); {
      Tcl_Obj **apObj;
      int nObj;
      if( TCL_OK==Tcl_ListObjGetElements(interp, objv[2], &nObj, &apObj) ){
        return testConfigureLsm(interp, p->db, nObj, apObj);
      }
      return TCL_ERROR;
    }

    case 11: assert( 0==strcmp(aCmd[11].zCmd, "checkpoint") ); {
      rc = lsm_checkpoint(p->db, 0);
      return test_lsm_error(interp, "lsm_checkpoint", rc);
    }

    case 12: assert( 0==strcmp(aCmd[12].zCmd, "info") ); {
      return testInfoLsm(interp, p->db, objv[2]);
    }

    default:
      assert( 0 );
  }

  Tcl_AppendResult(interp, "internal error", 0);
  return TCL_ERROR;
}

static void xLog(void *pCtx, int rc, const char *z){
  (void)(rc);
  (void)(pCtx);
  fprintf(stderr, "%s\n", z);
  fflush(stderr);
}

/*
** Usage: lsm_open DB filename ?config?
*/
static int test_lsm_open(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  TclLsm *p;
  int rc;
  const char *zDb = 0;
  const char *zFile = 0;

  if( objc!=3 && objc!=4 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB FILENAME ?CONFIG?");
    return TCL_ERROR;
  }

  zDb = Tcl_GetString(objv[1]);
  zFile = Tcl_GetString(objv[2]);

  p = (TclLsm *)ckalloc(sizeof(TclLsm));
  rc = lsm_new(0, &p->db);
  if( rc!=LSM_OK ){
    test_lsm_del((void *)p);
    test_lsm_error(interp, "lsm_new", rc);
    return TCL_ERROR;
  }

  if( objc==4 ){
    Tcl_Obj **apObj;
    int nObj;
    rc = Tcl_ListObjGetElements(interp, objv[3], &nObj, &apObj);
    if( rc==TCL_OK ){
      rc = testConfigureLsm(interp, p->db, nObj, apObj);
    }
    if( rc!=TCL_OK ){ 
      test_lsm_del((void *)p);
      return rc;
    }
  }

  lsm_config_log(p->db, xLog, 0);

  rc = lsm_open(p->db, zFile);
  if( rc!=LSM_OK ){
    test_lsm_del((void *)p);
    test_lsm_error(interp, "lsm_open", rc);
    return TCL_ERROR;
  }

  Tcl_CreateObjCommand(interp, zDb, test_lsm_cmd, (ClientData)p, test_lsm_del);
  Tcl_SetObjResult(interp, objv[1]);
  return TCL_OK;
}

int SqlitetestLsm_Init(Tcl_Interp *interp){
  struct SyscallCmd {
    const char *zName;
    Tcl_ObjCmdProc *xCmd;
  } aCmd[] = {
    { "sqlite4_lsm_work",       test_sqlite4_lsm_work                },
    { "sqlite4_lsm_checkpoint", test_sqlite4_lsm_checkpoint          },
    { "sqlite4_lsm_info",       test_sqlite4_lsm_info                },
    { "sqlite4_lsm_config",     test_sqlite4_lsm_config              },
    { "lsm_open",               test_lsm_open                        },
  };
  int i;

  for(i=0; i<sizeof(aCmd)/sizeof(aCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aCmd[i].zName, aCmd[i].xCmd, 0, 0);
  }
  return TCL_OK;
}
