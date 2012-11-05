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
**
*/

#include <tcl.h>
#include "lsm.h"
#include "sqlite4.h"
#include <assert.h>
#include <string.h>

extern int getDbPointer(Tcl_Interp *interp, const char *zA, sqlite4 **ppDb);
extern const char *sqlite4TestErrorName(int);

/*
** TCLCMD:    sqlite4_lsm_config DB DBNAME PARAM ...
*/
static int test_sqlite4_lsm_config(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  struct Switch {
    const char *zSwitch;
    int iVal;
  } aParam[] = {
    { "log-size",       LSM_CONFIG_LOG_SIZE }, 
    { "safety",         LSM_CONFIG_SAFETY }, 
    { "write-buffer",   LSM_CONFIG_WRITE_BUFFER }, 
    { "mmap",           LSM_CONFIG_MMAP }, 
    { "page-size",      LSM_CONFIG_PAGE_SIZE }, 
    { "autowork",       LSM_CONFIG_AUTOWORK }, 
    { 0, 0 }
  };

  const char *zDb;                /* objv[1] as a string */
  const char *zName;              /* objv[2] as a string */
  int iParam;                     /* Second argument for lsm_config() */
  int iConfig = -1;               /* Third argument for lsm_config() */
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
  rc = Tcl_GetIndexFromObjStruct(
      interp, objv[3], aParam, sizeof(aParam[0]), "param", 0, &iParam
  );
  if( rc!=TCL_OK ) return rc;
  if( rc==TCL_OK ){
    iParam = aParam[iParam].iVal;
    rc = getDbPointer(interp, zDb, &db);
  }
  if( rc==TCL_OK && objc==5 ){
    rc = Tcl_GetIntFromObj(interp, objv[4], &iConfig);
  }
  if( rc!=TCL_OK ) return rc;

  rc = sqlite4_kvstore_control(db, zName, SQLITE4_KVCTRL_LSM_HANDLE, &pLsm);
  if( rc==SQLITE4_OK ){
    rc = lsm_config(pLsm, iParam, &iConfig);
    Tcl_SetObjResult(interp, Tcl_NewIntObj(iConfig));
  }

  if( rc!=SQLITE4_OK ){
    Tcl_SetResult(interp, (char *)sqlite4TestErrorName(rc), TCL_STATIC);
    return TCL_ERROR;
  }
  return TCL_OK;
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
** TCLCMD:    sqlite4_lsm_work DB DBNAME ?SWITCHES? ?N?
*/
static int test_sqlite4_lsm_work(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  struct Switch {
    const char *zSwitch;
    int flags;
  } aSwitch[] = {
    { "-optimize",   LSM_WORK_OPTIMIZE }, 
    { 0, 0 }
  };

  int flags = 0;
  int nPage = 0;
  const char *zDb;
  const char *zName;
  int i;
  int rc;
  sqlite4 *db;
  lsm_db *pLsm;
  int nWork;

  if( objc<3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB DBNAME ?SWITCHES? ?N?");
    return TCL_ERROR;
  }
  zDb = Tcl_GetString(objv[1]);
  zName = Tcl_GetString(objv[2]);

  for(i=3; i<objc; i++){
    const char *z = Tcl_GetString(objv[i]);

    if( z[0]=='-' ){
      int iIdx;
      rc = Tcl_GetIndexFromObjStruct(
          interp, objv[i], aSwitch, sizeof(aSwitch[0]), "switch", 0, &iIdx
      );
      if( rc!=TCL_OK ) return rc;
      flags |= aSwitch[iIdx].flags;
    }else{
      rc = Tcl_GetIntFromObj(interp, objv[i], &nPage);
      if( rc!=TCL_OK ) return rc;
    }
  }

  rc = getDbPointer(interp, zDb, &db);
  if( rc!=TCL_OK ) return rc;

  rc = sqlite4_kvstore_control(db, zName, SQLITE4_KVCTRL_LSM_HANDLE, &pLsm);
  if( rc==SQLITE4_OK ){
    rc = lsm_work(pLsm, flags, nPage, &nWork);
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

/*
** TCLCMD:    sqlite4_lsm_flush DB DBNAME 
*/
static int test_sqlite4_lsm_flush(
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
    int nZero = 0;
    int nOrig = -1;
    lsm_config(pLsm, LSM_CONFIG_WRITE_BUFFER, &nOrig);
    lsm_config(pLsm, LSM_CONFIG_WRITE_BUFFER, &nZero);
    rc = lsm_begin(pLsm, 1);
    if( rc==LSM_OK ) rc = lsm_commit(pLsm, 0);
    lsm_config(pLsm, LSM_CONFIG_WRITE_BUFFER, &nOrig);
  }
  if( rc!=SQLITE4_OK ){
    Tcl_SetResult(interp, (char *)sqlite4TestErrorName(rc), TCL_STATIC);
    return TCL_ERROR;
  }

  Tcl_ResetResult(interp);
  return TCL_OK;
}

static int testConfigureLsm(Tcl_Interp *interp, lsm_db *db, Tcl_Obj *pObj){
  struct Lsmconfig {
    const char *zOpt;
    int eOpt;
  } aConfig[] = {
    { "write_buffer",     LSM_CONFIG_WRITE_BUFFER },
    { "page_size",        LSM_CONFIG_PAGE_SIZE },
    { "block_size",       LSM_CONFIG_BLOCK_SIZE },
    { "safety",           LSM_CONFIG_SAFETY },
    { "autowork",         LSM_CONFIG_AUTOWORK },
    { "autocheckpoint",   LSM_CONFIG_AUTOCHECKPOINT },
    { "log_size",         LSM_CONFIG_LOG_SIZE },
    { "mmap",             LSM_CONFIG_MMAP },
    { "use_log",          LSM_CONFIG_USE_LOG },
    { "nmerge",           LSM_CONFIG_NMERGE },
    { "max_freelist",     LSM_CONFIG_MAX_FREELIST },
    { "multi_proc",       LSM_CONFIG_MULTIPLE_PROCESSES },
    { 0, 0 }
  };
  int nElem;
  int i;
  Tcl_Obj **apElem;
  int rc;

  rc = Tcl_ListObjGetElements(interp, pObj, &nElem, &apElem);
  for(i=0; rc==TCL_OK && i<nElem; i+=2){
    int iOpt;
    rc = Tcl_GetIndexFromObjStruct(
        interp, apElem[i], aConfig, sizeof(aConfig[0]), "option", 0, &iOpt
    );
    if( rc==TCL_OK ){
      if( i==(nElem-1) ){
        Tcl_ResetResult(interp);
        Tcl_AppendResult(interp, "option \"", Tcl_GetString(apElem[i]), 
            "\" requires an argument", 0
            );
        rc = TCL_ERROR;
      }else{
        int iVal;
        rc = Tcl_GetIntFromObj(interp, apElem[i+1], &iVal);
        if( rc==TCL_OK ){
          lsm_config(db, aConfig[iOpt].eOpt, &iVal);
        }
      }
    }
  }

  return rc;
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
    /*  8 */ {"work",        -1, "NPAGE ?SWITCHES?"},
    /*  9 */ {"flush",        0, ""},
    /* 10 */ {"config",       1, "LIST"},
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

      rc = lsm_write(p->db, zKey, nKey, zVal, nVal);
      return test_lsm_error(interp, "lsm_write", rc);
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
      int nWork;
      int nWrite = 0;
      int flags = 0;
      int i;

      rc = Tcl_GetIntFromObj(interp, objv[2], &nWork);
      if( rc!=TCL_OK ) return rc;

      for(i=3; i<objc; i++){
        int iOpt;
        const char *azOpt[] = { "-optimize", "-flush", 0 };

        rc = Tcl_GetIndexFromObj(interp, objv[i], azOpt, "option", 0, &iOpt);
        if( rc!=TCL_OK ) return rc;

        if( iOpt==0 ) flags |= LSM_WORK_OPTIMIZE;
      }

      rc = lsm_work(p->db, flags, nWork, &nWrite);
      if( rc!=LSM_OK ) return test_lsm_error(interp, "lsm_work", rc);
      Tcl_SetObjResult(interp, Tcl_NewIntObj(nWrite));
      return TCL_OK;
    }

    case 9: assert( 0==strcmp(aCmd[9].zCmd, "flush") ); {
      rc = lsm_flush(p->db);
      return test_lsm_error(interp, "lsm_flush", rc);
    }

    case 10: assert( 0==strcmp(aCmd[10].zCmd, "config") ); {
      return testConfigureLsm(interp, p->db, objv[2]);
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
    rc = testConfigureLsm(interp, p->db, objv[3]);
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
    { "sqlite4_lsm_flush",      test_sqlite4_lsm_flush               },
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
