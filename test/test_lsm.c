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

extern int getDbPointer(Tcl_Interp *interp, const char *zA, sqlite4 **ppDb);
extern const char *sqlite4TestErrorName(int);

/*
** TCLCMD:    sqlite4_lsm_config DB DBNAME PARAM ...
*/
static int test_lsm_config(
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

  rc = sqlite4_kvstore_control(db, zName, SQLITE_KVCTRL_LSM_HANDLE, &pLsm);
  if( rc==SQLITE_OK ){
    rc = lsm_config(pLsm, iParam, &iConfig);
    Tcl_SetObjResult(interp, Tcl_NewIntObj(iConfig));
  }

  if( rc!=SQLITE_OK ){
    Tcl_SetResult(interp, (char *)sqlite4TestErrorName(rc), TCL_STATIC);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** TCLCMD:    sqlite4_lsm_info DB DBNAME PARAM
*/
static int test_lsm_info(
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

  rc = sqlite4_kvstore_control(db, zName, SQLITE_KVCTRL_LSM_HANDLE, &pLsm);
  if( rc==SQLITE_OK ){
    char *zList;
    lsm_info(pLsm, iParam, &zList);
    Tcl_SetResult(interp, zList, TCL_VOLATILE);
    lsm_free(lsm_get_env(pLsm), zList);
  }

  if( rc!=SQLITE_OK ){
    Tcl_SetResult(interp, (char *)sqlite4TestErrorName(rc), TCL_STATIC);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** TCLCMD:    sqlite4_lsm_work DB DBNAME ?SWITCHES? ?N?
*/
static int test_lsm_work(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  struct Switch {
    const char *zSwitch;
    int flags;
  } aSwitch[] = {
    { "-flush",      LSM_WORK_FLUSH }, 
    { "-checkpoint", LSM_WORK_CHECKPOINT }, 
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

  rc = sqlite4_kvstore_control(db, zName, SQLITE_KVCTRL_LSM_HANDLE, &pLsm);
  if( rc==SQLITE_OK ){
    rc = lsm_work(pLsm, flags, nPage, &nWork);
  }
  if( rc!=SQLITE_OK ){
    Tcl_SetResult(interp, (char *)sqlite4TestErrorName(rc), TCL_STATIC);
    return TCL_ERROR;
  }

  Tcl_SetObjResult(interp, Tcl_NewIntObj(nWork));
  return TCL_OK;
}

int SqlitetestLsm_Init(Tcl_Interp *interp){
  struct SyscallCmd {
    const char *zName;
    Tcl_ObjCmdProc *xCmd;
  } aCmd[] = {
    { "sqlite4_lsm_work",     test_lsm_work                },
    { "sqlite4_lsm_info",     test_lsm_info                },
    { "sqlite4_lsm_config",   test_lsm_config              },
  };
  int i;

  for(i=0; i<sizeof(aCmd)/sizeof(aCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aCmd[i].zName, aCmd[i].xCmd, 0, 0);
  }
  return TCL_OK;
}
