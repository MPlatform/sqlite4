/*
** 2011 January 21
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
** Code for testing the storage subsystem using the Storage interface.
*/
#include "sqliteInt.h"

/* Defined in test1.c */
extern void *sqlite3TestTextToPtr(const char*);

/* Set the TCL result to an integer.
*/
static void storageSetTclErrorName(Tcl_Interp *interp, int rc){
  extern const char *sqlite3TestErrorName(int);
  Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3TestErrorName(rc), -1));
}

/*
** TCLCMD:    storage_open URI
**
** Return a string that identifies the new storage object.
*/
static int test_storage_open(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  KVStore *pNew = 0;
  int rc;
  char zRes[50];
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 2, objv, "URI");
    return TCL_ERROR;
  }
  rc = sqlite3KVStoreOpen(Tcl_GetString(objv[1]), &pNew);
  if( rc ){
    sqlite3KVStoreClose(pNew);
    storageSetTclErrorName(interp, rc);
    return TCL_ERROR;
  }
  sqlite3_snprintf(sizeof(zRes),zRes, "%p", pNew);
  Tcl_SetObjResult(interp, Tcl_NewStringObj(zRes,-1));
  return TCL_OK;
}

/*
** TCLCMD:    storage_close STORAGE
**
** Close a storage object.
*/
static int test_storage_close(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  KVStore *pOld = 0;
  int rc;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 2, objv, "STORAGE");
    return TCL_ERROR;
  }
  pOld = sqlite3TestTextToPtr(Tcl_GetString(objv[1]));
  rc = sqlite3KVStoreClose(pOld);
  if( rc ){
    storageSetTclErrorName(interp, rc);
    return TCL_ERROR;
  }
  return TCL_OK;
}


/*
** Register the TCL commands defined above with the TCL interpreter.
**
** This routine should be the only externally visible symbol in this
** source code file.
*/
int Sqliteteststorage_Init(Tcl_Interp *interp){
  struct SyscallCmd {
    const char *zName;
    Tcl_ObjCmdProc *xCmd;
  } aCmd[] = {
    { "storage_open",        test_storage_open},
    { "storage_close",       test_storage_close},
  };
  int i;

  for(i=0; i<sizeof(aCmd)/sizeof(aCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aCmd[i].zName, aCmd[i].xCmd, 0, 0);
  }
  return TCL_OK;
}
