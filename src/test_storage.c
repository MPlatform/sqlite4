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

/* Defined in test_hexio.c */
extern void sqlite3TestBinToHex(unsigned char*,int);
extern int sqlite3TestHexToBin(const unsigned char *in,int,unsigned char *out);

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
** TCLCMD:    storage_open_cursor STORAGE
**
** Return a string that identifies the new storage cursor
*/
static int test_storage_open_cursor(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  KVCursor *pNew = 0;
  KVStore *pStore = 0;
  int rc;
  char zRes[50];
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 2, objv, "STORAGE");
    return TCL_ERROR;
  }
  pStore = sqlite3TestTextToPtr(Tcl_GetString(objv[1]));
  rc = sqlite3KVStoreOpenCursor(pStore, &pNew);
  if( rc ){
    sqlite3KVCursorClose(pNew);
    storageSetTclErrorName(interp, rc);
    return TCL_ERROR;
  }
  sqlite3_snprintf(sizeof(zRes),zRes, "%p", pNew);
  Tcl_SetObjResult(interp, Tcl_NewStringObj(zRes,-1));
  return TCL_OK;
}

/*
** TCLCMD:    storage_close_cursor CURSOR
**
** Close a cursor object.
*/
static int test_storage_close_cursor(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  KVCursor *pOld = 0;
  int rc;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 2, objv, "STORAGE");
    return TCL_ERROR;
  }
  pOld = sqlite3TestTextToPtr(Tcl_GetString(objv[1]));
  rc = sqlite3KVCursorClose(pOld);
  if( rc ){
    storageSetTclErrorName(interp, rc);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/* Decode hex into a binary key */
static void sqlite3DecodeHex(Tcl_Obj *pObj, unsigned char *a, int *pN){
  const unsigned char *pIn;
  int nIn;
  pIn = (const unsigned char*)Tcl_GetStringFromObj(pObj, &nIn);
  *pN = sqlite3TestHexToBin(pIn, nIn, a);
}

/*
** TCLCMD:    storage_replace STORAGE KEY VALUE
**
** Insert content into a KV storage object.  KEY and VALUE are hex.
*/
static int test_storage_replace(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  KVStore *p = 0;
  int rc;
  int nKey, nData;
  unsigned char zKey[200];
  unsigned char zData[200];
  if( objc!=4 ){
    Tcl_WrongNumArgs(interp, 2, objv, "STORAGE KEY VALUE");
    return TCL_ERROR;
  }
  p = sqlite3TestTextToPtr(Tcl_GetString(objv[1]));
  sqlite3DecodeHex(objv[2], zKey, &nKey);
  sqlite3DecodeHex(objv[3], zData, &nData);
  rc = sqlite3KVStoreReplace(p, zKey, nKey, zData, nData);
  if( rc ){
    storageSetTclErrorName(interp, rc);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** TCLCMD:    storage_begin STORAGE LEVEL
**
** Increase the transaction level
*/
static int test_storage_begin(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  KVStore *p = 0;
  int rc;
  int iLevel;
  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 2, objv, "STORAGE LEVEL");
    return TCL_ERROR;
  }
  p = sqlite3TestTextToPtr(Tcl_GetString(objv[1]));
  if( Tcl_GetIntFromObj(interp, objv[2], &iLevel) ) return TCL_ERROR;
  rc = sqlite3KVStoreBegin(p, iLevel);
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
    { "storage_open",         test_storage_open            },
    { "storage_close",        test_storage_close           },
    { "storage_open_cursor",  test_storage_open_cursor     },
    { "storage_close_cursor", test_storage_close_cursor    },
    { "storage_replace",      test_storage_replace         },
    { "storage_begin",        test_storage_begin           },
  };
  int i;

  for(i=0; i<sizeof(aCmd)/sizeof(aCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aCmd[i].zName, aCmd[i].xCmd, 0, 0);
  }
  return TCL_OK;
}
