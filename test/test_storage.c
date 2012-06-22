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
extern void *sqlite4TestTextToPtr(const char*);

/* Defined in test_hexio.c */
extern void sqlite4TestBinToHex(unsigned char*,int);
extern int sqlite4TestHexToBin(const unsigned char *in,int,unsigned char *out);

/* Set the TCL result to an integer.
*/
static void storageSetTclErrorName(Tcl_Interp *interp, int rc){
  extern const char *sqlite4TestErrorName(int);
  Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite4TestErrorName(rc), -1));
}

/*
** TCLCMD:    storage_open URI ?FLAGS?
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
  int flags = 0;
  sqlite4 db;
  char zRes[50];
  if( objc!=2 && objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "URI ?FLAGS?");
    return TCL_ERROR;
  }
  if( objc==3 && Tcl_GetIntFromObj(interp, objv[2], &flags) ){
    return TCL_ERROR;
  }
  memset(&db, 0, sizeof(db));
  rc = sqlite4KVStoreOpen(&db, "test", Tcl_GetString(objv[1]), &pNew, flags);
  if( rc ){
    sqlite4KVStoreClose(pNew);
    storageSetTclErrorName(interp, rc);
    return TCL_ERROR;
  }
  sqlite4_snprintf(zRes,sizeof(zRes), "%p", pNew);
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
  pOld = sqlite4TestTextToPtr(Tcl_GetString(objv[1]));
  rc = sqlite4KVStoreClose(pOld);
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
  pStore = sqlite4TestTextToPtr(Tcl_GetString(objv[1]));
  rc = sqlite4KVStoreOpenCursor(pStore, &pNew);
  if( rc ){
    sqlite4KVCursorClose(pNew);
    storageSetTclErrorName(interp, rc);
    return TCL_ERROR;
  }
  sqlite4_snprintf(zRes,sizeof(zRes), "%p", pNew);
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
    Tcl_WrongNumArgs(interp, 2, objv, "CURSOR");
    return TCL_ERROR;
  }
  pOld = sqlite4TestTextToPtr(Tcl_GetString(objv[1]));
  rc = sqlite4KVCursorClose(pOld);
  if( rc ){
    storageSetTclErrorName(interp, rc);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/* Decode hex into a binary key */
static void sqlite4DecodeHex(Tcl_Obj *pObj, unsigned char *a, int *pN){
  const unsigned char *pIn;
  int nIn;
  pIn = (const unsigned char*)Tcl_GetStringFromObj(pObj, &nIn);
  *pN = sqlite4TestHexToBin(pIn, nIn, a);
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
  p = sqlite4TestTextToPtr(Tcl_GetString(objv[1]));
  sqlite4DecodeHex(objv[2], zKey, &nKey);
  sqlite4DecodeHex(objv[3], zData, &nData);
  rc = sqlite4KVStoreReplace(p, zKey, nKey, zData, nData);
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
  p = sqlite4TestTextToPtr(Tcl_GetString(objv[1]));
  if( Tcl_GetIntFromObj(interp, objv[2], &iLevel) ) return TCL_ERROR;
  rc = sqlite4KVStoreBegin(p, iLevel);
  if( rc ){
    storageSetTclErrorName(interp, rc);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** TCLCMD:    storage_commit STORAGE LEVEL
**
** Increase the transaction level
*/
static int test_storage_commit(
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
  p = sqlite4TestTextToPtr(Tcl_GetString(objv[1]));
  if( Tcl_GetIntFromObj(interp, objv[2], &iLevel) ) return TCL_ERROR;
  rc = sqlite4KVStoreCommitPhaseOne(p, iLevel);
  if( rc ){
    storageSetTclErrorName(interp, rc);
    return TCL_ERROR;
  }
  rc = sqlite4KVStoreCommitPhaseTwo(p, iLevel);
  if( rc ){
    storageSetTclErrorName(interp, rc);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** TCLCMD:    storage_rollback STORAGE LEVEL
**
** Increase the transaction level
*/
static int test_storage_rollback(
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
  p = sqlite4TestTextToPtr(Tcl_GetString(objv[1]));
  if( Tcl_GetIntFromObj(interp, objv[2], &iLevel) ) return TCL_ERROR;
  rc = sqlite4KVStoreRollback(p, iLevel);
  if( rc ){
    storageSetTclErrorName(interp, rc);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** TCLCMD:    storage_seek CURSOR KEY DIRECTION
**
** Move a cursor object
*/
static int test_storage_seek(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  KVCursor *p = 0;
  int rc;
  int nKey, dir;
  unsigned char aKey[100];
  if( objc!=4 ){
    Tcl_WrongNumArgs(interp, 4, objv, "CURSOR KEY DIRECTION");
    return TCL_ERROR;
  }
  p = sqlite4TestTextToPtr(Tcl_GetString(objv[1]));
  sqlite4DecodeHex(objv[2], aKey, &nKey);
  if( Tcl_GetIntFromObj(interp, objv[3], &dir) ) return TCL_ERROR;
  rc = sqlite4KVCursorSeek(p, aKey, nKey, dir);
  storageSetTclErrorName(interp, rc);
  return TCL_OK;
}

/*
** TCLCMD:    storage_next CURSOR
**
** Move the cursor to the next entry
*/
static int test_storage_next(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  KVCursor *p = 0;
  int rc;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 2, objv, "CURSOR");
    return TCL_ERROR;
  }
  p = sqlite4TestTextToPtr(Tcl_GetString(objv[1]));
  rc = sqlite4KVCursorNext(p);
  storageSetTclErrorName(interp, rc);
  return TCL_OK;
}

/*
** TCLCMD:    storage_prev CURSOR
**
** Move the cursor to the previous entry
*/
static int test_storage_prev(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  KVCursor *p = 0;
  int rc;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 2, objv, "CURSOR");
    return TCL_ERROR;
  }
  p = sqlite4TestTextToPtr(Tcl_GetString(objv[1]));
  rc = sqlite4KVCursorPrev(p);
  storageSetTclErrorName(interp, rc);
  return TCL_OK;
}

/*
** TCLCMD:    storage_delete CURSOR
**
** delete the entry under the cursor
*/
static int test_storage_delete(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  KVCursor *p = 0;
  int rc;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 2, objv, "CURSOR");
    return TCL_ERROR;
  }
  p = sqlite4TestTextToPtr(Tcl_GetString(objv[1]));
  rc = sqlite4KVCursorDelete(p);
  storageSetTclErrorName(interp, rc);
  return TCL_OK;
}

/*
** TCLCMD:    storage_reset CURSOR
**
** Reset the cursor
*/
static int test_storage_reset(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  KVCursor *p = 0;
  int rc;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 2, objv, "CURSOR");
    return TCL_ERROR;
  }
  p = sqlite4TestTextToPtr(Tcl_GetString(objv[1]));
  rc = sqlite4KVCursorReset(p);
  storageSetTclErrorName(interp, rc);
  return TCL_OK;
}

/*
** TCLCMD:    storage_key CURSOR
**
** Return the complete key of the cursor
*/
static int test_storage_key(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  KVCursor *p = 0;
  int rc;
  const unsigned char *aKey;
  int nKey;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 2, objv, "CURSOR");
    return TCL_ERROR;
  }
  p = sqlite4TestTextToPtr(Tcl_GetString(objv[1]));
  rc = sqlite4KVCursorKey(p, &aKey, &nKey);
  if( rc ){
    storageSetTclErrorName(interp, rc);
  }else{
    unsigned char zBuf[500];
    memcpy(zBuf, aKey, nKey);
    sqlite4TestBinToHex(zBuf, nKey);
    Tcl_SetObjResult(interp, Tcl_NewStringObj((char*)zBuf, -1));
  }
  return TCL_OK;
}

/*
** TCLCMD:    storage_data CURSOR
**
** Return the complete data of the cursor
*/
static int test_storage_data(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  KVCursor *p = 0;
  int rc;
  const unsigned char *aData;
  int nData;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 2, objv, "CURSOR");
    return TCL_ERROR;
  }
  p = sqlite4TestTextToPtr(Tcl_GetString(objv[1]));
  rc = sqlite4KVCursorData(p, 0, 0, &aData, &nData);
  if( rc ){
    storageSetTclErrorName(interp, rc);
  }else{
    unsigned char zBuf[500];
    memcpy(zBuf, aData, nData);
    sqlite4TestBinToHex(zBuf, nData);
    Tcl_SetObjResult(interp, Tcl_NewStringObj((char*)zBuf, -1));
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
    { "storage_commit",       test_storage_commit          },
    { "storage_rollback",     test_storage_rollback        },
    { "storage_seek",         test_storage_seek            },
    { "storage_next",         test_storage_next            },
    { "storage_prev",         test_storage_prev            },
    { "storage_delete",       test_storage_delete          },
    { "storage_reset",        test_storage_reset           },
    { "storage_key",          test_storage_key             },
    { "storage_data",         test_storage_data            },
  };
  int i;

  for(i=0; i<sizeof(aCmd)/sizeof(aCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aCmd[i].zName, aCmd[i].xCmd, 0, 0);
  }
  return TCL_OK;
}
