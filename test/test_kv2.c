/*
** 2012 April 19
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

#include "sqliteInt.h"


static struct KVWrapGlobal {
  int (*xFactory)(sqlite4_env*, KVStore **, const char *, unsigned int);
  int nStep;                      /* Total number of successful next/prev */
  int nSeek;                      /* Total number of calls to xSeek */
} kvwg = {0};

typedef struct KVWrap KVWrap;
typedef struct KVWrapCsr KVWrapCsr;

struct KVWrap {
  KVStore base;                   /* Base class, must be first */
  KVStore *pReal;                 /* "Real" KVStore object */
};

struct KVWrapCsr {
  KVCursor base;                  /* Base class. Must be first */
  KVCursor *pReal;                /* "Real" Cursor obecjt */
};

static int kvwrapBegin(KVStore *pKVStore, int iLevel){
  int rc;
  KVWrap *p = (KVWrap *)pKVStore;
  rc = p->pReal->pStoreVfunc->xBegin(p->pReal, iLevel);
  p->base.iTransLevel = p->pReal->iTransLevel;
  return rc;
}

static int kvwrapCommitPhaseOne(KVStore *pKVStore, int iLevel){
  int rc;
  KVWrap *p = (KVWrap *)pKVStore;
  rc = p->pReal->pStoreVfunc->xCommitPhaseOne(p->pReal, iLevel);
  p->base.iTransLevel = p->pReal->iTransLevel;
  return rc;
}

static int kvwrapCommitPhaseTwo(KVStore *pKVStore, int iLevel){
  int rc;
  KVWrap *p = (KVWrap *)pKVStore;
  rc = p->pReal->pStoreVfunc->xCommitPhaseTwo(p->pReal, iLevel);
  p->base.iTransLevel = p->pReal->iTransLevel;
  return rc;
}

static int kvwrapRollback(KVStore *pKVStore, int iLevel){
  int rc;
  KVWrap *p = (KVWrap *)pKVStore;
  rc = p->pReal->pStoreVfunc->xRollback(p->pReal, iLevel);
  p->base.iTransLevel = p->pReal->iTransLevel;
  return rc;
}

static int kvwrapRevert(KVStore *pKVStore, int iLevel){
  int rc;
  KVWrap *p = (KVWrap *)pKVStore;
  rc = p->pReal->pStoreVfunc->xRevert(p->pReal, iLevel);
  p->base.iTransLevel = p->pReal->iTransLevel;
  return rc;
}

static int kvwrapReplace(
  KVStore *pKVStore,
  const KVByteArray *aKey, KVSize nKey,
  const KVByteArray *aData, KVSize nData
){
  KVWrap *p = (KVWrap *)pKVStore;
  return p->pReal->pStoreVfunc->xReplace(p->pReal, aKey, nKey, aData, nData);
}

/*
** Create a new cursor object.
*/
static int kvwrapOpenCursor(KVStore *pKVStore, KVCursor **ppKVCursor){
  int rc = SQLITE4_OK;
  KVWrap *p = (KVWrap *)pKVStore;
  KVWrapCsr *pCsr;

  pCsr = (KVWrapCsr *)sqlite4_malloc(0, sizeof(KVWrapCsr));
  if( pCsr==0 ){
    rc = SQLITE4_NOMEM;
  }else{
    memset(pCsr, 0, sizeof(KVWrapCsr));
    rc = p->pReal->pStoreVfunc->xOpenCursor(p->pReal, &pCsr->pReal);
    if( rc!=SQLITE4_OK ){
      sqlite4_free(0, pCsr);
      pCsr = 0;
    }else{
      pCsr->base.pStore = pKVStore;
      pCsr->base.pStoreVfunc = pKVStore->pStoreVfunc;
    }
  }

  *ppKVCursor = (KVCursor*)pCsr;
  return rc;
}

/*
** Reset a cursor
*/
static int kvwrapReset(KVCursor *pKVCursor){
  KVWrap *p = (KVWrap *)(pKVCursor->pStore);
  KVWrapCsr *pCsr = (KVWrapCsr *)pKVCursor;
  return p->pReal->pStoreVfunc->xReset(pCsr->pReal);
}

/*
** Destroy a cursor object
*/
static int kvwrapCloseCursor(KVCursor *pKVCursor){
  int rc;
  KVWrap *p = (KVWrap *)(pKVCursor->pStore);
  KVWrapCsr *pCsr = (KVWrapCsr *)pKVCursor;
  rc = p->pReal->pStoreVfunc->xCloseCursor(pCsr->pReal);
  sqlite4_free(0, pCsr);
  return rc;
}

/*
** Move a cursor to the next non-deleted node.
*/
static int kvwrapNextEntry(KVCursor *pKVCursor){
  int rc;
  KVWrap *p = (KVWrap *)(pKVCursor->pStore);
  KVWrapCsr *pCsr = (KVWrapCsr *)pKVCursor;
  rc = p->pReal->pStoreVfunc->xNext(pCsr->pReal);
  kvwg.nStep++;
  return rc;
}

/*
** Move a cursor to the previous non-deleted node.
*/
static int kvwrapPrevEntry(KVCursor *pKVCursor){
  int rc;
  KVWrap *p = (KVWrap *)(pKVCursor->pStore);
  KVWrapCsr *pCsr = (KVWrapCsr *)pKVCursor;
  rc = p->pReal->pStoreVfunc->xPrev(pCsr->pReal);
  kvwg.nStep++;
  return rc;
}

/*
** Seek a cursor.
*/
static int kvwrapSeek(
  KVCursor *pKVCursor, 
  const KVByteArray *aKey,
  KVSize nKey,
  int dir
){
  KVWrap *p = (KVWrap *)(pKVCursor->pStore);
  KVWrapCsr *pCsr = (KVWrapCsr *)pKVCursor;

  /* If aKey[0]==0, this is a seek to retrieve meta-data. Don't count this. */
  if( aKey[0] ) kvwg.nSeek++;

  return p->pReal->pStoreVfunc->xSeek(pCsr->pReal, aKey, nKey, dir);
}

/*
** Delete the entry that the cursor is pointing to.
**
** Though the entry is "deleted", it still continues to exist as a
** phantom.  Subsequent xNext or xPrev calls will work, as will
** calls to xKey and xData, thought the result from xKey and xData
** are undefined.
*/
static int kvwrapDelete(KVCursor *pKVCursor){
  KVWrap *p = (KVWrap *)(pKVCursor->pStore);
  KVWrapCsr *pCsr = (KVWrapCsr *)pKVCursor;
  return p->pReal->pStoreVfunc->xDelete(pCsr->pReal);
}

/*
** Return the key of the node the cursor is pointing to.
*/
static int kvwrapKey(
  KVCursor *pKVCursor,         /* The cursor whose key is desired */
  const KVByteArray **paKey,   /* Make this point to the key */
  KVSize *pN                   /* Make this point to the size of the key */
){
  KVWrap *p = (KVWrap *)(pKVCursor->pStore);
  KVWrapCsr *pCsr = (KVWrapCsr *)pKVCursor;
  return p->pReal->pStoreVfunc->xKey(pCsr->pReal, paKey, pN);
}

/*
** Return the data of the node the cursor is pointing to.
*/
static int kvwrapData(
  KVCursor *pKVCursor,         /* The cursor from which to take the data */
  KVSize ofst,                 /* Offset into the data to begin reading */
  KVSize n,                    /* Number of bytes requested */
  const KVByteArray **paData,  /* Pointer to the data written here */
  KVSize *pNData               /* Number of bytes delivered */
){
  KVWrap *p = (KVWrap *)(pKVCursor->pStore);
  KVWrapCsr *pCsr = (KVWrapCsr *)pKVCursor;
  return p->pReal->pStoreVfunc->xData(pCsr->pReal, ofst, n, paData, pNData);
}

/*
** Destructor for the entire in-memory storage tree.
*/
static int kvwrapClose(KVStore *pKVStore){
  int rc;
  KVWrap *p = (KVWrap *)pKVStore;
  rc = p->pReal->pStoreVfunc->xClose(p->pReal);
  sqlite4_free(0, p);
  return rc;
}

/*
** Invoke the xControl() method of the underlying KVStore object.
*/
static int kvwrapControl(KVStore *pKVStore, int op, void *pArg){
  KVWrap *p = (KVWrap *)pKVStore;
  return p->pReal->pStoreVfunc->xControl(p->pReal, op, pArg);
}

static int newFileStorage(
  sqlite4_env *pEnv,
  KVStore **ppKVStore,
  const char *zName,
  unsigned openFlags
){

  /* Virtual methods for an LSM data store */
  static const KVStoreMethods kvwrapMethods = {
    1,
    sizeof(KVStoreMethods),
    kvwrapReplace,
    kvwrapOpenCursor,
    kvwrapSeek,
    kvwrapNextEntry,
    kvwrapPrevEntry,
    kvwrapDelete,
    kvwrapKey,
    kvwrapData,
    kvwrapReset,
    kvwrapCloseCursor,
    kvwrapBegin,
    kvwrapCommitPhaseOne,
    kvwrapCommitPhaseTwo,
    kvwrapRollback,
    kvwrapRevert,
    kvwrapClose,
    kvwrapControl
  };

  KVWrap *pNew;
  int rc = SQLITE4_OK;

  pNew = (KVWrap *)sqlite4_malloc(0, sizeof(KVWrap));
  if( pNew==0 ){
    rc = SQLITE4_NOMEM;
  }else{
    memset(pNew, 0, sizeof(KVWrap));
    pNew->base.pStoreVfunc = &kvwrapMethods;
    rc = kvwg.xFactory(pEnv, &pNew->pReal, zName, openFlags);
    if( rc!=SQLITE4_OK ){
      sqlite4_free(0, pNew);
      pNew = 0;
    }
  }

  *ppKVStore = (KVStore*)pNew;
  return rc;
}

static int kvwrap_install_cmd(Tcl_Interp *interp, int objc, Tcl_Obj **objv){
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 2, objv, "");
    return TCL_ERROR;
  }

  if( kvwg.xFactory==0 ){
    sqlite4_env_config(0, SQLITE4_ENVCONFIG_KVSTORE_GET, "main", &kvwg.xFactory);
    sqlite4_env_config(0, SQLITE4_ENVCONFIG_KVSTORE_PUSH, "main",newFileStorage);
  }
  return TCL_OK;
}

static int kvwrap_seek_cmd(Tcl_Interp *interp, int objc, Tcl_Obj **objv){
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 2, objv, "");
    return TCL_ERROR;
  }

  Tcl_SetObjResult(interp, Tcl_NewIntObj(kvwg.nSeek));
  return TCL_OK;
}

static int kvwrap_step_cmd(Tcl_Interp *interp, int objc, Tcl_Obj **objv){
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 2, objv, "");
    return TCL_ERROR;
  }

  Tcl_SetObjResult(interp, Tcl_NewIntObj(kvwg.nStep));
  return TCL_OK;
}

static int kvwrap_reset_cmd(Tcl_Interp *interp, int objc, Tcl_Obj **objv){
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 2, objv, "");
    return TCL_ERROR;
  }

  kvwg.nStep = 0;
  kvwg.nSeek = 0;

  Tcl_ResetResult(interp);
  return TCL_OK;
}


/*
** TCLCMD:    kvwrap SUB-COMMAND
*/
static int kvwrap_command(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  struct Subcmd {
    const char *zCmd;
    int (*xCmd)(Tcl_Interp *, int, Tcl_Obj **);
  } aSub[] = {
    { "install", kvwrap_install_cmd },
    { "step",    kvwrap_step_cmd },
    { "seek",    kvwrap_seek_cmd },
    { "reset",   kvwrap_reset_cmd },
  };
  int iSub;
  int rc;

  rc = Tcl_GetIndexFromObjStruct(
      interp, objv[1], aSub, sizeof(aSub[0]), "sub-command", 0, &iSub
  );
  if( rc==TCL_OK ){
    rc = aSub[iSub].xCmd(interp, objc, (Tcl_Obj **)objv); 
  }

  return rc;
}

/*
** Register the TCL commands defined above with the TCL interpreter.
**
** This routine should be the only externally visible symbol in this
** source code file.
*/
int Sqliteteststorage2_Init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp, "kvwrap", kvwrap_command, 0, 0);
  return TCL_OK;
}
