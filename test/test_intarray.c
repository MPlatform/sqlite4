/*
** 2009 November 10
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
** This file implements a read-only VIRTUAL TABLE that contains the
** content of a C-language array of integer values.  See the corresponding
** header file for full details.
*/
#include "test_intarray.h"
#include <string.h>
#include <assert.h>


/*
** Definition of the sqlite4_intarray object.
**
** The internal representation of an intarray object is subject
** to change, is not externally visible, and should be used by
** the implementation of intarray only.  This object is opaque
** to users.
*/
struct sqlite4_intarray {
  int n;                    /* Number of elements in the array */
  sqlite4_int64 *a;         /* Contents of the array */
  void (*xFree)(void*);     /* Function used to free a[] */
};

/* Objects used internally by the virtual table implementation */
typedef struct intarray_vtab intarray_vtab;
typedef struct intarray_cursor intarray_cursor;

/* A intarray table object */
struct intarray_vtab {
  sqlite4_vtab base;            /* Base class */
  sqlite4_intarray *pContent;   /* Content of the integer array */
};

/* A intarray cursor object */
struct intarray_cursor {
  sqlite4_vtab_cursor base;    /* Base class */
  int i;                       /* Current cursor position */
};

/*
** None of this works unless we have virtual tables.
*/
#ifndef SQLITE_OMIT_VIRTUALTABLE

/*
** Free an sqlite4_intarray object.
*/
static void intarrayFree(sqlite4_intarray *p){
  if( p->xFree ){
    p->xFree(p->a);
  }
  sqlite4_free(p);
}

/*
** Table destructor for the intarray module.
*/
static int intarrayDestroy(sqlite4_vtab *p){
  intarray_vtab *pVtab = (intarray_vtab*)p;
  sqlite4_free(pVtab);
  return 0;
}

/*
** Table constructor for the intarray module.
*/
static int intarrayCreate(
  sqlite4 *db,              /* Database where module is created */
  void *pAux,               /* clientdata for the module */
  int argc,                 /* Number of arguments */
  const char *const*argv,   /* Value for all arguments */
  sqlite4_vtab **ppVtab,    /* Write the new virtual table object here */
  char **pzErr              /* Put error message text here */
){
  int rc = SQLITE_NOMEM;
  intarray_vtab *pVtab = sqlite4_malloc(sizeof(intarray_vtab));

  if( pVtab ){
    memset(pVtab, 0, sizeof(intarray_vtab));
    pVtab->pContent = (sqlite4_intarray*)pAux;
    rc = sqlite4_declare_vtab(db, "CREATE TABLE x(value INTEGER PRIMARY KEY)");
  }
  *ppVtab = (sqlite4_vtab *)pVtab;
  return rc;
}

/*
** Open a new cursor on the intarray table.
*/
static int intarrayOpen(sqlite4_vtab *pVTab, sqlite4_vtab_cursor **ppCursor){
  int rc = SQLITE_NOMEM;
  intarray_cursor *pCur;
  pCur = sqlite4_malloc(sizeof(intarray_cursor));
  if( pCur ){
    memset(pCur, 0, sizeof(intarray_cursor));
    *ppCursor = (sqlite4_vtab_cursor *)pCur;
    rc = SQLITE_OK;
  }
  return rc;
}

/*
** Close a intarray table cursor.
*/
static int intarrayClose(sqlite4_vtab_cursor *cur){
  intarray_cursor *pCur = (intarray_cursor *)cur;
  sqlite4_free(pCur);
  return SQLITE_OK;
}

/*
** Retrieve a column of data.
*/
static int intarrayColumn(sqlite4_vtab_cursor *cur, sqlite4_context *ctx, int i){
  intarray_cursor *pCur = (intarray_cursor*)cur;
  intarray_vtab *pVtab = (intarray_vtab*)cur->pVtab;
  if( pCur->i>=0 && pCur->i<pVtab->pContent->n ){
    sqlite4_result_int64(ctx, pVtab->pContent->a[pCur->i]);
  }
  return SQLITE_OK;
}

/*
** Retrieve the current rowid.
*/
static int intarrayRowid(sqlite4_vtab_cursor *cur, sqlite_int64 *pRowid){
  intarray_cursor *pCur = (intarray_cursor *)cur;
  *pRowid = pCur->i;
  return SQLITE_OK;
}

static int intarrayEof(sqlite4_vtab_cursor *cur){
  intarray_cursor *pCur = (intarray_cursor *)cur;
  intarray_vtab *pVtab = (intarray_vtab *)cur->pVtab;
  return pCur->i>=pVtab->pContent->n;
}

/*
** Advance the cursor to the next row.
*/
static int intarrayNext(sqlite4_vtab_cursor *cur){
  intarray_cursor *pCur = (intarray_cursor *)cur;
  pCur->i++;
  return SQLITE_OK;
}

/*
** Reset a intarray table cursor.
*/
static int intarrayFilter(
  sqlite4_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite4_value **argv
){
  intarray_cursor *pCur = (intarray_cursor *)pVtabCursor;
  pCur->i = 0;
  return SQLITE_OK;
}

/*
** Analyse the WHERE condition.
*/
static int intarrayBestIndex(sqlite4_vtab *tab, sqlite4_index_info *pIdxInfo){
  return SQLITE_OK;
}

/*
** A virtual table module that merely echos method calls into TCL
** variables.
*/
static sqlite4_module intarrayModule = {
  0,                           /* iVersion */
  intarrayCreate,              /* xCreate - create a new virtual table */
  intarrayCreate,              /* xConnect - connect to an existing vtab */
  intarrayBestIndex,           /* xBestIndex - find the best query index */
  intarrayDestroy,             /* xDisconnect - disconnect a vtab */
  intarrayDestroy,             /* xDestroy - destroy a vtab */
  intarrayOpen,                /* xOpen - open a cursor */
  intarrayClose,               /* xClose - close a cursor */
  intarrayFilter,              /* xFilter - configure scan constraints */
  intarrayNext,                /* xNext - advance a cursor */
  intarrayEof,                 /* xEof */
  intarrayColumn,              /* xColumn - read data */
  intarrayRowid,               /* xRowid - read data */
  0,                           /* xUpdate */
  0,                           /* xBegin */
  0,                           /* xSync */
  0,                           /* xCommit */
  0,                           /* xRollback */
  0,                           /* xFindMethod */
  0,                           /* xRename */
};

#endif /* !defined(SQLITE_OMIT_VIRTUALTABLE) */

/*
** Invoke this routine to create a specific instance of an intarray object.
** The new intarray object is returned by the 3rd parameter.
**
** Each intarray object corresponds to a virtual table in the TEMP table
** with a name of zName.
**
** Destroy the intarray object by dropping the virtual table.  If not done
** explicitly by the application, the virtual table will be dropped implicitly
** by the system when the database connection is closed.
*/
int sqlite4_intarray_create(
  sqlite4 *db,
  const char *zName,
  sqlite4_intarray **ppReturn
){
  int rc = SQLITE_OK;
#ifndef SQLITE_OMIT_VIRTUALTABLE
  sqlite4_intarray *p;

  *ppReturn = p = sqlite4_malloc( sizeof(*p) );
  if( p==0 ){
    return SQLITE_NOMEM;
  }
  memset(p, 0, sizeof(*p));
  rc = sqlite4_create_module_v2(db, zName, &intarrayModule, p,
                                (void(*)(void*))intarrayFree);
  if( rc==SQLITE_OK ){
    char *zSql;
    zSql = sqlite4_mprintf("CREATE VIRTUAL TABLE temp.%Q USING %Q",
                           zName, zName);
    rc = sqlite4_exec(db, zSql, 0, 0, 0);
    sqlite4_free(zSql);
  }
#endif
  return rc;
}

/*
** Bind a new array array of integers to a specific intarray object.
**
** The array of integers bound must be unchanged for the duration of
** any query against the corresponding virtual table.  If the integer
** array does change or is deallocated undefined behavior will result.
*/
int sqlite4_intarray_bind(
  sqlite4_intarray *pIntArray,   /* The intarray object to bind to */
  int nElements,                 /* Number of elements in the intarray */
  sqlite4_int64 *aElements,      /* Content of the intarray */
  void (*xFree)(void*)           /* How to dispose of the intarray when done */
){
  if( pIntArray->xFree ){
    pIntArray->xFree(pIntArray->a);
  }
  pIntArray->n = nElements;
  pIntArray->a = aElements;
  pIntArray->xFree = xFree;
  return SQLITE_OK;
}


/*****************************************************************************
** Everything below is interface for testing this module.
*/
#ifdef SQLITE_TEST
#include <tcl.h>

/*
** Routines to encode and decode pointers
*/
extern int getDbPointer(Tcl_Interp *interp, const char *zA, sqlite4 **ppDb);
extern void *sqlite4TestTextToPtr(const char*);
extern int sqlite4TestMakePointerStr(Tcl_Interp*, char *zPtr, void*);
extern const char *sqlite4TestErrorName(int);

/*
**    sqlite4_intarray_create  DB  NAME
**
** Invoke the sqlite4_intarray_create interface.  A string that becomes
** the first parameter to sqlite4_intarray_bind.
*/
static int test_intarray_create(
  ClientData clientData, /* Not used */
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int objc,              /* Number of arguments */
  Tcl_Obj *CONST objv[]  /* Command arguments */
){
  sqlite4 *db;
  const char *zName;
  sqlite4_intarray *pArray;
  int rc = SQLITE_OK;
  char zPtr[100];

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB");
    return TCL_ERROR;
  }
  if( getDbPointer(interp, Tcl_GetString(objv[1]), &db) ) return TCL_ERROR;
  zName = Tcl_GetString(objv[2]);
#ifndef SQLITE_OMIT_VIRTUALTABLE
  rc = sqlite4_intarray_create(db, zName, &pArray);
#endif
  if( rc!=SQLITE_OK ){
    assert( pArray==0 );
    Tcl_AppendResult(interp, sqlite4TestErrorName(rc), (char*)0);
    return TCL_ERROR;
  }
  sqlite4TestMakePointerStr(interp, zPtr, pArray);
  Tcl_AppendResult(interp, zPtr, (char*)0);
  return TCL_OK;
}

/*
**    sqlite4_intarray_bind  INTARRAY  ?VALUE ...?
**
** Invoke the sqlite4_intarray_bind interface on the given array of integers.
*/
static int test_intarray_bind(
  ClientData clientData, /* Not used */
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int objc,              /* Number of arguments */
  Tcl_Obj *CONST objv[]  /* Command arguments */
){
  sqlite4_intarray *pArray;
  int rc = SQLITE_OK;
  int i, n;
  sqlite4_int64 *a;

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "INTARRAY");
    return TCL_ERROR;
  }
  pArray = (sqlite4_intarray*)sqlite4TestTextToPtr(Tcl_GetString(objv[1]));
  n = objc - 2;
#ifndef SQLITE_OMIT_VIRTUALTABLE
  a = sqlite4_malloc( sizeof(a[0])*n );
  if( a==0 ){
    Tcl_AppendResult(interp, "SQLITE_NOMEM", (char*)0);
    return TCL_ERROR;
  }
  for(i=0; i<n; i++){
    a[i] = 0;
    Tcl_GetWideIntFromObj(0, objv[i+2], &a[i]);
  }
  rc = sqlite4_intarray_bind(pArray, n, a, sqlite4_free);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, sqlite4TestErrorName(rc), (char*)0);
    return TCL_ERROR;
  }
#endif
  return TCL_OK;
}

/*
** Register commands with the TCL interpreter.
*/
int Sqlitetestintarray_Init(Tcl_Interp *interp){
  static struct {
     char *zName;
     Tcl_ObjCmdProc *xProc;
     void *clientData;
  } aObjCmd[] = {
     { "sqlite4_intarray_create", test_intarray_create, 0 },
     { "sqlite4_intarray_bind", test_intarray_bind, 0 },
  };
  int i;
  for(i=0; i<sizeof(aObjCmd)/sizeof(aObjCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aObjCmd[i].zName, 
        aObjCmd[i].xProc, aObjCmd[i].clientData, 0);
  }
  return TCL_OK;
}

#endif /* SQLITE_TEST */
