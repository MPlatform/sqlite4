/*
** 2006 June 10
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Code for testing the virtual table interfaces.  This code
** is not included in the SQLite library.  It is used for automated
** testing of the SQLite library.
*/

/* The code in this file defines a sqlite4 virtual-table module that
** provides a read-only view of the current database schema. There is one
** row in the schema table for each column in the database schema.
*/
#define SCHEMA \
"CREATE TABLE x("                                                            \
  "database,"          /* Name of database (i.e. main, temp etc.) */         \
  "tablename,"         /* Name of table */                                   \
  "cid,"               /* Column number (from left-to-right, 0 upward) */    \
  "name,"              /* Column name */                                     \
  "type,"              /* Specified type (i.e. VARCHAR(32)) */               \
  "not_null,"          /* Boolean. True if NOT NULL was specified */         \
  "dflt_value,"        /* Default value for this column */                   \
  "pk"                 /* True if this column is part of the primary key */  \
")"

/* If SQLITE_TEST is defined this code is preprocessed for use as part
** of the sqlite test binary "testfixture". Otherwise it is preprocessed
** to be compiled into an sqlite dynamic extension.
*/
#ifdef SQLITE_TEST
  #include "sqliteInt.h"
  #include "tcl.h"
#else
  #include "sqlite4ext.h"
  SQLITE_EXTENSION_INIT1
#endif

#include <stdlib.h>
#include <string.h>
#include <assert.h>

typedef struct schema_vtab schema_vtab;
typedef struct schema_cursor schema_cursor;

/* A schema table object */
struct schema_vtab {
  sqlite4_vtab base;
  sqlite4 *db;
};

/* A schema table cursor object */
struct schema_cursor {
  sqlite4_vtab_cursor base;
  sqlite4_stmt *pDbList;
  sqlite4_stmt *pTableList;
  sqlite4_stmt *pColumnList;
  int rowid;
};

/*
** None of this works unless we have virtual tables.
*/
#ifndef SQLITE_OMIT_VIRTUALTABLE

/*
** Table destructor for the schema module.
*/
static int schemaDestroy(sqlite4_vtab *pVtab){
  sqlite4_free(pVtab);
  return 0;
}

/*
** Table constructor for the schema module.
*/
static int schemaCreate(
  sqlite4 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite4_vtab **ppVtab,
  char **pzErr
){
  int rc = SQLITE_NOMEM;
  schema_vtab *pVtab = sqlite4_malloc(sizeof(schema_vtab));
  if( pVtab ){
    memset(pVtab, 0, sizeof(schema_vtab));
    pVtab->db = db;
#ifndef SQLITE_OMIT_VIRTUALTABLE
    rc = sqlite4_declare_vtab(db, SCHEMA);
#endif
  }
  *ppVtab = (sqlite4_vtab *)pVtab;
  return rc;
}

/*
** Open a new cursor on the schema table.
*/
static int schemaOpen(sqlite4_vtab *pVTab, sqlite4_vtab_cursor **ppCursor){
  int rc = SQLITE_NOMEM;
  schema_cursor *pCur;
  pCur = sqlite4_malloc(sizeof(schema_cursor));
  if( pCur ){
    memset(pCur, 0, sizeof(schema_cursor));
    *ppCursor = (sqlite4_vtab_cursor *)pCur;
    rc = SQLITE_OK;
  }
  return rc;
}

/*
** Close a schema table cursor.
*/
static int schemaClose(sqlite4_vtab_cursor *cur){
  schema_cursor *pCur = (schema_cursor *)cur;
  sqlite4_finalize(pCur->pDbList);
  sqlite4_finalize(pCur->pTableList);
  sqlite4_finalize(pCur->pColumnList);
  sqlite4_free(pCur);
  return SQLITE_OK;
}

/*
** Retrieve a column of data.
*/
static int schemaColumn(sqlite4_vtab_cursor *cur, sqlite4_context *ctx, int i){
  schema_cursor *pCur = (schema_cursor *)cur;
  switch( i ){
    case 0:
      sqlite4_result_value(ctx, sqlite4_column_value(pCur->pDbList, 1));
      break;
    case 1:
      sqlite4_result_value(ctx, sqlite4_column_value(pCur->pTableList, 0));
      break;
    default:
      sqlite4_result_value(ctx, sqlite4_column_value(pCur->pColumnList, i-2));
      break;
  }
  return SQLITE_OK;
}

/*
** Retrieve the current rowid.
*/
static int schemaRowid(sqlite4_vtab_cursor *cur, sqlite_int64 *pRowid){
  schema_cursor *pCur = (schema_cursor *)cur;
  *pRowid = pCur->rowid;
  return SQLITE_OK;
}

static int finalize(sqlite4_stmt **ppStmt){
  int rc = sqlite4_finalize(*ppStmt);
  *ppStmt = 0;
  return rc;
}

static int schemaEof(sqlite4_vtab_cursor *cur){
  schema_cursor *pCur = (schema_cursor *)cur;
  return (pCur->pDbList ? 0 : 1);
}

/*
** Advance the cursor to the next row.
*/
static int schemaNext(sqlite4_vtab_cursor *cur){
  int rc = SQLITE_OK;
  schema_cursor *pCur = (schema_cursor *)cur;
  schema_vtab *pVtab = (schema_vtab *)(cur->pVtab);
  char *zSql = 0;

  while( !pCur->pColumnList || SQLITE_ROW!=sqlite4_step(pCur->pColumnList) ){
    if( SQLITE_OK!=(rc = finalize(&pCur->pColumnList)) ) goto next_exit;

    while( !pCur->pTableList || SQLITE_ROW!=sqlite4_step(pCur->pTableList) ){
      if( SQLITE_OK!=(rc = finalize(&pCur->pTableList)) ) goto next_exit;

      assert(pCur->pDbList);
      while( SQLITE_ROW!=sqlite4_step(pCur->pDbList) ){
        rc = finalize(&pCur->pDbList);
        goto next_exit;
      }

      /* Set zSql to the SQL to pull the list of tables from the 
      ** sqlite_master (or sqlite_temp_master) table of the database
      ** identfied by the row pointed to by the SQL statement pCur->pDbList
      ** (iterating through a "PRAGMA database_list;" statement).
      */
      if( sqlite4_column_int(pCur->pDbList, 0)==1 ){
        zSql = sqlite4_mprintf(
            "SELECT name FROM sqlite_temp_master WHERE type='table'"
        );
      }else{
        sqlite4_stmt *pDbList = pCur->pDbList;
        zSql = sqlite4_mprintf(
            "SELECT name FROM %Q.sqlite_master WHERE type='table'",
             sqlite4_column_text(pDbList, 1)
        );
      }
      if( !zSql ){
        rc = SQLITE_NOMEM;
        goto next_exit;
      }

      rc = sqlite4_prepare(pVtab->db, zSql, -1, &pCur->pTableList, 0);
      sqlite4_free(zSql);
      if( rc!=SQLITE_OK ) goto next_exit;
    }

    /* Set zSql to the SQL to the table_info pragma for the table currently
    ** identified by the rows pointed to by statements pCur->pDbList and
    ** pCur->pTableList.
    */
    zSql = sqlite4_mprintf("PRAGMA %Q.table_info(%Q)", 
        sqlite4_column_text(pCur->pDbList, 1),
        sqlite4_column_text(pCur->pTableList, 0)
    );

    if( !zSql ){
      rc = SQLITE_NOMEM;
      goto next_exit;
    }
    rc = sqlite4_prepare(pVtab->db, zSql, -1, &pCur->pColumnList, 0);
    sqlite4_free(zSql);
    if( rc!=SQLITE_OK ) goto next_exit;
  }
  pCur->rowid++;

next_exit:
  /* TODO: Handle rc */
  return rc;
}

/*
** Reset a schema table cursor.
*/
static int schemaFilter(
  sqlite4_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite4_value **argv
){
  int rc;
  schema_vtab *pVtab = (schema_vtab *)(pVtabCursor->pVtab);
  schema_cursor *pCur = (schema_cursor *)pVtabCursor;
  pCur->rowid = 0;
  finalize(&pCur->pTableList);
  finalize(&pCur->pColumnList);
  finalize(&pCur->pDbList);
  rc = sqlite4_prepare(pVtab->db,"PRAGMA database_list", -1, &pCur->pDbList, 0);
  return (rc==SQLITE_OK ? schemaNext(pVtabCursor) : rc);
}

/*
** Analyse the WHERE condition.
*/
static int schemaBestIndex(sqlite4_vtab *tab, sqlite4_index_info *pIdxInfo){
  return SQLITE_OK;
}

/*
** A virtual table module that merely echos method calls into TCL
** variables.
*/
static sqlite4_module schemaModule = {
  0,                           /* iVersion */
  schemaCreate,
  schemaCreate,
  schemaBestIndex,
  schemaDestroy,
  schemaDestroy,
  schemaOpen,                  /* xOpen - open a cursor */
  schemaClose,                 /* xClose - close a cursor */
  schemaFilter,                /* xFilter - configure scan constraints */
  schemaNext,                  /* xNext - advance a cursor */
  schemaEof,                   /* xEof */
  schemaColumn,                /* xColumn - read data */
  schemaRowid,                 /* xRowid - read data */
  0,                           /* xUpdate */
  0,                           /* xBegin */
  0,                           /* xSync */
  0,                           /* xCommit */
  0,                           /* xRollback */
  0,                           /* xFindMethod */
  0,                           /* xRename */
};

#endif /* !defined(SQLITE_OMIT_VIRTUALTABLE) */

#ifdef SQLITE_TEST

/*
** Decode a pointer to an sqlite4 object.
*/
extern int getDbPointer(Tcl_Interp *interp, const char *zA, sqlite4 **ppDb);

/*
** Register the schema virtual table module.
*/
static int register_schema_module(
  ClientData clientData, /* Not used */
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int objc,              /* Number of arguments */
  Tcl_Obj *CONST objv[]  /* Command arguments */
){
  sqlite4 *db;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB");
    return TCL_ERROR;
  }
  if( getDbPointer(interp, Tcl_GetString(objv[1]), &db) ) return TCL_ERROR;
#ifndef SQLITE_OMIT_VIRTUALTABLE
  sqlite4_create_module(db, "schema", &schemaModule, 0);
#endif
  return TCL_OK;
}

/*
** Register commands with the TCL interpreter.
*/
int Sqlitetestschema_Init(Tcl_Interp *interp){
  static struct {
     char *zName;
     Tcl_ObjCmdProc *xProc;
     void *clientData;
  } aObjCmd[] = {
     { "register_schema_module", register_schema_module, 0 },
  };
  int i;
  for(i=0; i<sizeof(aObjCmd)/sizeof(aObjCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aObjCmd[i].zName, 
        aObjCmd[i].xProc, aObjCmd[i].clientData, 0);
  }
  return TCL_OK;
}

#else

/*
** Extension load function.
*/
int sqlite4_extension_init(
  sqlite4 *db, 
  char **pzErrMsg, 
  const sqlite4_api_routines *pApi
){
  SQLITE_EXTENSION_INIT2(pApi);
#ifndef SQLITE_OMIT_VIRTUALTABLE
  sqlite4_create_module(db, "schema", &schemaModule, 0);
#endif
  return 0;
}

#endif
