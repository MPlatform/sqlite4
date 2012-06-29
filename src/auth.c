/*
** 2003 January 11
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code used to implement the sqlite4_set_authorizer()
** API.  This facility is an optional feature of the library.  Embedded
** systems that do not need this facility may omit it by recompiling
** the library with -DSQLITE4_OMIT_AUTHORIZATION=1
*/
#include "sqliteInt.h"

/*
** All of the code in this file may be omitted by defining a single
** macro.
*/
#ifndef SQLITE4_OMIT_AUTHORIZATION

/*
** Set or clear the access authorization function.
**
** The access authorization function is be called during the compilation
** phase to verify that the user has read and/or write access permission on
** various fields of the database.  The first argument to the auth function
** is a copy of the 3rd argument to this routine.  The second argument
** to the auth function is one of these constants:
**
**       SQLITE4_CREATE_INDEX
**       SQLITE4_CREATE_TABLE
**       SQLITE4_CREATE_TEMP_INDEX
**       SQLITE4_CREATE_TEMP_TABLE
**       SQLITE4_CREATE_TEMP_TRIGGER
**       SQLITE4_CREATE_TEMP_VIEW
**       SQLITE4_CREATE_TRIGGER
**       SQLITE4_CREATE_VIEW
**       SQLITE4_DELETE
**       SQLITE4_DROP_INDEX
**       SQLITE4_DROP_TABLE
**       SQLITE4_DROP_TEMP_INDEX
**       SQLITE4_DROP_TEMP_TABLE
**       SQLITE4_DROP_TEMP_TRIGGER
**       SQLITE4_DROP_TEMP_VIEW
**       SQLITE4_DROP_TRIGGER
**       SQLITE4_DROP_VIEW
**       SQLITE4_INSERT
**       SQLITE4_PRAGMA
**       SQLITE4_READ
**       SQLITE4_SELECT
**       SQLITE4_TRANSACTION
**       SQLITE4_UPDATE
**
** The third and fourth arguments to the auth function are the name of
** the table and the column that are being accessed.  The auth function
** should return either SQLITE4_OK, SQLITE4_DENY, or SQLITE4_IGNORE.  If
** SQLITE4_OK is returned, it means that access is allowed.  SQLITE4_DENY
** means that the SQL statement will never-run - the sqlite4_exec() call
** will return with an error.  SQLITE4_IGNORE means that the SQL statement
** should run but attempts to read the specified column will return NULL
** and attempts to write the column will be ignored.
**
** Setting the auth function to NULL disables this hook.  The default
** setting of the auth function is NULL.
*/
int sqlite4_set_authorizer(
  sqlite4 *db,
  int (*xAuth)(void*,int,const char*,const char*,const char*,const char*),
  void *pArg
){
  sqlite4_mutex_enter(db->mutex);
  db->xAuth = xAuth;
  db->pAuthArg = pArg;
  sqlite4ExpirePreparedStatements(db);
  sqlite4_mutex_leave(db->mutex);
  return SQLITE4_OK;
}

/*
** Write an error message into pParse->zErrMsg that explains that the
** user-supplied authorization function returned an illegal value.
*/
static void sqliteAuthBadReturnCode(Parse *pParse){
  sqlite4ErrorMsg(pParse, "authorizer malfunction");
  pParse->rc = SQLITE4_ERROR;
}

/*
** Invoke the authorization callback for permission to read column zCol from
** table zTab in database zDb. This function assumes that an authorization
** callback has been registered (i.e. that sqlite4.xAuth is not NULL).
**
** If SQLITE4_IGNORE is returned and pExpr is not NULL, then pExpr is changed
** to an SQL NULL expression. Otherwise, if pExpr is NULL, then SQLITE4_IGNORE
** is treated as SQLITE4_DENY. In this case an error is left in pParse.
*/
int sqlite4AuthReadCol(
  Parse *pParse,                  /* The parser context */
  const char *zTab,               /* Table name */
  const char *zCol,               /* Column name */
  int iDb                         /* Index of containing database. */
){
  sqlite4 *db = pParse->db;       /* Database handle */
  char *zDb = db->aDb[iDb].zName; /* Name of attached database */
  int rc;                         /* Auth callback return code */

  rc = db->xAuth(db->pAuthArg, SQLITE4_READ, zTab,zCol,zDb,pParse->zAuthContext);
  if( rc==SQLITE4_DENY ){
    if( db->nDb>2 || iDb!=0 ){
      sqlite4ErrorMsg(pParse, "access to %s.%s.%s is prohibited",zDb,zTab,zCol);
    }else{
      sqlite4ErrorMsg(pParse, "access to %s.%s is prohibited", zTab, zCol);
    }
    pParse->rc = SQLITE4_AUTH;
  }else if( rc!=SQLITE4_IGNORE && rc!=SQLITE4_OK ){
    sqliteAuthBadReturnCode(pParse);
  }
  return rc;
}

/*
** The pExpr should be a TK_COLUMN expression.  The table referred to
** is in pTabList or else it is the NEW or OLD table of a trigger.  
** Check to see if it is OK to read this particular column.
**
** If the auth function returns SQLITE4_IGNORE, change the TK_COLUMN 
** instruction into a TK_NULL.  If the auth function returns SQLITE4_DENY,
** then generate an error.
*/
void sqlite4AuthRead(
  Parse *pParse,        /* The parser context */
  Expr *pExpr,          /* The expression to check authorization on */
  Schema *pSchema,      /* The schema of the expression */
  SrcList *pTabList     /* All table that pExpr might refer to */
){
  sqlite4 *db = pParse->db;
  Table *pTab = 0;      /* The table being read */
  const char *zCol;     /* Name of the column of the table */
  int iSrc;             /* Index in pTabList->a[] of table being read */
  int iDb;              /* The index of the database the expression refers to */
  int iCol;             /* Index of column in table */

  if( db->xAuth==0 ) return;
  iDb = sqlite4SchemaToIndex(pParse->db, pSchema);
  if( iDb<0 ){
    /* An attempt to read a column out of a subquery or other
    ** temporary table. */
    return;
  }

  assert( pExpr->op==TK_COLUMN || pExpr->op==TK_TRIGGER );
  if( pExpr->op==TK_TRIGGER ){
    pTab = pParse->pTriggerTab;
  }else{
    assert( pTabList );
    for(iSrc=0; ALWAYS(iSrc<pTabList->nSrc); iSrc++){
      if( pExpr->iTable==pTabList->a[iSrc].iCursor ){
        pTab = pTabList->a[iSrc].pTab;
        break;
      }
    }
  }
  iCol = pExpr->iColumn;
  if( NEVER(pTab==0) ) return;

  if( iCol>=0 ){
    assert( iCol<pTab->nCol );
    zCol = pTab->aCol[iCol].zName;
  }else{
    zCol = "ROWID";
  }
  assert( iDb>=0 && iDb<db->nDb );
  if( SQLITE4_IGNORE==sqlite4AuthReadCol(pParse, pTab->zName, zCol, iDb) ){
    pExpr->op = TK_NULL;
  }
}

/*
** Do an authorization check using the code and arguments given.  Return
** either SQLITE4_OK (zero) or SQLITE4_IGNORE or SQLITE4_DENY.  If SQLITE4_DENY
** is returned, then the error count and error message in pParse are
** modified appropriately.
*/
int sqlite4AuthCheck(
  Parse *pParse,
  int code,
  const char *zArg1,
  const char *zArg2,
  const char *zArg3
){
  sqlite4 *db = pParse->db;
  int rc;

  /* Don't do any authorization checks if the database is initialising
  ** or if the parser is being invoked from within sqlite4_declare_vtab.
  */
  if( db->init.busy || IN_DECLARE_VTAB ){
    return SQLITE4_OK;
  }

  if( db->xAuth==0 ){
    return SQLITE4_OK;
  }
  rc = db->xAuth(db->pAuthArg, code, zArg1, zArg2, zArg3, pParse->zAuthContext);
  if( rc==SQLITE4_DENY ){
    sqlite4ErrorMsg(pParse, "not authorized");
    pParse->rc = SQLITE4_AUTH;
  }else if( rc!=SQLITE4_OK && rc!=SQLITE4_IGNORE ){
    rc = SQLITE4_DENY;
    sqliteAuthBadReturnCode(pParse);
  }
  return rc;
}

/*
** Push an authorization context.  After this routine is called, the
** zArg3 argument to authorization callbacks will be zContext until
** popped.  Or if pParse==0, this routine is a no-op.
*/
void sqlite4AuthContextPush(
  Parse *pParse,
  AuthContext *pContext, 
  const char *zContext
){
  assert( pParse );
  pContext->pParse = pParse;
  pContext->zAuthContext = pParse->zAuthContext;
  pParse->zAuthContext = zContext;
}

/*
** Pop an authorization context that was previously pushed
** by sqlite4AuthContextPush
*/
void sqlite4AuthContextPop(AuthContext *pContext){
  if( pContext->pParse ){
    pContext->pParse->zAuthContext = pContext->zAuthContext;
    pContext->pParse = 0;
  }
}

#endif /* SQLITE4_OMIT_AUTHORIZATION */
