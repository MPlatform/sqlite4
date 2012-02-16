/*
** 2005 May 25
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains the implementation of the sqlite4_prepare()
** interface, and routines that contribute to loading the database schema
** from disk.
*/
#include "sqliteInt.h"

/*
** Fill the InitData structure with an error message that indicates
** that the database is corrupt.
*/
static void corruptSchema(
  InitData *pData,     /* Initialization context */
  const char *zObj,    /* Object being parsed at the point of error */
  const char *zExtra   /* Error information */
){
  sqlite4 *db = pData->db;
  if( !db->mallocFailed && (db->flags & SQLITE_RecoveryMode)==0 ){
    if( zObj==0 ) zObj = "?";
    sqlite4SetString(pData->pzErrMsg, db,
      "malformed database schema (%s)", zObj);
    if( zExtra ){
      *pData->pzErrMsg = sqlite4MAppendf(db, *pData->pzErrMsg, 
                                 "%s - %s", *pData->pzErrMsg, zExtra);
    }
  }
  pData->rc = db->mallocFailed ? SQLITE_NOMEM : SQLITE_CORRUPT_BKPT;
}

/*
** This is the callback routine for the code that initializes the
** database.  See sqlite4Init() below for additional information.
** This routine is also called from the OP_ParseSchema opcode of the VDBE.
**
** Each callback contains the following information:
**
**     argv[0] = name of thing being created
**     argv[1] = root page number for table or index. 0 for trigger or view.
**     argv[2] = SQL text for the CREATE statement.
**
*/
int sqlite4InitCallback(void *pInit, int argc, char **argv, char **NotUsed){
  InitData *pData = (InitData*)pInit;
  sqlite4 *db = pData->db;
  int iDb = pData->iDb;

  assert( argc==3 );
  UNUSED_PARAMETER2(NotUsed, argc);
  assert( sqlite4_mutex_held(db->mutex) );
  DbClearProperty(db, iDb, DB_Empty);
  if( db->mallocFailed ){
    corruptSchema(pData, argv[0], 0);
    return 1;
  }

  assert( iDb>=0 && iDb<db->nDb );
  if( argv==0 ) return 0;   /* Might happen if EMPTY_RESULT_CALLBACKS are on */
  if( argv[1]==0 ){
    corruptSchema(pData, argv[0], 0);
  }else if( argv[2] && argv[2][0] ){
    /* Call the parser to process a CREATE TABLE, INDEX or VIEW.
    ** But because db->init.busy is set to 1, no VDBE code is generated
    ** or executed.  All the parser does is build the internal data
    ** structures that describe the table, index, or view.
    */
    int rc;
    sqlite4_stmt *pStmt;
    TESTONLY(int rcp);            /* Return code from sqlite4_prepare() */

    assert( db->init.busy );
    db->init.iDb = iDb;
    db->init.newTnum = sqlite4Atoi(argv[1]);
    db->init.orphanTrigger = 0;
    TESTONLY(rcp = ) sqlite4_prepare(db, argv[2], -1, &pStmt, 0);
    rc = db->errCode;
    assert( (rc&0xFF)==(rcp&0xFF) );
    db->init.iDb = 0;
    if( SQLITE_OK!=rc ){
      if( db->init.orphanTrigger ){
        assert( iDb==1 );
      }else{
        pData->rc = rc;
        if( rc==SQLITE_NOMEM ){
          db->mallocFailed = 1;
        }else if( rc!=SQLITE_INTERRUPT && (rc&0xFF)!=SQLITE_LOCKED ){
          corruptSchema(pData, argv[0], sqlite4_errmsg(db));
        }
      }
    }
    sqlite4_finalize(pStmt);
  }else if( argv[0]==0 ){
    corruptSchema(pData, 0, 0);
  }else{
    /* If the SQL column is blank it means this is an index that
    ** was created to be the PRIMARY KEY or to fulfill a UNIQUE
    ** constraint for a CREATE TABLE.  The index should have already
    ** been created when we processed the CREATE TABLE.  All we have
    ** to do here is record the root page number for that index.
    */
    Index *pIndex;
    pIndex = sqlite4FindIndex(db, argv[0], db->aDb[iDb].zName);
    if( pIndex==0 ){
      /* This can occur if there exists an index on a TEMP table which
      ** has the same name as another index on a permanent index.  Since
      ** the permanent table is hidden by the TEMP table, we can also
      ** safely ignore the index on the permanent table.
      */
      /* Do Nothing */;
    }else if( sqlite4GetInt32(argv[1], &pIndex->tnum)==0 ){
      corruptSchema(pData, argv[0], "invalid rootpage");
    }
  }
  return 0;
}

/*
** Attempt to read the database schema and initialize internal
** data structures for a single database file.  The index of the
** database file is given by iDb.  iDb==0 is used for the main
** database.  iDb==1 should never be used.  iDb>=2 is used for
** auxiliary databases.  Return one of the SQLITE_ error codes to
** indicate success or failure.
*/
static int sqlite4InitOne(sqlite4 *db, int iDb, char **pzErrMsg){
  int rc;
  int i;
  int size;
  Table *pTab;
  Db *pDb;
  char const *azArg[4];
  int meta[5];
  InitData initData;
  char const *zMasterSchema;
  char const *zMasterName;
  int openedTransaction = 0;

  /*
  ** The master database table has a structure like this
  */
  static const char master_schema[] = 
     "CREATE TABLE sqlite_master(\n"
     "  type text,\n"
     "  name text,\n"
     "  tbl_name text,\n"
     "  rootpage integer,\n"
     "  sql text\n"
     ")"
  ;
#ifndef SQLITE_OMIT_TEMPDB
  static const char temp_master_schema[] = 
     "CREATE TEMP TABLE sqlite_temp_master(\n"
     "  type text,\n"
     "  name text,\n"
     "  tbl_name text,\n"
     "  rootpage integer,\n"
     "  sql text\n"
     ")"
  ;
#else
  #define temp_master_schema 0
#endif

  assert( iDb>=0 && iDb<db->nDb );
  assert( db->aDb[iDb].pSchema );
  assert( sqlite4_mutex_held(db->mutex) );

  /* zMasterSchema and zInitScript are set to point at the master schema
  ** and initialisation script appropriate for the database being
  ** initialised. zMasterName is the name of the master table.
  */
  if( !OMIT_TEMPDB && iDb==1 ){
    zMasterSchema = temp_master_schema;
  }else{
    zMasterSchema = master_schema;
  }
  zMasterName = SCHEMA_TABLE(iDb);

  /* Construct the schema tables.  */
  azArg[0] = zMasterName;
  azArg[1] = "1";
  azArg[2] = zMasterSchema;
  azArg[3] = 0;
  initData.db = db;
  initData.iDb = iDb;
  initData.rc = SQLITE_OK;
  initData.pzErrMsg = pzErrMsg;
  sqlite4InitCallback(&initData, 3, (char **)azArg, 0);
  if( initData.rc ){
    rc = initData.rc;
    goto error_out;
  }
  pTab = sqlite4FindTable(db, zMasterName, db->aDb[iDb].zName);
  if( ALWAYS(pTab) ){
    pTab->tabFlags |= TF_Readonly;
  }

  /* Create a cursor to hold the database open
  */
  pDb = &db->aDb[iDb];
  if( pDb->pKV==0 ){
    if( !OMIT_TEMPDB && ALWAYS(iDb==1) ){
      DbSetProperty(db, 1, DB_SchemaLoaded);
    }
    return SQLITE_OK;
  }

  /* If there is not already a read-only (or read-write) transaction opened
  ** on the database, open one now. If a transaction is opened, it 
  ** will be closed before this function returns.  */
  if( pDb->pKV->iTransLevel==0 ){
    rc = sqlite4KVStoreBegin(pDb->pKV, 1);
    if( rc!=SQLITE_OK ){
      sqlite4SetString(pzErrMsg, db, "%s", sqlite4ErrStr(rc));
      goto initone_error_out;
    }
    openedTransaction = 1;
  }

  /* Get the database meta information.
  **
  ** Meta values are as follows:
  **    meta[0]   Schema cookie.  Changes with each schema change.
  **    meta[1]   File format of schema layer.
  **    meta[2]   unused
  **    meta[3]   unused
  **    meta[4]   Db text encoding. 1:UTF-8 2:UTF-16LE 3:UTF-16BE
  **    meta[5]   User version
  **    meta[6]   unused
  **    meta[7]   unused
  **    meta[8]   unused
  **    meta[9]   unused
  **
  ** Note: The #defined SQLITE_UTF* symbols in sqliteInt.h correspond to
  ** the possible values of meta[4].
  */
  for(i=0; i<ArraySize(meta); i++){
    sqlite4KVStoreGetMeta(pDb->pKV, i+1, (u32 *)&meta[i]);
  }
  pDb->pSchema->schema_cookie = meta[0];

  /* Read the schema information out of the schema tables
  */
  assert( db->init.busy );
  {
    char *zSql;
    zSql = sqlite4MPrintf(db, 
        "SELECT name, rootpage, sql FROM '%q'.%s ORDER BY rowid",
        db->aDb[iDb].zName, zMasterName);
#ifndef SQLITE_OMIT_AUTHORIZATION
    {
      int (*xAuth)(void*,int,const char*,const char*,const char*,const char*);
      xAuth = db->xAuth;
      db->xAuth = 0;
#endif
      rc = sqlite4_exec(db, zSql, sqlite4InitCallback, &initData, 0);
#ifndef SQLITE_OMIT_AUTHORIZATION
      db->xAuth = xAuth;
    }
#endif
    if( rc==SQLITE_OK ) rc = initData.rc;
    sqlite4DbFree(db, zSql);
#ifndef SQLITE_OMIT_ANALYZE
    if( rc==SQLITE_OK ){
      sqlite4AnalysisLoad(db, iDb);
    }
#endif
  }
  if( db->mallocFailed ){
    rc = SQLITE_NOMEM;
    sqlite4ResetInternalSchema(db, -1);
  }
  if( rc==SQLITE_OK || (db->flags&SQLITE_RecoveryMode)){
    /* Black magic: If the SQLITE_RecoveryMode flag is set, then consider
    ** the schema loaded, even if errors occurred. In this situation the 
    ** current sqlite4_prepare() operation will fail, but the following one
    ** will attempt to compile the supplied statement against whatever subset
    ** of the schema was loaded before the error occurred. The primary
    ** purpose of this is to allow access to the sqlite_master table
    ** even when its contents have been corrupted.
    */
    DbSetProperty(db, iDb, DB_SchemaLoaded);
    rc = SQLITE_OK;
  }

  /* Jump here for an error that occurs after successfully allocating
  ** curMain. For an error that occurs before that point, jump to error_out.
  */
initone_error_out:
  if( openedTransaction ){
    sqlite4KVStoreCommit(pDb->pKV, 0);
  }

error_out:
  if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ){
    db->mallocFailed = 1;
  }
  return rc;
}

/*
** Initialize all database files - the main database file, the file
** used to store temporary tables, and any additional database files
** created using ATTACH statements.  Return a success code.  If an
** error occurs, write an error message into *pzErrMsg.
**
** After a database is initialized, the DB_SchemaLoaded bit is set
** bit is set in the flags field of the Db structure. If the database
** file was of zero-length, then the DB_Empty flag is also set.
*/
int sqlite4Init(sqlite4 *db, char **pzErrMsg){
  int i, rc;
  int commit_internal = !(db->flags&SQLITE_InternChanges);
  
  assert( sqlite4_mutex_held(db->mutex) );
  rc = SQLITE_OK;
  db->init.busy = 1;
  for(i=0; rc==SQLITE_OK && i<db->nDb; i++){
    if( DbHasProperty(db, i, DB_SchemaLoaded) || i==1 ) continue;
    rc = sqlite4InitOne(db, i, pzErrMsg);
    if( rc ){
      sqlite4ResetInternalSchema(db, i);
    }
  }

  /* Once all the other databases have been initialised, load the schema
  ** for the TEMP database. This is loaded last, as the TEMP database
  ** schema may contain references to objects in other databases.
  */
#ifndef SQLITE_OMIT_TEMPDB
  if( rc==SQLITE_OK && ALWAYS(db->nDb>1)
                    && !DbHasProperty(db, 1, DB_SchemaLoaded) ){
    rc = sqlite4InitOne(db, 1, pzErrMsg);
    if( rc ){
      sqlite4ResetInternalSchema(db, 1);
    }
  }
#endif

  db->init.busy = 0;
  if( rc==SQLITE_OK && commit_internal ){
    sqlite4CommitInternalChanges(db);
  }

  return rc; 
}

/*
** This routine is a no-op if the database schema is already initialised.
** Otherwise, the schema is loaded. An error code is returned.
*/
int sqlite4ReadSchema(Parse *pParse){
  int rc = SQLITE_OK;
  sqlite4 *db = pParse->db;
  assert( sqlite4_mutex_held(db->mutex) );
  if( !db->init.busy ){
    rc = sqlite4Init(db, &pParse->zErrMsg);
  }
  if( rc!=SQLITE_OK ){
    pParse->rc = rc;
    pParse->nErr++;
  }
  return rc;
}


/*
** Check schema cookies in all databases.  If any cookie is out
** of date set pParse->rc to SQLITE_SCHEMA.  If all schema cookies
** make no changes to pParse->rc.
*/
static void schemaIsValid(Parse *pParse){
  sqlite4 *db = pParse->db;
  int iDb;
  int rc;
  int cookie;

  assert( pParse->checkSchema );
  assert( sqlite4_mutex_held(db->mutex) );
  for(iDb=0; iDb<db->nDb; iDb++){
    int openedTransaction = 0;         /* True if a transaction is opened */
    KVStore *pKV = db->aDb[iDb].pKV;   /* Database to read cookie from */
    if( pKV==0 ) continue;

    /* If there is not already a read-only (or read-write) transaction opened
    ** on the b-tree database, open one now. If a transaction is opened, it 
    ** will be closed immediately after reading the meta-value. */
    if( pKV->iTransLevel==0 ){
      rc = sqlite4KVStoreBegin(pKV, 1);
      if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ){
        db->mallocFailed = 1;
      }
      if( rc!=SQLITE_OK ) return;
      openedTransaction = 1;
    }

    /* Read the schema cookie from the database. If it does not match the 
    ** value stored as part of the in-memory schema representation,
    ** set Parse.rc to SQLITE_SCHEMA. */
    /* sqlite4KVStoreGetMeta(pBt, BTREE_SCHEMA_VERSION, (u32 *)&cookie); */
    if( cookie!=db->aDb[iDb].pSchema->schema_cookie ){
      sqlite4ResetInternalSchema(db, iDb);
      pParse->rc = SQLITE_SCHEMA;
    }

    /* Close the transaction, if one was opened. */
    if( openedTransaction ){
      sqlite4KVStoreCommit(pKV, 0);
    }
  }
}

/*
** Convert a schema pointer into the iDb index that indicates
** which database file in db->aDb[] the schema refers to.
**
** If the same database is attached more than once, the first
** attached database is returned.
*/
int sqlite4SchemaToIndex(sqlite4 *db, Schema *pSchema){
  int i = -1000000;

  /* If pSchema is NULL, then return -1000000. This happens when code in 
  ** expr.c is trying to resolve a reference to a transient table (i.e. one
  ** created by a sub-select). In this case the return value of this 
  ** function should never be used.
  **
  ** We return -1000000 instead of the more usual -1 simply because using
  ** -1000000 as the incorrect index into db->aDb[] is much 
  ** more likely to cause a segfault than -1 (of course there are assert()
  ** statements too, but it never hurts to play the odds).
  */
  assert( sqlite4_mutex_held(db->mutex) );
  if( pSchema ){
    for(i=0; ALWAYS(i<db->nDb); i++){
      if( db->aDb[i].pSchema==pSchema ){
        break;
      }
    }
    assert( i>=0 && i<db->nDb );
  }
  return i;
}

/*
** Compile the UTF-8 encoded SQL statement zSql into a statement handle.
*/
static int sqlite4Prepare(
  sqlite4 *db,              /* Database handle. */
  const char *zSql,         /* UTF-8 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  int saveSqlFlag,          /* True to copy SQL text into the sqlite4_stmt */
  Vdbe *pReprepare,         /* VM being reprepared */
  sqlite4_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const char **pzTail       /* OUT: End of parsed string */
){
  Parse *pParse;            /* Parsing context */
  char *zErrMsg = 0;        /* Error message */
  int rc = SQLITE_OK;       /* Result code */
  int i;                    /* Loop counter */

  /* Allocate the parsing context */
  pParse = sqlite4StackAllocZero(db, sizeof(*pParse));
  if( pParse==0 ){
    rc = SQLITE_NOMEM;
    goto end_prepare;
  }
  pParse->pReprepare = pReprepare;
  assert( ppStmt && *ppStmt==0 );
  assert( !db->mallocFailed );
  assert( sqlite4_mutex_held(db->mutex) );

  sqlite4VtabUnlockList(db);

  pParse->db = db;
  pParse->nQueryLoop = (double)1;
  if( nBytes>=0 && (nBytes==0 || zSql[nBytes-1]!=0) ){
    char *zSqlCopy;
    int mxLen = db->aLimit[SQLITE_LIMIT_SQL_LENGTH];
    testcase( nBytes==mxLen );
    testcase( nBytes==mxLen+1 );
    if( nBytes>mxLen ){
      sqlite4Error(db, SQLITE_TOOBIG, "statement too long");
      rc = sqlite4ApiExit(db, SQLITE_TOOBIG);
      goto end_prepare;
    }
    zSqlCopy = sqlite4DbStrNDup(db, zSql, nBytes);
    if( zSqlCopy ){
      sqlite4RunParser(pParse, zSqlCopy, &zErrMsg);
      sqlite4DbFree(db, zSqlCopy);
      pParse->zTail = &zSql[pParse->zTail-zSqlCopy];
    }else{
      pParse->zTail = &zSql[nBytes];
    }
  }else{
    sqlite4RunParser(pParse, zSql, &zErrMsg);
  }
  assert( 1==(int)pParse->nQueryLoop );

  if( db->mallocFailed ){
    pParse->rc = SQLITE_NOMEM;
  }
  if( pParse->rc==SQLITE_DONE ) pParse->rc = SQLITE_OK;
  if( pParse->checkSchema ){
    schemaIsValid(pParse);
  }
  if( db->mallocFailed ){
    pParse->rc = SQLITE_NOMEM;
  }
  if( pzTail ){
    *pzTail = pParse->zTail;
  }
  rc = pParse->rc;

#ifndef SQLITE_OMIT_EXPLAIN
  if( rc==SQLITE_OK && pParse->pVdbe && pParse->explain ){
    static const char * const azColName[] = {
       "addr", "opcode", "p1", "p2", "p3", "p4", "p5", "comment",
       "selectid", "order", "from", "detail"
    };
    int iFirst, mx;
    if( pParse->explain==2 ){
      sqlite4VdbeSetNumCols(pParse->pVdbe, 4);
      iFirst = 8;
      mx = 12;
    }else{
      sqlite4VdbeSetNumCols(pParse->pVdbe, 8);
      iFirst = 0;
      mx = 8;
    }
    for(i=iFirst; i<mx; i++){
      sqlite4VdbeSetColName(pParse->pVdbe, i-iFirst, COLNAME_NAME,
                            azColName[i], SQLITE_STATIC);
    }
  }
#endif

  assert( db->init.busy==0 || saveSqlFlag==0 );
  if( db->init.busy==0 ){
    Vdbe *pVdbe = pParse->pVdbe;
    sqlite4VdbeSetSql(pVdbe, zSql, (int)(pParse->zTail-zSql), saveSqlFlag);
  }
  if( pParse->pVdbe && (rc!=SQLITE_OK || db->mallocFailed) ){
    sqlite4VdbeFinalize(pParse->pVdbe);
    assert(!(*ppStmt));
  }else{
    *ppStmt = (sqlite4_stmt*)pParse->pVdbe;
  }

  if( zErrMsg ){
    sqlite4Error(db, rc, "%s", zErrMsg);
    sqlite4DbFree(db, zErrMsg);
  }else{
    sqlite4Error(db, rc, 0);
  }

  /* Delete any TriggerPrg structures allocated while parsing this statement. */
  while( pParse->pTriggerPrg ){
    TriggerPrg *pT = pParse->pTriggerPrg;
    pParse->pTriggerPrg = pT->pNext;
    sqlite4DbFree(db, pT);
  }

end_prepare:

  sqlite4StackFree(db, pParse);
  rc = sqlite4ApiExit(db, rc);
  assert( (rc&db->errMask)==rc );
  return rc;
}
static int sqlite4LockAndPrepare(
  sqlite4 *db,              /* Database handle. */
  const char *zSql,         /* UTF-8 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  int saveSqlFlag,          /* True to copy SQL text into the sqlite4_stmt */
  Vdbe *pOld,               /* VM being reprepared */
  sqlite4_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const char **pzTail       /* OUT: End of parsed string */
){
  int rc;
  assert( ppStmt!=0 );
  *ppStmt = 0;
  if( !sqlite4SafetyCheckOk(db) ){
    return SQLITE_MISUSE_BKPT;
  }
  sqlite4_mutex_enter(db->mutex);
  rc = sqlite4Prepare(db, zSql, nBytes, saveSqlFlag, pOld, ppStmt, pzTail);
  if( rc==SQLITE_SCHEMA ){
    sqlite4_finalize(*ppStmt);
    rc = sqlite4Prepare(db, zSql, nBytes, saveSqlFlag, pOld, ppStmt, pzTail);
  }
  sqlite4_mutex_leave(db->mutex);
  return rc;
}

/*
** Rerun the compilation of a statement after a schema change.
**
** If the statement is successfully recompiled, return SQLITE_OK. Otherwise,
** if the statement cannot be recompiled because another connection has
** locked the sqlite4_master table, return SQLITE_LOCKED. If any other error
** occurs, return SQLITE_SCHEMA.
*/
int sqlite4Reprepare(Vdbe *p){
  int rc;
  sqlite4_stmt *pNew;
  const char *zSql;
  sqlite4 *db;

  assert( sqlite4_mutex_held(sqlite4VdbeDb(p)->mutex) );
  zSql = sqlite4_sql((sqlite4_stmt *)p);
  assert( zSql!=0 );  /* Reprepare only called for prepare_v2() statements */
  db = sqlite4VdbeDb(p);
  assert( sqlite4_mutex_held(db->mutex) );
  rc = sqlite4LockAndPrepare(db, zSql, -1, 0, p, &pNew, 0);
  if( rc ){
    if( rc==SQLITE_NOMEM ){
      db->mallocFailed = 1;
    }
    assert( pNew==0 );
    return rc;
  }else{
    assert( pNew!=0 );
  }
  sqlite4VdbeSwap((Vdbe*)pNew, p);
  sqlite4TransferBindings(pNew, (sqlite4_stmt*)p);
  sqlite4VdbeResetStepResult((Vdbe*)pNew);
  sqlite4VdbeFinalize((Vdbe*)pNew);
  return SQLITE_OK;
}


/*
** Two versions of the official API.  Legacy and new use.  In the legacy
** version, the original SQL text is not saved in the prepared statement
** and so if a schema change occurs, SQLITE_SCHEMA is returned by
** sqlite4_step().  In the new version, the original SQL text is retained
** and the statement is automatically recompiled if an schema change
** occurs.
*/
int sqlite4_prepare(
  sqlite4 *db,              /* Database handle. */
  const char *zSql,         /* UTF-8 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  sqlite4_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const char **pzTail       /* OUT: End of parsed string */
){
  int rc;
  rc = sqlite4LockAndPrepare(db,zSql,nBytes,0,0,ppStmt,pzTail);
  assert( rc==SQLITE_OK || ppStmt==0 || *ppStmt==0 );  /* VERIFY: F13021 */
  return rc;
}
int sqlite4_prepare_v2(
  sqlite4 *db,              /* Database handle. */
  const char *zSql,         /* UTF-8 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  sqlite4_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const char **pzTail       /* OUT: End of parsed string */
){
  int rc;
  rc = sqlite4LockAndPrepare(db,zSql,nBytes,1,0,ppStmt,pzTail);
  assert( rc==SQLITE_OK || ppStmt==0 || *ppStmt==0 );  /* VERIFY: F13021 */
  return rc;
}


#ifndef SQLITE_OMIT_UTF16
/*
** Compile the UTF-16 encoded SQL statement zSql into a statement handle.
*/
static int sqlite4Prepare16(
  sqlite4 *db,              /* Database handle. */ 
  const void *zSql,         /* UTF-16 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  int saveSqlFlag,          /* True to save SQL text into the sqlite4_stmt */
  sqlite4_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const void **pzTail       /* OUT: End of parsed string */
){
  /* This function currently works by first transforming the UTF-16
  ** encoded string to UTF-8, then invoking sqlite4_prepare(). The
  ** tricky bit is figuring out the pointer to return in *pzTail.
  */
  char *zSql8;
  const char *zTail8 = 0;
  int rc = SQLITE_OK;

  assert( ppStmt );
  *ppStmt = 0;
  if( !sqlite4SafetyCheckOk(db) ){
    return SQLITE_MISUSE_BKPT;
  }
  sqlite4_mutex_enter(db->mutex);
  zSql8 = sqlite4Utf16to8(db, zSql, nBytes, SQLITE_UTF16NATIVE);
  if( zSql8 ){
    rc = sqlite4LockAndPrepare(db, zSql8, -1, saveSqlFlag, 0, ppStmt, &zTail8);
  }

  if( zTail8 && pzTail ){
    /* If sqlite4_prepare returns a tail pointer, we calculate the
    ** equivalent pointer into the UTF-16 string by counting the unicode
    ** characters between zSql8 and zTail8, and then returning a pointer
    ** the same number of characters into the UTF-16 string.
    */
    int chars_parsed = sqlite4Utf8CharLen(zSql8, (int)(zTail8-zSql8));
    *pzTail = (u8 *)zSql + sqlite4Utf16ByteLen(zSql, chars_parsed);
  }
  sqlite4DbFree(db, zSql8); 
  rc = sqlite4ApiExit(db, rc);
  sqlite4_mutex_leave(db->mutex);
  return rc;
}

/*
** Two versions of the official API.  Legacy and new use.  In the legacy
** version, the original SQL text is not saved in the prepared statement
** and so if a schema change occurs, SQLITE_SCHEMA is returned by
** sqlite4_step().  In the new version, the original SQL text is retained
** and the statement is automatically recompiled if an schema change
** occurs.
*/
int sqlite4_prepare16(
  sqlite4 *db,              /* Database handle. */ 
  const void *zSql,         /* UTF-16 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  sqlite4_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const void **pzTail       /* OUT: End of parsed string */
){
  int rc;
  rc = sqlite4Prepare16(db,zSql,nBytes,0,ppStmt,pzTail);
  assert( rc==SQLITE_OK || ppStmt==0 || *ppStmt==0 );  /* VERIFY: F13021 */
  return rc;
}
int sqlite4_prepare16_v2(
  sqlite4 *db,              /* Database handle. */ 
  const void *zSql,         /* UTF-16 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  sqlite4_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const void **pzTail       /* OUT: End of parsed string */
){
  int rc;
  rc = sqlite4Prepare16(db,zSql,nBytes,1,ppStmt,pzTail);
  assert( rc==SQLITE_OK || ppStmt==0 || *ppStmt==0 );  /* VERIFY: F13021 */
  return rc;
}

#endif /* SQLITE_OMIT_UTF16 */
