/*
** 2005 February 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains C code routines that used to generate VDBE code
** that implements the ALTER TABLE command.
*/
#include "sqliteInt.h"

/*
** The code in this file only exists if we are not omitting the
** ALTER TABLE logic from the build.
*/
#ifndef SQLITE_OMIT_ALTERTABLE


/*
** This function is used by SQL generated to implement the 
** ALTER TABLE command. The first argument is the text of a CREATE TABLE or
** CREATE INDEX command. The second is a table name. The table name in 
** the CREATE TABLE or CREATE INDEX statement is replaced with the third
** argument and the result returned. Examples:
**
** sqlite_rename_table('CREATE TABLE abc(a, b, c)', 'def')
**     -> 'CREATE TABLE def(a, b, c)'
**
** sqlite_rename_table('CREATE INDEX i ON abc(a)', 'def')
**     -> 'CREATE INDEX i ON def(a, b, c)'
*/
static void renameTableFunc(
  sqlite4_context *context,
  int NotUsed,
  sqlite4_value **argv
){
  unsigned char const *zSql = sqlite4_value_text(argv[0]);
  unsigned char const *zTableName = sqlite4_value_text(argv[1]);

  int token;
  Token tname;
  unsigned char const *zCsr = zSql;
  int len = 0;
  char *zRet;

  sqlite4 *db = sqlite4_context_db_handle(context);

  UNUSED_PARAMETER(NotUsed);

  /* The principle used to locate the table name in the CREATE TABLE 
  ** statement is that the table name is the first non-space token that
  ** is immediately followed by a TK_LP or TK_USING token.
  */
  if( zSql ){
    do {
      if( !*zCsr ){
        /* Ran out of input before finding an opening bracket. Return NULL. */
        return;
      }

      /* Store the token that zCsr points to in tname. */
      tname.z = (char*)zCsr;
      tname.n = len;

      /* Advance zCsr to the next token. Store that token type in 'token',
      ** and its length in 'len' (to be used next iteration of this loop).
      */
      do {
        zCsr += len;
        len = sqlite4GetToken(zCsr, &token);
      } while( token==TK_SPACE );
      assert( len>0 );
    } while( token!=TK_LP && token!=TK_USING );

    zRet = sqlite4MPrintf(db, "%.*s\"%w\"%s", ((u8*)tname.z) - zSql, zSql, 
       zTableName, tname.z+tname.n);
    sqlite4_result_text(context, zRet, -1, SQLITE_TRANSIENT);
    sqlite4DbFree(db, zRet);
  }
}

/*
** This C function implements an SQL user function that is used by SQL code
** generated by the ALTER TABLE ... RENAME command to modify the definition
** of any foreign key constraints that use the table being renamed as the 
** parent table. It is passed three arguments:
**
**   1) The complete text of the CREATE TABLE statement being modified,
**   2) The old name of the table being renamed, and
**   3) The new name of the table being renamed.
**
** It returns the new CREATE TABLE statement. For example:
**
**   sqlite_rename_parent('CREATE TABLE t1(a REFERENCES t2)', 't2', 't3')
**       -> 'CREATE TABLE t1(a REFERENCES t3)'
*/
#ifndef SQLITE_OMIT_FOREIGN_KEY
static void renameParentFunc(
  sqlite4_context *context,
  int NotUsed,
  sqlite4_value **argv
){
  sqlite4 *db = sqlite4_context_db_handle(context);
  char *zOutput = 0;
  char *zResult;
  unsigned char const *zInput = sqlite4_value_text(argv[0]);
  unsigned char const *zOld = sqlite4_value_text(argv[1]);
  unsigned char const *zNew = sqlite4_value_text(argv[2]);

  unsigned const char *z;         /* Pointer to token */
  int n;                          /* Length of token z */
  int token;                      /* Type of token */

  UNUSED_PARAMETER(NotUsed);
  for(z=zInput; *z; z=z+n){
    n = sqlite4GetToken(z, &token);
    if( token==TK_REFERENCES ){
      char *zParent;
      do {
        z += n;
        n = sqlite4GetToken(z, &token);
      }while( token==TK_SPACE );

      zParent = sqlite4DbStrNDup(db, (const char *)z, n);
      if( zParent==0 ) break;
      sqlite4Dequote(zParent);
      if( 0==sqlite4StrICmp((const char *)zOld, zParent) ){
        char *zOut = sqlite4MPrintf(db, "%s%.*s\"%w\"", 
            (zOutput?zOutput:""), z-zInput, zInput, (const char *)zNew
        );
        sqlite4DbFree(db, zOutput);
        zOutput = zOut;
        zInput = &z[n];
      }
      sqlite4DbFree(db, zParent);
    }
  }

  zResult = sqlite4MPrintf(db, "%s%s", (zOutput?zOutput:""), zInput), 
  sqlite4_result_text(context, zResult, -1, SQLITE_TRANSIENT);
  sqlite4DbFree(db, zOutput);
  sqlite4DbFree(db, zResult);
}
#endif

#ifndef SQLITE_OMIT_TRIGGER
/* This function is used by SQL generated to implement the
** ALTER TABLE command. The first argument is the text of a CREATE TRIGGER 
** statement. The second is a table name. The table name in the CREATE 
** TRIGGER statement is replaced with the third argument and the result 
** returned. This is analagous to renameTableFunc() above, except for CREATE
** TRIGGER, not CREATE INDEX and CREATE TABLE.
*/
static void renameTriggerFunc(
  sqlite4_context *context,
  int NotUsed,
  sqlite4_value **argv
){
  unsigned char const *zSql = sqlite4_value_text(argv[0]);
  unsigned char const *zTableName = sqlite4_value_text(argv[1]);

  int token;
  Token tname;
  int dist = 3;
  unsigned char const *zCsr = zSql;
  int len = 0;
  char *zRet;
  sqlite4 *db = sqlite4_context_db_handle(context);

  UNUSED_PARAMETER(NotUsed);

  /* The principle used to locate the table name in the CREATE TRIGGER 
  ** statement is that the table name is the first token that is immediatedly
  ** preceded by either TK_ON or TK_DOT and immediatedly followed by one
  ** of TK_WHEN, TK_BEGIN or TK_FOR.
  */
  if( zSql ){
    do {

      if( !*zCsr ){
        /* Ran out of input before finding the table name. Return NULL. */
        return;
      }

      /* Store the token that zCsr points to in tname. */
      tname.z = (char*)zCsr;
      tname.n = len;

      /* Advance zCsr to the next token. Store that token type in 'token',
      ** and its length in 'len' (to be used next iteration of this loop).
      */
      do {
        zCsr += len;
        len = sqlite4GetToken(zCsr, &token);
      }while( token==TK_SPACE );
      assert( len>0 );

      /* Variable 'dist' stores the number of tokens read since the most
      ** recent TK_DOT or TK_ON. This means that when a WHEN, FOR or BEGIN 
      ** token is read and 'dist' equals 2, the condition stated above
      ** to be met.
      **
      ** Note that ON cannot be a database, table or column name, so
      ** there is no need to worry about syntax like 
      ** "CREATE TRIGGER ... ON ON.ON BEGIN ..." etc.
      */
      dist++;
      if( token==TK_DOT || token==TK_ON ){
        dist = 0;
      }
    } while( dist!=2 || (token!=TK_WHEN && token!=TK_FOR && token!=TK_BEGIN) );

    /* Variable tname now contains the token that is the old table-name
    ** in the CREATE TRIGGER statement.
    */
    zRet = sqlite4MPrintf(db, "%.*s\"%w\"%s", ((u8*)tname.z) - zSql, zSql, 
       zTableName, tname.z+tname.n);
    sqlite4_result_text(context, zRet, -1, SQLITE_TRANSIENT);
    sqlite4DbFree(db, zRet);
  }
}
#endif   /* !SQLITE_OMIT_TRIGGER */

/*
** Register built-in functions used to help implement ALTER TABLE
*/
void sqlite4AlterFunctions(sqlite4_env *pEnv){
  static SQLITE_WSD FuncDef aAlterTableFuncs[] = {
    FUNCTION(sqlite_rename_table,   2, 0, 0, renameTableFunc),
#ifndef SQLITE_OMIT_TRIGGER
    FUNCTION(sqlite_rename_trigger, 2, 0, 0, renameTriggerFunc),
#endif
#ifndef SQLITE_OMIT_FOREIGN_KEY
    FUNCTION(sqlite_rename_parent,  3, 0, 0, renameParentFunc),
#endif
  };
  int i;
  FuncDefTable *pFuncTab = &pEnv->aGlobalFuncs;
  FuncDef *aFunc = (FuncDef*)aAlterTableFuncs;

  for(i=0; i<ArraySize(aAlterTableFuncs); i++){
    sqlite4FuncDefInsert(pFuncTab, &aFunc[i], 1);
  }
}

/*
** This function is used to create the text of expressions of the form:
**
**   name=<constant1> OR name=<constant2> OR ...
**
** If argument zWhere is NULL, then a pointer string containing the text 
** "name=<constant>" is returned, where <constant> is the quoted version
** of the string passed as argument zConstant. The returned buffer is
** allocated using sqlite4DbMalloc(). It is the responsibility of the
** caller to ensure that it is eventually freed.
**
** If argument zWhere is not NULL, then the string returned is 
** "<where> OR name=<constant>", where <where> is the contents of zWhere.
** In this case zWhere is passed to sqlite4DbFree() before returning.
** 
*/
static char *whereOrName(sqlite4 *db, char *zWhere, char *zConstant){
  char *zNew;
  if( !zWhere ){
    zNew = sqlite4MPrintf(db, "name=%Q", zConstant);
  }else{
    zNew = sqlite4MPrintf(db, "%s OR name=%Q", zWhere, zConstant);
    sqlite4DbFree(db, zWhere);
  }
  return zNew;
}

#if !defined(SQLITE_OMIT_FOREIGN_KEY) && !defined(SQLITE_OMIT_TRIGGER)
/*
** Generate the text of a WHERE expression which can be used to select all
** tables that have foreign key constraints that refer to table pTab (i.e.
** constraints for which pTab is the parent table) from the sqlite_master
** table.
*/
static char *whereForeignKeys(Parse *pParse, Table *pTab){
  FKey *p;
  char *zWhere = 0;
  for(p=sqlite4FkReferences(pTab); p; p=p->pNextTo){
    zWhere = whereOrName(pParse->db, zWhere, p->pFrom->zName);
  }
  return zWhere;
}
#endif

/*
** Generate the text of a WHERE expression which can be used to select all
** temporary triggers on table pTab from the sqlite_temp_master table. If
** table pTab has no temporary triggers, or is itself stored in the 
** temporary database, NULL is returned.
*/
static char *whereTempTriggers(Parse *pParse, Table *pTab){
  Trigger *pTrig;
  char *zWhere = 0;
  const Schema *pTempSchema = pParse->db->aDb[1].pSchema; /* Temp db schema */

  /* If the table is not located in the temp-db (in which case NULL is 
  ** returned, loop through the tables list of triggers. For each trigger
  ** that is not part of the temp-db schema, add a clause to the WHERE 
  ** expression being built up in zWhere.
  */
  if( pTab->pSchema!=pTempSchema ){
    sqlite4 *db = pParse->db;
    for(pTrig=sqlite4TriggerList(pParse, pTab); pTrig; pTrig=pTrig->pNext){
      if( pTrig->pSchema==pTempSchema ){
        zWhere = whereOrName(db, zWhere, pTrig->zName);
      }
    }
  }
  if( zWhere ){
    char *zNew = sqlite4MPrintf(pParse->db, "type='trigger' AND (%s)", zWhere);
    sqlite4DbFree(pParse->db, zWhere);
    zWhere = zNew;
  }
  return zWhere;
}

/*
** Generate code to drop and reload the internal representation of table
** pTab from the database, including triggers and temporary triggers.
** Argument zName is the name of the table in the database schema at
** the time the generated code is executed. This can be different from
** pTab->zName if this function is being called to code part of an 
** "ALTER TABLE RENAME TO" statement.
*/
static void reloadTableSchema(Parse *pParse, Table *pTab, const char *zName){
  Vdbe *v;
  char *zWhere;
  int iDb;                   /* Index of database containing pTab */
#ifndef SQLITE_OMIT_TRIGGER
  Trigger *pTrig;
#endif

  v = sqlite4GetVdbe(pParse);
  if( NEVER(v==0) ) return;
  iDb = sqlite4SchemaToIndex(pParse->db, pTab->pSchema);
  assert( iDb>=0 );

#ifndef SQLITE_OMIT_TRIGGER
  /* Drop any table triggers from the internal schema. */
  for(pTrig=sqlite4TriggerList(pParse, pTab); pTrig; pTrig=pTrig->pNext){
    int iTrigDb = sqlite4SchemaToIndex(pParse->db, pTrig->pSchema);
    assert( iTrigDb==iDb || iTrigDb==1 );
    sqlite4VdbeAddOp4(v, OP_DropTrigger, iTrigDb, 0, 0, pTrig->zName, 0);
  }
#endif

  /* Drop the table and index from the internal schema.  */
  sqlite4VdbeAddOp4(v, OP_DropTable, iDb, 0, 0, pTab->zName, 0);

  /* Reload the table, index and permanent trigger schemas. */
  zWhere = sqlite4MPrintf(pParse->db, "tbl_name=%Q", zName);
  if( !zWhere ) return;
  sqlite4VdbeAddParseSchemaOp(v, iDb, zWhere);

#ifndef SQLITE_OMIT_TRIGGER
  /* Now, if the table is not stored in the temp database, reload any temp 
  ** triggers. Don't use IN(...) in case SQLITE_OMIT_SUBQUERY is defined. 
  */
  if( (zWhere=whereTempTriggers(pParse, pTab))!=0 ){
    sqlite4VdbeAddParseSchemaOp(v, 1, zWhere);
  }
#endif
}

/*
** Parameter zName is the name of a table that is about to be altered
** (either with ALTER TABLE ... RENAME TO or ALTER TABLE ... ADD COLUMN).
** If the table is a system table, this function leaves an error message
** in pParse->zErr (system tables may not be altered) and returns non-zero.
**
** Or, if zName is not a system table, zero is returned.
*/
static int isSystemTable(Parse *pParse, const char *zName){
  if( sqlite4Strlen30(zName)>6 && 0==sqlite4StrNICmp(zName, "sqlite_", 7) ){
    sqlite4ErrorMsg(pParse, "table %s may not be altered", zName);
    return 1;
  }
  return 0;
}

/*
** Generate code to implement the "ALTER TABLE xxx RENAME TO yyy" 
** command. 
*/
void sqlite4AlterRenameTable(
  Parse *pParse,            /* Parser context. */
  SrcList *pSrc,            /* The table to rename. */
  Token *pName              /* The new table name. */
){
  int iDb;                  /* Database that contains the table */
  char *zDb;                /* Name of database iDb */
  Table *pTab;              /* Table being renamed */
  char *zName = 0;          /* NULL-terminated version of pName */ 
  sqlite4 *db = pParse->db; /* Database connection */
  int nTabName;             /* Number of UTF-8 characters in zTabName */
  const char *zTabName;     /* Original name of the table */
  Vdbe *v;
#ifndef SQLITE_OMIT_TRIGGER
  char *zWhere = 0;         /* Where clause to locate temp triggers */
#endif
  VTable *pVTab = 0;        /* Non-zero if this is a v-tab with an xRename() */
  int savedDbFlags;         /* Saved value of db->flags */

  savedDbFlags = db->flags;  
  if( NEVER(db->mallocFailed) ) goto exit_rename_table;
  assert( pSrc->nSrc==1 );

  pTab = sqlite4LocateTable(pParse, 0, pSrc->a[0].zName, pSrc->a[0].zDatabase);
  if( !pTab ) goto exit_rename_table;
  iDb = sqlite4SchemaToIndex(pParse->db, pTab->pSchema);
  zDb = db->aDb[iDb].zName;
  db->flags |= SQLITE_PreferBuiltin;

  /* Get a NULL terminated version of the new table name. */
  zName = sqlite4NameFromToken(db, pName);
  if( !zName ) goto exit_rename_table;

  /* Check that a table or index named 'zName' does not already exist
  ** in database iDb. If so, this is an error.
  */
  if( sqlite4FindTable(db, zName, zDb) || sqlite4FindIndex(db, zName, zDb) ){
    sqlite4ErrorMsg(pParse, 
        "there is already another table or index with this name: %s", zName);
    goto exit_rename_table;
  }

  /* Make sure it is not a system table being altered, or a reserved name
  ** that the table is being renamed to.
  */
  if( SQLITE_OK!=isSystemTable(pParse, pTab->zName) ){
    goto exit_rename_table;
  }
  if( SQLITE_OK!=sqlite4CheckObjectName(pParse, zName) ){ goto
    exit_rename_table;
  }

#ifndef SQLITE_OMIT_VIEW
  if( pTab->pSelect ){
    sqlite4ErrorMsg(pParse, "view %s may not be altered", pTab->zName);
    goto exit_rename_table;
  }
#endif

#ifndef SQLITE_OMIT_AUTHORIZATION
  /* Invoke the authorization callback. */
  if( sqlite4AuthCheck(pParse, SQLITE_ALTER_TABLE, zDb, pTab->zName, 0) ){
    goto exit_rename_table;
  }
#endif

#ifndef SQLITE_OMIT_VIRTUALTABLE
  if( sqlite4ViewGetColumnNames(pParse, pTab) ){
    goto exit_rename_table;
  }
  if( IsVirtual(pTab) ){
    pVTab = sqlite4GetVTable(db, pTab);
    if( pVTab->pVtab->pModule->xRename==0 ){
      pVTab = 0;
    }
  }
#endif

  /* Begin a transaction and code the VerifyCookie for database iDb. 
  ** Then modify the schema cookie (since the ALTER TABLE modifies the
  ** schema). Open a statement transaction if the table is a virtual
  ** table.
  */
  v = sqlite4GetVdbe(pParse);
  if( v==0 ){
    goto exit_rename_table;
  }
  sqlite4BeginWriteOperation(pParse, pVTab!=0, iDb);
  sqlite4ChangeCookie(pParse, iDb);

  /* If this is a virtual table, invoke the xRename() function if
  ** one is defined. The xRename() callback will modify the names
  ** of any resources used by the v-table implementation (including other
  ** SQLite tables) that are identified by the name of the virtual table.
  */
#ifndef SQLITE_OMIT_VIRTUALTABLE
  if( pVTab ){
    int i = ++pParse->nMem;
    sqlite4VdbeAddOp4(v, OP_String8, 0, i, 0, zName, 0);
    sqlite4VdbeAddOp4(v, OP_VRename, i, 0, 0,(const char*)pVTab, P4_VTAB);
    sqlite4MayAbort(pParse);
  }
#endif

  /* figure out how many UTF-8 characters are in zName */
  zTabName = pTab->zName;
  nTabName = sqlite4Utf8CharLen(zTabName, -1);

#if !defined(SQLITE_OMIT_FOREIGN_KEY) && !defined(SQLITE_OMIT_TRIGGER)
  if( db->flags&SQLITE_ForeignKeys ){
    /* If foreign-key support is enabled, rewrite the CREATE TABLE 
    ** statements corresponding to all child tables of foreign key constraints
    ** for which the renamed table is the parent table.  */
    if( (zWhere=whereForeignKeys(pParse, pTab))!=0 ){
      sqlite4NestedParse(pParse, 
          "UPDATE \"%w\".%s SET "
              "sql = sqlite_rename_parent(sql, %Q, %Q) "
              "WHERE %s;", zDb, SCHEMA_TABLE(iDb), zTabName, zName, zWhere);
      sqlite4DbFree(db, zWhere);
    }
  }
#endif

  /* Modify the sqlite_master table to use the new table name. */
  sqlite4NestedParse(pParse,
      "UPDATE %Q.%s SET "
#ifdef SQLITE_OMIT_TRIGGER
          "sql = sqlite_rename_table(sql, %Q), "
#else
          "sql = CASE "
            "WHEN type = 'trigger' THEN sqlite_rename_trigger(sql, %Q)"
            "ELSE sqlite_rename_table(sql, %Q) END, "
#endif
          "tbl_name = %Q, "
          "name = CASE "
            "WHEN type='table' THEN %Q "
            "WHEN name LIKE 'sqlite_autoindex%%' AND type='index' THEN "
             "'sqlite_autoindex_' || %Q || substr(name,%d+18) "
            "ELSE name END "
      "WHERE tbl_name=%Q AND "
          "(type='table' OR type='index' OR type='trigger');", 
      zDb, SCHEMA_TABLE(iDb), zName, zName, zName, 
#ifndef SQLITE_OMIT_TRIGGER
      zName,
#endif
      zName, nTabName, zTabName
  );

#ifndef SQLITE_OMIT_AUTOINCREMENT
  /* If the sqlite_sequence table exists in this database, then update 
  ** it with the new table name.
  */
  if( sqlite4FindTable(db, "sqlite_sequence", zDb) ){
    sqlite4NestedParse(pParse,
        "UPDATE \"%w\".sqlite_sequence set name = %Q WHERE name = %Q",
        zDb, zName, pTab->zName);
  }
#endif

#ifndef SQLITE_OMIT_TRIGGER
  /* If there are TEMP triggers on this table, modify the sqlite_temp_master
  ** table. Don't do this if the table being ALTERed is itself located in
  ** the temp database.
  */
  if( (zWhere=whereTempTriggers(pParse, pTab))!=0 ){
    sqlite4NestedParse(pParse, 
        "UPDATE sqlite_temp_master SET "
            "sql = sqlite_rename_trigger(sql, %Q), "
            "tbl_name = %Q "
            "WHERE %s;", zName, zName, zWhere);
    sqlite4DbFree(db, zWhere);
  }
#endif

#if !defined(SQLITE_OMIT_FOREIGN_KEY) && !defined(SQLITE_OMIT_TRIGGER)
  if( db->flags&SQLITE_ForeignKeys ){
    FKey *p;
    for(p=sqlite4FkReferences(pTab); p; p=p->pNextTo){
      Table *pFrom = p->pFrom;
      if( pFrom!=pTab ){
        reloadTableSchema(pParse, p->pFrom, pFrom->zName);
      }
    }
  }
#endif

  /* Drop and reload the internal table schema. */
  reloadTableSchema(pParse, pTab, zName);

exit_rename_table:
  sqlite4SrcListDelete(db, pSrc);
  sqlite4DbFree(db, zName);
  db->flags = savedDbFlags;
}


/*
** This function is called after an "ALTER TABLE ... ADD" statement
** has been parsed. Argument pColDef contains the text of the new
** column definition.
**
** The Table structure pParse->pNewTable was extended to include
** the new column during parsing.
*/
void sqlite4AlterFinishAddColumn(Parse *pParse, Token *pColDef){
  Table *pNew;              /* Copy of pParse->pNewTable */
  Table *pTab;              /* Table being altered */
  int iDb;                  /* Database number */
  const char *zDb;          /* Database name */
  const char *zTab;         /* Table name */
  char *zCol;               /* Null-terminated column definition */
  Column *pCol;             /* The new column */
  Expr *pDflt;              /* Default value for the new column */
  sqlite4 *db;              /* The database connection; */

  db = pParse->db;
  if( pParse->nErr || db->mallocFailed ) return;
  pNew = pParse->pNewTable;
  assert( pNew );

  iDb = sqlite4SchemaToIndex(db, pNew->pSchema);
  zDb = db->aDb[iDb].zName;
  zTab = &pNew->zName[16];  /* Skip the "sqlite_altertab_" prefix on the name */
  pCol = &pNew->aCol[pNew->nCol-1];
  pDflt = pCol->pDflt;
  pTab = sqlite4FindTable(db, zTab, zDb);
  assert( pTab );

#ifndef SQLITE_OMIT_AUTHORIZATION
  /* Invoke the authorization callback. */
  if( sqlite4AuthCheck(pParse, SQLITE_ALTER_TABLE, zDb, pTab->zName, 0) ){
    return;
  }
#endif

  /* If the default value for the new column was specified with a 
  ** literal NULL, then set pDflt to 0. This simplifies checking
  ** for an SQL NULL default below.
  */
  if( pDflt && pDflt->op==TK_NULL ){
    pDflt = 0;
  }

  /* Check that the new column is not specified as PRIMARY KEY or UNIQUE.
  ** If there is a NOT NULL constraint, then the default value for the
  ** column must not be NULL.
  */
  if( pCol->isPrimKey ){
    sqlite4ErrorMsg(pParse, "Cannot add a PRIMARY KEY column");
    return;
  }
  if( pNew->pIndex ){
    sqlite4ErrorMsg(pParse, "Cannot add a UNIQUE column");
    return;
  }
  if( (db->flags&SQLITE_ForeignKeys) && pNew->pFKey && pDflt ){
    sqlite4ErrorMsg(pParse, 
        "Cannot add a REFERENCES column with non-NULL default value");
    return;
  }
  if( pCol->notNull && !pDflt ){
    sqlite4ErrorMsg(pParse, 
        "Cannot add a NOT NULL column with default value NULL");
    return;
  }

  /* Ensure the default expression is something that sqlite4ValueFromExpr()
  ** can handle (i.e. not CURRENT_TIME etc.)
  */
  if( pDflt ){
    sqlite4_value *pVal;
    if( sqlite4ValueFromExpr(db, pDflt, SQLITE_UTF8, SQLITE_AFF_NONE, &pVal) ){
      db->mallocFailed = 1;
      return;
    }
    if( !pVal ){
      sqlite4ErrorMsg(pParse, "Cannot add a column with non-constant default");
      return;
    }
    sqlite4ValueFree(pVal);
  }

  /* Modify the CREATE TABLE statement. */
  zCol = sqlite4DbStrNDup(db, (char*)pColDef->z, pColDef->n);
  if( zCol ){
    char *zEnd = &zCol[pColDef->n-1];
    int savedDbFlags = db->flags;
    while( zEnd>zCol && (*zEnd==';' || sqlite4Isspace(*zEnd)) ){
      *zEnd-- = '\0';
    }
    db->flags |= SQLITE_PreferBuiltin;
    sqlite4NestedParse(pParse, 
        "UPDATE \"%w\".%s SET "
          "sql = substr(sql,1,%d) || ', ' || %Q || substr(sql,%d) "
        "WHERE type = 'table' AND name = %Q", 
      zDb, SCHEMA_TABLE(iDb), pNew->addColOffset, zCol, pNew->addColOffset+1,
      zTab
    );
    sqlite4DbFree(db, zCol);
    db->flags = savedDbFlags;
  }

  /* Reload the schema of the modified table. */
  reloadTableSchema(pParse, pTab, pTab->zName);
}

/*
** This function is called by the parser after the table-name in
** an "ALTER TABLE <table-name> ADD" statement is parsed. Argument 
** pSrc is the full-name of the table being altered.
**
** This routine makes a (partial) copy of the Table structure
** for the table being altered and sets Parse.pNewTable to point
** to it. Routines called by the parser as the column definition
** is parsed (i.e. sqlite4AddColumn()) add the new Column data to 
** the copy. The copy of the Table structure is deleted by tokenize.c 
** after parsing is finished.
**
** Routine sqlite4AlterFinishAddColumn() will be called to complete
** coding the "ALTER TABLE ... ADD" statement.
*/
void sqlite4AlterBeginAddColumn(Parse *pParse, SrcList *pSrc){
  Table *pNew;
  Table *pTab;
  Vdbe *v;
  int iDb;
  int i;
  int nAlloc;
  sqlite4 *db = pParse->db;

  /* Look up the table being altered. */
  assert( pParse->pNewTable==0 );
  if( db->mallocFailed ) goto exit_begin_add_column;
  pTab = sqlite4LocateTable(pParse, 0, pSrc->a[0].zName, pSrc->a[0].zDatabase);
  if( !pTab ) goto exit_begin_add_column;

#ifndef SQLITE_OMIT_VIRTUALTABLE
  if( IsVirtual(pTab) ){
    sqlite4ErrorMsg(pParse, "virtual tables may not be altered");
    goto exit_begin_add_column;
  }
#endif

  /* Make sure this is not an attempt to ALTER a view. */
  if( pTab->pSelect ){
    sqlite4ErrorMsg(pParse, "Cannot add a column to a view");
    goto exit_begin_add_column;
  }
  if( SQLITE_OK!=isSystemTable(pParse, pTab->zName) ){
    goto exit_begin_add_column;
  }

  assert( pTab->addColOffset>0 );
  iDb = sqlite4SchemaToIndex(db, pTab->pSchema);

  /* Put a copy of the Table struct in Parse.pNewTable for the
  ** sqlite4AddColumn() function and friends to modify.  But modify
  ** the name by adding an "sqlite_altertab_" prefix.  By adding this
  ** prefix, we insure that the name will not collide with an existing
  ** table because user table are not allowed to have the "sqlite_"
  ** prefix on their name.
  */
  pNew = (Table*)sqlite4DbMallocZero(db, sizeof(Table));
  if( !pNew ) goto exit_begin_add_column;
  pParse->pNewTable = pNew;
  pNew->nRef = 1;
  pNew->nCol = pTab->nCol;
  assert( pNew->nCol>0 );
  nAlloc = (((pNew->nCol-1)/8)*8)+8;
  assert( nAlloc>=pNew->nCol && nAlloc%8==0 && nAlloc-pNew->nCol<8 );
  pNew->aCol = (Column*)sqlite4DbMallocZero(db, sizeof(Column)*nAlloc);
  pNew->zName = sqlite4MPrintf(db, "sqlite_altertab_%s", pTab->zName);
  if( !pNew->aCol || !pNew->zName ){
    db->mallocFailed = 1;
    goto exit_begin_add_column;
  }
  memcpy(pNew->aCol, pTab->aCol, sizeof(Column)*pNew->nCol);
  for(i=0; i<pNew->nCol; i++){
    Column *pCol = &pNew->aCol[i];
    pCol->zName = sqlite4DbStrDup(db, pCol->zName);
    pCol->zColl = 0;
    pCol->zType = 0;
    pCol->pDflt = 0;
    pCol->zDflt = 0;
  }
  pNew->pSchema = db->aDb[iDb].pSchema;
  pNew->addColOffset = pTab->addColOffset;
  pNew->nRef = 1;

  /* Begin a transaction and increment the schema cookie.  */
  sqlite4BeginWriteOperation(pParse, 0, iDb);
  v = sqlite4GetVdbe(pParse);
  if( !v ) goto exit_begin_add_column;
  sqlite4ChangeCookie(pParse, iDb);

exit_begin_add_column:
  sqlite4SrcListDelete(db, pSrc);
  return;
}
#endif  /* SQLITE_ALTER_TABLE */
