/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains C code routines that are called by the parser
** in order to generate code for DELETE FROM statements.
*/
#include "sqliteInt.h"

/*
** While a SrcList can in general represent multiple tables and subqueries
** (as in the FROM clause of a SELECT statement) in this case it contains
** the name of a single table, as one might find in an INSERT, DELETE,
** or UPDATE statement.  Look up that table in the symbol table and
** return a pointer.  Set an error message and return NULL if the table 
** name is not found or if any other error occurs.
**
** The following fields are initialized appropriate in pSrc:
**
**    pSrc->a[0].pTab       Pointer to the Table object
**    pSrc->a[0].pIndex     Pointer to the INDEXED BY index, if there is one
**
*/
Table *sqlite4SrcListLookup(Parse *pParse, SrcList *pSrc){
  struct SrcList_item *pItem = pSrc->a;
  Table *pTab;
  assert( pItem && pSrc->nSrc==1 );
  pTab = sqlite4LocateTable(pParse, 0, pItem->zName, pItem->zDatabase);
  sqlite4DeleteTable(pParse->db, pItem->pTab);
  pItem->pTab = pTab;
  if( pTab ){
    pTab->nRef++;
  }
  if( sqlite4IndexedByLookup(pParse, pItem) ){
    pTab = 0;
  }
  return pTab;
}

/*
** Check to make sure the given table is writable.  If it is not
** writable, generate an error message and return 1.  If it is
** writable return 0;
*/
int sqlite4IsReadOnly(Parse *pParse, Table *pTab, int viewOk){
  /* A table is not writable under the following circumstances:
  **
  **   1) It is a virtual table and no implementation of the xUpdate method
  **      has been provided, or
  **   2) It is a system table (i.e. sqlite_master), this call is not
  **      part of a nested parse and writable_schema pragma has not 
  **      been specified.
  **
  ** In either case leave an error message in pParse and return non-zero.
  */
  if( ( IsVirtual(pTab) 
     && sqlite4GetVTable(pParse->db, pTab)->pMod->pModule->xUpdate==0 )
   || ( (pTab->tabFlags & TF_Readonly)!=0
     && (pParse->db->flags & SQLITE_WriteSchema)==0
     && pParse->nested==0 )
  ){
    sqlite4ErrorMsg(pParse, "table %s may not be modified", pTab->zName);
    return 1;
  }

#ifndef SQLITE_OMIT_VIEW
  if( !viewOk && pTab->pSelect ){
    sqlite4ErrorMsg(pParse,"cannot modify %s because it is a view",pTab->zName);
    return 1;
  }
#endif
  return 0;
}


#if !defined(SQLITE_OMIT_VIEW) && !defined(SQLITE_OMIT_TRIGGER)
/*
** Evaluate a view and store its result in an ephemeral table.  The
** pWhere argument is an optional WHERE clause that restricts the
** set of rows in the view that are to be added to the ephemeral table.
*/
void sqlite4MaterializeView(
  Parse *pParse,       /* Parsing context */
  Table *pView,        /* View definition */
  Expr *pWhere,        /* Optional WHERE clause to be added */
  int iCur             /* Cursor number for ephemerial table */
){
  SelectDest dest;
  Select *pDup;
  sqlite4 *db = pParse->db;

  pDup = sqlite4SelectDup(db, pView->pSelect, 0);
  if( pWhere ){
    SrcList *pFrom;
    
    pWhere = sqlite4ExprDup(db, pWhere, 0);
    pFrom = sqlite4SrcListAppend(db, 0, 0, 0);
    if( pFrom ){
      assert( pFrom->nSrc==1 );
      pFrom->a[0].zAlias = sqlite4DbStrDup(db, pView->zName);
      pFrom->a[0].pSelect = pDup;
      assert( pFrom->a[0].pOn==0 );
      assert( pFrom->a[0].pUsing==0 );
    }else{
      sqlite4SelectDelete(db, pDup);
    }
    pDup = sqlite4SelectNew(pParse, 0, pFrom, pWhere, 0, 0, 0, 0, 0, 0);
  }
  sqlite4SelectDestInit(&dest, SRT_EphemTab, iCur);
  sqlite4Select(pParse, pDup, &dest);
  sqlite4SelectDelete(db, pDup);
}
#endif /* !defined(SQLITE_OMIT_VIEW) && !defined(SQLITE_OMIT_TRIGGER) */

#if defined(SQLITE_ENABLE_UPDATE_DELETE_LIMIT) && !defined(SQLITE_OMIT_SUBQUERY)
/*
** Generate an expression tree to implement the WHERE, ORDER BY,
** and LIMIT/OFFSET portion of DELETE and UPDATE statements.
**
**     DELETE FROM table_wxyz WHERE a<5 ORDER BY a LIMIT 1;
**                            \__________________________/
**                               pLimitWhere (pInClause)
*/
Expr *sqlite4LimitWhere(
  Parse *pParse,               /* The parser context */
  SrcList *pSrc,               /* the FROM clause -- which tables to scan */
  Expr *pWhere,                /* The WHERE clause.  May be null */
  ExprList *pOrderBy,          /* The ORDER BY clause.  May be null */
  Expr *pLimit,                /* The LIMIT clause.  May be null */
  Expr *pOffset,               /* The OFFSET clause.  May be null */
  char *zStmtType              /* Either DELETE or UPDATE.  For error messages. */
){
  Expr *pWhereRowid = NULL;    /* WHERE rowid .. */
  Expr *pInClause = NULL;      /* WHERE rowid IN ( select ) */
  Expr *pSelectRowid = NULL;   /* SELECT rowid ... */
  ExprList *pEList = NULL;     /* Expression list contaning only pSelectRowid */
  SrcList *pSelectSrc = NULL;  /* SELECT rowid FROM x ... (dup of pSrc) */
  Select *pSelect = NULL;      /* Complete SELECT tree */

  /* Check that there isn't an ORDER BY without a LIMIT clause.
  */
  if( pOrderBy && (pLimit == 0) ) {
    sqlite4ErrorMsg(pParse, "ORDER BY without LIMIT on %s", zStmtType);
    goto limit_where_cleanup_2;
  }

  /* We only need to generate a select expression if there
  ** is a limit/offset term to enforce.
  */
  if( pLimit == 0 ) {
    /* if pLimit is null, pOffset will always be null as well. */
    assert( pOffset == 0 );
    return pWhere;
  }

  /* Generate a select expression tree to enforce the limit/offset 
  ** term for the DELETE or UPDATE statement.  For example:
  **   DELETE FROM table_a WHERE col1=1 ORDER BY col2 LIMIT 1 OFFSET 1
  ** becomes:
  **   DELETE FROM table_a WHERE rowid IN ( 
  **     SELECT rowid FROM table_a WHERE col1=1 ORDER BY col2 LIMIT 1 OFFSET 1
  **   );
  */

  pSelectRowid = sqlite4PExpr(pParse, TK_ROW, 0, 0, 0);
  if( pSelectRowid == 0 ) goto limit_where_cleanup_2;
  pEList = sqlite4ExprListAppend(pParse, 0, pSelectRowid);
  if( pEList == 0 ) goto limit_where_cleanup_2;

  /* duplicate the FROM clause as it is needed by both the DELETE/UPDATE tree
  ** and the SELECT subtree. */
  pSelectSrc = sqlite4SrcListDup(pParse->db, pSrc, 0);
  if( pSelectSrc == 0 ) {
    sqlite4ExprListDelete(pParse->db, pEList);
    goto limit_where_cleanup_2;
  }

  /* generate the SELECT expression tree. */
  pSelect = sqlite4SelectNew(pParse,pEList,pSelectSrc,pWhere,0,0,
                             pOrderBy,0,pLimit,pOffset);
  if( pSelect == 0 ) return 0;

  /* now generate the new WHERE rowid IN clause for the DELETE/UDPATE */
  pWhereRowid = sqlite4PExpr(pParse, TK_ROW, 0, 0, 0);
  if( pWhereRowid == 0 ) goto limit_where_cleanup_1;
  pInClause = sqlite4PExpr(pParse, TK_IN, pWhereRowid, 0, 0);
  if( pInClause == 0 ) goto limit_where_cleanup_1;

  pInClause->x.pSelect = pSelect;
  pInClause->flags |= EP_xIsSelect;
  sqlite4ExprSetHeight(pParse, pInClause);
  return pInClause;

  /* something went wrong. clean up anything allocated. */
limit_where_cleanup_1:
  sqlite4SelectDelete(pParse->db, pSelect);
  return 0;

limit_where_cleanup_2:
  sqlite4ExprDelete(pParse->db, pWhere);
  sqlite4ExprListDelete(pParse->db, pOrderBy);
  sqlite4ExprDelete(pParse->db, pLimit);
  sqlite4ExprDelete(pParse->db, pOffset);
  return 0;
}
#endif /* defined(SQLITE_ENABLE_UPDATE_DELETE_LIMIT) && !defined(SQLITE_OMIT_SUBQUERY) */

/*
** Generate code for a DELETE FROM statement.
**
**     DELETE FROM table_wxyz WHERE a<5 AND b NOT NULL;
**                 \________/       \________________/
**                  pTabList              pWhere
*/
void sqlite4DeleteFrom(
  Parse *pParse,         /* The parser context */
  SrcList *pTabList,     /* The table from which we should delete things */
  Expr *pWhere           /* The WHERE clause.  May be null */
){
  Vdbe *v;               /* The virtual database engine */
  Table *pTab;           /* The table from which records will be deleted */
  const char *zDb;       /* Name of database holding pTab */
  int addr = 0;     /* A couple addresses of generated code */
  int i;                 /* Loop counter */
  WhereInfo *pWInfo;     /* Information about the WHERE clause */
  Index *pIdx;           /* For looping over indices of the table */
  int iCur;              /* VDBE Cursor number for pTab */
  sqlite4 *db;           /* Main database structure */
  AuthContext sContext;  /* Authorization context */
  NameContext sNC;       /* Name context to resolve expressions in */
  int iDb;               /* Database number */
  int rcauth;            /* Value returned by authorization callback */

#ifndef SQLITE_OMIT_TRIGGER
  int isView;                  /* True if attempting to delete from a view */
  Trigger *pTrigger;           /* List of table triggers, if required */
#endif

  memset(&sContext, 0, sizeof(sContext));
  db = pParse->db;
  if( pParse->nErr || db->mallocFailed ){
    goto delete_from_cleanup;
  }
  assert( pTabList->nSrc==1 );

  /* Locate the table which we want to delete.  This table has to be
  ** put in an SrcList structure because some of the subroutines we
  ** will be calling are designed to work with multiple tables and expect
  ** an SrcList* parameter instead of just a Table* parameter.  */
  pTab = sqlite4SrcListLookup(pParse, pTabList);
  if( pTab==0 )  goto delete_from_cleanup;

  /* Figure out if we have any triggers and if the table being
  ** deleted from is a view.  */
#ifndef SQLITE_OMIT_TRIGGER
  pTrigger = sqlite4TriggersExist(pParse, pTab, TK_DELETE, 0, 0);
  isView = pTab->pSelect!=0;
#else
# define pTrigger 0
# define isView 0
#endif
#ifdef SQLITE_OMIT_VIEW
# undef isView
# define isView 0
#endif

  /* If pTab is really a view, make sure it has been initialized. */
  if( sqlite4ViewGetColumnNames(pParse, pTab) ){
    goto delete_from_cleanup;
  }

  /* Check the table is not read-only (e.g. sqlite_master or sqlite_stat).
  ** Also, check that if pTab is really an SQL view, one or more INSTEAD 
  ** OF DELETE triggers have been configured.  */
  if( sqlite4IsReadOnly(pParse, pTab, (pTrigger?1:0)) ){
    goto delete_from_cleanup;
  }
  assert(!isView || pTrigger);

  /* Invoke the authorization callback */
  iDb = sqlite4SchemaToIndex(db, pTab->pSchema);
  assert( iDb<db->nDb );
  zDb = db->aDb[iDb].zName;
  rcauth = sqlite4AuthCheck(pParse, SQLITE_DELETE, pTab->zName, 0, zDb);
  assert( rcauth==SQLITE_OK || rcauth==SQLITE_DENY || rcauth==SQLITE_IGNORE );
  if( rcauth==SQLITE_DENY ){
    goto delete_from_cleanup;
  }

  /* Assign cursor numbers to each of the tables indexes. */
  assert( pTabList->nSrc==1 );
  iCur = pTabList->a[0].iCursor = pParse->nTab++;
  for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
    pParse->nTab++;
  }

  /* Start the view context
  */
  if( isView ){
    sqlite4AuthContextPush(pParse, &sContext, pTab->zName);
  }

  /* Begin generating code.
  */
  v = sqlite4GetVdbe(pParse);
  if( v==0 ){
    goto delete_from_cleanup;
  }
  if( pParse->nested==0 ) sqlite4VdbeCountChanges(v);
  sqlite4BeginWriteOperation(pParse, 1, iDb);

  /* If we are trying to delete from a view, realize that view into
  ** a ephemeral table.
  */
#if !defined(SQLITE_OMIT_VIEW) && !defined(SQLITE_OMIT_TRIGGER)
  if( isView ){
    sqlite4MaterializeView(pParse, pTab, pWhere, iCur);
  }
#endif

  /* Resolve the column names in the WHERE clause.
  */
  memset(&sNC, 0, sizeof(sNC));
  sNC.pParse = pParse;
  sNC.pSrcList = pTabList;
  if( sqlite4ResolveExprNames(&sNC, pWhere) ){
    goto delete_from_cleanup;
  }

#ifndef SQLITE_OMIT_TRUNCATE_OPTIMIZATION
  /* Special case: A DELETE without a WHERE clause deletes everything.
  ** It is easier just to erase the whole table. Prior to version 3.6.5,
  ** this optimization caused the row change count (the value returned by 
  ** API function sqlite4_count_changes) to be set incorrectly.  */
  if( rcauth==SQLITE_OK && pWhere==0 && !pTrigger && !IsVirtual(pTab) 
   && 0==sqlite4FkRequired(pParse, pTab, 0)
  ){
    assert( !isView );
    for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
      assert( pIdx->pSchema==pTab->pSchema );
      sqlite4VdbeAddOp2(v, OP_Clear, pIdx->tnum, iDb);
    }
  }else
#endif /* SQLITE_OMIT_TRUNCATE_OPTIMIZATION */
  /* The usual case: There is a WHERE clause so we have to scan through
  ** the table and pick which records to delete.
  */
  {
    int regSet = ++pParse->nMem;  /* Register for rowset of rows to delete */
    int regKey = ++pParse->nMem;  /* Used for storing row keys */
    int addrTop;                  /* Instruction (KeySetRead) at top of loop */

    /* Query the table for all rows that match the WHERE clause. Store the
    ** PRIMARY KEY for each matching row in the KeySet object in register
    ** regSet. After the scan is complete, the VM will loop through the set 
    ** of keys in the KeySet and delete each row. Rows must be deleted after 
    ** the scan is complete because deleting an item can change the scan 
    ** order.  */
    sqlite4VdbeAddOp2(v, OP_Null, 0, regSet);
    VdbeComment((v, "initialize KeySet"));
    pWInfo = sqlite4WhereBegin(
        pParse, pTabList, pWhere, 0, 0, WHERE_DUPLICATES_OK
    );
    if( pWInfo==0 ) goto delete_from_cleanup;
    sqlite4VdbeAddOp2(v, OP_RowKey, iCur, regKey);
    sqlite4VdbeAddOp2(v, OP_KeySetAdd, regSet, regKey);
    sqlite4WhereEnd(pWInfo);

    /* Unless this is a view, open cursors for all indexes on the table
    ** from which we are deleting.  */
    if( !isView ){
      sqlite4OpenAllIndexes(pParse, pTab, iCur, OP_OpenWrite);
    }

    addrTop = sqlite4VdbeAddOp3(v, OP_KeySetRead, regSet, 0, regKey);

    /* Delete the row */
#ifndef SQLITE_OMIT_VIRTUALTABLE
    if( IsVirtual(pTab) ){
      const char *pVTab = (const char *)sqlite4GetVTable(db, pTab);
      sqlite4VtabMakeWritable(pParse, pTab);
      sqlite4VdbeAddOp4(v, OP_VUpdate, 0, 1, iRowid, pVTab, P4_VTAB);
      sqlite4VdbeChangeP5(v, OE_Abort);
      sqlite4MayAbort(pParse);
    }else
#endif
    {
      int count = (pParse->nested==0);    /* True to count changes */
      sqlite4GenerateRowDelete(
          pParse, pTab, iCur, regKey, count, pTrigger, OE_Default
      );
    }

    /* End of the delete loop */
    sqlite4VdbeAddOp2(v, OP_Goto, 0, addrTop);
    sqlite4VdbeJumpHere(v, addrTop);

    /* Close all open cursors */
    sqlite4CloseAllIndexes(pParse, pTab, iCur);
  }

  /* Update the sqlite_sequence table by storing the content of the
  ** maximum rowid counter values recorded while inserting into
  ** autoincrement tables.
  */
  if( pParse->nested==0 && pParse->pTriggerTab==0 ){
    sqlite4AutoincrementEnd(pParse);
  }

delete_from_cleanup:
  sqlite4AuthContextPop(&sContext);
  sqlite4SrcListDelete(db, pTabList);
  sqlite4ExprDelete(db, pWhere);
  return;
}
/* Make sure "isView" and other macros defined above are undefined. Otherwise
** thely may interfere with compilation of other functions in this file
** (or in another file, if this file becomes part of the amalgamation).  */
#ifdef isView
 #undef isView
#endif
#ifdef pTrigger
 #undef pTrigger
#endif

/*
** This routine generates VDBE code that causes a single row of a
** single table to be deleted.
**
** The VDBE must be in a particular state when this routine is called.
** These are the requirements:
**
**   1.  A read/write cursor pointing to pTab, the table containing the row
**       to be deleted, must be opened as cursor number $iCur.
**
**   2.  Read/write cursors for all indices of pTab must be open as
**       cursor number base+i for the i-th index.
**
**   3.  The record number of the row to be deleted must be stored in
**       memory cell iRowid.
**
** This routine generates code to remove both the table record and all 
** index entries that point to that record.
*/
void sqlite4GenerateRowDelete(
  Parse *pParse,                  /* Parsing context */
  Table *pTab,                    /* Table containing the row to be deleted */
  int baseCur,                    /* Base cursor number */
  int regKey,                     /* Register containing PK of row to delete */
  int bCount,                     /* True to increment the row change counter */
  Trigger *pTrigger,              /* List of triggers to (potentially) fire */
  int onconf                      /* Default ON CONFLICT policy for triggers */
){
  Vdbe *v = pParse->pVdbe;        /* Vdbe */
  int regOld = 0;                 /* First register in OLD.* array */
  int iLabel;                     /* Label resolved to end of generated code */
  int iPk;                        /* Offset of PK cursor in cursor array */
  int iPkCsr;                     /* Primary key cursor number */
  Index *pPk;                     /* Primary key index */

  /* Vdbe is guaranteed to have been allocated by this stage. */
  assert( v );

  pPk = sqlite4FindPrimaryKey(pTab, &iPk);
  iPkCsr = baseCur + iPk;

  /* Seek the PK cursor to the row to delete. If this row no longer exists 
  ** (this can happen if a trigger program has already deleted it), do
  ** not attempt to delete it or fire any DELETE triggers.  */
  iLabel = sqlite4VdbeMakeLabel(v);
  sqlite4VdbeAddOp4Int(v, OP_NotFound, iPkCsr, iLabel, regKey, 0);
 
  /* If there are any triggers to fire, allocate a range of registers to
  ** use for the old.* references in the triggers.  */
  if( sqlite4FkRequired(pParse, pTab, 0) || pTrigger ){
    u32 mask;                     /* Mask of OLD.* columns in use */
    int iCol;                     /* Iterator used while populating OLD.* */

    /* Determine which table columns may be required by either foreign key
    ** logic or triggers. This block sets stack variable mask to a 32-bit mask
    ** where bit 0 corresponds to the left-most table column, bit 1 to the
    ** second left-most, and so on. If an OLD.* column may be required, then
    ** the corresponding bit is set.
    **
    ** Or, if the table contains more than 32 columns and at least one of
    ** the columns following the 32nd is required, set mask to 0xffffffff.  */
    mask = sqlite4TriggerColmask(
        pParse, pTrigger, 0, 0, TRIGGER_BEFORE|TRIGGER_AFTER, pTab, onconf
    );
    mask |= sqlite4FkOldmask(pParse, pTab);

    /* Allocate an array of registers - one for each column in the table.
    ** Then populate those array elements that may be used by FK or trigger
    ** logic with the OLD.* values.
    **
    ** The array is (nCol+1) registers in size, where nCol is the number of
    ** columns in the table. If the table has an implicit PK, the first 
    ** register in the array contains the rowid. Otherwise, its contents are
    ** undefined.  
    */
    regOld = pParse->nMem+1;
    pParse->nMem += (pTab->nCol+1);
    for(iCol=0; iCol<pTab->nCol; iCol++){
      if( mask==0xffffffff || mask&(1<<iCol) ){
        sqlite4ExprCodeGetColumnOfTable(v, pTab, iPkCsr, iCol, regOld+iCol+1);
      }
    }
    if( pPk->aiColumn[0]<0 ){
      sqlite4VdbeAddOp2(v, OP_Rowid, iPkCsr, regOld);
    }

    /* Invoke BEFORE DELETE trigger programs. */
    sqlite4CodeRowTrigger(pParse, pTrigger, 
        TK_DELETE, 0, TRIGGER_BEFORE, pTab, regOld, onconf, iLabel
    );

    /* Seek the cursor to the row to be deleted again. It may be that
    ** the BEFORE triggers coded above have already removed the row
    ** being deleted. Do not attempt to delete the row a second time, and 
    ** do not fire AFTER triggers.  */
    sqlite4VdbeAddOp4Int(v, OP_NotFound, iPkCsr, iLabel, regKey, 0);

    /* Do FK processing. This call checks that any FK constraints that
    ** refer to this table (i.e. constraints attached to other tables) 
    ** are not violated by deleting this row.  */
    sqlite4FkCheck(pParse, pTab, regOld+1, 0);
  }

  /* Delete the index and table entries. Skip this step if pTab is really
  ** a view (in which case the only effect of the DELETE statement is to
  ** fire the INSTEAD OF triggers).  */ 
  if( pTab->pSelect==0 ){
    sqlite4GenerateRowIndexDelete(pParse, pTab, baseCur, 0);
#if 0
    if( count ){
      sqlite4VdbeChangeP4(v, -1, pTab->zName, P4_TRANSIENT);
    }
#endif
  }

  /* Do any ON CASCADE, SET NULL or SET DEFAULT operations required to
  ** handle rows (possibly in other tables) that refer via a foreign key
  ** to the row just deleted. This is a no-op if there are no configured
  ** foreign keys that use this table as a parent table.  */ 
  sqlite4FkActions(pParse, pTab, 0, regOld+1);

  /* Invoke AFTER DELETE trigger programs. */
  sqlite4CodeRowTrigger(pParse, pTrigger, 
      TK_DELETE, 0, TRIGGER_AFTER, pTab, regOld, onconf, iLabel
  );

  /* Jump here if the row had already been deleted before any BEFORE
  ** trigger programs were invoked. Or if a trigger program throws a 
  ** RAISE(IGNORE) exception.  */
  sqlite4VdbeResolveLabel(v, iLabel);
}

static void generateIndexKey(
  Parse *pParse,
  Index *pPk, int iPkCsr,
  Index *pIdx, int iIdxCsr,
  int regOut
){
  Vdbe *v = pParse->pVdbe;        /* VM to write code to */
  int nTmpReg;                    /* Number of temp registers required */
  int regTmp;                     /* First register in temp array */
  int i;                          /* Iterator variable */

  /* Allocate temp registers */
  assert( pIdx!=pPk );
  nTmpReg = pIdx->nColumn + pPk->nColumn;
  regTmp = sqlite4GetTempRange(pParse, nTmpReg);

  /* Assemble the values for the key in the array of temp registers */
  for(i=0; i<pIdx->nColumn; i++){
    int regVal = regTmp + i;
    sqlite4VdbeAddOp3(v, OP_Column, iPkCsr, pIdx->aiColumn[i], regVal);
  }
  for(i=0; i<pPk->nColumn; i++){
    int iCol = pPk->aiColumn[i];
    int regVal = pIdx->nColumn + regTmp + i;
    if( iCol<0 ){
      sqlite4VdbeAddOp2(v, OP_Rowid, iPkCsr, regVal);
    }else{
      sqlite4VdbeAddOp3(v, OP_Column, iPkCsr, pPk->aiColumn[i], regVal);
    }
  }

  /* Build the index key */
  sqlite4VdbeAddOp3(v, OP_MakeIdxKey, iIdxCsr, regTmp, regOut);

  /* Release temp registers */
  sqlite4ReleaseTempRange(pParse, regTmp, nTmpReg);
}

/*
** This routine generates VDBE code that causes the deletion of all
** index entries associated with a single row of a single table.
**
** The VDBE must be in a particular state when this routine is called.
** These are the requirements:
**
**   1.  A read/write cursor pointing to pTab, the table containing the row
**       to be deleted, must be opened as cursor number "iCur".
**
**   2.  Read/write cursors for all indices of pTab must be open as
**       cursor number iCur+i for the i-th index.
**
**   3.  The "iCur" cursor must be pointing to the row that is to be
**       deleted.
*/
void sqlite4GenerateRowIndexDelete(
  Parse *pParse,                  /* Parsing and code generating context */
  Table *pTab,                    /* Table containing the row to be deleted */
  int baseCur,                    /* Cursor number for the table */
  int *aRegIdx                    /* Only delete if (aRegIdx && aRegIdx[i]>0) */
){
  Vdbe *v = pParse->pVdbe;
  Index *pPk;
  int iPk;
  int i;
  int regKey;
  Index *pIdx;

  regKey = sqlite4GetTempReg(pParse);
  pPk = sqlite4FindPrimaryKey(pTab, &iPk);

  for(i=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, i++){
    if( pIdx!=pPk && (aRegIdx==0 || aRegIdx[i]>0) ){
      int addrNotFound;
      generateIndexKey(pParse, pPk, baseCur+iPk, pIdx, baseCur+i, regKey);
      addrNotFound = sqlite4VdbeAddOp4(v,
          OP_NotFound, baseCur+i, 0, regKey, 0, P4_INT32
      );
      sqlite4VdbeAddOp1(v, OP_Delete, baseCur+i);
      sqlite4VdbeJumpHere(v, addrNotFound);
    }
  }

  sqlite4VdbeAddOp1(v, OP_Delete, baseCur+iPk);
  sqlite4ReleaseTempReg(pParse, regKey);
}

/*
** Generate code that will assemble an index key and put it in register
** regOut.  The key with be for index pIdx which is an index on pTab.
** iCur is the index of a cursor open on the pTab table and pointing to
** the entry that needs indexing.
**
** Return a register number which is the first in a block of
** registers that holds the elements of the index key.  The
** block of registers has already been deallocated by the time
** this routine returns.
*/
int sqlite4GenerateIndexKey(
  Parse *pParse,     /* Parsing context */
  Index *pIdx,       /* The index for which to generate a key */
  int iCur,          /* Cursor number for the pIdx->pTable table */
  int regOut,        /* Write the new index key to this register */
  int doMakeRec,     /* Run the OP_MakeRecord instruction if true */
  int iIdxCur        /* Index cursor number.  Only needed with doMakeRec */
){
  Vdbe *v = pParse->pVdbe;
  int j;
  Table *pTab = pIdx->pTable;
  int regBase;
  int nCol;

  nCol = pIdx->nColumn;
  regBase = sqlite4GetTempRange(pParse, nCol+1);
  sqlite4VdbeAddOp2(v, OP_Rowid, iCur, regBase+nCol);
  for(j=0; j<nCol; j++){
    int idx = pIdx->aiColumn[j];
    if( idx<0 ){
      sqlite4VdbeAddOp2(v, OP_SCopy, regBase+nCol, regBase+j);
    }else{
      sqlite4VdbeAddOp3(v, OP_Column, iCur, idx, regBase+j);
      sqlite4ColumnDefault(v, pTab, idx, -1);
    }
  }
  if( doMakeRec ){
    const char *zAff;
    if( pTab->pSelect || (pParse->db->flags & SQLITE_IdxRealAsInt)!=0 ){
      zAff = 0;
    }else{
      zAff = sqlite4IndexAffinityStr(v, pIdx);
    }
    sqlite4VdbeAddOp2(v, OP_MakeKey, iIdxCur, regOut+1);
    sqlite4VdbeAddOp3(v, OP_MakeRecord, regBase, nCol+1, regOut);
    sqlite4VdbeChangeP4(v, -1, zAff, P4_TRANSIENT);
  }
  sqlite4ReleaseTempRange(pParse, regBase, nCol+1);
  return regBase;
}
