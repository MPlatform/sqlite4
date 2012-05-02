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
** to handle UPDATE statements.
*/
#include "sqliteInt.h"

#ifndef SQLITE_OMIT_VIRTUALTABLE
/* Forward declaration */
static void updateVirtualTable(
  Parse *pParse,       /* The parsing context */
  SrcList *pSrc,       /* The virtual table to be modified */
  Table *pTab,         /* The virtual table */
  ExprList *pChanges,  /* The columns to change in the UPDATE statement */
  Expr *pRowidExpr,    /* Expression used to recompute the rowid */
  int *aXRef,          /* Mapping from columns of pTab to entries in pChanges */
  Expr *pWhere,        /* WHERE clause of the UPDATE statement */
  int onError          /* ON CONFLICT strategy */
);
#endif /* SQLITE_OMIT_VIRTUALTABLE */

/*
** The most recently coded instruction was an OP_Column to retrieve the
** i-th column of table pTab. This routine sets the P4 parameter of the 
** OP_Column to the default value, if any.
**
** The default value of a column is specified by a DEFAULT clause in the 
** column definition. This was either supplied by the user when the table
** was created, or added later to the table definition by an ALTER TABLE
** command. If the latter, then the row-records in the table btree on disk
** may not contain a value for the column and the default value, taken
** from the P4 parameter of the OP_Column instruction, is returned instead.
** If the former, then all row-records are guaranteed to include a value
** for the column and the P4 value is not required.
**
** Column definitions created by an ALTER TABLE command may only have 
** literal default values specified: a number, null or a string. (If a more
** complicated default expression value was provided, it is evaluated 
** when the ALTER TABLE is executed and one of the literal values written
** into the sqlite_master table.)
**
** Therefore, the P4 parameter is only required if the default value for
** the column is a literal number, string or null. The sqlite4ValueFromExpr()
** function is capable of transforming these types of expressions into
** sqlite4_value objects.
**
** If parameter iReg is not negative, code an OP_RealAffinity instruction
** on register iReg. This is used when an equivalent integer value is 
** stored in place of an 8-byte floating point value in order to save 
** space.
*/
void sqlite4ColumnDefault(Vdbe *v, Table *pTab, int i, int iReg){
  assert( pTab!=0 );
  if( !pTab->pSelect ){
    sqlite4_value *pValue;
    u8 enc = ENC(sqlite4VdbeDb(v));
    Column *pCol = &pTab->aCol[i];
    VdbeComment((v, "%s.%s", pTab->zName, pCol->zName));
    assert( i<pTab->nCol );
    sqlite4ValueFromExpr(sqlite4VdbeDb(v), pCol->pDflt, enc, 
                         pCol->affinity, &pValue);
    if( pValue ){
      sqlite4VdbeChangeP4(v, -1, (const char *)pValue, P4_MEM);
    }
#ifndef SQLITE_OMIT_FLOATING_POINT
    if( iReg>=0 && pTab->aCol[i].affinity==SQLITE_AFF_REAL ){
      sqlite4VdbeAddOp1(v, OP_RealAffinity, iReg);
    }
#endif
  }
}

/*
** Process an UPDATE statement.
**
**   UPDATE OR IGNORE table_wxyz SET a=b, c=d WHERE e<5 AND f NOT NULL;
**          \_______/ \________/     \______/       \________________/
*            onError   pSrc          pChanges             pWhere
*/
void sqlite4Update(
  Parse *pParse,         /* The parser context */
  SrcList *pSrc,         /* The table in which we should change things */
  ExprList *pChanges,    /* Things to be changed */
  Expr *pWhere,          /* The WHERE clause.  May be null */
  int onError            /* How to handle constraint errors */
){
  int i, j;              /* Loop counters */
  Table *pTab;           /* The table to be updated */
  int addr = 0;          /* VDBE instruction address of the start of the loop */
  WhereInfo *pWInfo;     /* Information about the WHERE clause */
  Vdbe *v;               /* The virtual database engine */
  Index *pIdx;           /* Iterator variable */
  int nIdx;              /* Total number of indexes on table (incl. PK) */
  int iCur;              /* VDBE Cursor number of pTab */
  sqlite4 *db;           /* The database structure */
  int *aRegIdx = 0;      /* One register assigned to each index to be updated */
  int *aXRef = 0;        /* aXRef[i] is the index in pChanges->a[] of the
                         ** an expression for the i-th column of the table.
                         ** aXRef[i]==-1 if the i-th column is not changed. */
  AuthContext sContext;  /* The authorization context */
  NameContext sNC;       /* The name-context to resolve expressions in */
  int iDb;               /* Database containing the table being updated */
  int okOnePass;         /* True for one-pass algorithm without the FIFO */
  int hasFK;             /* True if foreign key processing is required */

#ifndef SQLITE_OMIT_TRIGGER
  int isView;            /* True when updating a view (INSTEAD OF trigger) */
  Trigger *pTrigger;     /* List of triggers on pTab, if required */
  int tmask;             /* Mask of TRIGGER_BEFORE|TRIGGER_AFTER */
#endif
  int newmask;           /* Mask of NEW.* columns accessed by BEFORE triggers */

  int regOldKey;                  /* Register containing the original PK */
  int regNew;                     /* Content of the NEW.* table in triggers */
  int regOld = 0;                 /* Content of OLD.* table in triggers */
  int regKeySet = 0;              /* Register containing KeySet object */
  Index *pPk = 0;                 /* The primary key index of this table */
  int iPk = 0;                    /* Offset of primary key in aRegIdx[] */
  int bChngPk = 0;                /* True if any PK columns are updated */
  int bOpenAll = 0;               /* True if all indexes were opened */
  int bImplicitPk = 0;            /* True if pTab has an implicit PK */
  int regOldTr = 0;               /* Content of OLD.* table including IPK */
  int regNewTr = 0;               /* Content of NEW.* table including IPK */

  memset(&sContext, 0, sizeof(sContext));
  db = pParse->db;
  if( pParse->nErr || db->mallocFailed ){
    goto update_cleanup;
  }
  assert( pSrc->nSrc==1 );

  /* Locate and analyze the table to be updated. This block sets:
  **
  **   pTab
  **   iDb
  **   pPk
  **   bImplicitPk
  */
  pTab = sqlite4SrcListLookup(pParse, pSrc);
  if( pTab==0 ) goto update_cleanup;
  iDb = sqlite4SchemaToIndex(pParse->db, pTab->pSchema);
  if( IsView(pTab)==0 ){
    pPk = sqlite4FindPrimaryKey(pTab, &iPk);
    bImplicitPk = (pPk->aiColumn[0]<0);
  }

  /* Figure out if we have any triggers and if the table being
  ** updated is a view.
  */
#ifndef SQLITE_OMIT_TRIGGER
  pTrigger = sqlite4TriggersExist(pParse, pTab, TK_UPDATE, pChanges, &tmask);
  isView = pTab->pSelect!=0;
  assert( pTrigger || tmask==0 );
#else
# define pTrigger 0
# define isView 0
# define tmask 0
#endif
#ifdef SQLITE_OMIT_VIEW
# undef isView
# define isView 0
#endif

  if( sqlite4ViewGetColumnNames(pParse, pTab) ) goto update_cleanup;
  if( sqlite4IsReadOnly(pParse, pTab, tmask) ) goto update_cleanup;

  aXRef = sqlite4DbMallocRaw(db, sizeof(int) * pTab->nCol );
  if( aXRef==0 ) goto update_cleanup;
  for(i=0; i<pTab->nCol; i++) aXRef[i] = -1;

  /* Allocate a cursors for the main database table and for all indices.
  ** The index cursors might not be used, but if they are used they
  ** need to occur right after the database cursor.  So go ahead and
  ** allocate enough space, just in case.  */
  iCur = pParse->nTab;
  pSrc->a[0].iCursor = iCur+iPk;
  for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
    pParse->nTab++;
  }
  if( IsView(pTab) ) pParse->nTab++;

  /* Initialize the name-context */
  memset(&sNC, 0, sizeof(sNC));
  sNC.pParse = pParse;
  sNC.pSrcList = pSrc;

  /* Resolve the column names in all the expressions of the of the UPDATE 
  ** statement. Also find the column index for each column to be updated in 
  ** the pChanges array.  For each column to be updated, make sure we have
  ** authorization to change that column.  
  **
  ** Also, if any columns that are part of the tables primary key are
  ** to be modified, set the bChngPk variable to true. This is significant
  ** because if the primary key changes, *all* index entries need to be
  ** replaced (not just those that index modified columns).  */
  for(i=0; i<pChanges->nExpr; i++){
    int iPkCol;                      /* To iterate through PK columns */

    /* Resolve any names in the expression for this assignment */
    if( sqlite4ResolveExprNames(&sNC, pChanges->a[i].pExpr) ){
      goto update_cleanup;
    }

    /* Resolve the column name on the left of the assignment */
    for(j=0; j<pTab->nCol; j++){
      if( sqlite4StrICmp(pTab->aCol[j].zName, pChanges->a[i].zName)==0 ) break;
    }
    if( j==pTab->nCol ){
      sqlite4ErrorMsg(pParse, "no such column: %s", pChanges->a[i].zName);
      pParse->checkSchema = 1;
      goto update_cleanup;
    }
    aXRef[j] = i;

    /* Check if this column is part of the primary key. If so, set bChngPk. */
    if( !IsView(pTab) ){
      for(iPkCol=0; iPkCol<pPk->nColumn; iPkCol++){
        if( pPk->aiColumn[iPkCol]==j ) bChngPk = 1;
      }
    }

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
      int rc;
      rc = sqlite4AuthCheck(pParse, SQLITE_UPDATE, pTab->zName,
                           pTab->aCol[j].zName, db->aDb[iDb].zName);
      if( rc==SQLITE_DENY ){
        goto update_cleanup;
      }else if( rc==SQLITE_IGNORE ){
        aXRef[j] = -1;
      }
    }
#endif
  }

  /* Begin generating code. */
  v = sqlite4GetVdbe(pParse);
  if( v==0 ) goto update_cleanup;
  if( pParse->nested==0 ) sqlite4VdbeCountChanges(v);
  sqlite4BeginWriteOperation(pParse, 1, iDb);

#ifndef SQLITE_OMIT_VIRTUALTABLE
  /* TODO: This is currently broken */
  /* Virtual tables must be handled separately */
  if( IsVirtual(pTab) ){
    updateVirtualTable(pParse, pSrc, pTab, pChanges, 0, aXRef, pWhere, onError);
    pWhere = 0;
    pSrc = 0;
    goto update_cleanup;
  }
#endif

  hasFK = sqlite4FkRequired(pParse, pTab, aXRef);

  /* Allocate memory for the array aRegIdx[].  There is one entry in the
  ** array for each index associated with table being updated.  Fill in
  ** the value with a register number for indices that are to be used
  ** and with zero for unused indices.  */
  for(nIdx=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, nIdx++){}
  aRegIdx = sqlite4DbMallocZero(db, sizeof(Index*) * nIdx );
  if( aRegIdx==0 ) goto update_cleanup;

  /* Allocate registers for and populate the aRegIdx array. */
  for(j=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, j++){
    if( pIdx==pPk || hasFK || bChngPk ){
      aRegIdx[j] = ++pParse->nMem;
    }else{
      for(i=0; i<pIdx->nColumn; i++){
        if( aXRef[pIdx->aiColumn[i]]>=0 ){
          aRegIdx[j] = ++pParse->nMem;
          break;
        }
      }
    }
  }

  /* Allocate other required registers. Specifically:
  **
  **     regKeySet:     1 register
  **     regOldKey:     1 register
  **     regOldTr:      nCol+1 registers
  **     regNewTr:      nCol+1 registers
  **
  ** The regOldTr allocation is only required if there are either triggers 
  ** or foreign keys to be processed.
  **
  ** The regOldTr and regNewTr register arrays include space for the 
  ** implicit primary key value if the table in question does not have an
  ** explicit PRIMARY KEY.
  */
  regKeySet = ++pParse->nMem;
  regOldKey = ++pParse->nMem;
  if( pTrigger || hasFK ){
    regOldTr = pParse->nMem + 1;
    regOld = regOldTr+1;
    pParse->nMem += (pTab->nCol + 1);
  }
  regNewTr = pParse->nMem + 1;
  regNew = regNewTr+1;
  pParse->nMem += (pTab->nCol+1);

  /* Start the view context. */
  if( isView ){
    sqlite4AuthContextPush(pParse, &sContext, pTab->zName);
  }

  /* If we are trying to update a view, realize that view into
  ** a ephemeral table.
  */
#if !defined(SQLITE_OMIT_VIEW) && !defined(SQLITE_OMIT_TRIGGER)
  if( isView ){
    sqlite4MaterializeView(pParse, pTab, pWhere, iCur);
  }
#endif

  /* Resolve the column names in all the expressions in the
  ** WHERE clause.
  */
  if( sqlite4ResolveExprNames(&sNC, pWhere) ){
    goto update_cleanup;
  }

  /* This block codes a loop that iterates through all rows of the table
  ** identified by the UPDATE statements WHERE clause. The primary key
  ** of each row visited by the loop is added to the KeySet object stored
  ** in register regKeySet.
  **
  ** There is one exception to the above: If static analysis of the WHERE 
  ** clause indicates that the loop will visit at most one row, then the
  ** KeySet object is bypassed and the primary key of the single row (if
  ** any) left in register regOldKey. This is called the "one-pass"
  ** approach. Set okOnePass to true if it can be used in this case.  */
  sqlite4VdbeAddOp3(v, OP_Null, 0, regKeySet, regOldKey);
  pWInfo = sqlite4WhereBegin(pParse, pSrc, pWhere, 0, 0, WHERE_ONEPASS_DESIRED);
  if( pWInfo==0 ) goto update_cleanup;
  okOnePass = pWInfo->okOnePass;
  sqlite4VdbeAddOp2(v, OP_RowKey, iCur+iPk, regOldKey);
  if( !okOnePass ){
    sqlite4VdbeAddOp3(v, OP_KeySetAdd, regKeySet, 0, regOldKey);
  }
  sqlite4WhereEnd(pWInfo);

  /* Open every index that needs updating. If any index could potentially 
  ** invoke a REPLACE conflict resolution action, then we need to open all 
  ** indices because we might need to be deleting some records.  */
  if( !isView ){
    /* Set bOpenAll to true if this UPDATE might strike a REPLACE */
    bOpenAll = (onError==OE_Replace);
    for(i=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, i++){
      if( aRegIdx[i] && pIdx->onError==OE_Replace ) bOpenAll = 1;
    }

    /* If bOpenAll is true, open all indexes. Otherwise, just open those
    ** indexes for which the corresponding aRegIdx[] entry is non-zero
    ** (those that index columns that will be modified by this UPDATE
    ** statement). Also, if the one-pass approach is being used, do not
    ** open the primary key index here - it is already open.  */
    for(i=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, i++){
      if( (bOpenAll || aRegIdx[i]) && (okOnePass==0 || pIdx!=pPk) ){
        sqlite4OpenIndex(pParse, iCur+i, iDb, pIdx, OP_OpenWrite);
      }
    }
  }

  /* The next instruction coded is the top of the update loop (executed once
  ** for each row to be updated). 
  **
  ** If okOnePass is true, then regOldKey either contains the encoded PK of 
  ** the row to update, or it is NULL (indicating that this statement will 
  ** update zero rows). If this is the case, jump to the end of the loop 
  ** without doing anything. Otherwise - if okOnePass is true and regOldKey 
  ** contains something other than NULL - proceed.
  **
  ** Or, if okOnePass is false, then the KeySet object stored in register
  ** regKeySet contains the set of encoded PKs for the rows that will
  ** be updated by this statement. Read the next one into register regOldKey.
  ** Or, if the KeySet is already empty, jump to the end of the loop.
  */
  if( okOnePass ){
    int a1 = sqlite4VdbeAddOp1(v, OP_NotNull, regOldKey);
    addr = sqlite4VdbeAddOp0(v, OP_Goto);
    sqlite4VdbeJumpHere(v, a1);
  }else{
    addr = sqlite4VdbeAddOp3(v, OP_KeySetRead, regKeySet, 0, regOldKey);
  }

  /* Make cursor iCur point to the record that is being updated. If
  ** this record does not exist for some reason (deleted by a trigger,
  ** for example, then jump to the next iteration of the KeySet loop. 
  ** TODO: If okOnePass is true, does iCur already point to this record? */
  sqlite4VdbeAddOp4(v, OP_NotFound, iCur+iPk, addr, regOldKey, 0, P4_INT32);

  /* If there are triggers on this table, populate an array of registers 
  ** with the required old.* column data.  */
  if( hasFK || pTrigger ){
    u32 oldmask = (hasFK ? sqlite4FkOldmask(pParse, pTab) : 0);
    oldmask |= sqlite4TriggerColmask(pParse, 
        pTrigger, pChanges, 0, TRIGGER_BEFORE|TRIGGER_AFTER, pTab, onError
    );

    if( bImplicitPk ){
      sqlite4VdbeAddOp2(v, OP_Rowid, iCur+iPk, regOldTr);
    }
    for(i=0; i<pTab->nCol; i++){
      if( aXRef[i]<0 || oldmask==0xffffffff || (i<32 && (oldmask & (1<<i))) ){
        sqlite4ExprCodeGetColumnOfTable(v, pTab, iCur+iPk, i, regOld+i);
      }else{
        sqlite4VdbeAddOp2(v, OP_Null, 0, regOld+i);
      }
    }
  }

  /* Populate the array of registers beginning at regNew with the new
  ** row data. This array is used to check constaints, create the new
  ** table and index records, and as the values for any new.* references
  ** made by triggers.
  **
  ** If there are one or more BEFORE triggers, then do not populate the
  ** registers associated with columns that are (a) not modified by
  ** this UPDATE statement and (b) not accessed by new.* references. The
  ** values for registers not modified by the UPDATE must be reloaded from 
  ** the database after the BEFORE triggers are fired anyway (as the trigger 
  ** may have modified them). So not loading those that are not going to
  ** be used eliminates some redundant opcodes.
  */
  newmask = sqlite4TriggerColmask(
      pParse, pTrigger, pChanges, 1, TRIGGER_BEFORE, pTab, onError
  );
  sqlite4VdbeAddOp3(v, OP_Null, 0, regNew, regNew+pTab->nCol-1);
  for(i=0; i<pTab->nCol; i++){
    j = aXRef[i];
    if( j>=0 ){
      sqlite4ExprCode(pParse, pChanges->a[j].pExpr, regNew+i);
    }else if( 0==(tmask&TRIGGER_BEFORE) || i>31 || (newmask&(1<<i)) ){
      /* This branch loads the value of a column that will not be changed 
       ** into a register. This is done if there are no BEFORE triggers, or
       ** if there are one or more BEFORE triggers that use this value via
       ** a new.* reference in a trigger program.
       */
      testcase( i==31 );
      testcase( i==32 );
      sqlite4VdbeAddOp3(v, OP_Column, iCur+iPk, i, regNew+i);
      sqlite4ColumnDefault(v, pTab, i, regNew+i);
    }
  }
  if( bImplicitPk ){
    sqlite4VdbeAddOp2(v, OP_Rowid, iCur+iPk, regNew-1);
  }

  /* Fire any BEFORE UPDATE triggers. This happens before constraints are
  ** verified. One could argue that this is wrong.
  */
  if( tmask&TRIGGER_BEFORE ){
    sqlite4VdbeAddOp2(v, OP_Affinity, regNew, pTab->nCol);
    sqlite4TableAffinityStr(v, pTab);
    sqlite4CodeRowTrigger(pParse, pTrigger, TK_UPDATE, pChanges, 
        TRIGGER_BEFORE, pTab, regOldTr, onError, addr);

    /* The row-trigger may have deleted the row being updated. In this
    ** case, jump to the next row. No updates or AFTER triggers are 
    ** required. This behaviour - what happens when the row being updated
    ** is deleted or renamed by a BEFORE trigger - is left undefined in the
    ** documentation.
    */
    sqlite4VdbeAddOp4Int(v, OP_NotFound, iCur+iPk, addr, regOldKey, 0);

    /* If it did not delete it, the row-trigger may still have modified 
    ** some of the columns of the row being updated. Load the values for 
    ** all columns not modified by the update statement into their 
    ** registers in case this has happened.
    */
    for(i=0; i<pTab->nCol; i++){
      if( aXRef[i]<0 ){
        sqlite4VdbeAddOp3(v, OP_Column, iCur+iPk, i, regNew+i);
        sqlite4ColumnDefault(v, pTab, i, regNew+i);
      }
    }
  }

  if( !isView ){
    int j1;                       /* Address of jump instruction */

    /* Do constraint checks. */
    assert( bChngPk==0 || bImplicitPk==0 );
    if( bChngPk==0 ) aRegIdx[iPk] = 0;
    sqlite4GenerateConstraintChecks(
        pParse, pTab, iCur, regNew, aRegIdx, regOldKey, 1, onError, addr, 0
    );
    if( bChngPk==0 ) aRegIdx[iPk] = regOldKey;

    /* Do FK constraint checks. */
    if( hasFK ){
      sqlite4FkCheck(pParse, pTab, regOld, 0);
    }

    /* Delete the index entries associated with the current record.  */
    j1 = sqlite4VdbeAddOp4(v, OP_NotFound, iCur+iPk, 0, regOldKey, 0, P4_INT32);
    sqlite4GenerateRowIndexDelete(pParse, pTab, 0, iCur, aRegIdx);
  
    /* Delete the old record */
    if( hasFK || bChngPk ){
      sqlite4VdbeAddOp2(v, OP_Delete, iCur, 0);
    }
    sqlite4VdbeJumpHere(v, j1);

    if( hasFK ){
      sqlite4FkCheck(pParse, pTab, 0, regNew);
    }
  
    /* Insert the new index entries and the new record. */
    sqlite4CompleteInsertion(pParse, pTab, iCur, regNew, aRegIdx, 1, 0, 0);

    /* Do any ON CASCADE, SET NULL or SET DEFAULT operations required to
    ** handle rows (possibly in other tables) that refer via a foreign key
    ** to the row just updated. */ 
    if( hasFK ){
      sqlite4FkActions(pParse, pTab, pChanges, regOldTr);
    }
  }

  sqlite4CodeRowTrigger(pParse, pTrigger, TK_UPDATE, pChanges, 
      TRIGGER_AFTER, pTab, regOldTr, onError, addr);

  /* Repeat the above with the next record to be updated, until
  ** all record selected by the WHERE clause have been updated.
  */
  sqlite4VdbeAddOp2(v, OP_Goto, 0, addr);
  sqlite4VdbeJumpHere(v, addr);

  /* Close all cursors */
  for(i=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, i++){
    assert( aRegIdx );
    if( bOpenAll || aRegIdx[i] ){
      sqlite4VdbeAddOp2(v, OP_Close, iCur+i, 0);
    }
  }

  /* Update the sqlite_sequence table by storing the content of the
  ** maximum rowid counter values recorded while inserting into
  ** autoincrement tables.
  */
  if( pParse->nested==0 && pParse->pTriggerTab==0 ){
    sqlite4AutoincrementEnd(pParse);
  }

update_cleanup:
  sqlite4AuthContextPop(&sContext);
  sqlite4DbFree(db, aRegIdx);
  sqlite4DbFree(db, aXRef);
  sqlite4SrcListDelete(db, pSrc);
  sqlite4ExprListDelete(db, pChanges);
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

#ifndef SQLITE_OMIT_VIRTUALTABLE
/*
** Generate code for an UPDATE of a virtual table.
**
** The strategy is that we create an ephemerial table that contains
** for each row to be changed:
**
**   (A)  The original rowid of that row.
**   (B)  The revised rowid for the row. (note1)
**   (C)  The content of every column in the row.
**
** Then we loop over this ephemeral table and for each row in
** the ephermeral table call VUpdate.
**
** When finished, drop the ephemeral table.
**
** (note1) Actually, if we know in advance that (A) is always the same
** as (B) we only store (A), then duplicate (A) when pulling
** it out of the ephemeral table before calling VUpdate.
*/
static void updateVirtualTable(
  Parse *pParse,       /* The parsing context */
  SrcList *pSrc,       /* The virtual table to be modified */
  Table *pTab,         /* The virtual table */
  ExprList *pChanges,  /* The columns to change in the UPDATE statement */
  Expr *pRowid,        /* Expression used to recompute the rowid */
  int *aXRef,          /* Mapping from columns of pTab to entries in pChanges */
  Expr *pWhere,        /* WHERE clause of the UPDATE statement */
  int onError          /* ON CONFLICT strategy */
){
  Vdbe *v = pParse->pVdbe;  /* Virtual machine under construction */
  ExprList *pEList = 0;     /* The result set of the SELECT statement */
  Select *pSelect = 0;      /* The SELECT statement */
  Expr *pExpr;              /* Temporary expression */
  int ephemTab;             /* Table holding the result of the SELECT */
  int i;                    /* Loop counter */
  int addr;                 /* Address of top of loop */
  int iReg;                 /* First register in set passed to OP_VUpdate */
  sqlite4 *db = pParse->db; /* Database connection */
  const char *pVTab = (const char*)sqlite4GetVTable(db, pTab);
  SelectDest dest;

  /* Construct the SELECT statement that will find the new values for
  ** all updated rows. 
  */
  pEList = sqlite4ExprListAppend(pParse, 0, sqlite4Expr(db, TK_ID, "_rowid_"));
  if( pRowid ){
    pEList = sqlite4ExprListAppend(pParse, pEList,
                                   sqlite4ExprDup(db, pRowid, 0));
  }
  assert( pTab->iPKey<0 );
  for(i=0; i<pTab->nCol; i++){
    if( aXRef[i]>=0 ){
      pExpr = sqlite4ExprDup(db, pChanges->a[aXRef[i]].pExpr, 0);
    }else{
      pExpr = sqlite4Expr(db, TK_ID, pTab->aCol[i].zName);
    }
    pEList = sqlite4ExprListAppend(pParse, pEList, pExpr);
  }
  pSelect = sqlite4SelectNew(pParse, pEList, pSrc, pWhere, 0, 0, 0, 0, 0, 0);
  
  /* Create the ephemeral table into which the update results will
  ** be stored.
  */
  assert( v );
  ephemTab = pParse->nTab++;
  sqlite4VdbeAddOp2(v, OP_OpenEphemeral, ephemTab, pTab->nCol+1+(pRowid!=0));

  /* fill the ephemeral table 
  */
  sqlite4SelectDestInit(&dest, SRT_Table, ephemTab);
  sqlite4Select(pParse, pSelect, &dest);

  /* Generate code to scan the ephemeral table and call VUpdate. */
  iReg = ++pParse->nMem;
  pParse->nMem += pTab->nCol+1;
  addr = sqlite4VdbeAddOp2(v, OP_Rewind, ephemTab, 0);
  sqlite4VdbeAddOp3(v, OP_Column,  ephemTab, 0, iReg);
  sqlite4VdbeAddOp3(v, OP_Column, ephemTab, (pRowid?1:0), iReg+1);
  for(i=0; i<pTab->nCol; i++){
    sqlite4VdbeAddOp3(v, OP_Column, ephemTab, i+1+(pRowid!=0), iReg+2+i);
  }
  sqlite4VtabMakeWritable(pParse, pTab);
  sqlite4VdbeAddOp4(v, OP_VUpdate, 0, pTab->nCol+2, iReg, pVTab, P4_VTAB);
  sqlite4VdbeChangeP5(v, onError==OE_Default ? OE_Abort : onError);
  sqlite4MayAbort(pParse);
  sqlite4VdbeAddOp2(v, OP_Next, ephemTab, addr+1);
  sqlite4VdbeJumpHere(v, addr);
  sqlite4VdbeAddOp2(v, OP_Close, ephemTab, 0);

  /* Cleanup */
  sqlite4SelectDelete(db, pSelect);  
}
#endif /* SQLITE_OMIT_VIRTUALTABLE */
