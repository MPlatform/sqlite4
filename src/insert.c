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
** to handle INSERT statements in SQLite.
*/
#include "sqliteInt.h"

/*
** Generate code that will open a table for reading.
*/
void sqlite4OpenTable(
  Parse *p,       /* Generate code into this VDBE */
  int iCur,       /* The cursor number of the table */
  int iDb,        /* The database index in sqlite4.aDb[] */
  Table *pTab,    /* The table to be opened */
  int opcode      /* OP_OpenRead or OP_OpenWrite */
){
  assert( 0 );
}

/*
** Open VDBE cursor iCur to access index pIdx. pIdx is guaranteed to be
** a part of database iDb.
*/
void sqlite4OpenIndex(
  Parse *p,                       /* Current parser context */
  int iCur,                       /* The cursor number of the cursor to open */
  int iDb,                        /* The database index in sqlite4.aDb[] */
  Index *pIdx,                    /* The index to be opened */
  int opcode                      /* OP_OpenRead or OP_OpenWrite */
){
  KeyInfo *pKey;                /* KeyInfo structure describing PK index */
  Vdbe *v;                      /* VM to write code into */

  assert( opcode==OP_OpenWrite || opcode==OP_OpenRead );
  assert( pIdx->tnum>0 );

  v = sqlite4GetVdbe(p);
  pKey = sqlite4IndexKeyinfo(p, pIdx);
  testcase( pKey==0 );

  sqlite4VdbeAddOp3(v, opcode, iCur, pIdx->tnum, iDb);
  sqlite4VdbeChangeP4(v, -1, (const char *)pKey, P4_KEYINFO_HANDOFF);
  VdbeComment((v, "%s", pIdx->zName));
}

/*
** Generate code that will open the primary key of a table for either 
** reading (if opcode==OP_OpenRead) or writing (if opcode==OP_OpenWrite).
*/
void sqlite4OpenPrimaryKey(
  Parse *p,                       /* Current parser context */
  int iCur,                       /* The cursor number of the cursor to open */
  int iDb,                        /* The database index in sqlite4.aDb[] */
  Table *pTab,                    /* The table to be opened */
  int opcode                      /* OP_OpenRead or OP_OpenWrite */
){
  assert( opcode==OP_OpenWrite || opcode==OP_OpenRead );
  if( IsVirtual(pTab)==0 ){
    Index *pIdx;                  /* PRIMARY KEY index for table pTab */

    pIdx = sqlite4FindPrimaryKey(pTab, 0);
    sqlite4TableLock(p, iDb, pIdx->tnum, (opcode==OP_OpenWrite), pTab->zName);
    sqlite4OpenIndex(p, iCur, iDb, pIdx, opcode);
    assert( pIdx->eIndexType==SQLITE_INDEX_PRIMARYKEY );
  }
}

/*
** Return a pointer to the column affinity string associated with index
** pIdx. A column affinity string has one character for each column in 
** the index key. If the index is the PRIMARY KEY of its table, the key
** consists of the index columns only. Otherwise, it consists of the
** indexed columns, followed by the columns that make up the tables PRIMARY
** KEY. For each column in the index key, the corresponding character of
** the affinity string is set according to the column affinity, as follows:
**
**  Character      Column affinity
**  ------------------------------
**  'a'            TEXT
**  'b'            NONE
**  'c'            NUMERIC
**  'd'            INTEGER
**  'e'            REAL
**
** Memory for the buffer containing the column index affinity string
** is managed along with the rest of the Index structure. It will be
** released when sqlite4DeleteIndex() is called.
*/
const char *sqlite4IndexAffinityStr(Vdbe *v, Index *pIdx){
  /* The first time a column affinity string for a particular index is
  ** required, it is allocated and populated here. It is then stored as
  ** a member of the Index structure for subsequent use. The column 
  ** affinity string will eventually be deleted by sqliteDeleteIndex() 
  ** when the Index structure itself is cleaned up.  */
  if( !pIdx->zColAff ){
    sqlite4 *db = sqlite4VdbeDb(v);
    Table *pTab = pIdx->pTable;   /* Table pIdx is attached to */
    int n;                        /* Iterator variable for zAff */
    Index *pPk;                   /* Primary key on same table as pIdx */
    Index *p;                     /* Iterator variable */
    char *zAff;                   /* Affinity string to populate and return */
    int nAff;                     /* Characters in zAff */

    /* Determine how many characters are in the affinity string. There is
    ** one character for each indexed column, and, if the index is not itself
    ** the primary key, one character for each column in the primary key
    ** of the table pIdx indexes.  */ 
    nAff = pIdx->nColumn;
    pPk = sqlite4FindPrimaryKey(pTab, 0);
    if( pIdx!=pPk ){
      nAff += pPk->nColumn;
    }

    /* Allocate space for the affinity string */
    zAff = pIdx->zColAff = (char *)sqlite4DbMallocRaw(0, nAff+1);
    if( !zAff ){
      db->mallocFailed = 1;
      return 0;
    }

    /* Populate the affinity string. This loop runs either once or twice.
    ** The first iteration populates zAff with affinities according to the
    ** columns indexed by pIdx.  If pIdx is not itself the table's primary 
    ** key, then the second iteration of the loop adds the primary key 
    ** columns to zAff.  */
    for(n=0, p=pIdx; p; p=(p==pPk ? 0 : pPk)){
      int i;
      for(i=0; i<p->nColumn; i++){
        int iCol = p->aiColumn[i];
        zAff[n++] = (iCol<0) ? SQLITE_AFF_INTEGER : pTab->aCol[iCol].affinity;
      }
    }
    zAff[n] = 0;
  }
 
  return pIdx->zColAff;
}

/*
** Set P4 of the most recently inserted opcode to a column affinity
** string for table pTab. A column affinity string has one character
** for each column indexed by the index, according to the affinity of the
** column:
**
**  Character      Column affinity
**  ------------------------------
**  'a'            TEXT
**  'b'            NONE
**  'c'            NUMERIC
**  'd'            INTEGER
**  'e'            REAL
*/
void sqlite4TableAffinityStr(Vdbe *v, Table *pTab){
  /* The first time a column affinity string for a particular table
  ** is required, it is allocated and populated here. It is then 
  ** stored as a member of the Table structure for subsequent use.
  **
  ** The column affinity string will eventually be deleted by
  ** sqlite4DeleteTable() when the Table structure itself is cleaned up.
  */
  if( !pTab->zColAff ){
    char *zColAff;
    int i;
    sqlite4 *db = sqlite4VdbeDb(v);

    zColAff = (char *)sqlite4DbMallocRaw(0, pTab->nCol+1);
    if( !zColAff ){
      db->mallocFailed = 1;
      return;
    }

    for(i=0; i<pTab->nCol; i++){
      zColAff[i] = pTab->aCol[i].affinity;
    }
    zColAff[pTab->nCol] = '\0';

    pTab->zColAff = zColAff;
  }

  sqlite4VdbeChangeP4(v, -1, pTab->zColAff, P4_TRANSIENT);
}

/*
** Return non-zero if the table pTab in database iDb or any of its indices
** have been opened at any point in the VDBE program beginning at location
** iStartAddr throught the end of the program.  This is used to see if 
** a statement of the form  "INSERT INTO <iDb, pTab> SELECT ..." can 
** run without using temporary table for the results of the SELECT. 
*/
static int readsTable(Parse *p, int iStartAddr, int iDb, Table *pTab){
  Vdbe *v = sqlite4GetVdbe(p);
  int i;
  int iEnd = sqlite4VdbeCurrentAddr(v);
#ifndef SQLITE_OMIT_VIRTUALTABLE
  VTable *pVTab = IsVirtual(pTab) ? sqlite4GetVTable(p->db, pTab) : 0;
#endif

  for(i=iStartAddr; i<iEnd; i++){
    VdbeOp *pOp = sqlite4VdbeGetOp(v, i);
    assert( pOp!=0 );
    if( pOp->opcode==OP_OpenRead && pOp->p3==iDb ){
      Index *pIndex;
      int tnum = pOp->p2;
      for(pIndex=pTab->pIndex; pIndex; pIndex=pIndex->pNext){
        if( tnum==pIndex->tnum ){
          return 1;
        }
      }
    }
#ifndef SQLITE_OMIT_VIRTUALTABLE
    if( pOp->opcode==OP_VOpen && pOp->p4.pVtab==pVTab ){
      assert( pOp->p4.pVtab!=0 );
      assert( pOp->p4type==P4_VTAB );
      return 1;
    }
#endif
  }
  return 0;
}

#ifndef SQLITE_OMIT_AUTOINCREMENT
/*
** Locate or create an AutoincInfo structure associated with table pTab
** which is in database iDb.  Return the register number for the register
** that holds the maximum rowid.
**
** There is at most one AutoincInfo structure per table even if the
** same table is autoincremented multiple times due to inserts within
** triggers.  A new AutoincInfo structure is created if this is the
** first use of table pTab.  On 2nd and subsequent uses, the original
** AutoincInfo structure is used.
**
** Three memory locations are allocated:
**
**   (1)  Register to hold the name of the pTab table.
**   (2)  Register to hold the maximum ROWID of pTab.
**   (3)  Register to hold the rowid in sqlite_sequence of pTab
**
** The 2nd register is the one that is returned.  That is all the
** insert routine needs to know about.
*/
static int autoIncBegin(
  Parse *pParse,      /* Parsing context */
  int iDb,            /* Index of the database holding pTab */
  Table *pTab         /* The table we are writing to */
){
  int memId = 0;      /* Register holding maximum rowid */
  if( pTab->tabFlags & TF_Autoincrement ){
    Parse *pToplevel = sqlite4ParseToplevel(pParse);
    AutoincInfo *pInfo;

    pInfo = pToplevel->pAinc;
    while( pInfo && pInfo->pTab!=pTab ){ pInfo = pInfo->pNext; }
    if( pInfo==0 ){
      pInfo = sqlite4DbMallocRaw(pParse->db, sizeof(*pInfo));
      if( pInfo==0 ) return 0;
      pInfo->pNext = pToplevel->pAinc;
      pToplevel->pAinc = pInfo;
      pInfo->pTab = pTab;
      pInfo->iDb = iDb;
      pToplevel->nMem++;                  /* Register to hold name of table */
      pInfo->regCtr = ++pToplevel->nMem;  /* Max rowid register */
      pToplevel->nMem++;                  /* Rowid in sqlite_sequence */
    }
    memId = pInfo->regCtr;
  }
  return memId;
}

/*
** This routine generates code that will initialize all of the
** register used by the autoincrement tracker.  
*/
void sqlite4AutoincrementBegin(Parse *pParse){
  AutoincInfo *p;            /* Information about an AUTOINCREMENT */
  sqlite4 *db = pParse->db;  /* The database connection */
  Db *pDb;                   /* Database only autoinc table */
  int memId;                 /* Register holding max rowid */
  int addr;                  /* A VDBE address */
  Vdbe *v = pParse->pVdbe;   /* VDBE under construction */

  /* This routine is never called during trigger-generation.  It is
  ** only called from the top-level */
  assert( pParse->pTriggerTab==0 );
  assert( pParse==sqlite4ParseToplevel(pParse) );

  assert( v );   /* We failed long ago if this is not so */
  for(p = pParse->pAinc; p; p = p->pNext){
    pDb = &db->aDb[p->iDb];
    memId = p->regCtr;
    sqlite4OpenTable(pParse, 0, p->iDb, pDb->pSchema->pSeqTab, OP_OpenRead);
    sqlite4VdbeAddOp3(v, OP_Null, 0, memId, memId+1);
    addr = sqlite4VdbeCurrentAddr(v);
    sqlite4VdbeAddOp4(v, OP_String8, 0, memId-1, 0, p->pTab->zName, 0);
    sqlite4VdbeAddOp2(v, OP_Rewind, 0, addr+9);
    sqlite4VdbeAddOp3(v, OP_Column, 0, 0, memId);
    sqlite4VdbeAddOp3(v, OP_Ne, memId-1, addr+7, memId);
    sqlite4VdbeChangeP5(v, SQLITE_JUMPIFNULL);
    sqlite4VdbeAddOp2(v, OP_Rowid, 0, memId+1);
    sqlite4VdbeAddOp3(v, OP_Column, 0, 1, memId);
    sqlite4VdbeAddOp2(v, OP_Goto, 0, addr+9);
    sqlite4VdbeAddOp2(v, OP_Next, 0, addr+2);
    sqlite4VdbeAddOp2(v, OP_Integer, 0, memId);
    sqlite4VdbeAddOp0(v, OP_Close);
  }
}

/*
** Update the maximum rowid for an autoincrement calculation.
**
** This routine should be called when the top of the stack holds a
** new rowid that is about to be inserted.  If that new rowid is
** larger than the maximum rowid in the memId memory cell, then the
** memory cell is updated.  The stack is unchanged.
*/
static void autoIncStep(Parse *pParse, int memId, int regRowid){
  if( memId>0 ){
    sqlite4VdbeAddOp2(pParse->pVdbe, OP_MemMax, memId, regRowid);
  }
}

/*
** This routine generates the code needed to write autoincrement
** maximum rowid values back into the sqlite_sequence register.
** Every statement that might do an INSERT into an autoincrement
** table (either directly or through triggers) needs to call this
** routine just before the "exit" code.
*/
void sqlite4AutoincrementEnd(Parse *pParse){
  AutoincInfo *p;
  Vdbe *v = pParse->pVdbe;
  sqlite4 *db = pParse->db;

  assert( v );
  for(p = pParse->pAinc; p; p = p->pNext){
    Db *pDb = &db->aDb[p->iDb];
    int j1, j2, j3, j4, j5;
    int iRec;
    int memId = p->regCtr;

    iRec = sqlite4GetTempReg(pParse);
    sqlite4OpenTable(pParse, 0, p->iDb, pDb->pSchema->pSeqTab, OP_OpenWrite);
    j1 = sqlite4VdbeAddOp1(v, OP_NotNull, memId+1);
    j2 = sqlite4VdbeAddOp0(v, OP_Rewind);
    j3 = sqlite4VdbeAddOp3(v, OP_Column, 0, 0, iRec);
    j4 = sqlite4VdbeAddOp3(v, OP_Eq, memId-1, 0, iRec);
    sqlite4VdbeAddOp2(v, OP_Next, 0, j3);
    sqlite4VdbeJumpHere(v, j2);
    sqlite4VdbeAddOp2(v, OP_NewRowid, 0, memId+1);
    j5 = sqlite4VdbeAddOp0(v, OP_Goto);
    sqlite4VdbeJumpHere(v, j4);
    sqlite4VdbeAddOp2(v, OP_Rowid, 0, memId+1);
    sqlite4VdbeJumpHere(v, j1);
    sqlite4VdbeJumpHere(v, j5);
    sqlite4VdbeAddOp3(v, OP_MakeRecord, memId-1, 2, iRec);
    sqlite4VdbeAddOp3(v, OP_Insert, 0, iRec, memId+1);
    sqlite4VdbeChangeP5(v, OPFLAG_APPEND);
    sqlite4VdbeAddOp0(v, OP_Close);
    sqlite4ReleaseTempReg(pParse, iRec);
  }
}
#else
/*
** If SQLITE_OMIT_AUTOINCREMENT is defined, then the three routines
** above are all no-ops
*/
# define autoIncBegin(A,B,C) (0)
# define autoIncStep(A,B,C)
#endif /* SQLITE_OMIT_AUTOINCREMENT */


/* Forward declaration */
static int xferOptimization(
  Parse *pParse,        /* Parser context */
  Table *pDest,         /* The table we are inserting into */
  Select *pSelect,      /* A SELECT statement to use as the data source */
  int onError,          /* How to handle constraint errors */
  int iDbDest           /* The database of pDest */
);

/*
** This routine is call to handle SQL of the following forms:
**
**    insert into TABLE (IDLIST) values(EXPRLIST)
**    insert into TABLE (IDLIST) select
**
** The IDLIST following the table name is always optional.  If omitted,
** then a list of all columns for the table is substituted.  The IDLIST
** appears in the pColumn parameter.  pColumn is NULL if IDLIST is omitted.
**
** The pList parameter holds EXPRLIST in the first form of the INSERT
** statement above, and pSelect is NULL.  For the second form, pList is
** NULL and pSelect is a pointer to the select statement used to generate
** data for the insert.
**
** The code generated follows one of four templates.  For a simple
** select with data coming from a VALUES clause, the code executes
** once straight down through.  Pseudo-code follows (we call this
** the "1st template"):
**
**         open write cursor to <table> and its indices
**         puts VALUES clause expressions onto the stack
**         write the resulting record into <table>
**         cleanup
**
** The three remaining templates assume the statement is of the form
**
**   INSERT INTO <table> SELECT ...
**
** If the SELECT clause is of the restricted form "SELECT * FROM <table2>" -
** in other words if the SELECT pulls all columns from a single table
** and there is no WHERE or LIMIT or GROUP BY or ORDER BY clauses, and
** if <table2> and <table1> are distinct tables but have identical
** schemas, including all the same indices, then a special optimization
** is invoked that copies raw records from <table2> over to <table1>.
** See the xferOptimization() function for the implementation of this
** template.  This is the 2nd template.
**
**         open a write cursor to <table>
**         open read cursor on <table2>
**         transfer all records in <table2> over to <table>
**         close cursors
**         foreach index on <table>
**           open a write cursor on the <table> index
**           open a read cursor on the corresponding <table2> index
**           transfer all records from the read to the write cursors
**           close cursors
**         end foreach
**
** The 3rd template is for when the second template does not apply
** and the SELECT clause does not read from <table> at any time.
** The generated code follows this template:
**
**         EOF <- 0
**         X <- A
**         goto B
**      A: setup for the SELECT
**         loop over the rows in the SELECT
**           load values into registers R..R+n
**           yield X
**         end loop
**         cleanup after the SELECT
**         EOF <- 1
**         yield X
**         goto A
**      B: open write cursor to <table> and its indices
**      C: yield X
**         if EOF goto D
**         insert the select result into <table> from R..R+n
**         goto C
**      D: cleanup
**
** The 4th template is used if the insert statement takes its
** values from a SELECT but the data is being inserted into a table
** that is also read as part of the SELECT.  In the third form,
** we have to use a intermediate table to store the results of
** the select.  The template is like this:
**
**         EOF <- 0
**         X <- A
**         goto B
**      A: setup for the SELECT
**         loop over the tables in the SELECT
**           load value into register R..R+n
**           yield X
**         end loop
**         cleanup after the SELECT
**         EOF <- 1
**         yield X
**         halt-error
**      B: open temp table
**      L: yield X
**         if EOF goto M
**         insert row from R..R+n into temp table
**         goto L
**      M: open write cursor to <table> and its indices
**         rewind temp table
**      C: loop over rows of intermediate table
**           transfer values form intermediate table into <table>
**         end loop
**      D: cleanup
*/
void sqlite4Insert(
  Parse *pParse,        /* Parser context */
  SrcList *pTabList,    /* Name of table into which we are inserting */
  ExprList *pList,      /* List of values to be inserted */
  Select *pSelect,      /* A SELECT statement to use as the data source */
  IdList *pColumn,      /* Column names corresponding to IDLIST. */
  int onError           /* How to handle constraint errors */
){
  sqlite4 *db;          /* The main database structure */
  Table *pTab;          /* The table to insert into.  aka TABLE */
  char *zTab;           /* Name of the table into which we are inserting */
  const char *zDb;      /* Name of the database holding this table */
  int i, j, idx;        /* Loop counters */
  Vdbe *v;              /* Generate code into this virtual machine */
  Index *pIdx;          /* For looping over indices of the table */
  int nColumn;          /* Number of columns in the data */
  int nHidden = 0;      /* Number of hidden columns if TABLE is virtual */
  int baseCur = 0;      /* VDBE Cursor number for pTab */
  int keyColumn = -1;   /* Column that is the INTEGER PRIMARY KEY */
  int endOfLoop;        /* Label for the end of the insertion loop */
  int useTempTable = 0; /* Store SELECT results in intermediate table */
  int srcTab = 0;       /* Data comes from this temporary cursor if >=0 */
  int addrInsTop = 0;   /* Jump to label "D" */
  int addrCont = 0;     /* Top of insert loop. Label "C" in templates 3 and 4 */
  int addrSelect = 0;   /* Address of coroutine that implements the SELECT */
  SelectDest dest;      /* Destination for SELECT on rhs of INSERT */
  int iDb;              /* Index of database holding TABLE */
  Db *pDb;              /* The database containing table being inserted into */
  int appendFlag = 0;   /* True if the insert is likely to be an append */

  /* Register allocations */
  int regFromSelect = 0;/* Base register for data coming from SELECT */
  int regAutoinc = 0;   /* Register holding the AUTOINCREMENT counter */
  int regRowCount = 0;  /* Memory cell used for the row counter */
  int regIns;           /* Block of regs holding rowid+data being inserted */
  int regData;          /* register holding first column to insert */
  int regEof = 0;       /* Register recording end of SELECT data */
  int *aRegIdx = 0;     /* One register allocated to each index */

  int iPk;                        /* Cursor offset of PK index cursor */
  Index *pPk;                     /* Primary key for table pTab */
  int bImplicitPK;                /* True if table pTab has an implicit PK */
  int regContent;                 /* First register in column value array */
  int regRowid;                   /* If bImplicitPK, register holding IPK */


#ifndef SQLITE_OMIT_TRIGGER
  int isView;                 /* True if attempting to insert into a view */
  Trigger *pTrigger;          /* List of triggers on pTab, if required */
  int tmask;                  /* Mask of trigger times */
#endif

  db = pParse->db;
  memset(&dest, 0, sizeof(dest));
  if( pParse->nErr || db->mallocFailed ){
    goto insert_cleanup;
  }

  /* Locate the table into which we will be inserting new information.
  */
  assert( pTabList->nSrc==1 );
  zTab = pTabList->a[0].zName;
  if( NEVER(zTab==0) ) goto insert_cleanup;
  pTab = sqlite4SrcListLookup(pParse, pTabList);
  if( pTab==0 ){
    goto insert_cleanup;
  }
  iDb = sqlite4SchemaToIndex(db, pTab->pSchema);
  assert( iDb<db->nDb );
  pDb = &db->aDb[iDb];
  zDb = pDb->zName;
  if( sqlite4AuthCheck(pParse, SQLITE_INSERT, pTab->zName, 0, zDb) ){
    goto insert_cleanup;
  }

  /* Set bImplicitPK to true for an implicit PRIMARY KEY, or false otherwise */
  pPk = sqlite4FindPrimaryKey(pTab, &iPk);
  bImplicitPK = pPk->aiColumn[0]==-1;

  /* Figure out if we have any triggers and if the table being
  ** inserted into is a view
  */
#ifndef SQLITE_OMIT_TRIGGER
  pTrigger = sqlite4TriggersExist(pParse, pTab, TK_INSERT, 0, &tmask);
  isView = pTab->pSelect!=0;
#else
# define pTrigger 0
# define tmask 0
# define isView 0
#endif
#ifdef SQLITE_OMIT_VIEW
# undef isView
# define isView 0
#endif
  assert( (pTrigger && tmask) || (pTrigger==0 && tmask==0) );

  /* If pTab is really a view, make sure it has been initialized.
  ** ViewGetColumnNames() is a no-op if pTab is not a view (or virtual 
  ** module table).  */
  if( sqlite4ViewGetColumnNames(pParse, pTab) ){
    goto insert_cleanup;
  }

  /* Ensure that:
  **   (a) the table is not read-only (e.g. sqlite_master, sqlite_stat), and
  **   (b) that if it is a view then ON INSERT triggers exist
  */
  if( sqlite4IsReadOnly(pParse, pTab, tmask) ){
    goto insert_cleanup;
  }

  /* Allocate a VDBE and begin a write transaction */
  v = sqlite4GetVdbe(pParse);
  if( v==0 ) goto insert_cleanup;
  if( pParse->nested==0 ) sqlite4VdbeCountChanges(v);
  sqlite4BeginWriteOperation(pParse, pSelect || pTrigger, iDb);

#ifndef SQLITE_OMIT_XFER_OPT
  /* If the statement is of the form
  **
  **       INSERT INTO <table1> SELECT * FROM <table2>;
  **
  ** Then special optimizations can be applied that make the transfer
  ** very fast and which reduce fragmentation of indices.
  **
  ** This is the 2nd template.
  */
  if( pColumn==0 && xferOptimization(pParse, pTab, pSelect, onError, iDb) ){
    assert( !pTrigger );
    assert( pList==0 );
    goto insert_end;
  }
#endif /* SQLITE_OMIT_XFER_OPT */

  /* Figure out how many columns of data are supplied.  If the data
  ** is coming from a SELECT statement, then generate a co-routine that
  ** produces a single row of the SELECT on each invocation.  The
  ** co-routine is the common header to the 3rd and 4th templates.
  */
  if( pSelect ){
    /* Data is coming from a SELECT.  Generate code to implement that SELECT
    ** as a co-routine.  The code is common to both the 3rd and 4th
    ** templates:
    **
    **         EOF <- 0
    **         X <- A
    **         goto B
    **      A: setup for the SELECT
    **         loop over the tables in the SELECT
    **           load value into register R..R+n
    **           yield X
    **         end loop
    **         cleanup after the SELECT
    **         EOF <- 1
    **         yield X
    **         halt-error
    **
    ** On each invocation of the co-routine, it puts a single row of the
    ** SELECT result into registers dest.iMem...dest.iMem+dest.nMem-1.
    ** (These output registers are allocated by sqlite4Select().)  When
    ** the SELECT completes, it sets the EOF flag stored in regEof.
    */
    int rc, j1;

    regEof = ++pParse->nMem;
    sqlite4VdbeAddOp2(v, OP_Integer, 0, regEof);      /* EOF <- 0 */
    VdbeComment((v, "SELECT eof flag"));
    sqlite4SelectDestInit(&dest, SRT_Coroutine, ++pParse->nMem);
    addrSelect = sqlite4VdbeCurrentAddr(v)+2;
    sqlite4VdbeAddOp2(v, OP_Integer, addrSelect-1, dest.iParm);
    j1 = sqlite4VdbeAddOp2(v, OP_Goto, 0, 0);
    VdbeComment((v, "Jump over SELECT coroutine"));

    /* Resolve the expressions in the SELECT statement and execute it. */
    rc = sqlite4Select(pParse, pSelect, &dest);
    assert( pParse->nErr==0 || rc );
    if( rc || NEVER(pParse->nErr) || db->mallocFailed ){
      goto insert_cleanup;
    }
    sqlite4VdbeAddOp2(v, OP_Integer, 1, regEof);         /* EOF <- 1 */
    sqlite4VdbeAddOp1(v, OP_Yield, dest.iParm);   /* yield X */
    sqlite4VdbeAddOp2(v, OP_Halt, SQLITE_INTERNAL, OE_Abort);
    VdbeComment((v, "End of SELECT coroutine"));
    sqlite4VdbeJumpHere(v, j1);                          /* label B: */

    regFromSelect = dest.iMem;
    assert( pSelect->pEList );
    nColumn = pSelect->pEList->nExpr;
    assert( dest.nMem==nColumn );

    /* Set useTempTable to TRUE if the result of the SELECT statement
    ** should be written into a temporary table (template 4).  Set to
    ** FALSE if each* row of the SELECT can be written directly into
    ** the destination table (template 3).
    **
    ** A temp table must be used if the table being updated is also one
    ** of the tables being read by the SELECT statement.  Also use a 
    ** temp table in the case of row triggers.
    */
    if( pTrigger || readsTable(pParse, addrSelect, iDb, pTab) ){
      useTempTable = 1;
    }

    if( useTempTable ){
      /* Invoke the coroutine to extract information from the SELECT
      ** and add it to a transient table srcTab.  The code generated
      ** here is from the 4th template:
      **
      **      B: open temp table
      **      L: yield X
      **         if EOF goto M
      **         insert row from R..R+n into temp table
      **         goto L
      **      M: ...
      */
      int regRec;          /* Register to hold packed record */
      int regTempRowid;    /* Register to hold temp table ROWID */
      int regTempKey;      /* Register to hold key encoded rowid */
      int addrTop;         /* Label "L" */
      int addrIf;          /* Address of jump to M */

      srcTab = pParse->nTab++;
      regRec = sqlite4GetTempReg(pParse);
      regTempRowid = sqlite4GetTempReg(pParse);
      sqlite4VdbeAddOp2(v, OP_OpenEphemeral, srcTab, nColumn);
      addrTop = sqlite4VdbeAddOp1(v, OP_Yield, dest.iParm);
      addrIf = sqlite4VdbeAddOp1(v, OP_If, regEof);
      sqlite4VdbeAddOp3(v, OP_MakeRecord, regFromSelect, nColumn, regRec);
      sqlite4VdbeAddOp2(v, OP_NewRowid, srcTab, regTempRowid);
      sqlite4VdbeAddOp3(v, OP_Insert, srcTab, regRec, regTempRowid);
      sqlite4VdbeAddOp2(v, OP_Goto, 0, addrTop);
      sqlite4VdbeJumpHere(v, addrIf);
      sqlite4ReleaseTempReg(pParse, regRec);
      sqlite4ReleaseTempReg(pParse, regTempRowid);
    }
  }else{
    /* This is the case if the data for the INSERT is coming from a VALUES
    ** (or DEFAULT VALUES) clause. Resolve all references in the VALUES(...)
    ** expressions.  */ 
    NameContext sNC;
    memset(&sNC, 0, sizeof(sNC));
    sNC.pParse = pParse;
    srcTab = -1;
    assert( useTempTable==0 );
    nColumn = pList ? pList->nExpr : 0;
    for(i=0; i<nColumn; i++){
      if( sqlite4ResolveExprNames(&sNC, pList->a[i].pExpr) ){
        goto insert_cleanup;
      }
    }
  }

  /* Make sure the number of columns in the source data matches the number
  ** of columns to be inserted into the table.
  */
  if( IsVirtual(pTab) ){
    for(i=0; i<pTab->nCol; i++){
      nHidden += (IsHiddenColumn(&pTab->aCol[i]) ? 1 : 0);
    }
  }
  if( pColumn==0 && nColumn && nColumn!=(pTab->nCol-nHidden) ){
    sqlite4ErrorMsg(pParse,
       "table %S has %d columns but %d values were supplied",
       pTabList, 0, pTab->nCol-nHidden, nColumn);
    goto insert_cleanup;
  }
  if( pColumn!=0 && nColumn!=pColumn->nId ){
    sqlite4ErrorMsg(pParse, "%d values for %d columns", nColumn, pColumn->nId);
    goto insert_cleanup;
  }

  /* If the INSERT statement included an IDLIST term, then make sure
  ** all elements of the IDLIST really are columns of the table. Set
  ** the pColumn->a[iCol].idx variables to indicate which column of the
  ** table each IDLIST element corresponds to.
  */
  if( pColumn ){
    for(i=0; i<pColumn->nId; i++){
      pColumn->a[i].idx = -1;
    }
    for(i=0; i<pColumn->nId; i++){
      char *zTest = pColumn->a[i].zName;
      for(j=0; j<pTab->nCol; j++){
        if( sqlite4StrICmp(zTest, pTab->aCol[j].zName)==0 ){
          pColumn->a[i].idx = j;
          break;
        }
      }
      if( j==pTab->nCol ){
        sqlite4ErrorMsg(pParse, "table %S has no column named %s",
              pTabList, 0, pColumn->a[i].zName);
        pParse->checkSchema = 1;
        goto insert_cleanup;
      }
    }
  }

  /* Initialize the count of rows to be inserted
  */
  if( db->flags & SQLITE_CountRows ){
    regRowCount = ++pParse->nMem;
    sqlite4VdbeAddOp2(v, OP_Integer, 0, regRowCount);
  }

  /* If this is not a view, open a write cursor on each index. Allocate
  ** a contiguous array of (nIdx+1) registers, where nIdx is the total
  ** number of indexes (including the PRIMARY KEY index). 
  **
  **   Register aRegIdx[0]:         The PRIMARY KEY index key
  **   Register aRegIdx[1..nIdx-1]: Keys for other table indexes 
  **   Register aRegIdx[nIdx]:      Data record for table row.
  */
  if( !isView ){
    int nIdx;

    baseCur = pParse->nTab;
    nIdx = sqlite4OpenAllIndexes(pParse, pTab, baseCur, OP_OpenWrite);
    aRegIdx = sqlite4DbMallocRaw(db, sizeof(int)*(nIdx+1));
    if( aRegIdx==0 ){
      goto insert_cleanup;
    }
    for(i=0; i<nIdx; i++){
      aRegIdx[i] = ++pParse->nMem;  /* Register in which to store key */
      pParse->nMem++;               /* Extra register for data */
    }
  }

  /* This is the top of the main insertion loop */
  if( useTempTable ){
    /* This block codes the top of loop only.  The complete loop is the
    ** following pseudocode (template 4):
    **
    **         rewind temp table
    **      C: loop over rows of intermediate table
    **           transfer values form intermediate table into <table>
    **         end loop
    **      D: ...
    */
    addrInsTop = sqlite4VdbeAddOp1(v, OP_Rewind, srcTab);
    addrCont = sqlite4VdbeCurrentAddr(v);
  }else if( pSelect ){
    /* This block codes the top of loop only.  The complete loop is the
    ** following pseudocode (template 3):
    **
    **      C: yield X
    **         if EOF goto D
    **         insert the select result into <table> from R..R+n
    **         goto C
    **      D: ...
    */
    addrCont = sqlite4VdbeAddOp1(v, OP_Yield, dest.iParm);
    addrInsTop = sqlite4VdbeAddOp1(v, OP_If, regEof);
  }

  /* Allocate an array of registers in which to assemble the values for the
  ** new row. If the table has an explicit primary key, we need one register
  ** for each table column. If the table uses an implicit primary key, the
  ** nCol+1 registers are required.  */
  if( bImplicitPK ) regRowid = ++pParse->nMem;
  regContent = pParse->nMem+1;
  pParse->nMem += pTab->nCol;

  if( IsVirtual(pTab) ){
    /* TODO: Fix this */
    regContent++;
    regRowid++;
    pParse->nMem++;
  }

  /* Run the BEFORE and INSTEAD OF triggers, if there are any
  */
  endOfLoop = sqlite4VdbeMakeLabel(v);
  if( tmask & TRIGGER_BEFORE ){
    int regCols = sqlite4GetTempRange(pParse, pTab->nCol+1);

    /* build the NEW.* reference row.  Note that if there is an INTEGER
    ** PRIMARY KEY into which a NULL is being inserted, that NULL will be
    ** translated into a unique ID for the row.  But on a BEFORE trigger,
    ** we do not know what the unique ID will be (because the insert has
    ** not happened yet) so we substitute a rowid of -1
    */
    if( keyColumn<0 ){
      sqlite4VdbeAddOp2(v, OP_Integer, -1, regCols);
    }else{
      int j1;
      if( useTempTable ){
        sqlite4VdbeAddOp3(v, OP_Column, srcTab, keyColumn, regCols);
      }else{
        assert( pSelect==0 );  /* Otherwise useTempTable is true */
        sqlite4ExprCode(pParse, pList->a[keyColumn].pExpr, regCols);
      }
      j1 = sqlite4VdbeAddOp1(v, OP_NotNull, regCols);
      sqlite4VdbeAddOp2(v, OP_Integer, -1, regCols);
      sqlite4VdbeJumpHere(v, j1);
      sqlite4VdbeAddOp1(v, OP_MustBeInt, regCols);
    }

    /* Cannot have triggers on a virtual table. If it were possible,
    ** this block would have to account for hidden column.
    */
    assert( !IsVirtual(pTab) );

    /* Create the new column data
    */
    for(i=0; i<pTab->nCol; i++){
      if( pColumn==0 ){
        j = i;
      }else{
        for(j=0; j<pColumn->nId; j++){
          if( pColumn->a[j].idx==i ) break;
        }
      }
      if( (!useTempTable && !pList) || (pColumn && j>=pColumn->nId) ){
        sqlite4ExprCode(pParse, pTab->aCol[i].pDflt, regCols+i+1);
      }else if( useTempTable ){
        sqlite4VdbeAddOp3(v, OP_Column, srcTab, j, regCols+i+1); 
      }else{
        assert( pSelect==0 ); /* Otherwise useTempTable is true */
        sqlite4ExprCodeAndCache(pParse, pList->a[j].pExpr, regCols+i+1);
      }
    }

    /* If this is an INSERT on a view with an INSTEAD OF INSERT trigger,
    ** do not attempt any conversions before assembling the record.
    ** If this is a real table, attempt conversions as required by the
    ** table column affinities.
    */
    if( !isView ){
      sqlite4VdbeAddOp2(v, OP_Affinity, regCols+1, pTab->nCol);
      sqlite4TableAffinityStr(v, pTab);
    }

    /* Fire BEFORE or INSTEAD OF triggers */
    sqlite4CodeRowTrigger(pParse, pTrigger, TK_INSERT, 0, TRIGGER_BEFORE, 
        pTab, regCols-pTab->nCol-1, onError, endOfLoop);

    sqlite4ReleaseTempRange(pParse, regCols, pTab->nCol+1);
  }

  if( !isView ){
    /* If this table has an implicit PRIMARY KEY, populate the regRowid
    ** register with the value to use for the new row.  */
    if( bImplicitPK ){
      sqlite4VdbeAddOp2(v, OP_NewRowid, baseCur+iPk, regRowid);
    }

#if 0
    if( IsVirtual(pTab) ){
      /* The row that the VUpdate opcode will delete: none */
      sqlite4VdbeAddOp2(v, OP_Null, 0, regIns);
    }
    if( keyColumn>=0 ){
      if( useTempTable ){
        sqlite4VdbeAddOp3(v, OP_Column, srcTab, keyColumn, regRowid);
      }else if( pSelect ){
        sqlite4VdbeAddOp2(v, OP_SCopy, regFromSelect+keyColumn, regRowid);
      }else{
        VdbeOp *pOp;
        sqlite4ExprCode(pParse, pList->a[keyColumn].pExpr, regRowid);
        pOp = sqlite4VdbeGetOp(v, -1);
        if( ALWAYS(pOp) && pOp->opcode==OP_Null && !IsVirtual(pTab) ){
          appendFlag = 1;
          pOp->opcode = OP_NewRowid;
          pOp->p1 = baseCur;
          pOp->p2 = regRowid;
          pOp->p3 = regAutoinc;
        }
      }
      /* If the PRIMARY KEY expression is NULL, then use OP_NewRowid
      ** to generate a unique primary key value.
      */
      if( !appendFlag ){
        int j1;
        if( !IsVirtual(pTab) ){
          j1 = sqlite4VdbeAddOp1(v, OP_NotNull, regRowid);
          sqlite4VdbeAddOp3(v, OP_NewRowid, baseCur, regRowid, regAutoinc);
          sqlite4VdbeJumpHere(v, j1);
        }else{
          j1 = sqlite4VdbeCurrentAddr(v);
          sqlite4VdbeAddOp2(v, OP_IsNull, regRowid, j1+2);
        }
        sqlite4VdbeAddOp1(v, OP_MustBeInt, regRowid);
      }
    }else if( IsVirtual(pTab) ){
      sqlite4VdbeAddOp2(v, OP_Null, 0, regRowid);
    }else{
      sqlite4VdbeAddOp3(v, OP_NewRowid, baseCur, regRowid, regAutoinc);
      appendFlag = 1;
    }
    autoIncStep(pParse, regAutoinc, regRowid);
#endif

    /* Push onto the stack, data for all columns of the new entry, beginning
    ** with the first column.
    */
    nHidden = 0;
    for(i=0; i<pTab->nCol; i++){
      int iRegStore = regContent + i;
      if( pColumn==0 ){
        if( IsHiddenColumn(&pTab->aCol[i]) ){
          assert( IsVirtual(pTab) );
          j = -1;
          nHidden++;
        }else{
          j = i - nHidden;
        }
      }else{
        for(j=0; j<pColumn->nId; j++){
          if( pColumn->a[j].idx==i ) break;
        }
      }
      if( j<0 || nColumn==0 || (pColumn && j>=pColumn->nId) ){
        sqlite4ExprCode(pParse, pTab->aCol[i].pDflt, iRegStore);
      }else if( useTempTable ){
        sqlite4VdbeAddOp3(v, OP_Column, srcTab, j, iRegStore); 
      }else if( pSelect ){
        sqlite4VdbeAddOp2(v, OP_SCopy, regFromSelect+j, iRegStore);
      }else{
        sqlite4ExprCode(pParse, pList->a[j].pExpr, iRegStore);
      }
    }

    /* Generate code to check constraints and generate index keys and
    ** do the insertion.
    */
#ifndef SQLITE_OMIT_VIRTUALTABLE
    if( IsVirtual(pTab) ){
      const char *pVTab = (const char *)sqlite4GetVTable(db, pTab);
      sqlite4VtabMakeWritable(pParse, pTab);
      sqlite4VdbeAddOp4(v, OP_VUpdate, 1, pTab->nCol+2, regIns, pVTab, P4_VTAB);
      sqlite4VdbeChangeP5(v, onError==OE_Default ? OE_Abort : onError);
      sqlite4MayAbort(pParse);
    }else
#endif
    {
      int isReplace;    /* Set to true if constraints may cause a replace */
      sqlite4GenerateConstraintChecks(pParse, pTab, baseCur, 
          regContent, aRegIdx, keyColumn>=0, 0, onError, endOfLoop, &isReplace
      );
      sqlite4FkCheck(pParse, pTab, 0, regIns);
      sqlite4CompleteInsertion(pParse, pTab, baseCur, 
          regContent, aRegIdx, 0, appendFlag, isReplace==0
      );
    }
  }

  /* Update the count of rows that are inserted
  */
  if( (db->flags & SQLITE_CountRows)!=0 ){
    sqlite4VdbeAddOp2(v, OP_AddImm, regRowCount, 1);
  }

  if( pTrigger ){
    /* Code AFTER triggers */
    sqlite4CodeRowTrigger(pParse, pTrigger, TK_INSERT, 0, TRIGGER_AFTER, 
        pTab, regData-2-pTab->nCol, onError, endOfLoop);
  }

  /* The bottom of the main insertion loop, if the data source
  ** is a SELECT statement.
  */
  sqlite4VdbeResolveLabel(v, endOfLoop);
  if( useTempTable ){
    sqlite4VdbeAddOp2(v, OP_Next, srcTab, addrCont);
    sqlite4VdbeJumpHere(v, addrInsTop);
    sqlite4VdbeAddOp1(v, OP_Close, srcTab);
  }else if( pSelect ){
    sqlite4VdbeAddOp2(v, OP_Goto, 0, addrCont);
    sqlite4VdbeJumpHere(v, addrInsTop);
  }

  if( !IsVirtual(pTab) && !isView ){
    /* Close all tables opened */
    for(idx=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, idx++){
      sqlite4VdbeAddOp1(v, OP_Close, idx+baseCur);
    }
  }

insert_end:
  /* Update the sqlite_sequence table by storing the content of the
  ** maximum rowid counter values recorded while inserting into
  ** autoincrement tables.
  */
  if( pParse->nested==0 && pParse->pTriggerTab==0 ){
    sqlite4AutoincrementEnd(pParse);
  }

  /*
  ** Return the number of rows inserted. If this routine is 
  ** generating code because of a call to sqlite4NestedParse(), do not
  ** invoke the callback function.
  */
  if( (db->flags&SQLITE_CountRows) && !pParse->nested && !pParse->pTriggerTab ){
    sqlite4VdbeAddOp2(v, OP_ResultRow, regRowCount, 1);
    sqlite4VdbeSetNumCols(v, 1);
    sqlite4VdbeSetColName(v, 0, COLNAME_NAME, "rows inserted", SQLITE_STATIC);
  }

insert_cleanup:
  sqlite4SrcListDelete(db, pTabList);
  sqlite4ExprListDelete(db, pList);
  sqlite4SelectDelete(db, pSelect);
  sqlite4IdListDelete(db, pColumn);
  sqlite4DbFree(db, aRegIdx);
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
#ifdef tmask
 #undef tmask
#endif

/*
** Return the name of the iCol'th column in index pIdx.
*/
const char *indexColumnName(Index *pIdx, int iCol){
  int iTbl = pIdx->aiColumn[iCol];
  assert( iTbl>=-1 && iTbl<pIdx->pTable->nCol );
  if( iTbl<0 ){
    assert( pIdx->eIndexType==SQLITE_INDEX_PRIMARYKEY && pIdx->nColumn==1 );
    return "rowid";
  }
  return pIdx->pTable->aCol[iTbl].zName;
}

static void generateNotNullChecks(
  Parse *pParse,                  /* Parse context */
  Table *pTab,                    /* Table to generate checks for */
  int regContent,                 /* Index of the range of input registers */
  int overrideError,              /* Override default OE_* with this */
  int ignoreDest                  /* Jump to this lable if OE_Ignore */
){
  Vdbe *v = pParse->pVdbe;
  int i;

  for(i=0; i<pTab->nCol; i++){
    int onError = pTab->aCol[i].notNull;
    if( onError ){
      if( overrideError!=OE_Default ){
        onError = overrideError;
      }else if( onError==OE_Default ){
        onError = OE_Abort;
      }
      if( onError==OE_Replace && pTab->aCol[i].pDflt==0 ){
        onError = OE_Abort;
      }

      switch( onError ){
        case OE_Abort:
          sqlite4MayAbort(pParse);
        case OE_Rollback:
        case OE_Fail: {
          char *zMsg = sqlite4MPrintf(pParse->db, "%s.%s may not be NULL",
              pTab->zName, pTab->aCol[i].zName
          );
          sqlite4VdbeAddOp4(v, OP_HaltIfNull, 
              SQLITE_CONSTRAINT, onError, regContent+i, zMsg, P4_DYNAMIC
          );
          break;
        }

        case OE_Ignore:
          sqlite4VdbeAddOp2(v, OP_IsNull, regContent+i, ignoreDest);
          break;

        default: {
          int j1 = sqlite4VdbeAddOp1(v, OP_NotNull, regContent+i);
          sqlite4ExprCode(pParse, pTab->aCol[i].pDflt, regContent+i);
          sqlite4VdbeJumpHere(v, j1);
          assert( onError==OE_Replace );
          break;
        }
      }
    }
  }
}

#ifndef SQLITE_OMIT_CHECK
static void generateCheckChecks(
  Parse *pParse,                  /* Parse context */
  Table *pTab,                    /* Table to generate checks for */
  int regContent,                 /* Index of the range of input registers */
  int overrideError,              /* Override default OE_* with this */
  int ignoreDest                  /* Jump to this lable if OE_Ignore */
){
  Vdbe *v = pParse->pVdbe;

  if( pTab->pCheck && (pParse->db->flags & SQLITE_IgnoreChecks)==0 ){
    int onError;
    int allOk = sqlite4VdbeMakeLabel(v);
    pParse->ckBase = regContent;
    sqlite4ExprIfTrue(pParse, pTab->pCheck, allOk, SQLITE_JUMPIFNULL);
    onError = overrideError!=OE_Default ? overrideError : OE_Abort;
    if( onError==OE_Ignore ){
      sqlite4VdbeAddOp2(v, OP_Goto, 0, ignoreDest);
    }else{
      if( onError==OE_Replace ) onError = OE_Abort; /* IMP: R-15569-63625 */
      sqlite4HaltConstraint(pParse, onError, 0, 0);
    }
    sqlite4VdbeResolveLabel(v, allOk);
  }
}
#else /* !defined(SQLITE_OMIT_CHECK) */
# define generateCheckChecks(a,b,c,d,e)
#endif

Index *sqlite4FindPrimaryKey(
  Table *pTab,                    /* Table to locate primary key for */
  int *piPk                       /* OUT: Index of PRIMARY KEY */
){
  Index *p;
  int iPk = 0;
  for(p=pTab->pIndex; p && p->eIndexType!=SQLITE_INDEX_PRIMARYKEY; p=p->pNext){
    iPk++;
  }
  if( piPk ) *piPk = iPk;
  return p;
}

/*
** Index pIdx is a UNIQUE index. This function returns a pointer to a buffer
** containing an error message to tell the user that the UNIQUE constraint
** has failed.
**
** The returned buffer should be freed by the caller using sqlite4DbFree().
*/
static char *notUniqueMessage(
  Parse *pParse,                  /* Parse context */
  Index *pIdx                     /* Index to generate error message for */
){
  const int nCol = pIdx->nColumn; /* Number of columns indexed by pIdx */
  StrAccum errMsg;                /* Buffer to build error message within */
  int iCol;                       /* Used to iterate through indexed columns */

  sqlite4StrAccumInit(&errMsg, 0, 0, 200);
  errMsg.db = pParse->db;
  sqlite4StrAccumAppend(&errMsg, (nCol>1 ? "columns " : "column "), -1);
  for(iCol=0; iCol<pIdx->nColumn; iCol++){
    const char *zCol = indexColumnName(pIdx, iCol);
    sqlite4StrAccumAppend(&errMsg, (iCol==0 ? "" : ", "), -1);
    sqlite4StrAccumAppend(&errMsg, zCol, -1);
  }
  sqlite4StrAccumAppend(&errMsg, (nCol>1 ? " are" : " is"), -1);
  sqlite4StrAccumAppend(&errMsg, " not unique", -1);
  return sqlite4StrAccumFinish(&errMsg);
}

/*
** This function generates code used as part of both INSERT and UPDATE
** statements. The generated code performs two tasks:
**
**   1. Checks all NOT NULL, CHECK and UNIQUE database constraints, 
**      including the implicit NOT NULL and UNIQUE constraints imposed
**      by the PRIMARY KEY definition.
**
**   2. Generates serialized index keys (using OP_MakeKey) for the caller
**      to store in database indexes. This function does not encode the
**      actual data record, just the index keys.
**
** Both INSERT and UPDATE use this function in concert with the
** sqlite4CompleteInsertion(). This function does as described above, and
** then CompleteInsertion() generates code to serialize the data record 
** and do the actual inserts into the database.
**
** regContent:
**   The first in an array of registers that contain the column values
**   for the new row. Register regContent contains the value for the 
**   left-most table column, (regContent+1) contains the value for the next 
**   column, and so on. All entries in this array have had any required
**   affinity transformations applied already. All zero-blobs have been 
**   expanded.
**
**   If the table has an implicit primary key and aRegIdx[0] is not 0 (see
**   below), register (regContent-1) is also valid. It contains the new 
**   implicit integer PRIMARY KEY value.
**
** aRegIdx:
**   Array sized so that there is one entry for each index (including the
**   PK index) attached to the database table. Entries are in the same order
**   as the linked list of Index structures attached to the table. 
**
**   If an array entry is non-zero, it contains the register that the 
**   corresponding index key should be written to. If an entry is zero, then
**   the corresponding index key is not required by the caller. In this case
**   any UNIQUE constraint enforced by the index does not need to be checked.
**
** 
**
** Generate code to do constraint checks prior to an INSERT or an UPDATE.
**
** The input is a range of consecutive registers as follows:
**
**    1.  The rowid of the row after the update.
**
**    2.  The data in the first column of the entry after the update.
**
**    i.  Data from middle columns...
**
**    N.  The data in the last column of the entry after the update.
**
** The regRowid parameter is the index of the register containing (1).
**
** If isUpdate is true and rowidChng is non-zero, then rowidChng contains
** the address of a register containing the rowid before the update takes
** place. isUpdate is true for UPDATEs and false for INSERTs. If isUpdate
** is false, indicating an INSERT statement, then a non-zero rowidChng 
** indicates that the rowid was explicitly specified as part of the
** INSERT statement. If rowidChng is false, it means that  the rowid is
** computed automatically in an insert or that the rowid value is not 
** modified by an update.
**
** The code generated by this routine store new index entries into
** registers identified by aRegIdx[].  No index entry is created for
** indices where aRegIdx[i]==0.  The order of indices in aRegIdx[] is
** the same as the order of indices on the linked list of indices
** attached to the table.
**
** This routine also generates code to check constraints.  NOT NULL,
** CHECK, and UNIQUE constraints are all checked.  If a constraint fails,
** then the appropriate action is performed.  There are five possible
** actions: ROLLBACK, ABORT, FAIL, REPLACE, and IGNORE.
**
**  Constraint type  Action       What Happens
**  ---------------  ----------   ----------------------------------------
**  any              ROLLBACK     The current transaction is rolled back and
**                                sqlite4_exec() returns immediately with a
**                                return code of SQLITE_CONSTRAINT.
**
**  any              ABORT        Back out changes from the current command
**                                only (do not do a complete rollback) then
**                                cause sqlite4_exec() to return immediately
**                                with SQLITE_CONSTRAINT.
**
**  any              FAIL         Sqlite3_exec() returns immediately with a
**                                return code of SQLITE_CONSTRAINT.  The
**                                transaction is not rolled back and any
**                                prior changes are retained.
**
**  any              IGNORE       The record number and data is popped from
**                                the stack and there is an immediate jump
**                                to label ignoreDest.
**
**  NOT NULL         REPLACE      The NULL value is replace by the default
**                                value for that column.  If the default value
**                                is NULL, the action is the same as ABORT.
**
**  UNIQUE           REPLACE      The other row that conflicts with the row
**                                being inserted is removed.
**
**  CHECK            REPLACE      Illegal.  The results in an exception.
**
** Which action to take is determined by the overrideError parameter.
** Or if overrideError==OE_Default, then the pParse->onError parameter
** is used.  Or if pParse->onError==OE_Default then the onError value
** for the constraint is used.
**
** The calling routine must open a read/write cursor for pTab with
** cursor number "baseCur".  All indices of pTab must also have open
** read/write cursors with cursor number baseCur+i for the i-th cursor.
** Except, if there is no possibility of a REPLACE action then
** cursors do not need to be open for indices where aRegIdx[i]==0.
*/
void sqlite4GenerateConstraintChecks(
  Parse *pParse,      /* The parser context */
  Table *pTab,        /* the table into which we are inserting */
  int baseCur,        /* First in array of cursors for pTab indexes */
  int regContent,     /* Index of the range of input registers */
  int *aRegIdx,       /* Register used by each index.  0 for unused indices */

  int rowidChng,      /* True if the rowid might collide with existing entry */
  int isUpdate,       /* True for UPDATE, False for INSERT */

  int overrideError,  /* Override onError to this if not OE_Default */
  int ignoreDest,     /* Jump to this label on an OE_Ignore resolution */
  int *pbMayReplace   /* OUT: Set to true if constraint may cause a replace */
){
  u8 aPkRoot[10];                 /* Root page number for pPk as a varint */ 
  int nPkRoot;                    /* Size of aPkRoot in bytes */
  Index *pPk;                     /* Primary key index for table pTab */
  int i;              /* loop counter */
  Vdbe *v;            /* VDBE under constrution */
  int nCol;           /* Number of columns */
  int onError;        /* Conflict resolution strategy */
  int iCur;           /* Table cursor number */
  Index *pIdx;         /* Pointer to one of the indices */
  int seenReplace = 0; /* True if REPLACE is used to resolve INT PK conflict */

  v = sqlite4GetVdbe(pParse);
  assert( v!=0 );
  assert( pTab->pSelect==0 );  /* This table is not a VIEW */
  nCol = pTab->nCol;
  pPk = sqlite4FindPrimaryKey(pTab, 0);
  nPkRoot = sqlite4PutVarint64(aPkRoot, pPk->tnum);

  assert( pPk->eIndexType==SQLITE_INDEX_PRIMARYKEY );

  /* Test all NOT NULL constraints. */
  generateNotNullChecks(pParse, pTab, regContent, overrideError, ignoreDest);

  /* Test all CHECK constraints */
  generateCheckChecks(pParse, pTab, regContent, overrideError, ignoreDest);

  /* Test all UNIQUE constraints by creating entries for each UNIQUE
  ** index and making sure that duplicate entries do not already exist.
  ** Add the new records to the indices as we go.
  */
  for(iCur=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, iCur++){
    int nTmpReg;                  /* Number of temp registers required */
    int regTmp;                   /* First temp register allocated */
    int regPk;                    /* PK of conflicting row (for REPLACE) */

    if( aRegIdx[iCur]==0 ) continue;  /* Skip unused indices */

    /* Create an index key. Primary key indexes consists of just the primary
    ** key values. Other indexes consists of the indexed columns followed by
    ** the primary key values.  */
    nTmpReg = 1 + pIdx->nColumn + (pIdx==pPk ? 0 : pPk->nColumn);
    regTmp = sqlite4GetTempRange(pParse, nTmpReg);
    regPk = regTmp + nTmpReg - 1;

    for(i=0; i<pIdx->nColumn; i++){
      int idx = pIdx->aiColumn[i];
      sqlite4VdbeAddOp2(v, OP_SCopy, regContent+idx, regTmp+i);
    }
    if( pIdx!=pPk ){
      for(i=0; i<pPk->nColumn; i++){
        int idx = pPk->aiColumn[i];
        sqlite4VdbeAddOp2(v, OP_SCopy, regContent+idx, regTmp+i+pIdx->nColumn);
      }
    }
    sqlite4VdbeAddOp3(v, OP_MakeIdxKey, baseCur+iCur, regTmp, aRegIdx[iCur]);
    VdbeComment((v, "key for %s", pIdx->zName));

    /* If Index.onError==OE_None, then pIdx is not a UNIQUE or PRIMARY KEY 
    ** index. In this case there is no need to test the index for uniqueness
    ** - all that is required is to populate the aRegIdx[iCur] register. Jump 
    ** to the next iteration of the loop if this is the case.  */
    onError = pIdx->onError;
    if( onError!=OE_None ){
      int iTest;                  /* Address of OP_IsUnique instruction */
      int regOut = 0;

      /* Figure out what to do if a UNIQUE constraint is encountered. 
      **
      ** TODO: If a previous constraint is a REPLACE, why change IGNORE to
      ** REPLACE and FAIL to ABORT here?  */
      if( overrideError!=OE_Default ){
        onError = overrideError;
      }else if( onError==OE_Default ){
        onError = OE_Abort;
      }
      if( seenReplace ){
        if( onError==OE_Ignore ) onError = OE_Replace;
        else if( onError==OE_Fail ) onError = OE_Abort;
      }

      if( onError==OE_Replace ){
        sqlite4VdbeAddOp3(v, OP_Blob, nPkRoot, regPk, 0);
        sqlite4VdbeChangeP4(v, -1, aPkRoot, nPkRoot);
        regOut = regPk;
      }
      iTest = sqlite4VdbeAddOp3(v, OP_IsUnique, baseCur+iCur, 0, aRegIdx[iCur]);
      sqlite4VdbeChangeP4(v, -1, SQLITE_INT_TO_PTR(regOut), P4_INT32);
      
      switch( onError ){
        case OE_Rollback:
        case OE_Abort:
        case OE_Fail: {
          char *zErr = notUniqueMessage(pParse, pIdx);
          sqlite4HaltConstraint(pParse, onError, zErr, 0);
          sqlite4DbFree(pParse->db, zErr);
          break;
        }

        case OE_Ignore: {
          assert( seenReplace==0 );
          sqlite4VdbeAddOp2(v, OP_Goto, 0, ignoreDest);
          break;
        }
        default: {
          Trigger *pTrigger;
          assert( onError==OE_Replace );
          sqlite4MultiWrite(pParse);
          pTrigger = sqlite4TriggersExist(pParse, pTab, TK_DELETE, 0, 0);
          sqlite4GenerateRowDelete(
              pParse, pTab, baseCur, regOut, 0, pTrigger, OE_Replace
          );
          seenReplace = 1;
          break;
        }
      }

      /* If the OP_IsUnique passes (no constraint violation) jump here */
      sqlite4VdbeJumpHere(v, iTest);
    }

    sqlite4ReleaseTempRange(pParse, regTmp, nTmpReg);
  }
  
  if( pbMayReplace ){
    *pbMayReplace = seenReplace;
  }
}

/*
** This routine generates code to finish the INSERT or UPDATE operation
** that was started by a prior call to sqlite4GenerateConstraintChecks.
** The arguments to this routine should be the same as the first six
** arguments to sqlite4GenerateConstraintChecks.
**
** Argument regContent points to the first in a contiguous array of 
** registers that contain the row content. This function uses OP_MakeRecord
** to encode them into a record before inserting them into the database.
**
** The array aRegIdx[] contains one entry for each index attached to
** the table, in the same order as the Table.pIndex linked list. If an
** aRegIdx[] entry is 0, this indicates that the entry in the corresponding
** index does not need to be modified. Otherwise, it is the number of
** a register containing the serialized key to insert into the index.
** aRegIdx[0] (the PRIMARY KEY index key) is never 0.
*/
void sqlite4CompleteInsertion(
  Parse *pParse,      /* The parser context */
  Table *pTab,        /* the table into which we are inserting */
  int baseCur,        /* Index of a read/write cursor pointing at pTab */
  int regContent,     /* First register of content */
  int *aRegIdx,       /* Register used by each index.  0 for unused indices */
  int isUpdate,       /* True for UPDATE, False for INSERT */
  int appendBias,     /* True if this is likely to be an append */
  int useSeekResult   /* True to set the USESEEKRESULT flag on OP_[Idx]Insert */
){
  int i;
  Vdbe *v;
  Index *pIdx;
  u8 pik_flags;
  int regRec;

  v = sqlite4GetVdbe(pParse);
  assert( aRegIdx[0] );
  assert( v!=0 );
  assert( pTab->pSelect==0 );  /* This table is not a VIEW */

  if( pParse->nested ){
    pik_flags = 0;
  }else{
    pik_flags = OPFLAG_NCHANGE | (isUpdate?OPFLAG_ISUPDATE:0);
  }

  /* Generate code to serialize array of registers into a database record. */
  regRec = sqlite4GetTempReg(pParse);
  sqlite4VdbeAddOp3(v, OP_MakeRecord, regContent, pTab->nCol, regRec);
  sqlite4TableAffinityStr(v, pTab);
  sqlite4ExprCacheAffinityChange(pParse, regContent, pTab->nCol);

  /* Write the entry to each index. */
  for(i=0, pIdx=pTab->pIndex; pIdx; i++, pIdx=pIdx->pNext){
    if( aRegIdx[i] ){
      int regData = 0;
      int flags = 0;
      if( pIdx->eIndexType==SQLITE_INDEX_PRIMARYKEY ){
        regData = regRec;
        flags = pik_flags;
      }
      sqlite4VdbeAddOp3(v, OP_IdxInsert, baseCur+i, regData, aRegIdx[i]);
    }
  }
}

/*
** Generate code that will open cursors for a table and for all
** indices of that table.  The "baseCur" parameter is the cursor number used
** for the table.  Indices are opened on subsequent cursors.
**
** Return the number of indices on the table.
*/
int sqlite4OpenAllIndexes(
  Parse *pParse,   /* Parsing context */
  Table *pTab,     /* Table to be opened */
  int baseCur,     /* Cursor number assigned to the table */
  int op           /* OP_OpenRead or OP_OpenWrite */
){
  int i = 0;
  if( IsVirtual(pTab)==0 ){
    int iDb;
    Index *pIdx;

    iDb = sqlite4SchemaToIndex(pParse->db, pTab->pSchema);
    for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
      sqlite4OpenIndex(pParse, baseCur+i, iDb, pIdx, op);
      i++;
    }
    if( pParse->nTab<baseCur+i ){
      pParse->nTab = baseCur+i;
    }
  }
  return i;
}

void sqlite4CloseAllIndexes(
  Parse *pParse,
  Table *pTab,
  int baseCur
){
  int i;
  Index *pIdx;
  Vdbe *v;

  assert( pTab->pIndex==0 || IsVirtual(pTab)==0 );
#ifndef SQLITE_OMIT_VIEW
  assert( pTab->pIndex==0 || pTab->pSelect==0 );
#endif

  v = sqlite4GetVdbe(pParse);
  for(i=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, i++){
    sqlite4VdbeAddOp1(v, OP_Close, baseCur+i);
  }
}


#ifdef SQLITE_TEST
/*
** The following global variable is incremented whenever the
** transfer optimization is used.  This is used for testing
** purposes only - to make sure the transfer optimization really
** is happening when it is suppose to.
*/
int sqlite4_xferopt_count;
#endif /* SQLITE_TEST */


#ifndef SQLITE_OMIT_XFER_OPT
/*
** Check to collation names to see if they are compatible.
*/
static int xferCompatibleCollation(const char *z1, const char *z2){
  if( z1==0 ){
    return z2==0;
  }
  if( z2==0 ){
    return 0;
  }
  return sqlite4StrICmp(z1, z2)==0;
}


/*
** Check to see if index pSrc is compatible as a source of data
** for index pDest in an insert transfer optimization.  The rules
** for a compatible index:
**
**    *   The index is over the same set of columns
**    *   The same DESC and ASC markings occurs on all columns
**    *   The same onError processing (OE_Abort, OE_Ignore, etc)
**    *   The same collating sequence on each column
*/
static int xferCompatibleIndex(Index *pDest, Index *pSrc){
  int i;
  assert( pDest && pSrc );
  assert( pDest->pTable!=pSrc->pTable );
  if( pDest->nColumn!=pSrc->nColumn ){
    return 0;   /* Different number of columns */
  }
  if( pDest->onError!=pSrc->onError ){
    return 0;   /* Different conflict resolution strategies */
  }
  for(i=0; i<pSrc->nColumn; i++){
    if( pSrc->aiColumn[i]!=pDest->aiColumn[i] ){
      return 0;   /* Different columns indexed */
    }
    if( pSrc->aSortOrder[i]!=pDest->aSortOrder[i] ){
      return 0;   /* Different sort orders */
    }
    if( !xferCompatibleCollation(pSrc->azColl[i],pDest->azColl[i]) ){
      return 0;   /* Different collating sequences */
    }
  }

  /* If no test above fails then the indices must be compatible */
  return 1;
}

/*
** Attempt the transfer optimization on INSERTs of the form
**
**     INSERT INTO tab1 SELECT * FROM tab2;
**
** The xfer optimization transfers raw records from tab2 over to tab1.  
** Columns are not decoded and reassemblied, which greatly improves
** performance.  Raw index records are transferred in the same way.
**
** The xfer optimization is only attempted if tab1 and tab2 are compatible.
** There are lots of rules for determining compatibility - see comments
** embedded in the code for details.
**
** This routine returns TRUE if the optimization is guaranteed to be used.
** Sometimes the xfer optimization will only work if the destination table
** is empty - a factor that can only be determined at run-time.  In that
** case, this routine generates code for the xfer optimization but also
** does a test to see if the destination table is empty and jumps over the
** xfer optimization code if the test fails.  In that case, this routine
** returns FALSE so that the caller will know to go ahead and generate
** an unoptimized transfer.  This routine also returns FALSE if there
** is no chance that the xfer optimization can be applied.
**
** This optimization is particularly useful at making VACUUM run faster.
*/
static int xferOptimization(
  Parse *pParse,        /* Parser context */
  Table *pDest,         /* The table we are inserting into */
  Select *pSelect,      /* A SELECT statement to use as the data source */
  int onError,          /* How to handle constraint errors */
  int iDbDest           /* The database of pDest */
){
  ExprList *pEList;                /* The result set of the SELECT */
  Table *pSrc;                     /* The table in the FROM clause of SELECT */
  Index *pSrcIdx, *pDestIdx;       /* Source and destination indices */
  struct SrcList_item *pItem;      /* An element of pSelect->pSrc */
  int i;                           /* Loop counter */
  int iDbSrc;                      /* The database of pSrc */
  int iSrc, iDest;                 /* Cursors from source and destination */
  int addr1, addr2;                /* Loop addresses */
  int emptyDestTest;               /* Address of test for empty pDest */
  int emptySrcTest;                /* Address of test for empty pSrc */
  Vdbe *v;                         /* The VDBE we are building */
  KeyInfo *pKey;                   /* Key information for an index */
  int regAutoinc;                  /* Memory register used by AUTOINC */
  int destHasUniqueIdx = 0;        /* True if pDest has a UNIQUE index */
  int regData, regRowid, regKey;   /* Registers holding data and rowid */

  if( pSelect==0 ){
    return 0;   /* Must be of the form  INSERT INTO ... SELECT ... */
  }
  if( sqlite4TriggerList(pParse, pDest) ){
    return 0;   /* tab1 must not have triggers */
  }
#ifndef SQLITE_OMIT_VIRTUALTABLE
  if( pDest->tabFlags & TF_Virtual ){
    return 0;   /* tab1 must not be a virtual table */
  }
#endif
  if( onError==OE_Default ){
    if( pDest->iPKey>=0 ) onError = pDest->keyConf;
    if( onError==OE_Default ) onError = OE_Abort;
  }
  assert(pSelect->pSrc);   /* allocated even if there is no FROM clause */
  if( pSelect->pSrc->nSrc!=1 ){
    return 0;   /* FROM clause must have exactly one term */
  }
  if( pSelect->pSrc->a[0].pSelect ){
    return 0;   /* FROM clause cannot contain a subquery */
  }
  if( pSelect->pWhere ){
    return 0;   /* SELECT may not have a WHERE clause */
  }
  if( pSelect->pOrderBy ){
    return 0;   /* SELECT may not have an ORDER BY clause */
  }
  /* Do not need to test for a HAVING clause.  If HAVING is present but
  ** there is no ORDER BY, we will get an error. */
  if( pSelect->pGroupBy ){
    return 0;   /* SELECT may not have a GROUP BY clause */
  }
  if( pSelect->pLimit ){
    return 0;   /* SELECT may not have a LIMIT clause */
  }
  assert( pSelect->pOffset==0 );  /* Must be so if pLimit==0 */
  if( pSelect->pPrior ){
    return 0;   /* SELECT may not be a compound query */
  }
  if( pSelect->selFlags & SF_Distinct ){
    return 0;   /* SELECT may not be DISTINCT */
  }
  pEList = pSelect->pEList;
  assert( pEList!=0 );
  if( pEList->nExpr!=1 ){
    return 0;   /* The result set must have exactly one column */
  }
  assert( pEList->a[0].pExpr );
  if( pEList->a[0].pExpr->op!=TK_ALL ){
    return 0;   /* The result set must be the special operator "*" */
  }

  /* At this point we have established that the statement is of the
  ** correct syntactic form to participate in this optimization.  Now
  ** we have to check the semantics.
  */
  pItem = pSelect->pSrc->a;
  pSrc = sqlite4LocateTable(pParse, 0, pItem->zName, pItem->zDatabase);
  if( pSrc==0 ){
    return 0;   /* FROM clause does not contain a real table */
  }
  if( pSrc==pDest ){
    return 0;   /* tab1 and tab2 may not be the same table */
  }
#ifndef SQLITE_OMIT_VIRTUALTABLE
  if( pSrc->tabFlags & TF_Virtual ){
    return 0;   /* tab2 must not be a virtual table */
  }
#endif
  if( pSrc->pSelect ){
    return 0;   /* tab2 may not be a view */
  }
  if( pDest->nCol!=pSrc->nCol ){
    return 0;   /* Number of columns must be the same in tab1 and tab2 */
  }
  if( pDest->iPKey!=pSrc->iPKey ){
    return 0;   /* Both tables must have the same INTEGER PRIMARY KEY */
  }
  for(i=0; i<pDest->nCol; i++){
    if( pDest->aCol[i].affinity!=pSrc->aCol[i].affinity ){
      return 0;    /* Affinity must be the same on all columns */
    }
    if( !xferCompatibleCollation(pDest->aCol[i].zColl, pSrc->aCol[i].zColl) ){
      return 0;    /* Collating sequence must be the same on all columns */
    }
    if( pDest->aCol[i].notNull && !pSrc->aCol[i].notNull ){
      return 0;    /* tab2 must be NOT NULL if tab1 is */
    }
  }
  for(pDestIdx=pDest->pIndex; pDestIdx; pDestIdx=pDestIdx->pNext){
    if( pDestIdx->onError!=OE_None ){
      destHasUniqueIdx = 1;
    }
    for(pSrcIdx=pSrc->pIndex; pSrcIdx; pSrcIdx=pSrcIdx->pNext){
      if( xferCompatibleIndex(pDestIdx, pSrcIdx) ) break;
    }
    if( pSrcIdx==0 ){
      return 0;    /* pDestIdx has no corresponding index in pSrc */
    }
  }
#ifndef SQLITE_OMIT_CHECK
  if( pDest->pCheck && sqlite4ExprCompare(pSrc->pCheck, pDest->pCheck) ){
    return 0;   /* Tables have different CHECK constraints.  Ticket #2252 */
  }
#endif
#ifndef SQLITE_OMIT_FOREIGN_KEY
  /* Disallow the transfer optimization if the destination table constains
  ** any foreign key constraints.  This is more restrictive than necessary.
  ** But the main beneficiary of the transfer optimization is the VACUUM 
  ** command, and the VACUUM command disables foreign key constraints.  So
  ** the extra complication to make this rule less restrictive is probably
  ** not worth the effort.  Ticket [6284df89debdfa61db8073e062908af0c9b6118e]
  */
  if( (pParse->db->flags & SQLITE_ForeignKeys)!=0 && pDest->pFKey!=0 ){
    return 0;
  }
#endif
  if( (pParse->db->flags & SQLITE_CountRows)!=0 ){
    return 0;  /* xfer opt does not play well with PRAGMA count_changes */
  }

  /* If we get this far, it means that the xfer optimization is at
  ** least a possibility, though it might only work if the destination
  ** table (tab1) is initially empty.
  */
#ifdef SQLITE_TEST
  sqlite4_xferopt_count++;
#endif
  iDbSrc = sqlite4SchemaToIndex(pParse->db, pSrc->pSchema);
  v = sqlite4GetVdbe(pParse);
  sqlite4CodeVerifySchema(pParse, iDbSrc);
  iSrc = pParse->nTab++;
  iDest = pParse->nTab++;
  regAutoinc = autoIncBegin(pParse, iDbDest, pDest);
  sqlite4OpenTable(pParse, iDest, iDbDest, pDest, OP_OpenWrite);
  if( (pDest->iPKey<0 && pDest->pIndex!=0)          /* (1) */
   || destHasUniqueIdx                              /* (2) */
   || (onError!=OE_Abort && onError!=OE_Rollback)   /* (3) */
  ){
    /* In some circumstances, we are able to run the xfer optimization
    ** only if the destination table is initially empty.  This code makes
    ** that determination.  Conditions under which the destination must
    ** be empty:
    **
    ** (1) There is no INTEGER PRIMARY KEY but there are indices.
    **     (If the destination is not initially empty, the rowid fields
    **     of index entries might need to change.)
    **
    ** (2) The destination has a unique index.  (The xfer optimization 
    **     is unable to test uniqueness.)
    **
    ** (3) onError is something other than OE_Abort and OE_Rollback.
    */
    addr1 = sqlite4VdbeAddOp2(v, OP_Rewind, iDest, 0);
    emptyDestTest = sqlite4VdbeAddOp2(v, OP_Goto, 0, 0);
    sqlite4VdbeJumpHere(v, addr1);
  }else{
    emptyDestTest = 0;
  }
  sqlite4OpenTable(pParse, iSrc, iDbSrc, pSrc, OP_OpenRead);
  emptySrcTest = sqlite4VdbeAddOp2(v, OP_Rewind, iSrc, 0);
  regKey = sqlite4GetTempReg(pParse);
  regData = sqlite4GetTempReg(pParse);
  regRowid = sqlite4GetTempReg(pParse);
  if( pDest->iPKey>=0 ){
    addr1 = sqlite4VdbeAddOp2(v, OP_Rowid, iSrc, regRowid);
    addr2 = sqlite4VdbeAddOp3(v, OP_NotExists, iDest, 0, regRowid);
    sqlite4HaltConstraint(
        pParse, onError, "PRIMARY KEY must be unique", P4_STATIC);
    sqlite4VdbeJumpHere(v, addr2);
    autoIncStep(pParse, regAutoinc, regRowid);
  }else if( pDest->pIndex==0 ){
    addr1 = sqlite4VdbeAddOp2(v, OP_NewRowid, iDest, regRowid);
  }else{
    addr1 = sqlite4VdbeAddOp2(v, OP_Rowid, iSrc, regRowid);
    assert( (pDest->tabFlags & TF_Autoincrement)==0 );
  }
  sqlite4VdbeAddOp2(v, OP_RowData, iSrc, regData);
  sqlite4VdbeAddOp3(v, OP_Insert, iDest, regData, regRowid);
  sqlite4VdbeChangeP5(v, OPFLAG_NCHANGE|OPFLAG_LASTROWID|OPFLAG_APPEND);
  sqlite4VdbeChangeP4(v, -1, pDest->zName, 0);
  sqlite4VdbeAddOp2(v, OP_Next, iSrc, addr1);
  for(pDestIdx=pDest->pIndex; pDestIdx; pDestIdx=pDestIdx->pNext){
    for(pSrcIdx=pSrc->pIndex; ALWAYS(pSrcIdx); pSrcIdx=pSrcIdx->pNext){
      if( xferCompatibleIndex(pDestIdx, pSrcIdx) ) break;
    }
    assert( pSrcIdx );
    sqlite4VdbeAddOp2(v, OP_Close, iSrc, 0);
    sqlite4VdbeAddOp2(v, OP_Close, iDest, 0);
    pKey = sqlite4IndexKeyinfo(pParse, pSrcIdx);
    sqlite4VdbeAddOp4(v, OP_OpenRead, iSrc, pSrcIdx->tnum, iDbSrc,
                      (char*)pKey, P4_KEYINFO_HANDOFF);
    VdbeComment((v, "%s", pSrcIdx->zName));
    pKey = sqlite4IndexKeyinfo(pParse, pDestIdx);
    sqlite4VdbeAddOp4(v, OP_OpenWrite, iDest, pDestIdx->tnum, iDbDest,
                      (char*)pKey, P4_KEYINFO_HANDOFF);
    VdbeComment((v, "%s", pDestIdx->zName));
    addr1 = sqlite4VdbeAddOp2(v, OP_Rewind, iSrc, 0);
    sqlite4VdbeAddOp2(v, OP_RowKey, iSrc, regKey);
    sqlite4VdbeAddOp2(v, OP_RowData, iSrc, regData);
    sqlite4VdbeAddOp3(v, OP_IdxInsert, iDest, regKey, regData);
    sqlite4VdbeChangeP5(v, OPFLAG_APPENDBIAS);
    sqlite4VdbeAddOp2(v, OP_Next, iSrc, addr1+1);
    sqlite4VdbeJumpHere(v, addr1);
  }
  sqlite4VdbeJumpHere(v, emptySrcTest);
  sqlite4ReleaseTempReg(pParse, regRowid);
  sqlite4ReleaseTempReg(pParse, regData);
  sqlite4ReleaseTempReg(pParse, regKey);
  sqlite4VdbeAddOp2(v, OP_Close, iSrc, 0);
  sqlite4VdbeAddOp2(v, OP_Close, iDest, 0);
  if( emptyDestTest ){
    sqlite4VdbeAddOp2(v, OP_Halt, SQLITE_OK, 0);
    sqlite4VdbeJumpHere(v, emptyDestTest);
    sqlite4VdbeAddOp2(v, OP_Close, iDest, 0);
    return 0;
  }else{
    return 1;
  }
}
#endif /* SQLITE_OMIT_XFER_OPT */
