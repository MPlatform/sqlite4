/*
** 2007 May 1
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
** This file contains code used to implement incremental BLOB I/O.
*/

#include "sqliteInt.h"
#include "vdbeInt.h"

#ifndef SQLITE_OMIT_INCRBLOB

/*
** Valid sqlite4_blob* handles point to Incrblob structures.
*/
typedef struct Incrblob Incrblob;
struct Incrblob {
  int flags;              /* Copy of "flags" passed to sqlite4_blob_open() */
  int nByte;              /* Size of open blob, in bytes */
  int iOffset;            /* Byte offset of blob in cursor data */
  int iCol;               /* Table column this handle is open on */
  BtCursor *pCsr;         /* Cursor pointing at blob row */
  sqlite4_stmt *pStmt;    /* Statement holding cursor open */
  sqlite4 *db;            /* The associated database */
};


/*
** This function is used by both blob_open() and blob_reopen(). It seeks
** the b-tree cursor associated with blob handle p to point to row iRow.
** If successful, SQLITE_OK is returned and subsequent calls to
** sqlite4_blob_read() or sqlite4_blob_write() access the specified row.
**
** If an error occurs, or if the specified row does not exist or does not
** contain a value of type TEXT or BLOB in the column nominated when the
** blob handle was opened, then an error code is returned and *pzErr may
** be set to point to a buffer containing an error message. It is the
** responsibility of the caller to free the error message buffer using
** sqlite4DbFree().
**
** If an error does occur, then the b-tree cursor is closed. All subsequent
** calls to sqlite4_blob_read(), blob_write() or blob_reopen() will 
** immediately return SQLITE_ABORT.
*/
static int blobSeekToRow(Incrblob *p, sqlite4_int64 iRow, char **pzErr){
  int rc;                         /* Error code */
  char *zErr = 0;                 /* Error message */
  Vdbe *v = (Vdbe *)p->pStmt;

  /* Set the value of the SQL statements only variable to integer iRow. 
  ** This is done directly instead of using sqlite4_bind_int64() to avoid 
  ** triggering asserts related to mutexes.
  */
  assert( v->aVar[0].flags&MEM_Int );
  v->aVar[0].u.i = iRow;

  rc = sqlite4_step(p->pStmt);
  if( rc==SQLITE_ROW ){
    u32 type = v->apCsr[0]->aType[p->iCol];
    if( type<12 ){
      zErr = sqlite4MPrintf(p->db, "cannot open value of type %s",
          type==0?"null": type==7?"real": "integer"
      );
      rc = SQLITE_ERROR;
      sqlite4_finalize(p->pStmt);
      p->pStmt = 0;
    }else{
      p->iOffset = v->apCsr[0]->aOffset[p->iCol];
      p->nByte = sqlite4VdbeSerialTypeLen(type);
      p->pCsr =  v->apCsr[0]->pCursor;
      sqlite4BtreeEnterCursor(p->pCsr);
      sqlite4BtreeCacheOverflow(p->pCsr);
      sqlite4BtreeLeaveCursor(p->pCsr);
    }
  }

  if( rc==SQLITE_ROW ){
    rc = SQLITE_OK;
  }else if( p->pStmt ){
    rc = sqlite4_finalize(p->pStmt);
    p->pStmt = 0;
    if( rc==SQLITE_OK ){
      zErr = sqlite4MPrintf(p->db, "no such rowid: %lld", iRow);
      rc = SQLITE_ERROR;
    }else{
      zErr = sqlite4MPrintf(p->db, "%s", sqlite4_errmsg(p->db));
    }
  }

  assert( rc!=SQLITE_OK || zErr==0 );
  assert( rc!=SQLITE_ROW && rc!=SQLITE_DONE );

  *pzErr = zErr;
  return rc;
}

/*
** Open a blob handle.
*/
int sqlite4_blob_open(
  sqlite4* db,            /* The database connection */
  const char *zDb,        /* The attached database containing the blob */
  const char *zTable,     /* The table containing the blob */
  const char *zColumn,    /* The column containing the blob */
  sqlite_int64 iRow,      /* The row containing the glob */
  int flags,              /* True -> read/write access, false -> read-only */
  sqlite4_blob **ppBlob   /* Handle for accessing the blob returned here */
){
  int nAttempt = 0;
  int iCol;               /* Index of zColumn in row-record */

  /* This VDBE program seeks a btree cursor to the identified 
  ** db/table/row entry. The reason for using a vdbe program instead
  ** of writing code to use the b-tree layer directly is that the
  ** vdbe program will take advantage of the various transaction,
  ** locking and error handling infrastructure built into the vdbe.
  **
  ** After seeking the cursor, the vdbe executes an OP_ResultRow.
  ** Code external to the Vdbe then "borrows" the b-tree cursor and
  ** uses it to implement the blob_read(), blob_write() and 
  ** blob_bytes() functions.
  **
  ** The sqlite4_blob_close() function finalizes the vdbe program,
  ** which closes the b-tree cursor and (possibly) commits the 
  ** transaction.
  */
  static const VdbeOpList openBlob[] = {
    {OP_Transaction, 0, 0, 0},     /* 0: Start a transaction */
    {OP_VerifyCookie, 0, 0, 0},    /* 1: Check the schema cookie */
    {OP_TableLock, 0, 0, 0},       /* 2: Acquire a read or write lock */

    /* One of the following two instructions is replaced by an OP_Noop. */
    {OP_OpenRead, 0, 0, 0},        /* 3: Open cursor 0 for reading */
    {OP_OpenWrite, 0, 0, 0},       /* 4: Open cursor 0 for read/write */

    {OP_Variable, 1, 1, 1},        /* 5: Push the rowid to the stack */
    {OP_NotExists, 0, 10, 1},      /* 6: Seek the cursor */
    {OP_Column, 0, 0, 1},          /* 7  */
    {OP_ResultRow, 1, 0, 0},       /* 8  */
    {OP_Goto, 0, 5, 0},            /* 9  */
    {OP_Close, 0, 0, 0},           /* 10 */
    {OP_Halt, 0, 0, 0},            /* 11 */
  };

  int rc = SQLITE_OK;
  char *zErr = 0;
  Table *pTab;
  Parse *pParse = 0;
  Incrblob *pBlob = 0;

  flags = !!flags;                /* flags = (flags ? 1 : 0); */
  *ppBlob = 0;

  sqlite4_mutex_enter(db->mutex);

  pBlob = (Incrblob *)sqlite4DbMallocZero(db, sizeof(Incrblob));
  if( !pBlob ) goto blob_open_out;
  pParse = sqlite4StackAllocRaw(db, sizeof(*pParse));
  if( !pParse ) goto blob_open_out;

  do {
    memset(pParse, 0, sizeof(Parse));
    pParse->db = db;
    sqlite4DbFree(db, zErr);
    zErr = 0;

    sqlite4BtreeEnterAll(db);
    pTab = sqlite4LocateTable(pParse, 0, zTable, zDb);
    if( pTab && IsVirtual(pTab) ){
      pTab = 0;
      sqlite4ErrorMsg(pParse, "cannot open virtual table: %s", zTable);
    }
#ifndef SQLITE_OMIT_VIEW
    if( pTab && pTab->pSelect ){
      pTab = 0;
      sqlite4ErrorMsg(pParse, "cannot open view: %s", zTable);
    }
#endif
    if( !pTab ){
      if( pParse->zErrMsg ){
        sqlite4DbFree(db, zErr);
        zErr = pParse->zErrMsg;
        pParse->zErrMsg = 0;
      }
      rc = SQLITE_ERROR;
      sqlite4BtreeLeaveAll(db);
      goto blob_open_out;
    }

    /* Now search pTab for the exact column. */
    for(iCol=0; iCol<pTab->nCol; iCol++) {
      if( sqlite4StrICmp(pTab->aCol[iCol].zName, zColumn)==0 ){
        break;
      }
    }
    if( iCol==pTab->nCol ){
      sqlite4DbFree(db, zErr);
      zErr = sqlite4MPrintf(db, "no such column: \"%s\"", zColumn);
      rc = SQLITE_ERROR;
      sqlite4BtreeLeaveAll(db);
      goto blob_open_out;
    }

    /* If the value is being opened for writing, check that the
    ** column is not indexed, and that it is not part of a foreign key. 
    ** It is against the rules to open a column to which either of these
    ** descriptions applies for writing.  */
    if( flags ){
      const char *zFault = 0;
      Index *pIdx;
#ifndef SQLITE_OMIT_FOREIGN_KEY
      if( db->flags&SQLITE_ForeignKeys ){
        /* Check that the column is not part of an FK child key definition. It
        ** is not necessary to check if it is part of a parent key, as parent
        ** key columns must be indexed. The check below will pick up this 
        ** case.  */
        FKey *pFKey;
        for(pFKey=pTab->pFKey; pFKey; pFKey=pFKey->pNextFrom){
          int j;
          for(j=0; j<pFKey->nCol; j++){
            if( pFKey->aCol[j].iFrom==iCol ){
              zFault = "foreign key";
            }
          }
        }
      }
#endif
      for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
        int j;
        for(j=0; j<pIdx->nColumn; j++){
          if( pIdx->aiColumn[j]==iCol ){
            zFault = "indexed";
          }
        }
      }
      if( zFault ){
        sqlite4DbFree(db, zErr);
        zErr = sqlite4MPrintf(db, "cannot open %s column for writing", zFault);
        rc = SQLITE_ERROR;
        sqlite4BtreeLeaveAll(db);
        goto blob_open_out;
      }
    }

    pBlob->pStmt = (sqlite4_stmt *)sqlite4VdbeCreate(db);
    assert( pBlob->pStmt || db->mallocFailed );
    if( pBlob->pStmt ){
      Vdbe *v = (Vdbe *)pBlob->pStmt;
      int iDb = sqlite4SchemaToIndex(db, pTab->pSchema);

      sqlite4VdbeAddOpList(v, sizeof(openBlob)/sizeof(VdbeOpList), openBlob);


      /* Configure the OP_Transaction */
      sqlite4VdbeChangeP1(v, 0, iDb);
      sqlite4VdbeChangeP2(v, 0, flags);

      /* Configure the OP_VerifyCookie */
      sqlite4VdbeChangeP1(v, 1, iDb);
      sqlite4VdbeChangeP2(v, 1, pTab->pSchema->schema_cookie);
      sqlite4VdbeChangeP3(v, 1, pTab->pSchema->iGeneration);

      /* Make sure a mutex is held on the table to be accessed */
      sqlite4VdbeUsesBtree(v, iDb); 

      /* Configure the OP_TableLock instruction */
#ifdef SQLITE_OMIT_SHARED_CACHE
      sqlite4VdbeChangeToNoop(v, 2);
#else
      sqlite4VdbeChangeP1(v, 2, iDb);
      sqlite4VdbeChangeP2(v, 2, pTab->tnum);
      sqlite4VdbeChangeP3(v, 2, flags);
      sqlite4VdbeChangeP4(v, 2, pTab->zName, P4_TRANSIENT);
#endif

      /* Remove either the OP_OpenWrite or OpenRead. Set the P2 
      ** parameter of the other to pTab->tnum.  */
      sqlite4VdbeChangeToNoop(v, 4 - flags);
      sqlite4VdbeChangeP2(v, 3 + flags, pTab->tnum);
      sqlite4VdbeChangeP3(v, 3 + flags, iDb);

      /* Configure the number of columns. Configure the cursor to
      ** think that the table has one more column than it really
      ** does. An OP_Column to retrieve this imaginary column will
      ** always return an SQL NULL. This is useful because it means
      ** we can invoke OP_Column to fill in the vdbe cursors type 
      ** and offset cache without causing any IO.
      */
      sqlite4VdbeChangeP4(v, 3+flags, SQLITE_INT_TO_PTR(pTab->nCol+1),P4_INT32);
      sqlite4VdbeChangeP2(v, 7, pTab->nCol);
      if( !db->mallocFailed ){
        pParse->nVar = 1;
        pParse->nMem = 1;
        pParse->nTab = 1;
        sqlite4VdbeMakeReady(v, pParse);
      }
    }
   
    pBlob->flags = flags;
    pBlob->iCol = iCol;
    pBlob->db = db;
    sqlite4BtreeLeaveAll(db);
    if( db->mallocFailed ){
      goto blob_open_out;
    }
    sqlite4_bind_int64(pBlob->pStmt, 1, iRow);
    rc = blobSeekToRow(pBlob, iRow, &zErr);
  } while( (++nAttempt)<5 && rc==SQLITE_SCHEMA );

blob_open_out:
  if( rc==SQLITE_OK && db->mallocFailed==0 ){
    *ppBlob = (sqlite4_blob *)pBlob;
  }else{
    if( pBlob && pBlob->pStmt ) sqlite4VdbeFinalize((Vdbe *)pBlob->pStmt);
    sqlite4DbFree(db, pBlob);
  }
  sqlite4Error(db, rc, (zErr ? "%s" : 0), zErr);
  sqlite4DbFree(db, zErr);
  sqlite4StackFree(db, pParse);
  rc = sqlite4ApiExit(db, rc);
  sqlite4_mutex_leave(db->mutex);
  return rc;
}

/*
** Close a blob handle that was previously created using
** sqlite4_blob_open().
*/
int sqlite4_blob_close(sqlite4_blob *pBlob){
  Incrblob *p = (Incrblob *)pBlob;
  int rc;
  sqlite4 *db;

  if( p ){
    db = p->db;
    sqlite4_mutex_enter(db->mutex);
    rc = sqlite4_finalize(p->pStmt);
    sqlite4DbFree(db, p);
    sqlite4_mutex_leave(db->mutex);
  }else{
    rc = SQLITE_OK;
  }
  return rc;
}

/*
** Perform a read or write operation on a blob
*/
static int blobReadWrite(
  sqlite4_blob *pBlob, 
  void *z, 
  int n, 
  int iOffset, 
  int (*xCall)(BtCursor*, u32, u32, void*)
){
  int rc;
  Incrblob *p = (Incrblob *)pBlob;
  Vdbe *v;
  sqlite4 *db;

  if( p==0 ) return SQLITE_MISUSE_BKPT;
  db = p->db;
  sqlite4_mutex_enter(db->mutex);
  v = (Vdbe*)p->pStmt;

  if( n<0 || iOffset<0 || (iOffset+n)>p->nByte ){
    /* Request is out of range. Return a transient error. */
    rc = SQLITE_ERROR;
    sqlite4Error(db, SQLITE_ERROR, 0);
  }else if( v==0 ){
    /* If there is no statement handle, then the blob-handle has
    ** already been invalidated. Return SQLITE_ABORT in this case.
    */
    rc = SQLITE_ABORT;
  }else{
    /* Call either BtreeData() or BtreePutData(). If SQLITE_ABORT is
    ** returned, clean-up the statement handle.
    */
    assert( db == v->db );
    sqlite4BtreeEnterCursor(p->pCsr);
    rc = xCall(p->pCsr, iOffset+p->iOffset, n, z);
    sqlite4BtreeLeaveCursor(p->pCsr);
    if( rc==SQLITE_ABORT ){
      sqlite4VdbeFinalize(v);
      p->pStmt = 0;
    }else{
      db->errCode = rc;
      v->rc = rc;
    }
  }
  rc = sqlite4ApiExit(db, rc);
  sqlite4_mutex_leave(db->mutex);
  return rc;
}

/*
** Read data from a blob handle.
*/
int sqlite4_blob_read(sqlite4_blob *pBlob, void *z, int n, int iOffset){
  return blobReadWrite(pBlob, z, n, iOffset, sqlite4BtreeData);
}

/*
** Write data to a blob handle.
*/
int sqlite4_blob_write(sqlite4_blob *pBlob, const void *z, int n, int iOffset){
  return blobReadWrite(pBlob, (void *)z, n, iOffset, sqlite4BtreePutData);
}

/*
** Query a blob handle for the size of the data.
**
** The Incrblob.nByte field is fixed for the lifetime of the Incrblob
** so no mutex is required for access.
*/
int sqlite4_blob_bytes(sqlite4_blob *pBlob){
  Incrblob *p = (Incrblob *)pBlob;
  return (p && p->pStmt) ? p->nByte : 0;
}

/*
** Move an existing blob handle to point to a different row of the same
** database table.
**
** If an error occurs, or if the specified row does not exist or does not
** contain a blob or text value, then an error code is returned and the
** database handle error code and message set. If this happens, then all 
** subsequent calls to sqlite4_blob_xxx() functions (except blob_close()) 
** immediately return SQLITE_ABORT.
*/
int sqlite4_blob_reopen(sqlite4_blob *pBlob, sqlite4_int64 iRow){
  int rc;
  Incrblob *p = (Incrblob *)pBlob;
  sqlite4 *db;

  if( p==0 ) return SQLITE_MISUSE_BKPT;
  db = p->db;
  sqlite4_mutex_enter(db->mutex);

  if( p->pStmt==0 ){
    /* If there is no statement handle, then the blob-handle has
    ** already been invalidated. Return SQLITE_ABORT in this case.
    */
    rc = SQLITE_ABORT;
  }else{
    char *zErr;
    rc = blobSeekToRow(p, iRow, &zErr);
    if( rc!=SQLITE_OK ){
      sqlite4Error(db, rc, (zErr ? "%s" : 0), zErr);
      sqlite4DbFree(db, zErr);
    }
    assert( rc!=SQLITE_SCHEMA );
  }

  rc = sqlite4ApiExit(db, rc);
  assert( rc==SQLITE_OK || p->pStmt==0 );
  sqlite4_mutex_leave(db->mutex);
  return rc;
}

#endif /* #ifndef SQLITE_OMIT_INCRBLOB */
