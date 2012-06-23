/*
** 2004 May 26
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
** This file contains code use to implement APIs that are part of the
** VDBE.
*/
#include "sqliteInt.h"
#include "vdbeInt.h"

#ifndef SQLITE_OMIT_DEPRECATED
/*
** Return TRUE (non-zero) of the statement supplied as an argument needs
** to be recompiled.  A statement needs to be recompiled whenever the
** execution environment changes in a way that would alter the program
** that sqlite4_prepare() generates.  For example, if new functions or
** collating sequences are registered or if an authorizer function is
** added or changed.
*/
int sqlite4_expired(sqlite4_stmt *pStmt){
  Vdbe *p = (Vdbe*)pStmt;
  return p==0 || p->expired;
}
#endif

/*
** Check on a Vdbe to make sure it has not been finalized.  Log
** an error and return true if it has been finalized (or is otherwise
** invalid).  Return false if it is ok.
*/
static int vdbeSafety(Vdbe *p){
  if( p->db==0 ){
    sqlite4_log(SQLITE_MISUSE, "API called with finalized prepared statement");
    return 1;
  }else{
    return 0;
  }
}
static int vdbeSafetyNotNull(Vdbe *p){
  if( p==0 ){
    sqlite4_log(SQLITE_MISUSE, "API called with NULL prepared statement");
    return 1;
  }else{
    return vdbeSafety(p);
  }
}

/*
** The following routine destroys a virtual machine that is created by
** the sqlite4_compile() routine. The integer returned is an SQLITE_
** success/failure code that describes the result of executing the virtual
** machine.
**
** This routine sets the error code and string returned by
** sqlite4_errcode(), sqlite4_errmsg() and sqlite4_errmsg16().
*/
int sqlite4_finalize(sqlite4_stmt *pStmt){
  int rc;
  if( pStmt==0 ){
    /* IMPLEMENTATION-OF: R-57228-12904 Invoking sqlite4_finalize() on a NULL
    ** pointer is a harmless no-op. */
    rc = SQLITE_OK;
  }else{
    Vdbe *v = (Vdbe*)pStmt;
    sqlite4 *db = v->db;
#if SQLITE_THREADSAFE
    sqlite4_mutex *mutex;
#endif
    if( vdbeSafety(v) ) return SQLITE_MISUSE_BKPT;
#if SQLITE_THREADSAFE
    mutex = v->db->mutex;
#endif
    sqlite4_mutex_enter(mutex);
    rc = sqlite4VdbeFinalize(v);
    rc = sqlite4ApiExit(db, rc);
    sqlite4_mutex_leave(mutex);
  }
  return rc;
}

/*
** Terminate the current execution of an SQL statement and reset it
** back to its starting state so that it can be reused. A success code from
** the prior execution is returned.
**
** This routine sets the error code and string returned by
** sqlite4_errcode(), sqlite4_errmsg() and sqlite4_errmsg16().
*/
int sqlite4_reset(sqlite4_stmt *pStmt){
  int rc;
  if( pStmt==0 ){
    rc = SQLITE_OK;
  }else{
    Vdbe *v = (Vdbe*)pStmt;
    sqlite4_mutex_enter(v->db->mutex);
    rc = sqlite4VdbeReset(v);
    sqlite4VdbeRewind(v);
    rc = sqlite4ApiExit(v->db, rc);
    sqlite4_mutex_leave(v->db->mutex);
  }
  return rc;
}

/*
** Set all the parameters in the compiled SQL statement to NULL.
*/
int sqlite4_clear_bindings(sqlite4_stmt *pStmt){
  int i;
  int rc = SQLITE_OK;
  Vdbe *p = (Vdbe*)pStmt;
#if SQLITE_THREADSAFE
  sqlite4_mutex *mutex = ((Vdbe*)pStmt)->db->mutex;
#endif
  sqlite4_mutex_enter(mutex);
  for(i=0; i<p->nVar; i++){
    sqlite4VdbeMemRelease(&p->aVar[i]);
    p->aVar[i].flags = MEM_Null;
  }
  if( p->expmask ){
    p->expired = 1;
  }
  sqlite4_mutex_leave(mutex);
  return rc;
}


/**************************** sqlite4_value_  *******************************
** The following routines extract information from a Mem or sqlite4_value
** structure.
*/
const void *sqlite4_value_blob(sqlite4_value *pVal){
  Mem *p = (Mem*)pVal;
  if( p->flags & (MEM_Blob|MEM_Str) ){
    sqlite4VdbeMemExpandBlob(p);
    p->flags &= ~MEM_Str;
    p->flags |= MEM_Blob;
    return p->n ? p->z : 0;
  }else{
    return sqlite4_value_text(pVal);
  }
}
int sqlite4_value_bytes(sqlite4_value *pVal){
  return sqlite4ValueBytes(pVal, SQLITE_UTF8);
}
int sqlite4_value_bytes16(sqlite4_value *pVal){
  return sqlite4ValueBytes(pVal, SQLITE_UTF16NATIVE);
}
double sqlite4_value_double(sqlite4_value *pVal){
  return sqlite4VdbeRealValue((Mem*)pVal);
}
int sqlite4_value_int(sqlite4_value *pVal){
  return (int)sqlite4VdbeIntValue((Mem*)pVal);
}
sqlite_int64 sqlite4_value_int64(sqlite4_value *pVal){
  return sqlite4VdbeIntValue((Mem*)pVal);
}
const unsigned char *sqlite4_value_text(sqlite4_value *pVal){
  return (const unsigned char *)sqlite4ValueText(pVal, SQLITE_UTF8);
}
#ifndef SQLITE_OMIT_UTF16
const void *sqlite4_value_text16(sqlite4_value* pVal){
  return sqlite4ValueText(pVal, SQLITE_UTF16NATIVE);
}
const void *sqlite4_value_text16be(sqlite4_value *pVal){
  return sqlite4ValueText(pVal, SQLITE_UTF16BE);
}
const void *sqlite4_value_text16le(sqlite4_value *pVal){
  return sqlite4ValueText(pVal, SQLITE_UTF16LE);
}
#endif /* SQLITE_OMIT_UTF16 */
int sqlite4_value_type(sqlite4_value* pVal){
  return pVal->type;
}

/**************************** sqlite4_result_  *******************************
** The following routines are used by user-defined functions to specify
** the function result.
**
** The setStrOrError() funtion calls sqlite4VdbeMemSetStr() to store the
** result as a string or blob but if the string or blob is too large, it
** then sets the error code to SQLITE_TOOBIG
*/
static void setResultStrOrError(
  sqlite4_context *pCtx,  /* Function context */
  const char *z,          /* String pointer */
  int n,                  /* Bytes in string, or negative */
  u8 enc,                 /* Encoding of z.  0 for BLOBs */
  void (*xDel)(void*)     /* Destructor function */
){
  if( xDel==SQLITE_DYNAMIC ){
    assert( sqlite4MemdebugHasType(z, MEMTYPE_HEAP) );
    assert( sqlite4MemdebugNoType(z, ~MEMTYPE_HEAP) );
    sqlite4MemdebugSetType(z, MEMTYPE_DB | MEMTYPE_HEAP);
  }
  if( sqlite4VdbeMemSetStr(&pCtx->s, z, n, enc, xDel)==SQLITE_TOOBIG ){
    sqlite4_result_error_toobig(pCtx);
  }
}
void sqlite4_result_blob(
  sqlite4_context *pCtx, 
  const void *z, 
  int n, 
  void (*xDel)(void *)
){
  assert( n>=0 );
  assert( sqlite4_mutex_held(pCtx->s.db->mutex) );
  setResultStrOrError(pCtx, z, n, 0, xDel);
}
void sqlite4_result_double(sqlite4_context *pCtx, double rVal){
  assert( sqlite4_mutex_held(pCtx->s.db->mutex) );
  sqlite4VdbeMemSetDouble(&pCtx->s, rVal);
}
void sqlite4_result_error(sqlite4_context *pCtx, const char *z, int n){
  assert( sqlite4_mutex_held(pCtx->s.db->mutex) );
  pCtx->isError = SQLITE_ERROR;
  sqlite4VdbeMemSetStr(&pCtx->s, z, n, SQLITE_UTF8, SQLITE_TRANSIENT);
}
#ifndef SQLITE_OMIT_UTF16
void sqlite4_result_error16(sqlite4_context *pCtx, const void *z, int n){
  assert( sqlite4_mutex_held(pCtx->s.db->mutex) );
  pCtx->isError = SQLITE_ERROR;
  sqlite4VdbeMemSetStr(&pCtx->s, z, n, SQLITE_UTF16NATIVE, SQLITE_TRANSIENT);
}
#endif
void sqlite4_result_int(sqlite4_context *pCtx, int iVal){
  assert( sqlite4_mutex_held(pCtx->s.db->mutex) );
  sqlite4VdbeMemSetInt64(&pCtx->s, (i64)iVal);
}
void sqlite4_result_int64(sqlite4_context *pCtx, i64 iVal){
  assert( sqlite4_mutex_held(pCtx->s.db->mutex) );
  sqlite4VdbeMemSetInt64(&pCtx->s, iVal);
}
void sqlite4_result_null(sqlite4_context *pCtx){
  assert( sqlite4_mutex_held(pCtx->s.db->mutex) );
  sqlite4VdbeMemSetNull(&pCtx->s);
}
void sqlite4_result_text(
  sqlite4_context *pCtx, 
  const char *z, 
  int n,
  void (*xDel)(void *)
){
  assert( sqlite4_mutex_held(pCtx->s.db->mutex) );
  setResultStrOrError(pCtx, z, n, SQLITE_UTF8, xDel);
}
#ifndef SQLITE_OMIT_UTF16
void sqlite4_result_text16(
  sqlite4_context *pCtx, 
  const void *z, 
  int n, 
  void (*xDel)(void *)
){
  assert( sqlite4_mutex_held(pCtx->s.db->mutex) );
  setResultStrOrError(pCtx, z, n, SQLITE_UTF16NATIVE, xDel);
}
void sqlite4_result_text16be(
  sqlite4_context *pCtx, 
  const void *z, 
  int n, 
  void (*xDel)(void *)
){
  assert( sqlite4_mutex_held(pCtx->s.db->mutex) );
  setResultStrOrError(pCtx, z, n, SQLITE_UTF16BE, xDel);
}
void sqlite4_result_text16le(
  sqlite4_context *pCtx, 
  const void *z, 
  int n, 
  void (*xDel)(void *)
){
  assert( sqlite4_mutex_held(pCtx->s.db->mutex) );
  setResultStrOrError(pCtx, z, n, SQLITE_UTF16LE, xDel);
}
#endif /* SQLITE_OMIT_UTF16 */
void sqlite4_result_value(sqlite4_context *pCtx, sqlite4_value *pValue){
  assert( sqlite4_mutex_held(pCtx->s.db->mutex) );
  sqlite4VdbeMemCopy(&pCtx->s, pValue);
}
void sqlite4_result_zeroblob(sqlite4_context *pCtx, int n){
  assert( sqlite4_mutex_held(pCtx->s.db->mutex) );
  sqlite4VdbeMemSetZeroBlob(&pCtx->s, n);
}
void sqlite4_result_error_code(sqlite4_context *pCtx, int errCode){
  pCtx->isError = errCode;
  if( pCtx->s.flags & MEM_Null ){
    sqlite4VdbeMemSetStr(&pCtx->s, sqlite4ErrStr(errCode), -1, 
                         SQLITE_UTF8, SQLITE_STATIC);
  }
}

/* Force an SQLITE_TOOBIG error. */
void sqlite4_result_error_toobig(sqlite4_context *pCtx){
  assert( sqlite4_mutex_held(pCtx->s.db->mutex) );
  pCtx->isError = SQLITE_TOOBIG;
  sqlite4VdbeMemSetStr(&pCtx->s, "string or blob too big", -1, 
                       SQLITE_UTF8, SQLITE_STATIC);
}

/* An SQLITE_NOMEM error. */
void sqlite4_result_error_nomem(sqlite4_context *pCtx){
  assert( sqlite4_mutex_held(pCtx->s.db->mutex) );
  sqlite4VdbeMemSetNull(&pCtx->s);
  pCtx->isError = SQLITE_NOMEM;
  pCtx->s.db->mallocFailed = 1;
}

/*
** Execute the statement pStmt, either until a row of data is ready, the
** statement is completely executed or an error occurs.
**
** This routine implements the bulk of the logic behind the sqlite_step()
** API.  The only thing omitted is the automatic recompile if a 
** schema change has occurred.  That detail is handled by the
** outer sqlite4_step() wrapper procedure.
*/
static int sqlite4Step(Vdbe *p){
  sqlite4 *db;
  int rc;

  assert(p);
  if( p->magic!=VDBE_MAGIC_RUN ){
    /* We used to require that sqlite4_reset() be called before retrying
    ** sqlite4_step() after any error or after SQLITE_DONE.  But beginning
    ** with version 3.7.0, we changed this so that sqlite4_reset() would
    ** be called automatically instead of throwing the SQLITE_MISUSE error.
    ** This "automatic-reset" change is not technically an incompatibility, 
    ** since any application that receives an SQLITE_MISUSE is broken by
    ** definition.
    **
    ** Nevertheless, some published applications that were originally written
    ** for version 3.6.23 or earlier do in fact depend on SQLITE_MISUSE 
    ** returns, and those were broken by the automatic-reset change.  As a
    ** a work-around, the SQLITE_OMIT_AUTORESET compile-time restores the
    ** legacy behavior of returning SQLITE_MISUSE for cases where the 
    ** previous sqlite4_step() returned something other than a SQLITE_LOCKED
    ** or SQLITE_BUSY error.
    */
#ifdef SQLITE_OMIT_AUTORESET
    if( p->rc==SQLITE_BUSY || p->rc==SQLITE_LOCKED ){
      sqlite4_reset((sqlite4_stmt*)p);
    }else{
      return SQLITE_MISUSE_BKPT;
    }
#else
    sqlite4_reset((sqlite4_stmt*)p);
#endif
  }

  /* Check that malloc() has not failed. If it has, return early. */
  db = p->db;
  if( db->mallocFailed ){
    p->rc = SQLITE_NOMEM;
    return SQLITE_NOMEM;
  }

  if( p->pc<=0 && p->expired ){
    p->rc = SQLITE_SCHEMA;
    rc = SQLITE_ERROR;
    goto end_of_step;
  }
  if( p->pc<0 ){
    /* If there are no other statements currently running, then
    ** reset the interrupt flag.  This prevents a call to sqlite4_interrupt
    ** from interrupting a statement that has not yet started.
    */
    if( db->activeVdbeCnt==0 ){
      db->u1.isInterrupted = 0;
    }

    assert( db->writeVdbeCnt>0 || db->pSavepoint || db->nDeferredCons==0 );

#ifndef SQLITE_OMIT_TRACE
    if( db->xProfile && !db->init.busy ){
      sqlite4OsCurrentTime(0, &p->startTime);
    }
#endif

    db->activeVdbeCnt++;
    if( p->readOnly==0 ) db->writeVdbeCnt++;
    p->pc = 0;
  }
#ifndef SQLITE_OMIT_EXPLAIN
  if( p->explain ){
    rc = sqlite4VdbeList(p);
  }else
#endif /* SQLITE_OMIT_EXPLAIN */
  {
    db->vdbeExecCnt++;
    rc = sqlite4VdbeExec(p);
    db->vdbeExecCnt--;
  }

#ifndef SQLITE_OMIT_TRACE
  /* Invoke the profile callback if there is one
  */
  if( rc!=SQLITE_ROW && db->xProfile && !db->init.busy && p->zSql ){
    sqlite4_int64 iNow;
    sqlite4OsCurrentTime(0, &iNow);
    db->xProfile(db->pProfileArg, p->zSql, (iNow - p->startTime)*1000000);
  }
#endif

  db->errCode = rc;
  if( SQLITE_NOMEM==sqlite4ApiExit(p->db, p->rc) ){
    p->rc = SQLITE_NOMEM;
  }
end_of_step:
  /* At this point local variable rc holds the value that should be 
  ** returned if this statement was compiled using the legacy 
  ** sqlite4_prepare() interface. According to the docs, this can only
  ** be one of the values in the first assert() below. Variable p->rc 
  ** contains the value that would be returned if sqlite4_finalize() 
  ** were called on statement p.
  */
  assert( rc==SQLITE_ROW  || rc==SQLITE_DONE   || rc==SQLITE_ERROR 
       || rc==SQLITE_BUSY || rc==SQLITE_MISUSE
  );
  assert( p->rc!=SQLITE_ROW && p->rc!=SQLITE_DONE );
  if( rc!=SQLITE_ROW && rc!=SQLITE_DONE ){
    rc = sqlite4VdbeTransferError(p);
  }
  return rc;
}

/*
** The maximum number of times that a statement will try to reparse
** itself before giving up and returning SQLITE_SCHEMA.
*/
#ifndef SQLITE_MAX_SCHEMA_RETRY
# define SQLITE_MAX_SCHEMA_RETRY 5
#endif

/*
** This is the top-level implementation of sqlite4_step().  Call
** sqlite4Step() to do most of the work.  If a schema error occurs,
** call sqlite4Reprepare() and try again.
*/
int sqlite4_step(sqlite4_stmt *pStmt){
  int rc = SQLITE_OK;      /* Result from sqlite4Step() */
  int rc2 = SQLITE_OK;     /* Result from sqlite4Reprepare() */
  Vdbe *v = (Vdbe*)pStmt;  /* the prepared statement */
  int cnt = 0;             /* Counter to prevent infinite loop of reprepares */
  sqlite4 *db;             /* The database connection */

  if( vdbeSafetyNotNull(v) ){
    return SQLITE_MISUSE_BKPT;
  }
  db = v->db;
  sqlite4_mutex_enter(db->mutex);
  while( (rc = sqlite4Step(v))==SQLITE_SCHEMA
         && cnt++ < SQLITE_MAX_SCHEMA_RETRY
         && (rc2 = rc = sqlite4Reprepare(v))==SQLITE_OK ){
    sqlite4_reset(pStmt);
    assert( v->expired==0 );
  }
  if( rc2!=SQLITE_OK && ALWAYS(db->pErr) ){
    /* This case occurs after failing to recompile an sql statement. 
    ** The error message from the SQL compiler has already been loaded 
    ** into the database handle. This block copies the error message 
    ** from the database handle into the statement and sets the statement
    ** program counter to 0 to ensure that when the statement is 
    ** finalized or reset the parser error message is available via
    ** sqlite4_errmsg() and sqlite4_errcode().
    */
    const char *zErr = (const char *)sqlite4_value_text(db->pErr); 
    sqlite4DbFree(db, v->zErrMsg);
    if( !db->mallocFailed ){
      v->zErrMsg = sqlite4DbStrDup(db, zErr);
      v->rc = rc2;
    } else {
      v->zErrMsg = 0;
      v->rc = rc = SQLITE_NOMEM;
    }
  }
  rc = sqlite4ApiExit(db, rc);
  sqlite4_mutex_leave(db->mutex);
  return rc;
}

/*
** Extract the user data from a sqlite4_context structure and return a
** pointer to it.
*/
void *sqlite4_user_data(sqlite4_context *p){
  assert( p && p->pFunc );
  return p->pFunc->pUserData;
}

/*
** Return the sqlite4 object that owns the sqlite4_context.
**
** IMPLEMENTATION-OF: R-46798-50301 The sqlite4_context_db_handle() interface
** returns a copy of the pointer to the database connection (the 1st
** parameter) of the sqlite4_create_function() and
** sqlite4_create_function16() routines that originally registered the
** application defined function.
*/
sqlite4 *sqlite4_context_db_handle(sqlite4_context *p){
  assert( p && p->pFunc );
  return p->s.db;
}

/*
** Return the sqlite4_env object associated with the sqlite4_context.
*/
sqlite4_env *sqlite4_context_env(sqlite4_context *p){
  assert( p && p->pFunc );
  return p->s.db->pEnv;
}

/*
** The following is the implementation of an SQL function that always
** fails with an error message stating that the function is used in the
** wrong context.  The sqlite4_overload_function() API might construct
** SQL function that use this routine so that the functions will exist
** for name resolution but are actually overloaded by the xFindFunction
** method of virtual tables.
*/
void sqlite4InvalidFunction(
  sqlite4_context *context,  /* The function calling context */
  int NotUsed,               /* Number of arguments to the function */
  sqlite4_value **NotUsed2   /* Value of each argument */
){
  const char *zName = context->pFunc->zName;
  char *zErr;
  sqlite4_env *pEnv = sqlite4_context_env(context);
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  zErr = sqlite4_mprintf(pEnv,
      "unable to use function %s in the requested context", zName);
  sqlite4_result_error(context, zErr, -1);
  sqlite4_free(pEnv, zErr);
}

/*
** Allocate or return the aggregate context for a user function.  A new
** context is allocated on the first call.  Subsequent calls return the
** same context that was returned on prior calls.
*/
void *sqlite4_aggregate_context(sqlite4_context *p, int nByte){
  Mem *pMem;
  assert( p && p->pFunc && p->pFunc->xStep );
  assert( sqlite4_mutex_held(p->s.db->mutex) );
  pMem = p->pMem;
  testcase( nByte<0 );
  if( (pMem->flags & MEM_Agg)==0 ){
    if( nByte<=0 ){
      sqlite4VdbeMemReleaseExternal(pMem);
      pMem->flags = MEM_Null;
      pMem->z = 0;
    }else{
      sqlite4VdbeMemGrow(pMem, nByte, 0);
      pMem->flags = MEM_Agg;
      pMem->u.pDef = p->pFunc;
      if( pMem->z ){
        memset(pMem->z, 0, nByte);
      }
    }
  }
  return (void*)pMem->z;
}

/*
** Return the auxilary data pointer, if any, for the iArg'th argument to
** the user-function defined by pCtx.
*/
void *sqlite4_get_auxdata(sqlite4_context *pCtx, int iArg){
  VdbeFunc *pVdbeFunc;

  assert( sqlite4_mutex_held(pCtx->s.db->mutex) );
  pVdbeFunc = pCtx->pVdbeFunc;
  if( !pVdbeFunc || iArg>=pVdbeFunc->nAux || iArg<0 ){
    return 0;
  }
  return pVdbeFunc->apAux[iArg].pAux;
}

/*
** Set the auxilary data pointer and delete function, for the iArg'th
** argument to the user-function defined by pCtx. Any previous value is
** deleted by calling the delete function specified when it was set.
*/
void sqlite4_set_auxdata(
  sqlite4_context *pCtx, 
  int iArg, 
  void *pAux, 
  void (*xDelete)(void*)
){
  struct AuxData *pAuxData;
  VdbeFunc *pVdbeFunc;
  if( iArg<0 ) goto failed;

  assert( sqlite4_mutex_held(pCtx->s.db->mutex) );
  pVdbeFunc = pCtx->pVdbeFunc;
  if( !pVdbeFunc || pVdbeFunc->nAux<=iArg ){
    int nAux = (pVdbeFunc ? pVdbeFunc->nAux : 0);
    int nMalloc = sizeof(VdbeFunc) + sizeof(struct AuxData)*iArg;
    pVdbeFunc = sqlite4DbRealloc(pCtx->s.db, pVdbeFunc, nMalloc);
    if( !pVdbeFunc ){
      goto failed;
    }
    pCtx->pVdbeFunc = pVdbeFunc;
    memset(&pVdbeFunc->apAux[nAux], 0, sizeof(struct AuxData)*(iArg+1-nAux));
    pVdbeFunc->nAux = iArg+1;
    pVdbeFunc->pFunc = pCtx->pFunc;
  }

  pAuxData = &pVdbeFunc->apAux[iArg];
  if( pAuxData->pAux && pAuxData->xDelete ){
    pAuxData->xDelete(pAuxData->pAux);
  }
  pAuxData->pAux = pAux;
  pAuxData->xDelete = xDelete;
  return;

failed:
  if( xDelete ){
    xDelete(pAux);
  }
}

#ifndef SQLITE_OMIT_DEPRECATED
/*
** Return the number of times the Step function of a aggregate has been 
** called.
**
** This function is deprecated.  Do not use it for new code.  It is
** provide only to avoid breaking legacy code.  New aggregate function
** implementations should keep their own counts within their aggregate
** context.
*/
int sqlite4_aggregate_count(sqlite4_context *p){
  assert( p && p->pMem && p->pFunc && p->pFunc->xStep );
  return p->pMem->n;
}
#endif

/*
** Return the number of columns in the result set for the statement pStmt.
*/
int sqlite4_column_count(sqlite4_stmt *pStmt){
  Vdbe *pVm = (Vdbe *)pStmt;
  return pVm ? pVm->nResColumn : 0;
}

/*
** Return the number of values available from the current row of the
** currently executing statement pStmt.
*/
int sqlite4_data_count(sqlite4_stmt *pStmt){
  Vdbe *pVm = (Vdbe *)pStmt;
  if( pVm==0 || pVm->pResultSet==0 ) return 0;
  return pVm->nResColumn;
}


/*
** Check to see if column iCol of the given statement is valid.  If
** it is, return a pointer to the Mem for the value of that column.
** If iCol is not valid, return a pointer to a Mem which has a value
** of NULL.
*/
static Mem *columnMem(sqlite4_stmt *pStmt, int i){
  Vdbe *pVm;
  Mem *pOut;

  pVm = (Vdbe *)pStmt;
  if( pVm && pVm->pResultSet!=0 && i<pVm->nResColumn && i>=0 ){
    sqlite4_mutex_enter(pVm->db->mutex);
    pOut = &pVm->pResultSet[i];
  }else{
    /* If the value passed as the second argument is out of range, return
    ** a pointer to the following static Mem object which contains the
    ** value SQL NULL. Even though the Mem structure contains an element
    ** of type i64, on certain architectures (x86) with certain compiler
    ** switches (-Os), gcc may align this Mem object on a 4-byte boundary
    ** instead of an 8-byte one. This all works fine, except that when
    ** running with SQLITE_DEBUG defined the SQLite code sometimes assert()s
    ** that a Mem structure is located on an 8-byte boundary. To prevent
    ** these assert()s from failing, when building with SQLITE_DEBUG defined
    ** using gcc, we force nullMem to be 8-byte aligned using the magical
    ** __attribute__((aligned(8))) macro.  */
    static const Mem nullMem 
#if defined(SQLITE_DEBUG) && defined(__GNUC__)
      __attribute__((aligned(8))) 
#endif
      = {0, "", (double)0, {0}, 0, MEM_Null, SQLITE_NULL, 0,
#ifdef SQLITE_DEBUG
         0, 0,  /* pScopyFrom, pFiller */
#endif
         0, 0 };

    if( pVm && ALWAYS(pVm->db) ){
      sqlite4_mutex_enter(pVm->db->mutex);
      sqlite4Error(pVm->db, SQLITE_RANGE, 0);
    }
    pOut = (Mem*)&nullMem;
  }
  return pOut;
}

/*
** This function is called after invoking an sqlite4_value_XXX function on a 
** column value (i.e. a value returned by evaluating an SQL expression in the
** select list of a SELECT statement) that may cause a malloc() failure. If 
** malloc() has failed, the threads mallocFailed flag is cleared and the result
** code of statement pStmt set to SQLITE_NOMEM.
**
** Specifically, this is called from within:
**
**     sqlite4_column_int()
**     sqlite4_column_int64()
**     sqlite4_column_text()
**     sqlite4_column_text16()
**     sqlite4_column_real()
**     sqlite4_column_bytes()
**     sqlite4_column_bytes16()
**     sqiite3_column_blob()
*/
static void columnMallocFailure(sqlite4_stmt *pStmt)
{
  /* If malloc() failed during an encoding conversion within an
  ** sqlite4_column_XXX API, then set the return code of the statement to
  ** SQLITE_NOMEM. The next call to _step() (if any) will return SQLITE_ERROR
  ** and _finalize() will return NOMEM.
  */
  Vdbe *p = (Vdbe *)pStmt;
  if( p ){
    p->rc = sqlite4ApiExit(p->db, p->rc);
    sqlite4_mutex_leave(p->db->mutex);
  }
}

/**************************** sqlite4_column_  *******************************
** The following routines are used to access elements of the current row
** in the result set.
*/
const void *sqlite4_column_blob(sqlite4_stmt *pStmt, int i){
  const void *val;
  val = sqlite4_value_blob( columnMem(pStmt,i) );
  /* Even though there is no encoding conversion, value_blob() might
  ** need to call malloc() to expand the result of a zeroblob() 
  ** expression. 
  */
  columnMallocFailure(pStmt);
  return val;
}
int sqlite4_column_bytes(sqlite4_stmt *pStmt, int i){
  int val = sqlite4_value_bytes( columnMem(pStmt,i) );
  columnMallocFailure(pStmt);
  return val;
}
int sqlite4_column_bytes16(sqlite4_stmt *pStmt, int i){
  int val = sqlite4_value_bytes16( columnMem(pStmt,i) );
  columnMallocFailure(pStmt);
  return val;
}
double sqlite4_column_double(sqlite4_stmt *pStmt, int i){
  double val = sqlite4_value_double( columnMem(pStmt,i) );
  columnMallocFailure(pStmt);
  return val;
}
int sqlite4_column_int(sqlite4_stmt *pStmt, int i){
  int val = sqlite4_value_int( columnMem(pStmt,i) );
  columnMallocFailure(pStmt);
  return val;
}
sqlite_int64 sqlite4_column_int64(sqlite4_stmt *pStmt, int i){
  sqlite_int64 val = sqlite4_value_int64( columnMem(pStmt,i) );
  columnMallocFailure(pStmt);
  return val;
}
const unsigned char *sqlite4_column_text(sqlite4_stmt *pStmt, int i){
  const unsigned char *val = sqlite4_value_text( columnMem(pStmt,i) );
  columnMallocFailure(pStmt);
  return val;
}
sqlite4_value *sqlite4_column_value(sqlite4_stmt *pStmt, int i){
  Mem *pOut = columnMem(pStmt, i);
  if( pOut->flags&MEM_Static ){
    pOut->flags &= ~MEM_Static;
    pOut->flags |= MEM_Ephem;
  }
  columnMallocFailure(pStmt);
  return (sqlite4_value *)pOut;
}
#ifndef SQLITE_OMIT_UTF16
const void *sqlite4_column_text16(sqlite4_stmt *pStmt, int i){
  const void *val = sqlite4_value_text16( columnMem(pStmt,i) );
  columnMallocFailure(pStmt);
  return val;
}
#endif /* SQLITE_OMIT_UTF16 */
int sqlite4_column_type(sqlite4_stmt *pStmt, int i){
  int iType = sqlite4_value_type( columnMem(pStmt,i) );
  columnMallocFailure(pStmt);
  return iType;
}

/* The following function is experimental and subject to change or
** removal */
/*int sqlite4_column_numeric_type(sqlite4_stmt *pStmt, int i){
**  return sqlite4_value_numeric_type( columnMem(pStmt,i) );
**}
*/

/*
** Convert the N-th element of pStmt->pColName[] into a string using
** xFunc() then return that string.  If N is out of range, return 0.
**
** There are up to 5 names for each column.  useType determines which
** name is returned.  Here are the names:
**
**    0      The column name as it should be displayed for output
**    1      The datatype name for the column
**    2      The name of the database that the column derives from
**    3      The name of the table that the column derives from
**    4      The name of the table column that the result column derives from
**
** If the result is not a simple column reference (if it is an expression
** or a constant) then useTypes 2, 3, and 4 return NULL.
*/
static const void *columnName(
  sqlite4_stmt *pStmt,
  int N,
  const void *(*xFunc)(Mem*),
  int useType
){
  const void *ret = 0;
  Vdbe *p = (Vdbe *)pStmt;
  int n;
  sqlite4 *db = p->db;
  
  assert( db!=0 );
  n = sqlite4_column_count(pStmt);
  if( N<n && N>=0 ){
    N += useType*n;
    sqlite4_mutex_enter(db->mutex);
    assert( db->mallocFailed==0 );
    ret = xFunc(&p->aColName[N]);
     /* A malloc may have failed inside of the xFunc() call. If this
    ** is the case, clear the mallocFailed flag and return NULL.
    */
    if( db->mallocFailed ){
      db->mallocFailed = 0;
      ret = 0;
    }
    sqlite4_mutex_leave(db->mutex);
  }
  return ret;
}

/*
** Return the name of the Nth column of the result set returned by SQL
** statement pStmt.
*/
const char *sqlite4_column_name(sqlite4_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlite4_value_text, COLNAME_NAME);
}
#ifndef SQLITE_OMIT_UTF16
const void *sqlite4_column_name16(sqlite4_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlite4_value_text16, COLNAME_NAME);
}
#endif

/*
** Constraint:  If you have ENABLE_COLUMN_METADATA then you must
** not define OMIT_DECLTYPE.
*/
#if defined(SQLITE_OMIT_DECLTYPE) && defined(SQLITE_ENABLE_COLUMN_METADATA)
# error "Must not define both SQLITE_OMIT_DECLTYPE \
         and SQLITE_ENABLE_COLUMN_METADATA"
#endif

#ifndef SQLITE_OMIT_DECLTYPE
/*
** Return the column declaration type (if applicable) of the 'i'th column
** of the result set of SQL statement pStmt.
*/
const char *sqlite4_column_decltype(sqlite4_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlite4_value_text, COLNAME_DECLTYPE);
}
#ifndef SQLITE_OMIT_UTF16
const void *sqlite4_column_decltype16(sqlite4_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlite4_value_text16, COLNAME_DECLTYPE);
}
#endif /* SQLITE_OMIT_UTF16 */
#endif /* SQLITE_OMIT_DECLTYPE */

#ifdef SQLITE_ENABLE_COLUMN_METADATA
/*
** Return the name of the database from which a result column derives.
** NULL is returned if the result column is an expression or constant or
** anything else which is not an unabiguous reference to a database column.
*/
const char *sqlite4_column_database_name(sqlite4_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlite4_value_text, COLNAME_DATABASE);
}
#ifndef SQLITE_OMIT_UTF16
const void *sqlite4_column_database_name16(sqlite4_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlite4_value_text16, COLNAME_DATABASE);
}
#endif /* SQLITE_OMIT_UTF16 */

/*
** Return the name of the table from which a result column derives.
** NULL is returned if the result column is an expression or constant or
** anything else which is not an unabiguous reference to a database column.
*/
const char *sqlite4_column_table_name(sqlite4_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlite4_value_text, COLNAME_TABLE);
}
#ifndef SQLITE_OMIT_UTF16
const void *sqlite4_column_table_name16(sqlite4_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlite4_value_text16, COLNAME_TABLE);
}
#endif /* SQLITE_OMIT_UTF16 */

/*
** Return the name of the table column from which a result column derives.
** NULL is returned if the result column is an expression or constant or
** anything else which is not an unabiguous reference to a database column.
*/
const char *sqlite4_column_origin_name(sqlite4_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlite4_value_text, COLNAME_COLUMN);
}
#ifndef SQLITE_OMIT_UTF16
const void *sqlite4_column_origin_name16(sqlite4_stmt *pStmt, int N){
  return columnName(
      pStmt, N, (const void*(*)(Mem*))sqlite4_value_text16, COLNAME_COLUMN);
}
#endif /* SQLITE_OMIT_UTF16 */
#endif /* SQLITE_ENABLE_COLUMN_METADATA */


/******************************* sqlite4_bind_  ***************************
** 
** Routines used to attach values to wildcards in a compiled SQL statement.
*/
/*
** Unbind the value bound to variable i in virtual machine p. This is the 
** the same as binding a NULL value to the column. If the "i" parameter is
** out of range, then SQLITE_RANGE is returned. Othewise SQLITE_OK.
**
** A successful evaluation of this routine acquires the mutex on p.
** the mutex is released if any kind of error occurs.
**
** The error code stored in database p->db is overwritten with the return
** value in any case.
*/
static int vdbeUnbind(Vdbe *p, int i){
  Mem *pVar;
  if( vdbeSafetyNotNull(p) ){
    return SQLITE_MISUSE_BKPT;
  }
  sqlite4_mutex_enter(p->db->mutex);
  if( p->magic!=VDBE_MAGIC_RUN || p->pc>=0 ){
    sqlite4Error(p->db, SQLITE_MISUSE, 0);
    sqlite4_mutex_leave(p->db->mutex);
    sqlite4_log(SQLITE_MISUSE, 
        "bind on a busy prepared statement: [%s]", p->zSql);
    return SQLITE_MISUSE_BKPT;
  }
  if( i<1 || i>p->nVar ){
    sqlite4Error(p->db, SQLITE_RANGE, 0);
    sqlite4_mutex_leave(p->db->mutex);
    return SQLITE_RANGE;
  }
  i--;
  pVar = &p->aVar[i];
  sqlite4VdbeMemRelease(pVar);
  pVar->flags = MEM_Null;
  sqlite4Error(p->db, SQLITE_OK, 0);

  /* If the bit corresponding to this variable in Vdbe.expmask is set, then 
  ** binding a new value to this variable invalidates the current query plan.
  **
  ** IMPLEMENTATION-OF: R-48440-37595 If the specific value bound to host
  ** parameter in the WHERE clause might influence the choice of query plan
  ** for a statement, then the statement will be automatically recompiled,
  ** as if there had been a schema change, on the first sqlite4_step() call
  ** following any change to the bindings of that parameter.
  */
  if( ((i<32 && p->expmask & ((u32)1 << i)) || p->expmask==0xffffffff)
  ){
    p->expired = 1;
  }
  return SQLITE_OK;
}

/*
** Bind a text or BLOB value.
*/
static int bindText(
  sqlite4_stmt *pStmt,   /* The statement to bind against */
  int i,                 /* Index of the parameter to bind */
  const void *zData,     /* Pointer to the data to be bound */
  int nData,             /* Number of bytes of data to be bound */
  void (*xDel)(void*),   /* Destructor for the data */
  u8 encoding            /* Encoding for the data */
){
  Vdbe *p = (Vdbe *)pStmt;
  Mem *pVar;
  int rc;

  rc = vdbeUnbind(p, i);
  if( rc==SQLITE_OK ){
    if( zData!=0 ){
      pVar = &p->aVar[i-1];
      rc = sqlite4VdbeMemSetStr(pVar, zData, nData, encoding, xDel);
      if( rc==SQLITE_OK && encoding!=0 ){
        rc = sqlite4VdbeChangeEncoding(pVar, ENC(p->db));
      }
      sqlite4Error(p->db, rc, 0);
      rc = sqlite4ApiExit(p->db, rc);
    }
    sqlite4_mutex_leave(p->db->mutex);
  }else if( xDel!=SQLITE_STATIC && xDel!=SQLITE_TRANSIENT ){
    xDel((void*)zData);
  }
  return rc;
}


/*
** Bind a blob value to an SQL statement variable.
*/
int sqlite4_bind_blob(
  sqlite4_stmt *pStmt, 
  int i, 
  const void *zData, 
  int nData, 
  void (*xDel)(void*)
){
  return bindText(pStmt, i, zData, nData, xDel, 0);
}
int sqlite4_bind_double(sqlite4_stmt *pStmt, int i, double rValue){
  int rc;
  Vdbe *p = (Vdbe *)pStmt;
  rc = vdbeUnbind(p, i);
  if( rc==SQLITE_OK ){
    sqlite4VdbeMemSetDouble(&p->aVar[i-1], rValue);
    sqlite4_mutex_leave(p->db->mutex);
  }
  return rc;
}
int sqlite4_bind_int(sqlite4_stmt *p, int i, int iValue){
  return sqlite4_bind_int64(p, i, (i64)iValue);
}
int sqlite4_bind_int64(sqlite4_stmt *pStmt, int i, sqlite_int64 iValue){
  int rc;
  Vdbe *p = (Vdbe *)pStmt;
  rc = vdbeUnbind(p, i);
  if( rc==SQLITE_OK ){
    sqlite4VdbeMemSetInt64(&p->aVar[i-1], iValue);
    sqlite4_mutex_leave(p->db->mutex);
  }
  return rc;
}
int sqlite4_bind_null(sqlite4_stmt *pStmt, int i){
  int rc;
  Vdbe *p = (Vdbe*)pStmt;
  rc = vdbeUnbind(p, i);
  if( rc==SQLITE_OK ){
    sqlite4_mutex_leave(p->db->mutex);
  }
  return rc;
}
int sqlite4_bind_text( 
  sqlite4_stmt *pStmt, 
  int i, 
  const char *zData, 
  int nData, 
  void (*xDel)(void*)
){
  return bindText(pStmt, i, zData, nData, xDel, SQLITE_UTF8);
}
#ifndef SQLITE_OMIT_UTF16
int sqlite4_bind_text16(
  sqlite4_stmt *pStmt, 
  int i, 
  const void *zData, 
  int nData, 
  void (*xDel)(void*)
){
  return bindText(pStmt, i, zData, nData, xDel, SQLITE_UTF16NATIVE);
}
#endif /* SQLITE_OMIT_UTF16 */
int sqlite4_bind_value(sqlite4_stmt *pStmt, int i, const sqlite4_value *pValue){
  int rc;
  switch( pValue->type ){
    case SQLITE_INTEGER: {
      rc = sqlite4_bind_int64(pStmt, i, pValue->u.i);
      break;
    }
    case SQLITE_FLOAT: {
      rc = sqlite4_bind_double(pStmt, i, pValue->r);
      break;
    }
    case SQLITE_BLOB: {
      if( pValue->flags & MEM_Zero ){
        rc = sqlite4_bind_zeroblob(pStmt, i, pValue->u.nZero);
      }else{
        rc = sqlite4_bind_blob(pStmt, i, pValue->z, pValue->n,SQLITE_TRANSIENT);
      }
      break;
    }
    case SQLITE_TEXT: {
      rc = bindText(pStmt,i,  pValue->z, pValue->n, SQLITE_TRANSIENT,
                              pValue->enc);
      break;
    }
    default: {
      rc = sqlite4_bind_null(pStmt, i);
      break;
    }
  }
  return rc;
}
int sqlite4_bind_zeroblob(sqlite4_stmt *pStmt, int i, int n){
  int rc;
  Vdbe *p = (Vdbe *)pStmt;
  rc = vdbeUnbind(p, i);
  if( rc==SQLITE_OK ){
    sqlite4VdbeMemSetZeroBlob(&p->aVar[i-1], n);
    sqlite4_mutex_leave(p->db->mutex);
  }
  return rc;
}

/*
** Return the number of wildcards that can be potentially bound to.
** This routine is added to support DBD::SQLite.  
*/
int sqlite4_bind_parameter_count(sqlite4_stmt *pStmt){
  Vdbe *p = (Vdbe*)pStmt;
  return p ? p->nVar : 0;
}

/*
** Return the name of a wildcard parameter.  Return NULL if the index
** is out of range or if the wildcard is unnamed.
**
** The result is always UTF-8.
*/
const char *sqlite4_bind_parameter_name(sqlite4_stmt *pStmt, int i){
  Vdbe *p = (Vdbe*)pStmt;
  if( p==0 || i<1 || i>p->nzVar ){
    return 0;
  }
  return p->azVar[i-1];
}

/*
** Given a wildcard parameter name, return the index of the variable
** with that name.  If there is no variable with the given name,
** return 0.
*/
int sqlite4VdbeParameterIndex(Vdbe *p, const char *zName, int nName){
  int i;
  if( p==0 ){
    return 0;
  }
  if( zName ){
    for(i=0; i<p->nzVar; i++){
      const char *z = p->azVar[i];
      if( z && memcmp(z,zName,nName)==0 && z[nName]==0 ){
        return i+1;
      }
    }
  }
  return 0;
}
int sqlite4_bind_parameter_index(sqlite4_stmt *pStmt, const char *zName){
  return sqlite4VdbeParameterIndex((Vdbe*)pStmt, zName, sqlite4Strlen30(zName));
}

/*
** Transfer all bindings from the first statement over to the second.
*/
int sqlite4TransferBindings(sqlite4_stmt *pFromStmt, sqlite4_stmt *pToStmt){
  Vdbe *pFrom = (Vdbe*)pFromStmt;
  Vdbe *pTo = (Vdbe*)pToStmt;
  int i;
  assert( pTo->db==pFrom->db );
  assert( pTo->nVar==pFrom->nVar );
  sqlite4_mutex_enter(pTo->db->mutex);
  for(i=0; i<pFrom->nVar; i++){
    sqlite4VdbeMemMove(&pTo->aVar[i], &pFrom->aVar[i]);
  }
  sqlite4_mutex_leave(pTo->db->mutex);
  return SQLITE_OK;
}

#ifndef SQLITE_OMIT_DEPRECATED
/*
** Deprecated external interface.  Internal/core SQLite code
** should call sqlite4TransferBindings.
**
** Is is misuse to call this routine with statements from different
** database connections.  But as this is a deprecated interface, we
** will not bother to check for that condition.
**
** If the two statements contain a different number of bindings, then
** an SQLITE_ERROR is returned.  Nothing else can go wrong, so otherwise
** SQLITE_OK is returned.
*/
int sqlite4_transfer_bindings(sqlite4_stmt *pFromStmt, sqlite4_stmt *pToStmt){
  Vdbe *pFrom = (Vdbe*)pFromStmt;
  Vdbe *pTo = (Vdbe*)pToStmt;
  if( pFrom->nVar!=pTo->nVar ){
    return SQLITE_ERROR;
  }
  if( pTo->expmask ){
    pTo->expired = 1;
  }
  if( pFrom->expmask ){
    pFrom->expired = 1;
  }
  return sqlite4TransferBindings(pFromStmt, pToStmt);
}
#endif

/*
** Return the sqlite4* database handle to which the prepared statement given
** in the argument belongs.  This is the same database handle that was
** the first argument to the sqlite4_prepare() that was used to create
** the statement in the first place.
*/
sqlite4 *sqlite4_db_handle(sqlite4_stmt *pStmt){
  return pStmt ? ((Vdbe*)pStmt)->db : 0;
}

/*
** Return true if the prepared statement is guaranteed to not modify the
** database.
*/
int sqlite4_stmt_readonly(sqlite4_stmt *pStmt){
  return pStmt ? ((Vdbe*)pStmt)->readOnly : 1;
}

/*
** Return true if the prepared statement is in need of being reset.
*/
int sqlite4_stmt_busy(sqlite4_stmt *pStmt){
  Vdbe *v = (Vdbe*)pStmt;
  return v!=0 && v->pc>0 && v->magic==VDBE_MAGIC_RUN;
}

/*
** Return a pointer to the next prepared statement after pStmt associated
** with database connection pDb.  If pStmt is NULL, return the first
** prepared statement for the database connection.  Return NULL if there
** are no more.
*/
sqlite4_stmt *sqlite4_next_stmt(sqlite4 *pDb, sqlite4_stmt *pStmt){
  sqlite4_stmt *pNext;
  sqlite4_mutex_enter(pDb->mutex);
  if( pStmt==0 ){
    pNext = (sqlite4_stmt*)pDb->pVdbe;
  }else{
    pNext = (sqlite4_stmt*)((Vdbe*)pStmt)->pNext;
  }
  sqlite4_mutex_leave(pDb->mutex);
  return pNext;
}

/*
** Return the value of a status counter for a prepared statement
*/
int sqlite4_stmt_status(sqlite4_stmt *pStmt, int op, int resetFlag){
  Vdbe *pVdbe = (Vdbe*)pStmt;
  int v = pVdbe->aCounter[op-1];
  if( resetFlag ) pVdbe->aCounter[op-1] = 0;
  return v;
}
