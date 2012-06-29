/*
** 2008 June 18
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
** This module implements the sqlite4_status() interface and related
** functionality.
*/
#include "sqliteInt.h"
#include "vdbeInt.h"

/*
** Add N to the value of a status record.  It is assumed that the
** caller holds appropriate locks.
*/
void sqlite4StatusAdd(sqlite4_env *pEnv, int op, sqlite4_int64 N){
  assert( op>=0 && op<ArraySize(pEnv->nowValue) );
  pEnv->nowValue[op] += N;
  if( pEnv->nowValue[op]>pEnv->mxValue[op] ){
    pEnv->mxValue[op] = pEnv->nowValue[op];
  }
}

/*
** Set the value of a status to X.
*/
void sqlite4StatusSet(sqlite4_env *pEnv, int op, sqlite4_uint64 X){
  assert( op>=0 && op<ArraySize(pEnv->nowValue) );
  pEnv->nowValue[op] = X;
  if( pEnv->nowValue[op]>pEnv->mxValue[op] ){
    pEnv->mxValue[op] = pEnv->nowValue[op];
  }
}

/*
** Query status information.
**
** This implementation assumes that reading or writing an aligned
** 32-bit integer is an atomic operation.  If that assumption is not true,
** then this routine is not threadsafe.
*/
int sqlite4_env_status(
  sqlite4_env *pEnv,
  int op,
  sqlite4_uint64 *pCurrent,
  sqlite4_uint64 *pHighwater,
  int resetFlag
){
  if( pEnv==0 ) pEnv = sqlite4_env_default();
  if( op<0 || op>=ArraySize(pEnv->nowValue) ){
    return SQLITE4_MISUSE_BKPT;
  }
  *pCurrent = pEnv->nowValue[op];
  *pHighwater = pEnv->mxValue[op];
  if( resetFlag ){
    pEnv->mxValue[op] = pEnv->nowValue[op];
  }
  return SQLITE4_OK;
}

/*
** Query status information for a single database connection
*/
int sqlite4_db_status(
  sqlite4 *db,          /* The database connection whose status is desired */
  int op,               /* Status verb */
  int *pCurrent,        /* Write current value here */
  int *pHighwater,      /* Write high-water mark here */
  int resetFlag         /* Reset high-water mark if true */
){
  int rc = SQLITE4_OK;   /* Return code */
  sqlite4_env *pEnv;
  sqlite4_mutex_enter(db->mutex);
  pEnv = db->pEnv;
  switch( op ){
    case SQLITE4_DBSTATUS_LOOKASIDE_USED: {
      *pCurrent = db->lookaside.nOut;
      *pHighwater = db->lookaside.mxOut;
      if( resetFlag ){
        db->lookaside.mxOut = db->lookaside.nOut;
      }
      break;
    }

    case SQLITE4_DBSTATUS_LOOKASIDE_HIT:
    case SQLITE4_DBSTATUS_LOOKASIDE_MISS_SIZE:
    case SQLITE4_DBSTATUS_LOOKASIDE_MISS_FULL: {
      testcase( op==SQLITE4_DBSTATUS_LOOKASIDE_HIT );
      testcase( op==SQLITE4_DBSTATUS_LOOKASIDE_MISS_SIZE );
      testcase( op==SQLITE4_DBSTATUS_LOOKASIDE_MISS_FULL );
      assert( (op-SQLITE4_DBSTATUS_LOOKASIDE_HIT)>=0 );
      assert( (op-SQLITE4_DBSTATUS_LOOKASIDE_HIT)<3 );
      *pCurrent = 0;
      *pHighwater = db->lookaside.anStat[op - SQLITE4_DBSTATUS_LOOKASIDE_HIT];
      if( resetFlag ){
        db->lookaside.anStat[op - SQLITE4_DBSTATUS_LOOKASIDE_HIT] = 0;
      }
      break;
    }

    /* 
    ** Return an approximation for the amount of memory currently used
    ** by all pagers associated with the given database connection.  The
    ** highwater mark is meaningless and is returned as zero.
    */
    case SQLITE4_DBSTATUS_CACHE_USED: {
      int totalUsed = 0;
      *pCurrent = totalUsed;
      *pHighwater = 0;
      break;
    }

    /*
    ** *pCurrent gets an accurate estimate of the amount of memory used
    ** to store the schema for all databases (main, temp, and any ATTACHed
    ** databases.  *pHighwater is set to zero.
    */
    case SQLITE4_DBSTATUS_SCHEMA_USED: {
      int i;                      /* Used to iterate through schemas */
      int nByte = 0;              /* Used to accumulate return value */

      db->pnBytesFreed = &nByte;
      for(i=0; i<db->nDb; i++){
        Schema *pSchema = db->aDb[i].pSchema;
        if( ALWAYS(pSchema!=0) ){
          HashElem *p;

          nByte += sizeof(HashElem) * (
              pSchema->tblHash.count 
            + pSchema->trigHash.count
            + pSchema->idxHash.count
            + pSchema->fkeyHash.count
          );
          nByte += sqlite4MallocSize(pEnv, pSchema->tblHash.ht);
          nByte += sqlite4MallocSize(pEnv, pSchema->trigHash.ht);
          nByte += sqlite4MallocSize(pEnv, pSchema->idxHash.ht);
          nByte += sqlite4MallocSize(pEnv, pSchema->fkeyHash.ht);

          for(p=sqliteHashFirst(&pSchema->trigHash); p; p=sqliteHashNext(p)){
            sqlite4DeleteTrigger(db, (Trigger*)sqliteHashData(p));
          }
          for(p=sqliteHashFirst(&pSchema->tblHash); p; p=sqliteHashNext(p)){
            sqlite4DeleteTable(db, (Table *)sqliteHashData(p));
          }
        }
      }
      db->pnBytesFreed = 0;

      *pHighwater = 0;
      *pCurrent = nByte;
      break;
    }

    /*
    ** *pCurrent gets an accurate estimate of the amount of memory used
    ** to store all prepared statements.
    ** *pHighwater is set to zero.
    */
    case SQLITE4_DBSTATUS_STMT_USED: {
      struct Vdbe *pVdbe;         /* Used to iterate through VMs */
      int nByte = 0;              /* Used to accumulate return value */

      db->pnBytesFreed = &nByte;
      for(pVdbe=db->pVdbe; pVdbe; pVdbe=pVdbe->pNext){
        sqlite4VdbeDeleteObject(db, pVdbe);
      }
      db->pnBytesFreed = 0;

      *pHighwater = 0;
      *pCurrent = nByte;

      break;
    }

    /*
    ** Set *pCurrent to the total cache hits or misses encountered by all
    ** pagers the database handle is connected to. *pHighwater is always set 
    ** to zero.
    */
    case SQLITE4_DBSTATUS_CACHE_HIT:
    case SQLITE4_DBSTATUS_CACHE_MISS: {
      int nRet = 0;
      *pHighwater = 0;
      *pCurrent = nRet;
      break;
    }

    default: {
      rc = SQLITE4_ERROR;
    }
  }
  sqlite4_mutex_leave(db->mutex);
  return rc;
}
