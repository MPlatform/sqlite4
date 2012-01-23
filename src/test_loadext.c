/*
** 2006 June 14
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Test extension for testing the sqlite4_load_extension() function.
*/
#include <string.h>
#include "sqlite4ext.h"
SQLITE_EXTENSION_INIT1

/*
** The half() SQL function returns half of its input value.
*/
static void halfFunc(
  sqlite4_context *context,
  int argc,
  sqlite4_value **argv
){
  sqlite4_result_double(context, 0.5*sqlite4_value_double(argv[0]));
}

/*
** SQL functions to call the sqlite4_status function and return results.
*/
static void statusFunc(
  sqlite4_context *context,
  int argc,
  sqlite4_value **argv
){
  int op, mx, cur, resetFlag, rc;
  if( sqlite4_value_type(argv[0])==SQLITE_INTEGER ){
    op = sqlite4_value_int(argv[0]);
  }else if( sqlite4_value_type(argv[0])==SQLITE_TEXT ){
    int i;
    const char *zName;
    static const struct {
      const char *zName;
      int op;
    } aOp[] = {
      { "MEMORY_USED",         SQLITE_STATUS_MEMORY_USED         },
      { "PAGECACHE_USED",      SQLITE_STATUS_PAGECACHE_USED      },
      { "PAGECACHE_OVERFLOW",  SQLITE_STATUS_PAGECACHE_OVERFLOW  },
      { "SCRATCH_USED",        SQLITE_STATUS_SCRATCH_USED        },
      { "SCRATCH_OVERFLOW",    SQLITE_STATUS_SCRATCH_OVERFLOW    },
      { "MALLOC_SIZE",         SQLITE_STATUS_MALLOC_SIZE         },
    };
    int nOp = sizeof(aOp)/sizeof(aOp[0]);
    zName = (const char*)sqlite4_value_text(argv[0]);
    for(i=0; i<nOp; i++){
      if( strcmp(aOp[i].zName, zName)==0 ){
        op = aOp[i].op;
        break;
      }
    }
    if( i>=nOp ){
      char *zMsg = sqlite4_mprintf("unknown status property: %s", zName);
      sqlite4_result_error(context, zMsg, -1);
      sqlite4_free(zMsg);
      return;
    }
  }else{
    sqlite4_result_error(context, "unknown status type", -1);
    return;
  }
  if( argc==2 ){
    resetFlag = sqlite4_value_int(argv[1]);
  }else{
    resetFlag = 0;
  }
  rc = sqlite4_status(op, &cur, &mx, resetFlag);
  if( rc!=SQLITE_OK ){
    char *zMsg = sqlite4_mprintf("sqlite4_status(%d,...) returns %d", op, rc);
    sqlite4_result_error(context, zMsg, -1);
    sqlite4_free(zMsg);
    return;
  } 
  if( argc==2 ){
    sqlite4_result_int(context, mx);
  }else{
    sqlite4_result_int(context, cur);
  }
}

/*
** Extension load function.
*/
int testloadext_init(
  sqlite4 *db, 
  char **pzErrMsg, 
  const sqlite4_api_routines *pApi
){
  int nErr = 0;
  SQLITE_EXTENSION_INIT2(pApi);
  nErr |= sqlite4_create_function(db, "half", 1, SQLITE_ANY, 0, halfFunc, 0, 0);
  nErr |= sqlite4_create_function(db, "sqlite4_status", 1, SQLITE_ANY, 0,
                          statusFunc, 0, 0);
  nErr |= sqlite4_create_function(db, "sqlite4_status", 2, SQLITE_ANY, 0,
                          statusFunc, 0, 0);
  return nErr ? SQLITE_ERROR : SQLITE_OK;
}

/*
** Another extension entry point. This one always fails.
*/
int testbrokenext_init(
  sqlite4 *db, 
  char **pzErrMsg, 
  const sqlite4_api_routines *pApi
){
  char *zErr;
  SQLITE_EXTENSION_INIT2(pApi);
  zErr = sqlite4_mprintf("broken!");
  *pzErrMsg = zErr;
  return 1;
}
