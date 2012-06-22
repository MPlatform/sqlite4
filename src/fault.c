/*
** 2008 Jan 22
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
** This file contains code to support the concept of "benign" 
** malloc failures (when the xMalloc() or xRealloc() method of the
** sqlite4_mem_methods structure fails to allocate a block of memory
** and returns 0). 
**
** Most malloc failures are non-benign. After they occur, SQLite
** abandons the current operation and returns an error code (usually
** SQLITE_NOMEM) to the user. However, sometimes a fault is not necessarily
** fatal. For example, if a malloc fails while resizing a hash table, this 
** is completely recoverable simply by not carrying out the resize. The 
** hash table will continue to function normally.  So a malloc failure 
** during a hash table resize is a benign fault.
*/

#include "sqliteInt.h"

#ifndef SQLITE_OMIT_BUILTIN_TEST

/*
** Global variables.
*/
typedef struct BenignMallocHooks BenignMallocHooks;
static SQLITE_WSD struct BenignMallocHooks {
  void (*xBenignBegin)(void);
  void (*xBenignEnd)(void);
} sqlite4Hooks = { 0, 0 };

/* The "wsdHooks" macro will resolve to the appropriate BenignMallocHooks
** structure.  If writable static data is unsupported on the target,
** we have to locate the state vector at run-time.  In the more common
** case where writable static data is supported, wsdHooks can refer directly
** to the "sqlite4Hooks" state vector declared above.
*/
#ifdef SQLITE_OMIT_WSD
# define wsdHooksInit \
  BenignMallocHooks *x = sqlite4Hooks;
# define wsdHooks x[0]
#else
# define wsdHooksInit
# define wsdHooks sqlite4Hooks
#endif


/*
** Register hooks to call when sqlite4BeginBenignMalloc() and
** sqlite4EndBenignMalloc() are called, respectively.
*/
void sqlite4BenignMallocHooks(
  sqlite4_env *pEnv,
  void (*xBenignBegin)(void),
  void (*xBenignEnd)(void)
){
  wsdHooksInit;
  wsdHooks.xBenignBegin = xBenignBegin;
  wsdHooks.xBenignEnd = xBenignEnd;
}

/*
** This (sqlite4EndBenignMalloc()) is called by SQLite code to indicate that
** subsequent malloc failures are benign. A call to sqlite4EndBenignMalloc()
** indicates that subsequent malloc failures are non-benign.
*/
void sqlite4BeginBenignMalloc(sqlite4_env *pEnv){
  wsdHooksInit;
  if( wsdHooks.xBenignBegin ){
    wsdHooks.xBenignBegin();
  }
}
void sqlite4EndBenignMalloc(sqlite4_env *pEnv){
  wsdHooksInit;
  if( wsdHooks.xBenignEnd ){
    wsdHooks.xBenignEnd();
  }
}

#endif   /* #ifndef SQLITE_OMIT_BUILTIN_TEST */
