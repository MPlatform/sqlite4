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
** SQLITE4_NOMEM) to the user. However, sometimes a fault is not necessarily
** fatal. For example, if a malloc fails while resizing a hash table, this 
** is completely recoverable simply by not carrying out the resize. The 
** hash table will continue to function normally.  So a malloc failure 
** during a hash table resize is a benign fault.
*/

#include "sqliteInt.h"

#ifndef SQLITE4_OMIT_BUILTIN_TEST
/*
** This (sqlite4EndBenignMalloc()) is called by SQLite code to indicate that
** subsequent malloc failures are benign. A call to sqlite4EndBenignMalloc()
** indicates that subsequent malloc failures are non-benign.
*/
void sqlite4BeginBenignMalloc(sqlite4_env *pEnv){
  if( pEnv->m.xBeginBenign ) pEnv->m.xBeginBenign(pEnv->m.pMemEnv);
}
void sqlite4EndBenignMalloc(sqlite4_env *pEnv){
  if( pEnv->m.xEndBenign ) pEnv->m.xEndBenign(pEnv->m.pMemEnv);
}
#endif   /* #ifndef SQLITE4_OMIT_BUILTIN_TEST */
