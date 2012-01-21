/*
** 2012 January 21
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
** General wrapper functions around the various KV storage engine
** implementations.
*/
#include "sqliteInt.h"

/*
** Open a storage engine via URI
*/
int sqlite3KVStoreOpen(const char *zUri, KVStore **ppKVStore){
  return sqlite3KVStoreOpenMem(ppKVStore);
}
