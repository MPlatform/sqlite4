/*
** 2012 December 17
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
*/

#include "sqliteInt.h"

static int fts5SimpleCreate(
  void *pCtx, 
  const char **azArg, 
  int nArg, 
  sqlite4_tokenizer **pp
){
  *pp = (sqlite4_tokenizer *)pCtx;
  return SQLITE4_OK;
}

static int fts5SimpleDestroy(sqlite4_tokenizer *p){
  return SQLITE4_OK;
}

static char fts5Tolower(char c){
  if( c>='A' && c<='Z' ) c = c + ('a' - 'A');
  return c;
}

static int fts5SimpleTokenize(
  void *pCtx, 
  sqlite4_tokenizer *p,
  const char *zDoc,
  int nDoc,
  int(*x)(void*, int, const char*, int, int, int)
){
  sqlite4_env *pEnv = (sqlite4_env *)p;
  char *aBuf;
  int nBuf;
  int iBuf;
  int i;
  int brk = 0;

  nBuf = 128;
  aBuf = (char *)sqlite4_malloc(pEnv, nBuf);
  if( !aBuf ) return SQLITE4_NOMEM;

  iBuf = 0;
  for(i=0; brk==0 && i<nDoc; i++){
    if( sqlite4Isalnum(zDoc[i]) ){
      aBuf[iBuf++] = fts5Tolower(zDoc[i]);
    }else if( iBuf>0 ){
      brk = x(pCtx, 0, aBuf, iBuf, i-iBuf, iBuf);
      iBuf = 0;
    }
  }
  if( iBuf>0 ) x(pCtx, 0, aBuf, iBuf, i-iBuf, iBuf);

  sqlite4_free(pEnv, aBuf);
  return SQLITE4_OK;
}

int sqlite4InitFts5Func(sqlite4 *db){
  int rc;
  sqlite4_env *pEnv = sqlite4_db_env(db);

  rc = sqlite4_create_tokenizer(db, "simple", (void *)pEnv, 
      fts5SimpleCreate, fts5SimpleTokenize, fts5SimpleDestroy
  );
  if( rc!=SQLITE4_OK ) return rc;

  return rc;
}

