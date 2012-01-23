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
int sqlite4KVStoreOpen(const char *zUri, KVStore **ppKVStore){
  KVStore *pNew = 0;
  int rc;

  rc = sqlite4KVStoreOpenMem(&pNew);
  *ppKVStore = pNew;
  if( pNew ){
    sqlite4_randomness(sizeof(pNew->kvId), &pNew->kvId);
    pNew->fTrace = 1;
    if( pNew->fTrace ){
      printf("KVopen(%s,%d)\n", zUri, pNew->kvId);
    }
  }
  return rc;
}

/* Convert binary data to hex for display in trace messages */
static void binToHex(char *zOut, int mxOut, const KVByteArray *a, KVSize n){
  int i;
  if( n>mxOut/2-1 ) n = mxOut/2-1;
  for(i=0; i<n; i++){
    zOut[i*2] = "0123456789abcdef"[(a[i]>>4)&0xf];
    zOut[i*2+1] = "0123456789abcdef"[a[i]&0xf];
  }
  zOut[i*2] = 0;
}

/*
** The following wrapper functions invoke the underlying methods of
** the storage object and add optional tracing.
*/
int sqlite4KVStoreReplace(
  KVStore *p,
  const KVByteArray *pKey, KVSize nKey,
  const KVByteArray *pData, KVSize nData
){
  if( p->fTrace ){
    char zKey[52], zData[52];
    binToHex(zKey, sizeof(zKey), pKey, nKey);
    binToHex(zData, sizeof(zKey), pData, nData);
    printf("KV.xReplace(%d,%s,%d,%s,%d)\n",
           p->kvId, zKey, (int)nKey, zData, (int)nData);
  }
  return p->pStoreVfunc->xReplace(p,pKey,nKey,pData,nData);
}
int sqlite4KVStoreOpenCursor(KVStore *p, KVCursor **ppKVCursor){
  KVCursor *pCur;
  int rc;

  rc = p->pStoreVfunc->xOpenCursor(p, &pCur);
  *ppKVCursor = pCur;
  if( pCur ){
    sqlite4_randomness(sizeof(pCur->curId), &pCur->curId);
    pCur->fTrace = p->fTrace;
  }
  if( p->fTrace ){
    printf("KV.xOpenCursor(%d,%d) -> %d\n", p->kvId, pCur?pCur->curId:-1, rc);
  }
  return rc;
}
int sqlite4KVCursorSeek(
  KVCursor *p,
  const KVByteArray *pKey, KVSize nKey,
  int dir
){
  int rc;
  rc = p->pStoreVfunc->xSeek(p,pKey,nKey,dir);
  if( p->fTrace ){
    char zKey[52];
    binToHex(zKey, sizeof(zKey), pKey, nKey);
    printf("KV.xSeek(%d,%s,%d,%d) -> %d\n", p->curId, zKey, (int)nKey,dir,rc);
  }
  return rc;
}
int sqlite4KVCursorNext(KVCursor *p){
  int rc;
  rc = p->pStoreVfunc->xNext(p);
  if( p->fTrace ){
    printf("KV.xNext(%d) -> %d\n", p->curId, rc);
  }
  return rc;
}
int sqlite4KVCursorPrev(KVCursor *p){
  int rc;
  rc = p->pStoreVfunc->xPrev(p);
  if( p->fTrace ){
    printf("KV.xPrev(%d) -> %d\n", p->curId, rc);
  }
  return rc;
}
int sqlite4KVCursorDelete(KVCursor *p){
  int rc;
  rc = p->pStoreVfunc->xDelete(p);
  if( p->fTrace ){
    printf("KV.xDelete(%d) -> %d\n", p->curId, rc);
  }
  return rc;
}
int sqlite4KVCursorReset(KVCursor *p){
  int rc;
  rc = p->pStoreVfunc->xReset(p);
  if( p->fTrace ){
    printf("KV.xReset(%d) -> %d\n", p->curId, rc);
  }
  return rc;
}
int sqlite4KVCursorKey(KVCursor *p, const KVByteArray **ppKey, KVSize *pnKey){
  int rc;
  rc = p->pStoreVfunc->xKey(p, ppKey, pnKey);
  if( p->fTrace ){
    if( rc==SQLITE_OK ){
      char zKey[52];
      binToHex(zKey, sizeof(zKey), *ppKey, *pnKey);
      printf("KV.xKey(%d,%s,%d)\n", p->curId, zKey, (int)*pnKey);
    }else{
      printf("KV.xKey(%d,<error-%d>)\n", p->curId, rc);
    }
  }
  return rc;
}
int sqlite4KVCursorData(
  KVCursor *p,
  KVSize ofst,
  KVSize n,
  const KVByteArray **ppData,
  KVSize *pnData
){
  int rc;
  rc = p->pStoreVfunc->xData(p, ofst, n, ppData, pnData);
  if( p->fTrace ){
    if( rc==SQLITE_OK ){
      char zData[52];
      binToHex(zData, sizeof(zData), *ppData, *pnData);
      printf("KV.xData(%d,%d,%d,%s,%d)\n",
             p->curId, (int)ofst, (int)n, zData, (int)*pnData);
    }else{
      printf("KV.xData(%d,%d,%d,<error-%d>)\n",
             p->curId, (int)ofst, (int)n, rc);
    }
  }
  return rc;
}
int sqlite4KVCursorClose(KVCursor *p){
  int rc = SQLITE_OK;
  if( p ){
    rc = p->pStoreVfunc->xCloseCursor(p);
    if( p->fTrace ){
      printf("KV.xCloseCursor(%d) -> %d\n", p->curId, rc);
    }
  }
  return rc;
}
int sqlite4KVStoreBegin(KVStore *p, int iLevel){
  int rc = p->pStoreVfunc->xBegin(p, iLevel);
  if( p->fTrace ){
    printf("KV.xBegin(%d,%d) -> %d\n", p->kvId, iLevel, rc);
  }
  return rc;
}
int sqlite4KVStoreCommit(KVStore *p, int iLevel){
  int rc = p->pStoreVfunc->xCommit(p, iLevel);
  if( p->fTrace ){
    printf("KV.xCommit(%d,%d) -> %d\n", p->kvId, iLevel, rc);
  }
  return rc;
}
int sqlite4KVStoreRollback(KVStore *p, int iLevel){
  int rc = p->pStoreVfunc->xRollback(p, iLevel);
  if( p->fTrace ){
    printf("KV.xRollback(%d,%d) -> %d\n", p->kvId, iLevel, rc);
  }
  return rc;
}
int sqlite4KVStoreClose(KVStore *p){
  int rc;
  if( p ){
    if( p->fTrace ){
      printf("KV.xClose(%d)\n", p->kvId);
    }
    rc = p->pStoreVfunc->xClose(p);
  }
  return rc;
}
