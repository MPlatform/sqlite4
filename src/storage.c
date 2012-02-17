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
int sqlite4KVStoreOpen(const char *zUri, KVStore **ppKVStore, unsigned flags){
  KVStore *pNew = 0;
  int rc;

  rc = sqlite4KVStoreOpenMem(&pNew, flags);
  *ppKVStore = pNew;
  if( pNew ){
    sqlite4_randomness(sizeof(pNew->kvId), &pNew->kvId);
    pNew->fTrace = 0;
    if( pNew->fTrace ){
      printf("KVopen(%s,%d, 0x%04x)\n", zUri, pNew->kvId, flags);
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
  int rc;
  assert( (iLevel==2 && p->iTransLevel==0) || p->iTransLevel+1==iLevel );
  rc = p->pStoreVfunc->xBegin(p, iLevel);
  if( p->fTrace ){
    printf("KV.xBegin(%d,%d) -> %d\n", p->kvId, iLevel, rc);
  }
  assert( p->iTransLevel==iLevel || rc!=SQLITE_OK );
  return rc;
}
int sqlite4KVStoreCommit(KVStore *p, int iLevel){
  int rc;
  assert( iLevel>=0 );
  assert( iLevel>=p->iTransLevel );
  rc = p->pStoreVfunc->xCommit(p, iLevel);
  if( p->fTrace ){
    printf("KV.xCommit(%d,%d) -> %d\n", p->kvId, iLevel, rc);
  }
  assert( p->iTransLevel==iLevel || rc!=SQLITE_OK );
  return rc;
}
int sqlite4KVStoreRollback(KVStore *p, int iLevel){
  int rc;
  assert( iLevel>=0 );
  assert( iLevel>=p->iTransLevel );
  rc = p->pStoreVfunc->xRollback(p, iLevel);
  if( p->fTrace ){
    printf("KV.xRollback(%d,%d) -> %d\n", p->kvId, iLevel, rc);
  }
  assert( p->iTransLevel==iLevel || rc!=SQLITE_OK );
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

/*
** Key for the meta-data
*/
static const KVByteArray metadataKey[] = { 0x00, 0x00 };

/*
** Read nMeta unsigned 32-bit integers of metadata beginning at iStart.
*/
int sqlite4KVStoreGetMeta(KVStore *p, int iStart, int nMeta, unsigned int *a){
  KVCursor *pCur;
  int rc;
  int i, j;
  KVSize nData;
  const KVByteArray *aData;

  rc = sqlite4KVStoreOpenCursor(p, &pCur);
  if( rc==SQLITE_OK ){
    rc = sqlite4KVCursorSeek(pCur, metadataKey, sizeof(metadataKey), 0);
    if( rc==SQLITE_OK ){
      rc = sqlite4KVCursorData(pCur, 0, -1, &aData, &nData);
      if( rc==SQLITE_OK ){
        i = 0;
        j = iStart*4;
        while( i<nMeta && j+3<nData ){
          a[i] = (aData[j]<<24) | (aData[j+1]<<16)
                       | (aData[j+2]<<8) | aData[j+3];
          i++;
          j += 4;
        }
        while( i<nMeta ) a[i++] = 0;
      }  
    }
    sqlite4KVCursorClose(pCur);
  }
  return rc;
}

/*
** Write nMeta unsigned 32-bit integers beginning with iStart.
*/
int sqlite4KVStorePutMeta(
  sqlite4 *db,            /* Database connection.  Needed to malloc */
  KVStore *p,             /* Write to this database */
  int iStart,             /* Start writing here */
  int nMeta,              /* number of 32-bit integers to be written */
  unsigned int *a         /* The integers to write */
){
  KVCursor *pCur;
  int rc;
  int i, j;
  KVSize nData;
  const KVByteArray *aData;
  KVByteArray *aNew;
  KVSize nNew;

  rc = sqlite4KVStoreOpenCursor(p, &pCur);
  if( rc==SQLITE_OK ){
    rc = sqlite4KVCursorSeek(pCur, metadataKey, sizeof(metadataKey), 0);
    if( rc==SQLITE_OK ){
      rc = sqlite4KVCursorData(pCur, 0, -1, &aData, &nData);
      if( rc==SQLITE_NOTFOUND ){
        nData = 0;
        rc = SQLITE_OK;
      }
      if( rc==SQLITE_OK ){
        nNew = iStart+nMeta;
        if( nNew<nData ) nNew = nData;
        aNew = sqlite4DbMallocRaw(db, nNew*sizeof(a[0]) );
        if( aNew==0 ){
          rc = SQLITE_NOMEM;
        }else{
          memcpy(aNew, aData, nData);
          i = 0;
          j = iStart*4;
          while( i<nMeta && j+3<nData ){
            aNew[j] = (a[i]>>24)&0xff;
            aNew[j+1] = (a[i]>>16)&0xff;
            aNew[j+2] = (a[i]>>8)&0xff;
            aNew[j+3] = a[i] & 0xff;
            i++;
            j += 4;
          }
          rc = sqlite4KVStoreReplace(p, metadataKey, sizeof(metadataKey),
                                     aNew, nNew);
          sqlite4DbFree(db, aNew);
        }
      }  
    }
    sqlite4KVCursorClose(pCur);
  }
  return rc;
}
