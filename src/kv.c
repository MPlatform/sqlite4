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
** implementations.  It also implements tracing of calls to the KV
** engine and some higher-level ensembles of the low-level storage
** calls.
*/
#include "sqliteInt.h"

/*
** Names of error codes used for tracing.
*/
static const char *kvErrName(int e){
  const char *zName;
  switch( e ){
    case SQLITE4_OK:                  zName = "OK";                break;
    case SQLITE4_ERROR:               zName = "ERROR";             break;
    case SQLITE4_INTERNAL:            zName = "INTERNAL";          break;
    case SQLITE4_PERM:                zName = "PERM";              break;
    case SQLITE4_ABORT:               zName = "ABORT";             break;
    case SQLITE4_BUSY:                zName = "BUSY";              break;
    case SQLITE4_LOCKED:              zName = "LOCKED";            break;
    case SQLITE4_NOMEM:               zName = "NOMEM";             break;
    case SQLITE4_READONLY:            zName = "READONLY";          break;
    case SQLITE4_INTERRUPT:           zName = "INTERRUPT";         break;
    case SQLITE4_IOERR:               zName = "IOERR";             break;
    case SQLITE4_CORRUPT:             zName = "CORRUPT";           break;
    case SQLITE4_NOTFOUND:            zName = "NOTFOUND";          break;
    case SQLITE4_FULL:                zName = "FULL";              break;
    case SQLITE4_CANTOPEN:            zName = "CANTOPEN";          break;
    case SQLITE4_PROTOCOL:            zName = "PROTOCOL";          break;
    case SQLITE4_EMPTY:               zName = "EMPTY";             break;
    case SQLITE4_SCHEMA:              zName = "SCHEMA";            break;
    case SQLITE4_TOOBIG:              zName = "TOOBIG";            break;
    case SQLITE4_CONSTRAINT:          zName = "CONSTRAINT";        break;
    case SQLITE4_MISMATCH:            zName = "MISMATCH";          break;
    case SQLITE4_MISUSE:              zName = "MISUSE";            break;
    case SQLITE4_NOLFS:               zName = "NOLFS";             break;
    case SQLITE4_AUTH:                zName = "AUTH";              break;
    case SQLITE4_FORMAT:              zName = "FORMAT";            break;
    case SQLITE4_RANGE:               zName = "RANGE";             break;
    case SQLITE4_NOTADB:              zName = "NOTADB";            break;
    case SQLITE4_ROW:                 zName = "ROW";               break;
    case SQLITE4_DONE:                zName = "DONE";              break;
    case SQLITE4_INEXACT:             zName = "INEXACT";           break;
    default:                         zName = "???";               break;
  }
  return zName;
}

/*
** Do any requested tracing
*/
static void kvTrace(KVStore *p, const char *zFormat, ...){
  if( p->fTrace ){
    va_list ap;
    char *z;

    va_start(ap, zFormat);
    z = sqlite4_vmprintf(p->pEnv, zFormat, ap);
    va_end(ap);
    printf("%s.%s\n", p->zKVName, z);
    fflush(stdout);
    sqlite4_free(p->pEnv, z);
  }
}

/*
** Open a storage engine via URI
*/
int sqlite4KVStoreOpen(
  sqlite4 *db,             /* The database connection doing the open */
  const char *zName,       /* Symbolic name for this database */
  const char *zUri,        /* URI for this database */
  KVStore **ppKVStore,     /* Write the new KVStore object here */
  unsigned flags           /* Option flags */
){
  KVStore *pNew = 0;
  int rc;
  sqlite4_env *pEnv = &sqlite4DefaultEnv;  /* OR db->pEnv */
  const char *zStorageName;
  KVFactory *pMkr;
  sqlite4_kvfactory xFactory;

  if( (flags & SQLITE4_KVOPEN_TEMPORARY)!=0 || zUri==0 || zUri[0]==0 ){
    zStorageName = "temp";
  }else{
    zStorageName = sqlite4_uri_parameter(zName, "kv");
    if( zStorageName==0 ){
      if( memcmp(":memory:", zUri, 8)==0 ){
        zStorageName = "temp";
      }else{
        zStorageName = "main";
      }
    }
  }
  *ppKVStore = 0;
  sqlite4_mutex_enter(pEnv->pFactoryMutex);
  for(pMkr=pEnv->pFactory; pMkr && strcmp(zStorageName,pMkr->zName);
      pMkr=pMkr->pNext){}
  xFactory = pMkr ? pMkr->xFactory : 0;
  sqlite4_mutex_leave(pEnv->pFactoryMutex);
  if( xFactory==0 ){
    return SQLITE4_ERROR;
  }
  rc = xFactory(pEnv, &pNew, zUri, flags);
  *ppKVStore = pNew;
  if( pNew ){
    sqlite4_randomness(pEnv, sizeof(pNew->kvId), &pNew->kvId);
    sqlite4_snprintf(pNew->zKVName, sizeof(pNew->zKVName),
                     "%s", zName);
    pNew->fTrace = (db->flags & SQLITE4_KvTrace)!=0;
    kvTrace(pNew, "open(%s,%d,0x%04x)", zUri, pNew->kvId, flags);
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
    binToHex(zData, sizeof(zData), pData, nData);
    kvTrace(p, "xReplace(%d,%s,%d,%s,%d)",
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
    sqlite4_randomness(pCur->pEnv, sizeof(pCur->curId), &pCur->curId);
    pCur->fTrace = p->fTrace;
    pCur->pStore = p;
  }
  kvTrace(p, "xOpenCursor(%d,%d) -> %s",
          p->kvId, pCur?pCur->curId:-1, kvErrName(rc));
  return rc;
}
int sqlite4KVCursorSeek(
  KVCursor *p,
  const KVByteArray *pKey, KVSize nKey,
  int dir
){
  int rc;
  assert( dir==0 || dir==(+1) || dir==(-1) || dir==(-2) );  
  rc = p->pStoreVfunc->xSeek(p,pKey,nKey,dir);
  if( p->fTrace ){
    char zKey[52];
    binToHex(zKey, sizeof(zKey), pKey, nKey);
    kvTrace(p->pStore, "xSeek(%d,%s,%d,%d) -> %s",
            p->curId, zKey, (int)nKey, dir, kvErrName(rc));
  }
  return rc;
}
int sqlite4KVCursorNext(KVCursor *p){
  int rc;
  rc = p->pStoreVfunc->xNext(p);
  kvTrace(p->pStore, "xNext(%d) -> %s", p->curId, kvErrName(rc));
  return rc;
}
int sqlite4KVCursorPrev(KVCursor *p){
  int rc;
  rc = p->pStoreVfunc->xPrev(p);
  kvTrace(p->pStore, "xPrev(%d) -> %s", p->curId, kvErrName(rc));
  return rc;
}
int sqlite4KVCursorDelete(KVCursor *p){
  int rc;
  rc = p->pStoreVfunc->xDelete(p);
  kvTrace(p->pStore, "xDelete(%d) -> %s", p->curId, kvErrName(rc));
  return rc;
}
int sqlite4KVCursorReset(KVCursor *p){
  int rc;
  rc = p->pStoreVfunc->xReset(p);
  kvTrace(p->pStore, "xReset(%d) -> %s", p->curId, kvErrName(rc));
  return rc;
}
int sqlite4KVCursorKey(KVCursor *p, const KVByteArray **ppKey, KVSize *pnKey){
  int rc;
  rc = p->pStoreVfunc->xKey(p, ppKey, pnKey);
  if( p->fTrace ){
    if( rc==SQLITE4_OK ){
      char zKey[52];
      binToHex(zKey, sizeof(zKey), *ppKey, *pnKey);
      kvTrace(p->pStore, "xKey(%d,%s,%d)", p->curId, zKey, (int)*pnKey);
    }else{
      kvTrace(p->pStore, "xKey(%d,<error-%d>)", p->curId, rc);
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
    if( rc==SQLITE4_OK ){
      char zData[52];
      binToHex(zData, sizeof(zData), *ppData, *pnData);
      kvTrace(p->pStore, "xData(%d,%d,%d,%s,%d)",
             p->curId, (int)ofst, (int)n, zData, (int)*pnData);
    }else{
      kvTrace(p->pStore, "xData(%d,%d,%d,<error-%d>)",
             p->curId, (int)ofst, (int)n, rc);
    }
  }
  return rc;
}
int sqlite4KVCursorClose(KVCursor *p){
  int rc = SQLITE4_OK;
  if( p ){
    KVStore *pStore = p->pStore;
    int curId = p->curId;
    rc = p->pStoreVfunc->xCloseCursor(p);
    kvTrace(pStore, "xCloseCursor(%d) -> %s", curId, kvErrName(rc));
  }
  return rc;
}
int sqlite4KVStoreBegin(KVStore *p, int iLevel){
  int rc;
  rc = p->pStoreVfunc->xBegin(p, iLevel);
  kvTrace(p, "xBegin(%d,%d) -> %s", p->kvId, iLevel, kvErrName(rc));
  assert( p->iTransLevel==iLevel || rc!=SQLITE4_OK );
  return rc;
}
int sqlite4KVStoreCommitPhaseOne(KVStore *p, int iLevel){
  int rc;
  assert( iLevel>=0 );
  assert( iLevel<=p->iTransLevel );
  if( p->iTransLevel==iLevel ) return SQLITE4_OK;
  if( p->pStoreVfunc->xCommitPhaseOne ){
    rc = p->pStoreVfunc->xCommitPhaseOne(p, iLevel);
  }else{
    rc = SQLITE4_OK;
  }
  kvTrace(p, "xCommitPhaseOne(%d,%d) -> %s", p->kvId, iLevel, kvErrName(rc));
  assert( p->iTransLevel>iLevel );
  return rc;
}
int sqlite4KVStoreCommitPhaseTwo(KVStore *p, int iLevel){
  int rc;
  assert( iLevel>=0 );
  assert( iLevel<=p->iTransLevel );
  if( p->iTransLevel==iLevel ) return SQLITE4_OK;
  rc = p->pStoreVfunc->xCommitPhaseTwo(p, iLevel);
  kvTrace(p, "xCommitPhaseTwo(%d,%d) -> %s", p->kvId, iLevel, kvErrName(rc));
  assert( p->iTransLevel==iLevel || rc!=SQLITE4_OK );
  return rc;
}
int sqlite4KVStoreCommit(KVStore *p, int iLevel){
  int rc;
  rc = sqlite4KVStoreCommitPhaseOne(p, iLevel);
  if( rc==SQLITE4_OK ) rc = sqlite4KVStoreCommitPhaseTwo(p, iLevel);
  return rc;
}
int sqlite4KVStoreRollback(KVStore *p, int iLevel){
  int rc;
  assert( iLevel>=0 );
  assert( iLevel<=p->iTransLevel );
  rc = p->pStoreVfunc->xRollback(p, iLevel);
  kvTrace(p, "xRollback(%d,%d) -> %s", p->kvId, iLevel, kvErrName(rc));
  assert( p->iTransLevel==iLevel || rc!=SQLITE4_OK );
  return rc;
}
int sqlite4KVStoreRevert(KVStore *p, int iLevel){
  int rc;
  assert( iLevel>0 );
  assert( iLevel<=p->iTransLevel );
  if( p->pStoreVfunc->xRevert ){
    rc = p->pStoreVfunc->xRevert(p, iLevel);
    kvTrace(p, "xRevert(%d,%d) -> %s", p->kvId, iLevel, kvErrName(rc));
  }else{
    rc = sqlite4KVStoreRollback(p, iLevel-1);
    if( rc==SQLITE4_OK ){
      rc = sqlite4KVStoreBegin(p, iLevel);
    }
  }
  assert( p->iTransLevel==iLevel || rc!=SQLITE4_OK );
  return rc;
}
int sqlite4KVStoreClose(KVStore *p){
  int rc;
  if( p ){
    kvTrace(p, "xClose(%d)", p->kvId);
    rc = p->pStoreVfunc->xClose(p);
  }
  return rc;
}

/*
** Key for the meta-data
*/
static const KVByteArray metadataKey[] = { 0x00, 0x00 };

static void writeMetaArray(KVByteArray *aMeta, int iElem, u32 iVal){
  int i = sizeof(u32) * iElem;
  aMeta[i+0] = (iVal>>24)&0xff;
  aMeta[i+1] = (iVal>>16)&0xff;
  aMeta[i+2] = (iVal>>8) &0xff;
  aMeta[i+3] = (iVal>>0) &0xff;
}

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
  if( rc==SQLITE4_OK ){
    rc = sqlite4KVCursorSeek(pCur, metadataKey, sizeof(metadataKey), 0);
    if( rc==SQLITE4_NOTFOUND ){
      rc = SQLITE4_OK;
      nData = 0;
    }else if( rc==SQLITE4_OK ){
      rc = sqlite4KVCursorData(pCur, 0, -1, &aData, &nData);
    }
    if( rc==SQLITE4_OK ){
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


  rc = sqlite4KVStoreOpenCursor(p, &pCur);
  if( rc==SQLITE4_OK ){
    const KVByteArray *aData;     /* Original database meta-array value */
    KVSize nData;                 /* Size of aData[] in bytes */
    KVByteArray *aNew;            /* New database meta-array value */
    KVSize nNew;                  /* Size of aNew[] in bytes */

    /* Read the current meta-array value from the database */
    rc = sqlite4KVCursorSeek(pCur, metadataKey, sizeof(metadataKey), 0);
    if( rc==SQLITE4_OK ){
      rc = sqlite4KVCursorData(pCur, 0, -1, &aData, &nData);
    }else if( rc==SQLITE4_NOTFOUND ){
      nData = 0;
      aData = 0;
      rc = SQLITE4_OK;
    }

    /* Encode and write the new meta-array value to the database */
    if( rc==SQLITE4_OK ){
      nNew = sizeof(a[0]) * (iStart+nMeta);
      if( nNew<nData ) nNew = nData;
      aNew = sqlite4DbMallocRaw(db, nNew);
      if( aNew==0 ){
        rc = SQLITE4_NOMEM;
      }else{
        int i;
        memcpy(aNew, aData, nData);
        for(i=iStart; i<iStart+nMeta; i++){
          writeMetaArray(aNew, i, a[i]);
        }
        rc = sqlite4KVStoreReplace(p, metadataKey, sizeof(metadataKey),
                                   aNew, nNew);
        sqlite4DbFree(db, aNew);
      }
    }

    sqlite4KVCursorClose(pCur);
  }
  return rc;
}

#if defined(SQLITE4_DEBUG)
/*
** Output binary data for debugging display purposes.
*/
static void outputBinary(
  KVByteArray const *a,
  KVSize n,
  const char *zPrefix
){
  int i;
  char zOut[80];
  static const char base16[] = "0123456789abcdef";
  memset(zOut, ' ', sizeof(zOut));
  zOut[16*3+3+16] = 0;
  while( n>=0 ){
    for(i=0; i<16 && i<n; i++){
      unsigned char v = a[i];
      zOut[i*3] = base16[v>>4];
      zOut[i*3+1] = base16[v&0xf];
      zOut[16*3+3+i] = (v>=0x20 && v<=0x7e) ? v : '.';
    }
    while( i<16 ){
      zOut[i*3] = ' ';
      zOut[i*3+1] = ' ';
      zOut[16*3+3+i] = ' ';
      i++;
    }
    sqlite4DebugPrintf("%.3s %s\n", zPrefix, zOut);
    n -= 16;
    if( n<=0 ) break;
    a += 16;
    zPrefix = "   ";
  }
}

/*
** Dump the entire content of a key-value database
*/
void sqlite4KVStoreDump(KVStore *pStore){
  int rc;
  int nRow = 0;
  KVCursor *pCur;
  KVSize nKey, nData;
  KVByteArray const *aKey;
  KVByteArray const *aData;
  static const KVByteArray aProbe[] = { 0x00 };

  rc = sqlite4KVStoreOpenCursor(pStore, &pCur);
  if( rc==SQLITE4_OK ){
    rc = sqlite4KVCursorSeek(pCur, aProbe, 1, +1);
    while( rc!=SQLITE4_NOTFOUND ){
      rc = sqlite4KVCursorKey(pCur, &aKey, &nKey);
      if( rc ) break;
      if( nRow>0 ) sqlite4DebugPrintf("\n");
      nRow++;
      outputBinary(aKey, nKey, "K: ");
      rc = sqlite4KVCursorData(pCur, 0, -1, &aData, &nData);
      if( rc ) break;
      outputBinary(aData, nData, "V: ");
      rc = sqlite4KVCursorNext(pCur);
    }
    sqlite4KVCursorClose(pCur);
  }
}
#endif /* SQLITE4_DEBUG */
