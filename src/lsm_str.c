/*
** 2012-04-27
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
** Dynamic string functions.
*/
#include "lsmInt.h"

/*
** Turn bulk and uninitialized memory into an LsmString object
*/
void lsmStringInit(LsmString *pStr, lsm_env *pEnv){
  memset(pStr, 0, sizeof(pStr[0]));
  pStr->pEnv = pEnv;
}

/*
** Increase the memory allocated for holding the string.  Realloc as needed.
**
** If a memory allocation error occurs, set pStr->n to -1 and free the existing
** allocation.  If a prior memory allocation has occurred, this routine is a
** no-op.
*/
int lsmStringExtend(LsmString *pStr, int nNew){
  assert( nNew>0 );
  if( pStr->n<0 ) return LSM_NOMEM;
  if( pStr->n + nNew >= pStr->nAlloc ){
    int nAlloc = pStr->n + nNew + 100;
    char *zNew = lsmRealloc(pStr->pEnv, pStr->z, nAlloc);
    if( zNew==0 ){
      lsmFree(pStr->pEnv, pStr->z);
      nAlloc = 0;
      pStr->n = -1;
    }else{
      pStr->nAlloc = nAlloc;
      pStr->z = zNew;
    }
  }
  return (pStr->z ? LSM_OK : LSM_NOMEM_BKPT);
}

/*
** Clear an LsmString object, releasing any allocated memory that it holds.
** This also clears the error indication (if any).
*/
void lsmStringClear(LsmString *pStr){
  lsmFree(pStr->pEnv, pStr->z);
  lsmStringInit(pStr, pStr->pEnv);
}

/*
** Append N bytes of text to the end of an LsmString object.  If
** N is negative, append the entire string.
**
** If the string is in an error state, this routine is a no-op.
*/
int lsmStringAppend(LsmString *pStr, const char *z, int N){
  int rc;
  if( N<0 ) N = (int)strlen(z);
  rc = lsmStringExtend(pStr, N+1);
  if( pStr->nAlloc ){
    memcpy(pStr->z+pStr->n, z, N+1);
    pStr->n += N;
  }
  return rc;
}

int lsmStringBinAppend(LsmString *pStr, const u8 *a, int n){
  int rc;
  rc = lsmStringExtend(pStr, n);
  if( pStr->nAlloc ){
    memcpy(pStr->z+pStr->n, a, n);
    pStr->n += n;
  }
  return rc;
}

/*
** Append printf-formatted content to an LsmString.
**
** FIXME!!!
*/
void lsmStringVAppendf(LsmString *pStr, const char *zFormat, va_list ap){
  lsmStringExtend(pStr, 5000);
  if( pStr->nAlloc ){
    pStr->n += vsnprintf(pStr->z+pStr->n, pStr->nAlloc-pStr->n-1, zFormat, ap);
    pStr->z[pStr->n] = 0;
  }
}
void lsmStringAppendf(LsmString *pStr, const char *zFormat, ...){
  va_list ap;
  va_start(ap, zFormat);
  lsmStringVAppendf(pStr, zFormat, ap);
  va_end(ap);
}

/*
** Write into memory obtained from lsm_malloc().
*/
char *lsmMallocPrintf(lsm_env *pEnv, const char *zFormat, ...){
  LsmString s;
  va_list ap;
  lsmStringInit(&s, pEnv);
  va_start(ap, zFormat);
  lsmStringVAppendf(&s, zFormat, ap);
  va_end(ap);
  return lsm_realloc(pEnv, s.z, s.n+1);
}
