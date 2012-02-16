/*
** 2012 February 16
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
** This file contains methods for the VdbeCursor object.
**
** A VdbeCursor is an abstraction of the KVCursor that includes knowledge
** about different "tables" in the key space.  A VdbeCursor is only active
** over a particular table.  Thus, for example, sqlite4VdbeNext() will
** return SQLITE_NOTFOUND when advancing off the end of a table into the
** next table whereas the lower-level sqlite4KVCursorNext() routine will
** not return SQLITE_NOTFOUND until it is advanced off the end of the very
** last table in the database.
*/
#include "sqliteInt.h"
#include "vdbeInt.h"


/*
** Move a VDBE cursor to the first or to the last element of its table.  The
** first element is sought if iEnd==+1 and the last element if iEnd==-1.
**
** Return SQLITE_OK on success. Return SQLITE_NOTFOUND if the table is empty.
*  Other error codes are also possible for various kinds of errors.
*/
int sqlite4VdbeSeekEnd(VdbeCursor *pC, int iEnd){
  KVCur *pKVCur = pC->pKVCur;
  KVByteArray *aKey;
  KVSize nKey;
  KVSize nProbe;
  KVByteArray aProbe[16]

  assert( iEnd==(+1) || iEnd==(-1) );  
  nProbe = sqlite4PutVarint64(aProbe, pC->iRoot);
  aProbe[nProbe++] = 10 + iEnd*6;
  rc = sqlite4KVCursorSeek(pCur, aProbe, nProbe, iEnd);
  if( rc==SQLITE_OK ){
    return SQLITE_CORRUPT;
  }
  if( rc==SQLITE_INEXACT ){
    rc = sqlite4KVCursorKey(pCur, &aKey, &nKey);
    if( rc==SQLITE_OK && (nKey<nProbe-1 || memcmp(aKey, aProbe, nProbe-1)!=0) ){
      rc = SQLITE_NOTFOUND;
    }
  }
  return rc;
}

/*
** Move a VDBE cursor to the next element in its table.
** Return SQLITE_NOTFOUND if the seek falls of the end of the table.
*/
int sqlite4VdbeNext(VdbeCursor *pC){
  KVCur *pKVCur = pC->pKVCur;
  KVByteArray *aKey;
  KVSize nKey;
  sqlite4_uint64 iTabno;

  rc = sqlite4KVCursorNext(pKVCur);
  if( rc==SQLITE_OK ){
    rc = sqlite4KVCursorKey(pKVCur, &aKey, &nKey);
    if( rc==SQLITE_OK ){
      iTabno = 0;
      sqlite4GetVarint64(aKey, nKey, &iTabno);
      if( iTabno!=pC->iRoot ) rc = SQLITE_NOTFOUND;
    }
  }
  return rc;
}

/*
** Move a VDBE cursor to the previous element in its table.
** Return SQLITE_NOTFOUND if the seek falls of the end of the table.
*/
int sqlite4VdbePrevious(VdbeCursor *pC){
  KVCur *pKVCur = pC->pKVCur;
  KVByteArray *aKey;
  KVSize nKey;
  sqlite4_uint64 iTabno;

  rc = sqlite4KVCursorPrevious(pKVCur);
  if( rc==SQLITE_OK ){
    rc = sqlite4KVCursorKey(pKVCur, &aKey, &nKey);
    if( rc==SQLITE_OK ){
      iTabno = 0;
      sqlite4GetVarint64(aKey, nKey, &iTabno);
      if( iTabno!=pC->iRoot ) rc = SQLITE_NOTFOUND;
    }
  }
  return rc;
}
