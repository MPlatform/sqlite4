/*
** 2012 January 24
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
** This file contains code for encoding and decoding values and keys for
** insertion and reading from the key/value storage engine.
*/
#include "sqliteInt.h"
#include "vdbeInt.h"

/*
** The decoder object.
*/
struct ValueDecoder {
  sqlite4 *db;                /* The database connection */
  const u8 *a;                /* Content to be decoded */
  int n;                      /* Bytes of content in a[] */
  int mxCol;                  /* Maximum number of columns */
};

/*
** Create an object that can be used to decode fields of the data encoding.
**
** The aIn[] value must remain stable for the life of the decoder.
*/
int sqlite4VdbeCreateDecoder(
  sqlite4 *db,                /* The database connection */
  const unsigned char *aIn,   /* The input data blob */
  int nIn,                    /* Number of bytes in aIn[] */
  int mxCol,                  /* Maximum number of columns in aIn[] */
  ValueDecoder **ppOut        /* The newly generated decoder object */
){
  ValueDecoder *p;

  p = sqlite4DbMallocZero(db, sizeof(*p));
  *ppOut = p;
  if( p==0 ) return SQLITE_NOMEM;
  p->db = db;
  p->a = aIn;
  p->n = nIn;
  p->mxCol = mxCol;
  return SQLITE_OK;
}

/*
** Destroy a decoder object previously created
** using sqlite4VdbeCreateDecoder().
*/
int sqlite4VdbeDestroyDecoder(ValueDecoder *p){
  if( p ){
    sqlite4DbFree(p->db, p);
  }
}

/*
** Decode a single value from a data string.
*/
int sqlite4VdbeDecodeValue(
  ValueDecoder *pDecoder,      /* The decoder for the whole string */
  int iVal,                    /* Index of the value to decode.  First is 0 */
  Mem *pDefault,               /* The default value.  Often NULL */
  Mem *pOut                    /* Write the result here */
){
  sqlite4VdbeMemSetNull(pOut);
  return SQLITE_OK;
}

/*
** Return the number of bytes needed to represent a 64-bit signed integer.
*/
static int significantBytes(sqlite4_int64 v){
  sqlite4_int64 x;
  int n = 1;
  if( v<0 ){
    x = -128;
    while( v<x && n<8 ){ n++; x *= 256; }
  }else{
    x = 127;
    while( v>x && n<8 ){ n++; x *= 256; }
  }
  return n;
}

/*
** Encode 1 or more values using the data encoding.
**
** Assume that affinity has already been applied to all elements of the
** input array aIn[].
**
** Space to hold the record is obtained from sqlite4DbMalloc() and should
** be freed by the caller using sqlite4DbFree() to avoid a memory leak.
*/
int sqlite4VdbeEncodeData(
  sqlite4 *db,                /* The database connection */
  Mem *aIn,                   /* Array of values to encode */
  int nIn,                    /* Number of entries in aIn[] */
  u8 **pzOut,                 /* The output data record */
  int *pnOut                  /* Bytes of content in pzOut */
){
  int i, j;
  int rc = SQLITE_OK;
  int nHdr;
  int n;
  u8 *aOut = 0;               /* The result */
  int nOut;                   /* Bytes of aOut used */
  int nPayload = 0;           /* Payload space required */
  int encoding = ENC(db);     /* Text encoding */
  struct dencAux {            /* For each input value of aIn[] */
    int n;                       /* Size of encoding at this position */
    char z[12];                  /* Encoding for number at this position */
  } *aAux;

  aAux = sqlite4StackAllocZero(db, sizeof(*aAux)*nIn);
  if( aAux==0 ) return SQLITE_NOMEM;
  aOut = sqlite4DbMallocZero(db, (nIn+1)*9);
  if( aOut==0 ){
    rc = SQLITE_NOMEM;
    goto vdbeEncodeData_error;
  }
  nOut = 9;
  for(i=0; i<nIn; i++){
    int flags = aIn[i].flags;
    if( flags & MEM_Null ){
      aOut[nOut++] = 0;
    }else if( flags & MEM_Int ){
      n = significantBytes(aIn[i].u.i);
      aOut[nOut++] = n+2;
      nPayload += n;
      aAux[i].n = n;
    }else if( flags & MEM_Real ){
      int e = 0;
      u8 sign = 0;
      double r = aIn[i].r;
      sqlite4_uint64 m;
      if( r<0 ){ r = -r; sign = 1; }
      while( r<1.0e+19 && r!=(sqlite4_uint64)r ){
        e--;
        r *= 10.0;
      }
      while( r>1.8e+19 ){
        e++;
        r /= 10.0;
      }
      m = r;
      if( e<0 ){
        e = (-e*4) + 2 + sign;
      }else{
        e = e*4 + sign;
      }
      n = sqlite4PutVarint64(aAux[i].z, (sqlite4_uint64)e);
      n += sqlite4PutVarint64(aAux[i].z+n, m);
      aAux[i].n = n;
      aOut[nOut++] = n+9;
      nPayload += n;
    }else if( flags & MEM_Str ){
      n = aIn[i].n;
      if( n && (encoding!=SQLITE_UTF8 || aIn[i].z[0]<2) ) n++;
      nPayload += n;
      nOut += sqlite4PutVarint64(aOut+nOut, 22+3*(sqlite4_int64)n);
    }else{
      n = aIn[i].n;
      assert( flags & MEM_Blob );
      nPayload += n;
      nOut += sqlite4PutVarint64(aOut+nOut, 23+3*(sqlite4_int64)n);
    }
  }
  nHdr = nOut - 9;
  n = sqlite4PutVarint64(aOut, nHdr);
  for(i=n, j=9; j<nOut; j++) aOut[i++] = aOut[j];
  nOut = i;
  aOut = sqlite4DbRealloc(db, aOut, nOut + nPayload);
  if( aOut==0 ){ rc = SQLITE_NOMEM; goto vdbeEncodeData_error; }
  for(i=0; i<nIn; i++){
    int flags = aIn[i].flags;
    if( flags & MEM_Null ){
      /* No content */
    }else if( flags & MEM_Int ){
      sqlite4_int64 v = aIn[i].u.i;
      n = aAux[i].n;
      aOut[nOut+(--n)] = v & 0xff;
      while( n ){
        v >>= 8;
        aOut[nOut+(--n)] = v & 0xff;
      }
      nOut += aAux[i].n;
    }else if( flags & MEM_Real ){
      memcpy(aOut+nOut, aAux[i].z, aAux[i].n);
      nOut += aAux[i].n;
    }else if( flags & MEM_Str ){
      n = aIn[i].n;
      if( n ){
        if( encoding==SQLITE_UTF16LE ) aOut[nOut++] = 1;
        else if( encoding==SQLITE_UTF16BE ) aOut[nOut++] = 2;
        else if( aIn[i].z[0]<2 ) aOut[nOut++] = 0;
        memcpy(aOut+nOut, aIn[i].z, n);
        nOut += n;
      }
    }else{
      assert( flags & MEM_Blob );
      memcpy(aOut+nOut, aIn[i].z, aIn[i].n);
      nOut += aIn[i].n;
    }
  }

  *pzOut = aOut;
  *pnOut = nOut;
  sqlite4StackFree(db, aAux);
  return SQLITE_OK;

vdbeEncodeData_error:
  sqlite4StackFree(db, aAux);
  sqlite4DbFree(db, aOut);
  return rc;
}

/*
** Generate a record key from one or more data values
**
** Space to hold the key is obtained from sqlite4DbMalloc() and should
** be freed by the caller using sqlite4DbFree() to avoid a memory leak.
*/
int sqlite4VdbeEncodeKey(
  sqlite4 *db,                 /* The database connection */
  Mem *aIn,                    /* Values to be encoded */
  int nIn,                     /* Number of entries in aIn[] */
  int iTabno,                  /* The table this key applies to */
  KeyInfo *pKeyInfo,           /* Collating sequence information */
  u8 **pzOut,                  /* Write the resulting key here */
  int *pnOut                   /* Number of bytes in the key */
){
  *pzOut = 0;
  *pnOut = 0;
  return SQLITE_INTERNAL;
}
