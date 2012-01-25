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
** An output buffer for EncodeKey
*/
typedef struct KeyEncoder KeyEncoder;
struct KeyEncoder {
  sqlite4 *db;   /* Database connection */
  u8 *aOut;      /* Output buffer */
  int nOut;      /* Slots of aOut[] used */
  int nAlloc;    /* Slots of aOut[] allocated */
};

/*
** Enlarge a memory allocation, if necessary
*/
static int enlargeEncoderAllocation(KeyEncoder *p, int needed){
  if( p->nOut+needed>p->nAlloc ){
    u8 *aNew;
    p->nAlloc = p->nAlloc + needed + 10;
    aNew = sqlite4DbRealloc(p->db, p->aOut, p->nAlloc);
    if( aNew==0 ){
      sqlite4DbFree(p->db, p->aOut);
      memset(p, 0, sizeof(*p));
      return SQLITE_NOMEM;
    }
    p->aOut = aNew;
    p->nAlloc = sqlite4DbMallocSize(p->db, p->aOut);
  }
  return SQLITE_OK;
}

/*
** Encode the positive integer m using the key encoding.
**
** The key encoding for a positive integer consists of a varint which
** is the number of digits in the integer, followed by the digits of
** the integer from most significant to least significant, packed to
** digits to a byte.  Each digit is represented by a number between 1
** and 10 with 1 representing 0 and 10 representing 9.  A zero value 
** marks the end of the significand.  An extra zero is added to fill out
** the final byte, if necessary.
*/
static void encodeIntKey(sqlite4_uint64 m, KeyEncoder *p){
  int i;
  unsigned char aDigits[30];
  aDigits[0] = 0;
  aDigits[1] = 0;
  for(i=2; m; i++){ aDigits[i] = (m%10)+1; m /= 10; }
  p->nOut += sqlite4PutVarint64(p->aOut+p->nOut, (i-2));
  i--;
  while( i>0 ){
    p->aOut[p->nOut++] = aDigits[i]*16 + aDigits[i-1];
    i -= 2;
  }
}

/*
** Encode the small positive floating point number r using the key
** encoding.  The caller guarantees that r will be less than 1.0 and
** greater than 0.0.
**
** The key encoding is the negative of the exponent E followed by the
** mantessa M.  The exponent E is one less than the number of digits to
** the left of the decimal point.  Since r is less than 1, E will always
** be negative here.  E is output as a varint, and varints must be
** positive, which is why we output -E.  The mantissa is stored two-digits
** per byte as described for the integer encoding above.
*/
static void encodeSmallFloatKey(double r, KeyEncoder *p){
  int e = 0;
  int i, j, n;
  unsigned char aDigits[30];
  assert( r>0.0 && r<1.0 );
  while( r<1e-8 ){ r *= 1e8; e=8; }
  while( r<1.0 ){ r *= 10.0; e++; }
  n = sqlite4PutVarint64(p->aOut+p->nOut, e);
  for(i=0; i<n; i++) p->aOut[i+p->nOut] ^= 0xff;
  p->nOut += n;
  for(i=0; i<18 && r!=0.0; i++){
    int d = r;
    aDigits[i] = 1+d;
    r -= d;
    r *= 10.0;
  }
  aDigits[i] = 0;
  aDigits[i+1] = 0;
  for(j=0; j<=i; j += 2){
    p->aOut[p->nOut++] = aDigits[j]*16 + aDigits[j+1];
  }
}

/*
** Encode the large positive floating point number r using the key
** encoding.  The caller guarantees that r will be finite and greater than
** or equal to 1.0.
**
** The key encoding is the exponent E followed by the mantessa M.  
** The exponent E is one less than the number of digits to the left 
** of the decimal point.  Since r is at least than 1.0, E will always
** be non-negative here. The mantissa is stored two-digits per byte
** as described for the integer encoding above.
*/
static void encodeLargeFloatKey(double r, KeyEncoder *p){
  int e = 0;
  int i, j, n;
  unsigned char aDigits[30];
  assert( r>=1.0 );
  while( r>=1e32 && e<=350 ){ r *= 1e-32; e+=32; }
  while( r>=1e8 && e<=350 ){ r *= 1e-8; e+=8; }
  while( r>=10.0 && e<=350 ){ r *= 0.1; e++; }
  while( r<1.0 ){ r *= 10.0; e--; }
  n = sqlite4PutVarint64(p->aOut+p->nOut, e);
  p->nOut += n;
  for(i=0; i<18 && r!=0.0; i++){
    int d = r;
    aDigits[i] = 1+d;
    r -= d;
    r *= 10.0;
  }
  aDigits[i] = 0;
  aDigits[i+1] = 0;
  for(j=0; j<=i; j += 2){
    p->aOut[p->nOut++] = aDigits[j]*16 + aDigits[j+1];
  }
}


/*
** Encode a single column of the key
*/
static int encodeOneKeyValue(
  KeyEncoder *p,
  Mem *pMem,
  u8 sortOrder,
  CollSeq *pColl
){
  int flags = pMem->flags;
  int i;
  int n;
  int iStart = p->nOut;
  if( flags & MEM_Null ){
    if( enlargeEncoderAllocation(p, 1) ) return SQLITE_NOMEM;
    p->aOut[p->nOut++] = 0x01;
  }else
  if( flags & MEM_Int ){
    sqlite4_int64 v = pMem->u.i;
    if( enlargeEncoderAllocation(p, 11) ) return SQLITE_NOMEM;
    if( v==0 ){
      p->aOut[p->nOut++] = 0x07;
    }else if( v<0 ){
      p->aOut[p->nOut++] = 0x04;
      i = p->nOut;
      encodeIntKey((sqlite4_uint64)-v, p);
      while( i<p->nOut ) p->aOut[i++] ^= 0xff;
    }else{
      p->aOut[p->nOut++] = 0x0a;
      encodeIntKey((sqlite4_uint64)v, p);
    }
  }else
  if( flags & MEM_Real ){
    double r = pMem->r;
    int e;
    sqlite4_uint64 m;
    if( enlargeEncoderAllocation(p, 16) ) return SQLITE_NOMEM;
    if( r==0.0 ){
      p->aOut[p->nOut++] = 0x07;
    }else if( sqlite4IsNaN(r) ){
      p->aOut[p->nOut++] = 0x02;
    }else if( (n = sqlite4IsInf(r))!=0 ){
      p->aOut[p->nOut++] = r<0 ? 0x03 : 0x0b;
    }else if( r<=-1.0 ){
      p->aOut[p->nOut++] = 0x04;
      i = p->nOut;
      encodeLargeFloatKey(-r, p);
      while( i<p->nOut ) p->aOut[i++] ^= 0xff;
    }else if( r<0.0 ){
      p->aOut[p->nOut++] = 0x06;
      i = p->nOut;
      encodeSmallFloatKey(-r, p);
      while( i<p->nOut ) p->aOut[i++] ^= 0xff;
    }else if( r<1.0 ){
      p->aOut[p->nOut++] = 0x08;
      encodeSmallFloatKey(r, p);
    }else{
      p->aOut[p->nOut++] = 0x0a;
      encodeLargeFloatKey(r, p);
    }
  }else
  if( flags & MEM_Str ){
  }else
  {
    const unsigned char *a;
    unsigned char s, t;
    assert( flags & MEM_Blob );
    n = pMem->n;
    a = (u8*)pMem->z;
    s = 1;
    t = 0;
    if( enlargeEncoderAllocation(p, (n*8+6)/7 + 2) ) return SQLITE_NOMEM;
    p->aOut[p->nOut++] = 0x0d;
     for(i=0; i<n; i++){
      unsigned char x = a[i];
      p->aOut[p->nOut++] = 0x80 | t | (x>>s);
      if( s<7 ){
        t = x<<(7-s);
        s++;
      }else{
        p->aOut[p->nOut++] = 0x80 | x;
        s = 1;
        t = 0;
      }
    }
    if( s>1 ) p->aOut[p->nOut++] = 0x80 | t;
    p->aOut[p->nOut++] = 0x00;
  }
  if( sortOrder==SQLITE_SO_DESC ){
    for(i=iStart; i<p->nOut; i++) p->aOut[i] ^= 0xff;
  }
  return SQLITE_OK;
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
  u8 **paOut,                  /* Write the resulting key here */
  int *pnOut                   /* Number of bytes in the key */
){
  int i;
  int rc = SQLITE_OK;
  KeyEncoder x;
  
  x.db = db;
  x.aOut = 0;
  x.nOut = 0;
  x.nAlloc = 0;
  *paOut = 0;
  *pnOut = 0;
  assert( pKeyInfo->aSortOrder!=0 );
  if( enlargeEncoderAllocation(&x, (nIn+1)*10) ) return SQLITE_NOMEM;
  x.nOut = sqlite4PutVarint64(x.aOut, iTabno);
  for(i=0; i<pKeyInfo->nField && rc==SQLITE_OK; i++){
    rc = encodeOneKeyValue(&x, aIn+i, pKeyInfo->aSortOrder[i],
                           pKeyInfo->aColl[i]);
  }
  for(; i<nIn && rc==SQLITE_OK; i++){
    rc = encodeOneKeyValue(&x, aIn+i, SQLITE_SO_ASC, pKeyInfo->aColl[0]);
  }
  if( rc ){
    sqlite4DbFree(db, x.aOut);
  }else{
    *paOut = x.aOut;
    *pnOut = x.nOut;
  }
  return rc;
}
