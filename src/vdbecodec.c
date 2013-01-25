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
  if( p==0 ) return SQLITE4_NOMEM;
  p->db = db;
  p->a = aIn;
  p->n = nIn;
  p->mxCol = mxCol;
  return SQLITE4_OK;
}

/*
** Destroy a decoder object previously created
** using sqlite4VdbeCreateDecoder().
*/
int sqlite4VdbeDestroyDecoder(ValueDecoder *p){
  if( p ){
    sqlite4DbFree(p->db, p);
  }
  return SQLITE4_OK;
}

/*
** Decode a single value from a data string.
*/
int sqlite4VdbeDecodeValue(
  ValueDecoder *p,             /* The decoder for the whole string */
  int iVal,                    /* Index of the value to decode.  First is 0 */
  Mem *pDefault,               /* The default value.  Often NULL */
  Mem *pOut                    /* Write the result here */
){
  u32 size;                    /* Size of a field */
  sqlite4_uint64 ofst;         /* Offset to the payload */
  sqlite4_uint64 type;         /* Datatype */
  sqlite4_uint64 subtype;      /* Subtype for a typed blob */
  int cclass;                  /* class of content */
  int n;                       /* Offset into the header */
  int i;                       /* Loop counter */
  int sz;                      /* Size of a varint */
  int endHdr;                  /* First byte past header */

  sqlite4VdbeMemSetNull(pOut);
  assert( iVal<=p->mxCol );
  n = sqlite4GetVarint64(p->a, p->n, &ofst);
  if( n==0 ) return SQLITE4_CORRUPT;
  ofst += n;
  endHdr = ofst;
  if( endHdr>p->n ) return SQLITE4_CORRUPT;
  for(i=0; i<=iVal && n<endHdr; i++){
    sz = sqlite4GetVarint64(p->a+n, p->n-n, &type);
    if( sz==0 ) return SQLITE4_CORRUPT;
    n += sz;
    if( type>=22 ){
      cclass = (type-22)%3;
      if( cclass==2 ){
         sz = sqlite4GetVarint64(p->a+n, p->n-n, &subtype);
         if( sz==0 ) return SQLITE4_CORRUPT;
         n += sz;
      }
      size = (type-22)/3;
    }else if( type<=2 ){
      size = 0;
    }else if( type<=10 ){
      size = type - 2;
    }else{
      size = type - 9;
    }
    if( i<iVal ){
      ofst += size;
    }else if( type==0 ){
      /* no-op */
    }else if( type<=2 ){
      sqlite4VdbeMemSetInt64(pOut, type-1);
    }else if( type<=10 ){
      int iByte;
      sqlite4_int64 v = ((char*)p->a)[ofst];
      for(iByte=1; iByte<size; iByte++){
        v = v*256 + p->a[ofst+iByte];
      }
      sqlite4VdbeMemSetInt64(pOut, v);
    }else if( type<=21 ){
      sqlite4_uint64 x;
      int e;
      double r;
      n = sqlite4GetVarint64(p->a+ofst, p->n-ofst, &x);
      e = (int)x;
      n += sqlite4GetVarint64(p->a+ofst+n, p->n-(ofst+n), &x);
      if( n!=size ) return SQLITE4_CORRUPT;
      r = (double)x;
      if( e&1 ) r = -r;
      if( e&2 ){
        e = -(e>>2);
        while( e<=-10 ){ r /= 1.0e10; e += 10; }
        while( e<0 ){ r /= 10.0; e++; }
      }else{
        e = e>>2;
        while( e>=10 ){ r *= 1.0e10; e -= 10; }
        while( e>0 ){ r *= 10.0; e--; }
      }
      sqlite4VdbeMemSetDouble(pOut, r);
    }else if( cclass==0 ){
      if( size==0 ){
        sqlite4VdbeMemSetStr(pOut, "", 0, SQLITE4_UTF8, SQLITE4_TRANSIENT);
      }else if( p->a[ofst]>0x02 ){
        sqlite4VdbeMemSetStr(pOut, (char*)(p->a+ofst), size, 
                             SQLITE4_UTF8, SQLITE4_TRANSIENT);
      }else{
        static const u8 enc[] = {SQLITE4_UTF8,SQLITE4_UTF16LE,SQLITE4_UTF16BE };
        sqlite4VdbeMemSetStr(pOut, (char*)(p->a+ofst+1), size-1, 
                             enc[p->a[ofst]], SQLITE4_TRANSIENT);
      }
    }else{
      sqlite4VdbeMemSetStr(pOut, (char*)(p->a+ofst), size, 0,SQLITE4_TRANSIENT);
    }
  }
  testcase( i==iVal );
  testcase( i==iVal+1 );
  if( i<=iVal ){
    if( pDefault ){
      sqlite4VdbeMemShallowCopy(pOut, pDefault, MEM_Static);
    }else{
      sqlite4VdbeMemSetNull(pOut);
    }
  }
  return SQLITE4_OK; 
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
  int rc = SQLITE4_OK;
  int nHdr;
  int n;
  u8 *aOut = 0;               /* The result */
  int nOut;                   /* Bytes of aOut used */
  int nPayload = 0;           /* Payload space required */
  int encoding = ENC(db);     /* Text encoding */
  struct dencAux {            /* For each input value of aIn[] */
    int n;                       /* Size of encoding at this position */
    u8 z[12];                    /* Encoding for number at this position */
  } *aAux;

  aAux = sqlite4StackAllocZero(db, sizeof(*aAux)*nIn);
  if( aAux==0 ) return SQLITE4_NOMEM;
  aOut = sqlite4DbMallocZero(db, (nIn+1)*9);
  if( aOut==0 ){
    rc = SQLITE4_NOMEM;
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
      if( sqlite4IsNaN(r) ){
        m = 0;
        e = 2;
      }else if( sqlite4IsInf(r)!=0 ){
        m = 1;
        e = 2 + (sqlite4IsInf(r)<0);
      }else{
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
      }
      n = sqlite4PutVarint64(aAux[i].z, (sqlite4_uint64)e);
      n += sqlite4PutVarint64(aAux[i].z+n, m);
      aAux[i].n = n;
      aOut[nOut++] = n+9;
      nPayload += n;
    }else if( flags & MEM_Str ){
      n = aIn[i].n;
      if( n && (encoding!=SQLITE4_UTF8 || aIn[i].z[0]<2) ) n++;
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
  aOut = sqlite4DbReallocOrFree(db, aOut, nOut + nPayload);
  if( aOut==0 ){ rc = SQLITE4_NOMEM; goto vdbeEncodeData_error; }
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
        if( encoding==SQLITE4_UTF16LE ) aOut[nOut++] = 1;
        else if( encoding==SQLITE4_UTF16BE ) aOut[nOut++] = 2;
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
  return SQLITE4_OK;

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
  assert( p->nOut<=p->nAlloc );
  if( p->nOut+needed>p->nAlloc ){
    u8 *aNew;
    p->nAlloc = p->nAlloc + needed + 10;
    aNew = sqlite4DbRealloc(p->db, p->aOut, p->nAlloc);
    if( aNew==0 ){
      sqlite4DbFree(p->db, p->aOut);
      memset(p, 0, sizeof(*p));
      return SQLITE4_NOMEM;
    }
    p->aOut = aNew;
    p->nAlloc = sqlite4DbMallocSize(p->db, p->aOut);
  }
  return SQLITE4_OK;
}

/*
** Encode the positive integer m using the key encoding.
**
** To encode an integer, the integer value is represented as centimal
** (base-100) with E digits.  Each centimal digit is stored in one byte
** with the most significant digits coming first.  For each centimal
** digit X (with X>=0 and X<=99) the byte value will be 2*X+1 except
** for the last digit for which the value is 2*X.  Trailing 0 digits are
** omitted, so that the encoding of the mantissa will never contain
** a zero byte.
**
** The key encoding consists of the E value (the number of
** centimal digits in the original number, before trailing zero digits
** are removed), followed by the mantissa encoding M.  This routine
** only writes the mantissa.  The E values will be embedded in the
** initial byte of the encoding by the calling function.  This
** routine returns the value of E.  E will always be at least 1 and
** no more than 10.
**
** Note that values encoded by this routine have exactly the same
** byte representation as the equivalent floating-point values encoded
** by the encodeLargeFloatKey() routine below.
*/
static int encodeIntKey(sqlite4_uint64 m, KeyEncoder *p){
  int i = 0;
  int e;
  unsigned char aDigits[20];
  assert( m>0 );
  do{
    aDigits[i++] = m%100; m /= 100;
  }while( m );
  e = i;
  while( i ) p->aOut[p->nOut++] = aDigits[--i]*2 + 1;
  p->aOut[p->nOut-1] &= 0xfe;
  return e;
}

/*
** Encode a single integer using the key encoding.  The caller must 
** ensure that sufficient space exits in a[] (at least 12 bytes).  
** The return value is the number of bytes of a[] used.  
*/
int sqlite4VdbeEncodeIntKey(u8 *a, sqlite4_int64 v){
  int i, e;
  KeyEncoder s;
  s.aOut = a;
  s.nOut = 1;
  if( v<0 ){
    e = encodeIntKey((sqlite4_uint64)-v, &s);
    assert( e<=10 );
    a[0] = 0x13-e;
    for(i=1; i<s.nOut; i++) a[i] ^= 0xff;
  }else if( v>0 ){
    e = encodeIntKey((sqlite4_uint64)v, &s);
    assert( e<=10 );
    a[0] = 0x17+e;
  }else{
    a[0] = 0x15;
  }
  return s.nOut;
}

/*
** Encode the small positive floating point number r using the key
** encoding.  The caller guarantees that r will be less than 1.0 and
** greater than 0.0.
**
** A floating point value is encoded as an integer exponent E and a 
** mantissa M.  The original value is equal to (M * 100^E). E is set
** to the smallest value possible without making M greater than or equal 
** to 1.0.
**
** For this routine, E will always be zero or negative, since the original
** value is less than one.  The encoding written by this routine is the
** ones-complement of the varint of the negative of E followed by the
** mantissa:
**
**   Encoding:   ~-E  M
*/
static void encodeSmallFloatKey(double r, KeyEncoder *p){
  int e = 0;
  int i, n;
  assert( r>0.0 && r<1.0 );
  while( r<1e-10 ){ r *= 1e8; e+=4; }
  while( r<0.01 ){ r *= 100.0; e++; }
  n = sqlite4PutVarint64(p->aOut+p->nOut, e);
  for(i=0; i<n; i++) p->aOut[i+p->nOut] ^= 0xff;
  p->nOut += n;
  for(i=0; i<18 && r!=0.0; i++){
    r *= 100.0;
    int d = r;
    p->aOut[p->nOut++] = 2*d + 1;
    r -= d;
  }
  p->aOut[p->nOut-1] &= 0xfe;
}

/*
** Encode the large positive floating point number r using the key
** encoding. The caller guarantees that r will be finite and greater than
** or equal to 1.0.
**
** A floating point value is encoded as an integer exponent E and a 
** mantissa M. The original value is equal to (M * 100^E). E is set to
** the smallest value possible without making M greater than or equal 
** to 1.0.
**
** Each centimal digit of the mantissa is stored in a byte. If the value 
** of the centimal digit is X (hence X>=0 and X<=99) then the byte value 
** will be 2*X+1 for every byte of the mantissa, except for the last byte 
** which will be 2*X+0. The mantissa must be the minimum number of bytes 
** necessary to represent the value; trailing X==0 digits are omitted. 
** This means that the mantissa will never contain a byte with the 
** value 0x00.
**
** If E is greater than 10, then this routine writes of E as a varint
** followed by the mantissa as described above. Otherwise, if E is 10 or
** less, this routine only writes the mantissa and leaves the E value
** to be encoded as part of the opening byte of the field by the
** calling function.
**
**   Encoding:  M       (if E<=10)
**              E M     (if E>10)
**
** This routine returns the value of E.
*/
static int encodeLargeFloatKey(double r, KeyEncoder *p){
  int e = 0;
  int i, n;
  assert( r>=1.0 );
  while( r>=1e32 && e<=350 ){ r *= 1e-32; e+=16; }
  while( r>=1e8 && e<=350 ){ r *= 1e-8; e+=4; }
  while( r>=1.0 && e<=350 ){ r *= 0.01; e++; }
  if( e>10 ){
    n = sqlite4PutVarint64(p->aOut+p->nOut, e);
    p->nOut += n;
  }
  for(i=0; i<18 && r!=0.0; i++){
    r *= 100.0;
    int d = r;
    p->aOut[p->nOut++] = 2*d + 1;
    r -= d;
  }
  p->aOut[p->nOut-1] &= 0xfe;
  return e;
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
  int i, e;
  int n;
  int iStart = p->nOut;
  if( flags & MEM_Null ){
    if( enlargeEncoderAllocation(p, 1) ) return SQLITE4_NOMEM;
    p->aOut[p->nOut++] = 0x05;   /* NULL */
  }else
  if( flags & MEM_Int ){
    sqlite4_int64 v = pMem->u.i;
    if( enlargeEncoderAllocation(p, 11) ) return SQLITE4_NOMEM;
    if( v==0 ){
      p->aOut[p->nOut++] = 0x15;  /* Numeric zero */
    }else if( v<0 ){
      p->aOut[p->nOut++] = 0x08;  /* Large negative number */
      i = p->nOut;
      e = encodeIntKey((sqlite4_uint64)-v, p);
      if( e<=10 ) p->aOut[i-1] = 0x13-e;
      while( i<p->nOut ) p->aOut[i++] ^= 0xff;
    }else{
      i = p->nOut;
      p->aOut[p->nOut++] = 0x22;  /* Large positive number */
      e = encodeIntKey((sqlite4_uint64)v, p);
      if( e<=10 ) p->aOut[i] = 0x17+e;
    }
  }else
  if( flags & MEM_Real ){
    double r = pMem->r;
    if( enlargeEncoderAllocation(p, 16) ) return SQLITE4_NOMEM;
    if( r==0.0 ){
      p->aOut[p->nOut++] = 0x15;  /* Numeric zero */
    }else if( sqlite4IsNaN(r) ){
      p->aOut[p->nOut++] = 0x06;  /* NaN */
    }else if( (n = sqlite4IsInf(r))!=0 ){
      p->aOut[p->nOut++] = n<0 ? 0x07 : 0x23;  /* Neg and Pos infinity */
    }else if( r<=-1.0 ){
      p->aOut[p->nOut++] = 0x08;  /* Large negative values */
      i = p->nOut;
      e = encodeLargeFloatKey(-r, p);
      if( e<=10 ) p->aOut[i-1] = 0x13-e;
      while( i<p->nOut ) p->aOut[i++] ^= 0xff;
    }else if( r<0.0 ){
      p->aOut[p->nOut++] = 0x14;  /* Small negative values */
      i = p->nOut;
      encodeSmallFloatKey(-r, p);
      while( i<p->nOut ) p->aOut[i++] ^= 0xff;
    }else if( r<1.0 ){
      p->aOut[p->nOut++] = 0x16;  /* Small positive values */
      encodeSmallFloatKey(r, p);
    }else{
      i = p->nOut;
      p->aOut[p->nOut++] = 0x22;  /* Large positive values */
      e = encodeLargeFloatKey(r, p);
      if( e<=10 ) p->aOut[i] = 0x17+e;
    }
  }else
  if( flags & MEM_Str ){
    Mem *pEnc;                    /* Pointer to memory cell in correct enc. */
    Mem sMem;                     /* Value converted to different encoding */
    int enc;                      /* Required encoding */

    /* Figure out the current encoding of pMem, and the encoding required
    ** (either the encoding specified by the collation sequence, or utf-8
    ** if there is no collation sequence).  */
    enc = ((pColl && pColl->xMkKey) ? pColl->enc : SQLITE4_UTF8);
    assert( enc==SQLITE4_UTF8 || enc==SQLITE4_UTF16LE || enc==SQLITE4_UTF16BE );
    assert( pMem->enc==SQLITE4_UTF8 || pMem->enc==SQLITE4_UTF16LE 
         || pMem->enc==SQLITE4_UTF16BE 
    );
    
    /* If necessary, convert the encoding of the input text. */
    if( pMem->enc!=enc ){
      memset(&sMem, 0, sizeof(sMem));
      sqlite4VdbeMemShallowCopy(&sMem, pMem, MEM_Static);
      sqlite4VdbeMemTranslate(&sMem, enc);
      pEnc = &sMem;
    }else{
      pEnc = pMem;
    }

    /* Write the encoded key to the output buffer. */
    if( enlargeEncoderAllocation(p, pMem->n*4 + 2) ) return SQLITE4_NOMEM;
    p->aOut[p->nOut++] = 0x24;   /* Text */
    if( pColl==0 || pColl->xMkKey==0 ){
      memcpy(p->aOut+p->nOut, pEnc->z, pEnc->n);
      p->nOut += pEnc->n;
    }else{
      int nSpc = p->nAlloc-p->nOut;
      n = pColl->xMkKey(pColl->pUser, pEnc->n, pEnc->z, nSpc, p->aOut+p->nOut);
      if( n+1>nSpc ){
        if( enlargeEncoderAllocation(p, n+1) ) return SQLITE4_NOMEM;
        n = pColl->xMkKey(pColl->pUser, pEnc->n, pEnc->z, n, p->aOut+p->nOut);
      }
      p->nOut += n;
    }
    p->aOut[p->nOut++] = 0x00;

    /* Release any memory allocated to hold the translated text */
    if( pEnc==&sMem ) sqlite4VdbeMemRelease(&sMem);

  }else
  {
    const unsigned char *a;
    unsigned char s, t;
    assert( flags & MEM_Blob );
    n = pMem->n;
    a = (u8*)pMem->z;
    s = 1;
    t = 0;
    if( enlargeEncoderAllocation(p, (n*8+6)/7 + 2) ) return SQLITE4_NOMEM;
    p->aOut[p->nOut++] = 0x25;   /* Blob */
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
  if( sortOrder==SQLITE4_SO_DESC ){
    for(i=iStart; i<p->nOut; i++) p->aOut[i] ^= 0xff;
  }
  assert( p->nOut<=p->nAlloc );
  return SQLITE4_OK;
}

/*
** Variables aKey/nKey contain an encoded index key. This function returns
** the length (in bytes) of the key with all but the first nField fields
** removed.
*/
int sqlite4VdbeShortKey(
  const u8 *aKey,                 /* Buffer containing encoded key */
  int nKey,                       /* Size of buffer aKey[] in bytes */
  int nField                      /* Number of fields */
){
  u8 *p = (u8*)aKey;
  u8 *pEnd = (u8*)&aKey[nKey];
  u64 dummy;
  int i;

  p += sqlite4GetVarint64(p, pEnd-p, &dummy);

  for(i=0; i<nField; i++){
    u8 c = *(p++);
    switch( c ){

      case 0x05: case 0xFA:       /* NULL */
      case 0x06: case 0xF9:       /* NaN */
      case 0x07: case 0xF8:       /* -ve infinity */
      case 0x15: case 0xEA:       /* zero */
      case 0x23: case 0xDC:       /* +ve infinity */
        break;

      case 0x24:                  /* Text (ascending index) */
      case 0x25:                  /* Blob (ascending index) */
        while( *(p++) );
        break;

      case 0xDB:                  /* Text (descending index) */
      case 0xDA:                  /* Blob (descending index) */
        while( (0xFF!=*(p++)) );
        break;

      case 0x22: case 0xDD:       /* Large positive number */
      case 0x14: case 0xEB:       /* Small negative number */
      case 0x16: case 0xE9:       /* Small positive number */
      case 0x08: case 0xF7: {     /* Large negative number */
        u8 d;                     /* Value of byte following "c" */

        /* For large positive and small negative integer keys skip over
        ** a varint here. For small positive integers and larger negative 
        ** integers, skip over the ones-complement varint.  */
        if( c==0x16 || c==0x08 || c==0xDD || c==0xEB ){
          d = ~(*(p++));
        }else{
          d = *(p++);
        }
        if( d>240 ){
          p++;
          if( d>248 ) p += (d - 248);
        }

        /* Fall through */
      }

      default:                    /* Medium sized number */
        if( c<0x15 || (c>0xDC && c<0xEA) ){
          while( !((*p++) & 0x01) );
        }else{
          while( ((*p++) & 0x01) );
        }
        break;
    }
  }

  return (p - aKey);
}

/*
** Generate a database key from one or more data values.
**
** Space to hold the key is obtained from sqlite4DbMalloc() and should
** be freed by the caller using sqlite4DbFree() to avoid a memory leak.
*/
int sqlite4VdbeEncodeKey(
  sqlite4 *db,                 /* The database connection */
  Mem *aIn,                    /* Values to be encoded */
  int nIn,                     /* Number of entries in aIn[] */
  int iTabno,                  /* The table this key applies to */
  KeyInfo *pKeyInfo,           /* Collating sequence and sort-order info */
  u8 **paOut,                  /* Write the resulting key here */
  int *pnOut,                  /* Number of bytes in the key */
  int nExtra                   /* See above */
){
  int i;
  int rc = SQLITE4_OK;
  KeyEncoder x;
  u8 *so;
  CollSeq **aColl;

  assert( pKeyInfo );
  assert( nIn<=pKeyInfo->nField );

  x.db = db;
  x.aOut = 0;
  x.nOut = 0;
  x.nAlloc = 0;
  *paOut = 0;
  *pnOut = 0;

  if( enlargeEncoderAllocation(&x, (nIn+1)*10) ) return SQLITE4_NOMEM;
  x.nOut = sqlite4PutVarint64(x.aOut, iTabno);
  aColl = pKeyInfo->aColl;
  so = pKeyInfo->aSortOrder;
  for(i=0; i<nIn && rc==SQLITE4_OK; i++){
    rc = encodeOneKeyValue(&x, aIn+i, so ? so[i] : SQLITE4_SO_ASC, aColl[i]);
  }

  if( rc==SQLITE4_OK && nExtra ){ rc = enlargeEncoderAllocation(&x, nExtra); }
  if( rc ){
    sqlite4DbFree(db, x.aOut);
  }else{
    *paOut = x.aOut;
    *pnOut = x.nOut;
  }
  return rc;
}

/*
** Decode an integer key encoding.  Return the number of bytes in the
** encoding on success.  On an error, return 0.
*/
int sqlite4VdbeDecodeIntKey(
  const KVByteArray *aKey,       /* Input encoding */
  KVSize nKey,                   /* Number of bytes in aKey[] */
  sqlite4_int64 *pVal            /* Write the result here */
){
  int isNeg;
  int e, x;
  int i;
  sqlite4_int64 m;
  KVByteArray aBuf[12];

  if( nKey<2 ) return 0;
  if( nKey>sizeof(aBuf) ) nKey = sizeof(aBuf);
  x = aKey[0];
  if( x>=0x09 && x<=0x13 ){
    isNeg = 1;
    memcpy(aBuf, aKey, nKey);
    aKey = aBuf;
    for(i=1; i<nKey; i++) aBuf[i] ^= 0xff;
    e = 0x13-x;
  }else if( x>=0x17 && x<=0x21 ){
    isNeg = 0;
    e = x-0x17;
  }else if( x==0x15 ){
    *pVal = 0;
    return 1;
  }else{
    return 0;
  }
  m = 0;
  i = 1;
  do{
    m = m*100 + aKey[i]/2;
    e--;
  }while( aKey[i++] & 1 );
  if( isNeg ){
    *pVal = -m;
  }else{
    *pVal = m;
  }
  return m==0 ? 0 : i;
}
