/*
** 2011 November 21
**
** The authors renounce all claim of copyright to this code and dedicate
** this code to the public domain.  In place of legal notice, here is
** a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** Routines for doing math operations on sqlite4_num objects.
*/
#include "sqliteInt.h"

#define SQLITE4_MX_EXP   999    /* Maximum exponent */
#define SQLITE4_NAN_EXP 2000    /* Exponent to use for NaN */

/*
** 1/10th the maximum value of an unsigned 64-bit integer
*/
#define TENTH_MAX (LARGEST_UINT64/10)

/*
** Adjust the significand and exponent of pA and pB so that the
** exponent is the same.
*/
static void adjustExponent(sqlite4_num *pA, sqlite4_num *pB){
  if( pA->e<pB->e ){
    sqlite4_num *t = pA;
    pA = pB;
    pB = t;
  }
  if( pB->m==0 ){
    pB->e = pA->e;
    return;
  }
  if( pA->m==0 ){
    pA->e = pB->e;
    return;
  }
  if( pA->e > pB->e+40 ){
    pB->approx = 1;
    pB->e = pA->e;
    pB->m = 0;
    return;
  }
  while( pA->e>pB->e && pB->m%10==0  ){
    pB->m /= 10;
    pB->e++;
  }
  while( pA->e>pB->e && pA->m<=TENTH_MAX ){
    pA->m *= 10;
    pA->e--;
  }
  while( pA->e>pB->e ){
    pB->m /= 10;
    pB->e++;
    pB->approx = 1;
  }
}

/*
** Add two numbers and return the result.
*/
sqlite4_num sqlite4_num_add(sqlite4_num A, sqlite4_num B){
  sqlite4_uint64 r;
  if( A.sign!=B.sign ){
    if( A.sign ){
      A.sign = 0;
      return sqlite4_num_sub(B,A);
    }else{
      B.sign = 0;
      return sqlite4_num_sub(A,B);
    }
  }
  if( A.e>SQLITE4_MX_EXP ){
    if( B.e>SQLITE4_MX_EXP && B.m==0 ) return B;
    return A;
  }
  if( B.e>SQLITE4_MX_EXP ){
    return B;
  }
  adjustExponent(&A, &B);
  r = A.m+B.m;
  A.approx |= B.approx;
  if( r>=A.m ){
    A.m = r;
  }else{
    if( A.approx==0 && (A.m%10)!=0 ) A.approx = 1;
    A.m /= 10;
    A.e++;
    if( A.e>SQLITE4_MX_EXP ) return A;
    if( A.approx==0 && (B.m%10)!=0 ) A.approx = 1;
    A.m += B.m/10;
  }
  return A;
}

/*
** Subtract the second number from the first and return the result.
*/
sqlite4_num sqlite4_num_sub(sqlite4_num A, sqlite4_num B){
  if( A.sign!=B.sign ){
    B.sign = A.sign;
    return sqlite4_num_add(A,B);
  }
  if( A.e>SQLITE4_MX_EXP || B.e>SQLITE4_MX_EXP ){
    A.e = SQLITE4_NAN_EXP;
    A.m = 0;
    return A;
  }
  adjustExponent(&A, &B);
  if( B.m > A.m ){
    sqlite4_num t = A;
    A = B;
    B = t;
    A.sign = 1-A.sign;
  }
  A.m -= B.m;
  A.approx |= B.approx;
  return A;
}

/*
** Return true if multiplying x and y will cause 64-bit unsigned overflow.
*/
static int multWillOverflow(sqlite4_uint64 x, sqlite4_uint64 y){
  sqlite4_uint64 xHi, xLo, yHi, yLo;
  xHi = x>>32;
  yHi = y>>32;
  if( xHi*yHi ) return 1;
  xLo = x & 0xffffffff;
  yLo = y & 0xffffffff;
  if( (xHi*yLo + yHi*xLo)>0xffffffff ) return 1;
  return 0;
}

/*
** Multiply two numbers and return the result.
*/
sqlite4_num sqlite4_num_mul(sqlite4_num A, sqlite4_num B){
  sqlite4_num r;
  if( A.e>SQLITE4_MX_EXP ){
    A.sign ^= B.sign;
    return A;
  }else if( B.e>SQLITE4_MX_EXP ){
    B.sign ^= A.sign;
    return B;
  }
  if( A.m==0 ) return A;
  if( B.m==0 ) return B;
  while( A.m%10==0 ){ A.m /= 10; A.e++; }
  while( B.m%10==0 ){ B.m /= 10; B.e++; }
  r.sign = A.sign ^ B.sign;
  r.approx = A.approx | B.approx;
  while( multWillOverflow(A.m, B.m) ){
    r.approx = 1;
    if( A.m>B.m ){
      A.m /= 10;
      A.e++;
    }else{
      B.m /= 10;
      B.e++;
    }
  }
  r.m = A.m*B.m;
  r.e = A.e + B.e;
  return r;
}

/*
** Divide two numbers and return the result.
*/
sqlite4_num sqlite4_num_div(sqlite4_num A, sqlite4_num B){
  sqlite4_num r;
  if( A.e>SQLITE4_MX_EXP ){
    A.m = 0;
    return A;
  }
  if( B.e>SQLITE4_MX_EXP ){
    if( B.m!=0 ){
      r.m = 0;
      r.e = 0;
      r.sign = 0;
      r.approx = 1;
      return r;
    }
    return B;
  }
  if( B.m==0 ){
    r.sign = A.sign ^ B.sign;
    r.e = SQLITE4_NAN_EXP;
    r.m = 1;
    r.approx = 1;
    return r;
  }
  if( A.m==0 ){
    return A;
  }
  while( A.m<TENTH_MAX ){
    A.m *= 10;
    A.e--;
  }
  while( B.m%10==0 ){
    B.m /= 10;
    B.e++;
  }
  r.sign = A.sign ^ B.sign;
  r.approx = A.approx | B.approx;
  if( r.approx==0 && A.m%B.m!=0 ) r.approx = 1;
  r.m = A.m/B.m;
  r.e = A.e - B.e;
  return r;
}

/*
** Test if A is NaN.
*/
int sqlite4_num_isnan(sqlite4_num A){
  return A.e>SQLITE4_MX_EXP && A.m==0; 
}

/*
** Compare numbers A and B.  Return:
**
**    1     if A<B
**    2     if A==B
**    3     if A>B
**    0     the values are not comparible.
**
** NaN values are always incompariable.  Also +inf returns 0 when 
** compared with +inf and -inf returns 0 when compared with -inf.
*/
int sqlite4_num_compare(sqlite4_num A, sqlite4_num B){
  if( A.e>SQLITE4_MX_EXP ){
    if( A.m==0 ) return 0;
    if( B.e>SQLITE4_MX_EXP ){
      if( B.m==0 ) return 0;
      if( B.sign==A.sign ) return 0;
    }
    return A.sign ? 1 : 3;
  }
  if( B.e>SQLITE4_MX_EXP ){
    if( B.m==0 ) return 0;
    return B.sign ? 3 : 1;
  }
  if( A.sign!=B.sign ){
    return A.sign ? 1 : 3;
  }
  adjustExponent(&A, &B);
  if( A.sign ){
    sqlite4_num t = A;
    A = B;
    B = t;
  }
  if( A.e!=B.e ){
    return A.e<B.e ? 1 : 3;
  }
  if( A.m!=B.m ){
    return A.m<B.m ? 1 : 3;
  }
  return 2;
}

/*
** Round the value so that it has at most N digits to the right of the
** decimal point.
*/
sqlite4_num sqlite4_num_round(sqlite4_num x, int N){
  if( N<0 ) N = 0;
  if( x.e >= -N ) return x;
  if( x.e < -(N+30) ){
    memset(&x, 0, sizeof(x));
    return x;
  }
  while( x.e < -(N+1) ){
    x.m /= 10;
    x.e++;
  }
  x.m = (x.m+5)/10;
  x.e++;
  return x;
}

/*
** Convert text into a number and return that number.
**
** When converting from UTF16, this routine only looks at the
** least significant byte of each character.  It is assumed that
** the most significant byte of every character in the string
** is 0.  If that assumption is violated, then this routine can
** yield an anomolous result.
**
** Conversion stops at the first \000 character.  At most nIn bytes
** of zIn are examined.  Or if nIn is negative, up to a billion bytes
** are scanned, which we assume is more than will be found in any valid
** numeric string.
*/
sqlite4_num sqlite4_num_from_text(const char *zIn, int nIn, unsigned flags){
  int incr = 1;
  sqlite4_num r;
  char c;
  int nDigit = 0;
  int seenRadix = 0;
  int i;
  static int one = 1;
  
  memset(&r, 0, sizeof(r));
  if( nIn<0 ) nIn = 1000000000;
  c = flags & 0xf;
  if( c==0 || c==SQLITE4_UTF8 ){
    incr = 1;
  }else if( c==SQLITE4_UTF16 ){
    incr = 2;
    c = *(char*)&one;
    zIn += c;
    nIn -= c;
  }
  
  if( nIn<=0 ) goto not_a_valid_number;
  if( zIn[0]=='-' ){
    r.sign = 1;
    i = incr;
  }else if( zIn[0]=='+' ){
    i = incr;
  }else{
    i = 0;
  }
  if( nIn<=0 ) goto not_a_valid_number;
  if( nIn>=incr*3
   && ((c=zIn[i])=='i' || c=='I')
   && ((c=zIn[i+incr])=='n' || c=='N')
   && ((c=zIn[i+incr*2])=='f' || c=='F')
  ){
    r.e = SQLITE4_MX_EXP+1;
    r.m = nIn<=i+incr*3 || zIn[i+incr*3]==0;
    return r;
  }
  while( i<nIn && (c = zIn[i])!=0 ){
    i += incr;
    if( c>='0' && c<='9' ){
      if( c=='0' && nDigit==0 ){
        if( seenRadix && r.e > -(SQLITE4_MX_EXP+1000) ) r.e--;
        continue;
      }
      nDigit++;
      if( nDigit<=18 ){
        r.m = (r.m*10) + c - '0';
        if( seenRadix ) r.e--;
      }else{
        if( c!='0' ) r.approx = 1;
        if( !seenRadix ) r.e++;
      }
    }else if( c=='.' ){
      seenRadix = 1;
    }else{
      break;
    }
  }
  if( c=='e' || c=='E' ){
    int exp = 0;
    int expsign = 0;
    int nEDigit = 0;
    if( zIn[i]=='-' ){
      expsign = 1;
      i += incr;
    }else if( zIn[i]=='+' ){
      i += incr;
    }
    if( i>=nIn ) goto not_a_valid_number;
    while( i<nIn && (c = zIn[i])!=0  ){
      i += incr;
      if( c<'0' || c>'9' ) break;
      if( c=='0' && nEDigit==0 ) continue;
      nEDigit++;
      if( nEDigit>3 ) goto not_a_valid_number;
      exp = exp*10 + c - '0';
    }
    if( expsign ) exp = -exp;
    r.e += exp;
  }
  if( c!=0 ) goto not_a_valid_number;
  return r;
  
not_a_valid_number:
  r.e = SQLITE4_MX_EXP+1;
  r.m = 0;
  return r;  
}

/*
** Convert an sqlite4_int64 to a number and return that number.
*/
sqlite4_num sqlite4_num_from_int64(sqlite4_int64 n){
  sqlite4_num r;
  r.approx = 0;
  r.e = 0;
  r.sign = n < 0;
  if( n>=0 ){
    r.m = n;
  }else if( n!=SMALLEST_INT64 ){
    r.m = -n;
  }else{
    r.m = 1+(u64)LARGEST_INT64;
  }
  return r;
}

/*
** Convert an integer into text in the buffer supplied. The
** text is zero-terminated and right-justified in the buffer.
** A pointer to the first character of text is returned.
**
** The buffer needs to be at least 21 bytes in length.
*/
static char *renderInt(sqlite4_uint64 v, char *zBuf, int nBuf){
  int i = nBuf-1;;
  zBuf[i--] = 0;
  do{
    zBuf[i--] = (v%10) + '0';
    v /= 10;
  }while( v>0 );
  return zBuf+(i+1);
}

/*
** Remove trailing zeros from a string.
*/
static void removeTrailingZeros(char *z, int *pN){
  int i = *pN;
  while( i>0 && z[i-1]=='0' ) i--;
  z[i] = 0;
  *pN = i;
}

/*
** Convert a number into text.  Store the result in zOut[].  The
** zOut buffer must be at laest 30 characters in length.  The output
** will be zero-terminated.
*/
int sqlite4_num_to_text(sqlite4_num x, char *zOut){
  char zBuf[24];
  int nOut = 0;
  char *zNum;
  int n;
  static const char zeros[] = "0000000000000000000000000";
  
  if( x.sign && x.m>0 ){
    /* Add initial "-" for negative non-zero values */
    zOut[0] = '-';
    zOut++;
    nOut++;
  }
  if( x.e>SQLITE4_MX_EXP ){
    /* Handle NaN and infinite values */
    if( x.m==0 ){
      memcpy(zOut, "NaN", 4);
    }else{
      memcpy(zOut, "inf", 4);
    }
    return nOut+3;
  }
  if( x.m==0 ){
    memcpy(zOut, "0", 2);
    return 1;
  }
  zNum = renderInt(x.m, zBuf, sizeof(zBuf));
  n = &zBuf[sizeof(zBuf)-1] - zNum;
  if( x.e>=0 && x.e+n<=25 ){
    /* Integer values with up to 25 digits */
    memcpy(zOut, zNum, n+1);
    nOut += n;
    if( x.e>0 ){
      memcpy(&zOut[nOut], zeros, x.e);
      zOut[nOut+x.e] = 0;
      nOut += x.e;
    }
    return nOut;
  }
  if( x.e<0 && n+x.e > 0 ){
    /* Fractional values where the decimal point occurs within the
    ** significant digits.  ex:  12.345 */
    int m = n+x.e;
    memcpy(zOut, zNum, m);
    nOut += m;
    zOut += m;
    zNum += m;
    n -= m;
    removeTrailingZeros(zNum, &n);
    if( n>0 ){
      zOut[0] = '.';
      memcpy(zOut+1, zNum, n);
      nOut += n;
    }
    zOut[n+1] = 0;
    return nOut;
  }
  if( x.e<0 && x.e >= -n-5 ){
    /* Values less than 1 and with no more than 5 subsequent zeros prior
    ** to the first significant digit.  Ex:  0.0000012345 */
    int j = -(n + x.e);
    memcpy(zOut, "0.", 2);
    nOut += 2;
    zOut += 2;
    if( j>0 ){
      memcpy(zOut, zeros, j);
      nOut += j;
      zOut += j;
    }
    removeTrailingZeros(zNum, &n);
    memcpy(zOut, zNum, n+1);
    nOut += n;
    zOut[n+1] = 0;
    return nOut;
  }
  /* Exponential notation from here to the end.  ex:  1.234e-15 */
  zOut[0] = zNum[0];
  if( n>1 ){
    int nOrig = n;
    removeTrailingZeros(zNum, &n);
    x.e += nOrig - n;
  }
  if( n==1 ){
    /* Exactly one significant digit.  ex:  8e12 */
    zOut++;
    nOut++;
  }else{
    /* Two or or more significant digits.  ex: 1.23e17 */
    zOut[1] = '.';
    memcpy(zOut+2, zNum+1, n-1);
    zOut += n+1;
    nOut += n+1;
    x.e += n-1;
  }
  zOut[0] = 'e';
  zOut++;
  nOut++;
  if( x.e<0 ){
    zOut[0] = '-';
    x.e = -x.e;
  }else{
    zOut[0] = '+';
  }
  zOut++;
  nOut++;
  zNum = renderInt(x.e&0x7fff, zBuf, sizeof(zBuf));
  while( (zOut[0] = zNum[0])!=0 ){ zOut++; zNum++; nOut++; }
  return nOut;
}
