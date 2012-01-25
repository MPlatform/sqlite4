/*
** 2012 January 17
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
** This file contains routines used to encode or decode variable-length
** integers.
**
** A variable length integer is an encoding of 64-bit unsigned integers
** into between 1 and 9 bytes.  The encoding is designed so that small
** (and common) values take much less space that larger values.  Additional
** properties:
**
**    *  The length of the varint can be determined after examining just
**       the first byte of the encoding.
**
**    *  Varints compare in numerical order using memcmp().
**
**************************************************************************
** 
** Treat each byte of the encoding as an unsigned integer between 0 and 255.
** Let the bytes of the encoding be called A0, A1, A2, ..., A8.
** 
** DECODE
** 
** If A0 is between 0 and 240 inclusive, then the result is the value of A0.
** 
** If A0 is between 241 and 248 inclusive, then the result is
** 240+256*(A0-241)+A1.
** 
** If A0 is 249 then the result is 2288+256*A1+A2.
** 
** If A0 is 250 then the result is A1..A3 as a 3-byte big-ending integer.
** 
** If A0 is 251 then the result is A1..A4 as a 4-byte big-ending integer.
** 
** If A0 is 252 then the result is A1..A5 as a 5-byte big-ending integer.
** 
** If A0 is 253 then the result is A1..A6 as a 6-byte big-ending integer.
** 
** If A0 is 254 then the result is A1..A7 as a 7-byte big-ending integer.
** 
** If A0 is 255 then the result is A1..A8 as a 8-byte big-ending integer.
** 
** ENCODE
** 
** Let the input value be V.
** 
** If V<=240 then output a single by A0 equal to V.
** 
** If V<=2287 then output A0 as (V-240)/256 + 241 and A1 as (V-240)%256.
** 
** If V<=67823 then output A0 as 249, A1 as (V-2288)/256, and A2 
** as (V-2288)%256.
** 
** If V<=16777215 then output A0 as 250 and A1 through A3 as a big-endian
** 3-byte integer.
** 
** If V<=4294967295 then output A0 as 251 and A1..A4 as a big-ending
** 4-byte integer.
** 
** If V<=1099511627775 then output A0 as 252 and A1..A5 as a big-ending
** 5-byte integer.
** 
** If V<=281474976710655 then output A0 as 253 and A1..A6 as a big-ending
** 6-byte integer.
** 
** If V<=72057594037927935 then output A0 as 254 and A1..A7 as a
** big-ending 7-byte integer.
** 
** Otherwise then output A0 as 255 and A1..A8 as a big-ending 8-byte integer.
** 
** SUMMARY
** 
**    Bytes    Max Value    Digits
**    -------  ---------    ---------
**      1      240           2.3
**      2      2287          3.3
**      3      67823         4.8
**      4      2**24-1       7.2
**      5      2**32-1       9.6
**      6      2**40-1      12.0
**      7      2**48-1      14.4
**      8      2**56-1      16.8
**      9      2**64-1      19.2
** 
*/
#include "sqliteInt.h"

/*
** Decode the varint in z[].  Write the integer value into *pResult and
** return the number of bytes in the varint.
*/
int sqlite4GetVarint64(const unsigned char *z, sqlite4_uint64 *pResult){
  unsigned int x;
  if( z[0]<=240 ){
    *pResult = z[0];
    return 1;
  }
  if( z[0]<=248 ){
    *pResult = (z[0]-241)*256 + z[1] + 240;
    return 2;
  }
  if( z[0]==249 ){
    *pResult = 2288 + 256*z[1] + z[2];
    return 3;
  }
  if( z[0]==250 ){
    *pResult = (z[1]<<16) + (z[2]<<8) + z[3];
    return 4;
  }
  x = (z[1]<<24) + (z[2]<<16) + (z[3]<<8) + z[4];
  if( z[0]==251 ){
    *pResult = x;
    return 5;
  }
  if( z[0]==252 ){
    *pResult = (((sqlite4_uint64)x)<<8) + z[5];
    return 6;
  }
  if( z[0]==253 ){
    *pResult = (((sqlite4_uint64)x)<<16) + (z[5]<<8) + z[6];
    return 7;
  }
  if( z[0]==254 ){
    *pResult = (((sqlite4_uint64)x)<<24) + (z[5]<<16) + (z[6]<<8) + z[7];
    return 8;
  }
  *pResult = (((sqlite4_uint64)x)<<32) +
               (0xffffffff & ((z[5]<<24) + (z[6]<<16) + (z[7]<<8) + z[8]));
  return 9;
}

/*
** Write a 32-bit unsigned integer as 4 big-endian bytes.
*/
static void varintWrite32(unsigned char *z, unsigned int y){
  z[0] = (unsigned char)(y>>24);
  z[1] = (unsigned char)(y>>16);
  z[2] = (unsigned char)(y>>8);
  z[3] = (unsigned char)(y);
}

/*
** Write a varint into z[].  The buffer z[] must be at least 9 characters
** long to accommodate the largest possible varint.  Return the number of
** bytes of z[] used.
*/
int sqlite4PutVarint64(unsigned char *z, sqlite4_uint64 x){
  unsigned int w, y;
  if( x<=240 ){
    z[0] = (unsigned char)x;
    return 1;
  }
  if( x<=2287 ){
    y = (unsigned int)(x - 240);
    z[0] = (unsigned char)(y/256 + 241);
    z[1] = (unsigned char)(y%256);
    return 2;
  }
  if( x<=67823 ){
    y = (unsigned int)(x - 2288);
    z[0] = 249;
    z[1] = (unsigned char)(y/256);
    z[2] = (unsigned char)(y%256);
    return 3;
  }
  y = (unsigned int)x;
  w = (unsigned int)(x>>32);
  if( w==0 ){
    if( y<=16777215 ){
      z[0] = 250;
      z[1] = (unsigned char)(y>>16);
      z[2] = (unsigned char)(y>>8);
      z[3] = (unsigned char)(y);
      return 4;
    }
    z[0] = 251;
    varintWrite32(z+1, y);
    return 5;
  }
  if( w<=255 ){
    z[0] = 252;
    z[1] = (unsigned char)w;
    varintWrite32(z+2, y);
    return 6;
  }
  if( w<=32767 ){
    z[0] = 253;
    z[1] = (unsigned char)(w>>8);
    z[2] = (unsigned char)w;
    varintWrite32(z+3, y);
    return 7;
  }
  if( w<=16777215 ){
    z[0] = 254;
    z[1] = (unsigned char)(w>>16);
    z[2] = (unsigned char)(w>>8);
    z[3] = (unsigned char)w;
    varintWrite32(z+4, y);
    return 8;
  }
  z[0] = 255;
  varintWrite32(z+1, w);
  varintWrite32(z+5, y);
  return 9;
}

/*
** Compile this one file with the -DTEST_VARINT option to run the simple
** test case below.  The test program generates 10 million random 64-bit
** values, weighted toward smaller numbers, and for each value it encodes
** and then decodes the varint to verify that the same number comes back.
** It also checks to make sure the if x<y then memcmp(varint(x),varint(y))<0.
*/
#ifdef TEST_VARINT
static unsigned int randInt(void){
  static unsigned int rx = 1;
  static unsigned int ry = 0;
  rx = (rx>>1) ^ (-(rx&1) & 0xd0000001);
  ry = ry*1103515245 + 12345;
  return rx ^ ry;
}
int main(int argc,char **argv){
  sqlite4_uint64 x, y, px;
  int i, n1, n2, pn;
  int nbit;
  unsigned char z[20], zp[20];

  for(i=0; i<10000000; i++){
    x = randInt();
    x = (x<<32) + randInt();
    nbit = randInt()%65;
    if( nbit<64 ){
      x &= (((sqlite4_uint64)1)<<nbit)-1;
    }
    n1 = sqlite4PutVarint64(z, x);
    assert( n1>=1 && n1<=9 );
    n2 = sqlite4GetVarint64(z, &y);
    assert( n1==n2 );
    assert( x==y );
    if( i>0 ){
      int c = memcmp(z,zp,pn<n1?pn:n1);
      if( x<px ){
        assert( c<0 );
      }else if( x>px ){
        assert( c>0 );
      }else{
        assert( c==0 );
      }
    }
    memcpy(zp, z, n1);
    pn = n1;
    px = x;
    /* printf("%24lld 0x%016llx n=%d ok\n",
              (long long int)x, (long long int)x, n1); */
  }
  printf("%d tests with 0 errors\n", i);
  return 0;
}
#endif

/*
** Compile this one file with -DVARINT_TOOL to generate a command-line
** program that converts the integers it finds as arguments into varints
** and then displays the hexadecimal representation of the varint.
*/
#ifdef VARINT_TOOL
int main(int argc, char **argv){
  int i, j, n;
  sqlite4_uint64 x;
  char out[20];
  for(i=1; i<argc; i++){
    const char *z = argv[i];
    x = 0;
    while( z[0]>='0' && z[0]<='9' ){
      x = x*10 + z[0] - '0';
      z++;
    }
    n = sqlite4PutVarint64(out, x);
    printf("%llu = ", (long long unsigned)x);
    for(j=0; j<n; j++) printf("%02x", out[j]&0xff);
    printf("\n");
  }
  return 0;
}
#endif
