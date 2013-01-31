/*
** 2004 April 13
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains routines used to translate between UTF-8, 
** UTF-16, UTF-16BE, and UTF-16LE.
**
** Notes on UTF-8:
**
**   Byte-0    Byte-1    Byte-2    Byte-3    Value
**  0xxxxxxx                                 00000000 00000000 0xxxxxxx
**  110yyyyy  10xxxxxx                       00000000 00000yyy yyxxxxxx
**  1110zzzz  10yyyyyy  10xxxxxx             00000000 zzzzyyyy yyxxxxxx
**  11110uuu  10uuzzzz  10yyyyyy  10xxxxxx   000uuuuu zzzzyyyy yyxxxxxx
**
**
** Notes on UTF-16:  (with wwww+1==uuuuu)
**
**      Word-0               Word-1          Value
**  110110ww wwzzzzyy   110111yy yyxxxxxx    000uuuuu zzzzyyyy yyxxxxxx
**  zzzzyyyy yyxxxxxx                        00000000 zzzzyyyy yyxxxxxx
**
**
** BOM or Byte Order Mark:
**     0xff 0xfe   little-endian utf-16 follows
**     0xfe 0xff   big-endian utf-16 follows
**
*/
#include "sqliteInt.h"
#include <assert.h>
#include "vdbeInt.h"

#ifndef SQLITE4_AMALGAMATION
/*
** The following constant value is used by the SQLITE4_BIGENDIAN and
** SQLITE4_LITTLEENDIAN macros.
*/
const int sqlite4one = 1;
#endif /* SQLITE4_AMALGAMATION */

/*
** This lookup table is used to help decode the first byte of
** a multi-byte UTF8 character.
*/
static const unsigned char sqlite4Utf8Trans1[] = {
  0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
  0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
  0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
  0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
  0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
  0x00, 0x01, 0x02, 0x03, 0x00, 0x01, 0x00, 0x00,
};


#define WRITE_UTF8(zOut, c) {                          \
  if( c<0x00080 ){                                     \
    *zOut++ = (u8)(c&0xFF);                            \
  }                                                    \
  else if( c<0x00800 ){                                \
    *zOut++ = 0xC0 + (u8)((c>>6)&0x1F);                \
    *zOut++ = 0x80 + (u8)(c & 0x3F);                   \
  }                                                    \
  else if( c<0x10000 ){                                \
    *zOut++ = 0xE0 + (u8)((c>>12)&0x0F);               \
    *zOut++ = 0x80 + (u8)((c>>6) & 0x3F);              \
    *zOut++ = 0x80 + (u8)(c & 0x3F);                   \
  }else{                                               \
    *zOut++ = 0xF0 + (u8)((c>>18) & 0x07);             \
    *zOut++ = 0x80 + (u8)((c>>12) & 0x3F);             \
    *zOut++ = 0x80 + (u8)((c>>6) & 0x3F);              \
    *zOut++ = 0x80 + (u8)(c & 0x3F);                   \
  }                                                    \
}

#define WRITE_UTF16LE(zOut, c) {                                    \
  if( c<=0xFFFF ){                                                  \
    *zOut++ = (u8)(c&0x00FF);                                       \
    *zOut++ = (u8)((c>>8)&0x00FF);                                  \
  }else{                                                            \
    *zOut++ = (u8)(((c>>10)&0x003F) + (((c-0x10000)>>10)&0x00C0));  \
    *zOut++ = (u8)(0x00D8 + (((c-0x10000)>>18)&0x03));              \
    *zOut++ = (u8)(c&0x00FF);                                       \
    *zOut++ = (u8)(0x00DC + ((c>>8)&0x03));                         \
  }                                                                 \
}

#define WRITE_UTF16BE(zOut, c) {                                    \
  if( c<=0xFFFF ){                                                  \
    *zOut++ = (u8)((c>>8)&0x00FF);                                  \
    *zOut++ = (u8)(c&0x00FF);                                       \
  }else{                                                            \
    *zOut++ = (u8)(0x00D8 + (((c-0x10000)>>18)&0x03));              \
    *zOut++ = (u8)(((c>>10)&0x003F) + (((c-0x10000)>>10)&0x00C0));  \
    *zOut++ = (u8)(0x00DC + ((c>>8)&0x03));                         \
    *zOut++ = (u8)(c&0x00FF);                                       \
  }                                                                 \
}

#define READ_UTF16LE(zIn, TERM, c){                                   \
  c = (*zIn++);                                                       \
  c += ((*zIn++)<<8);                                                 \
  if( c>=0xD800 && c<0xE000 && TERM ){                                \
    int c2 = (*zIn++);                                                \
    c2 += ((*zIn++)<<8);                                              \
    c = (c2&0x03FF) + ((c&0x003F)<<10) + (((c&0x03C0)+0x0040)<<10);   \
  }                                                                   \
}

#define READ_UTF16BE(zIn, TERM, c){                                   \
  c = ((*zIn++)<<8);                                                  \
  c += (*zIn++);                                                      \
  if( c>=0xD800 && c<0xE000 && TERM ){                                \
    int c2 = ((*zIn++)<<8);                                           \
    c2 += (*zIn++);                                                   \
    c = (c2&0x03FF) + ((c&0x003F)<<10) + (((c&0x03C0)+0x0040)<<10);   \
  }                                                                   \
}

/*
** Translate a single UTF-8 character.  Return the unicode value.
**
** During translation, assume that the byte that zTerm points
** is a 0x00.
**
** Write a pointer to the next unread byte back into *pzNext.
**
** Notes On Invalid UTF-8:
**
**  *  This routine never allows a 7-bit character (0x00 through 0x7f) to
**     be encoded as a multi-byte character.  Any multi-byte character that
**     attempts to encode a value between 0x00 and 0x7f is rendered as 0xfffd.
**
**  *  This routine never allows a UTF16 surrogate value to be encoded.
**     If a multi-byte character attempts to encode a value between
**     0xd800 and 0xe000 then it is rendered as 0xfffd.
**
**  *  Bytes in the range of 0x80 through 0xbf which occur as the first
**     byte of a character are interpreted as single-byte characters
**     and rendered as themselves even though they are technically
**     invalid characters.
**
**  *  This routine accepts an infinite number of different UTF8 encodings
**     for unicode values 0x80 and greater.  It do not change over-length
**     encodings to 0xfffd as some systems recommend.
*/
#define READ_UTF8(zIn, zTerm, c)                           \
  c = *(zIn++);                                            \
  if( c>=0xc0 ){                                           \
    c = sqlite4Utf8Trans1[c-0xc0];                         \
    while( zIn!=zTerm && (*zIn & 0xc0)==0x80 ){            \
      c = (c<<6) + (0x3f & *(zIn++));                      \
    }                                                      \
    if( c<0x80                                             \
        || (c&0xFFFFF800)==0xD800                          \
        || (c&0xFFFFFFFE)==0xFFFE ){  c = 0xFFFD; }        \
  }
u32 sqlite4Utf8Read(
  const unsigned char *zIn,       /* First byte of UTF-8 character */
  const unsigned char **pzNext    /* Write first byte past UTF-8 char here */
){
  unsigned int c;

  /* Same as READ_UTF8() above but without the zTerm parameter.
  ** For this routine, we assume the UTF8 string is always zero-terminated.
  */
  c = *(zIn++);
  if( c>=0xc0 ){
    c = sqlite4Utf8Trans1[c-0xc0];
    while( (*zIn & 0xc0)==0x80 ){
      c = (c<<6) + (0x3f & *(zIn++));
    }
    if( c<0x80
        || (c&0xFFFFF800)==0xD800
        || (c&0xFFFFFFFE)==0xFFFE ){  c = 0xFFFD; }
  }
  *pzNext = zIn;
  return c;
}


/*
** Versions of stricmp and strnicmp that work with (simple) unicode
** case mapping.
*/
int sqlite4_stricmp(const char *zLeft, const char *zRight){
  unsigned char *a, *b;
  unsigned int ac, bc;
  int x;
  a = (unsigned char *)zLeft;
  b = (unsigned char *)zRight;
  do{
    READ_UTF8(a, 0, ac);
    ac = sqlite4_tolower(ac);
    READ_UTF8(b, 0, bc);
    bc = sqlite4_tolower(bc);
  }while( ac==bc && ac!=0 );
  return ac - bc;
}
int sqlite4_strnicmp(const char *zLeft, const char *zRight, int N){
  unsigned char *a, *b, *aTerm, *bTerm;
  unsigned int ac, bc;
  int x;
  a = (unsigned char *)zLeft;
  b = (unsigned char *)zRight;
  aTerm = a + N;
  bTerm = b + N;
  do{
    READ_UTF8(a, aTerm, ac);
    ac = sqlite4_tolower(ac);
    READ_UTF8(b, bTerm, bc);
    bc = sqlite4_tolower(bc);
  }while( ac==bc && ac!=0 && a<aTerm );
  return ac - bc;
}

/*
** If the TRANSLATE_TRACE macro is defined, the value of each Mem is
** printed on stderr on the way into and out of sqlite4VdbeMemTranslate().
*/ 
/* #define TRANSLATE_TRACE 1 */

#ifndef SQLITE4_OMIT_UTF16
/*
** This routine transforms the internal text encoding used by pMem to
** desiredEnc. It is an error if the string is already of the desired
** encoding, or if *pMem does not contain a string value.
*/
int sqlite4VdbeMemTranslate(Mem *pMem, u8 desiredEnc){
  int len;                    /* Maximum length of output string in bytes */
  unsigned char *zOut;                  /* Output buffer */
  unsigned char *zIn;                   /* Input iterator */
  unsigned char *zTerm;                 /* End of input */
  unsigned char *z;                     /* Output iterator */
  unsigned int c;

  assert( pMem->db==0 || sqlite4_mutex_held(pMem->db->mutex) );
  assert( pMem->flags&MEM_Str );
  assert( pMem->enc!=desiredEnc );
  assert( pMem->enc!=0 );
  assert( pMem->n>=0 );

#if defined(TRANSLATE_TRACE) && defined(SQLITE4_DEBUG)
  {
    char zBuf[100];
    sqlite4VdbeMemPrettyPrint(pMem, zBuf);
    fprintf(stderr, "INPUT:  %s\n", zBuf);
  }
#endif

  /* If the translation is between UTF-16 little and big endian, then 
  ** all that is required is to swap the byte order. This case is handled
  ** differently from the others.
  */
  if( pMem->enc!=SQLITE4_UTF8 && desiredEnc!=SQLITE4_UTF8 ){
    u8 temp;
    int rc;
    rc = sqlite4VdbeMemMakeWriteable(pMem);
    if( rc!=SQLITE4_OK ){
      assert( rc==SQLITE4_NOMEM );
      return SQLITE4_NOMEM;
    }
    zIn = (u8*)pMem->z;
    zTerm = &zIn[pMem->n&~1];
    while( zIn<zTerm ){
      temp = *zIn;
      *zIn = *(zIn+1);
      zIn++;
      *zIn++ = temp;
    }
    pMem->enc = desiredEnc;
    goto translate_out;
  }

  /* Set len to the maximum number of bytes required in the output buffer. */
  if( desiredEnc==SQLITE4_UTF8 ){
    /* When converting from UTF-16, the maximum growth results from
    ** translating a 2-byte character to a 4-byte UTF-8 character.
    ** A single byte is required for the output string
    ** nul-terminator.
    */
    pMem->n &= ~1;
    len = pMem->n * 2 + 1;
  }else{
    /* When converting from UTF-8 to UTF-16 the maximum growth is caused
    ** when a 1-byte UTF-8 character is translated into a 2-byte UTF-16
    ** character. Two bytes are required in the output buffer for the
    ** nul-terminator.
    */
    len = pMem->n * 2 + 2;
  }

  /* Set zIn to point at the start of the input buffer and zTerm to point 1
  ** byte past the end.
  **
  ** Variable zOut is set to point at the output buffer, space obtained
  ** from sqlite4_malloc().
  */
  zIn = (u8*)pMem->z;
  zTerm = &zIn[pMem->n];
  zOut = sqlite4DbMallocRaw(pMem->db, len);
  if( !zOut ){
    return SQLITE4_NOMEM;
  }
  z = zOut;

  if( pMem->enc==SQLITE4_UTF8 ){
    if( desiredEnc==SQLITE4_UTF16LE ){
      /* UTF-8 -> UTF-16 Little-endian */
      while( zIn<zTerm ){
        /* c = sqlite4Utf8Read(zIn, zTerm, (const u8**)&zIn); */
        READ_UTF8(zIn, zTerm, c);
        WRITE_UTF16LE(z, c);
      }
    }else{
      assert( desiredEnc==SQLITE4_UTF16BE );
      /* UTF-8 -> UTF-16 Big-endian */
      while( zIn<zTerm ){
        /* c = sqlite4Utf8Read(zIn, zTerm, (const u8**)&zIn); */
        READ_UTF8(zIn, zTerm, c);
        WRITE_UTF16BE(z, c);
      }
    }
    pMem->n = (int)(z - zOut);
    *z++ = 0;
  }else{
    assert( desiredEnc==SQLITE4_UTF8 );
    if( pMem->enc==SQLITE4_UTF16LE ){
      /* UTF-16 Little-endian -> UTF-8 */
      while( zIn<zTerm ){
        READ_UTF16LE(zIn, zIn<zTerm, c); 
        WRITE_UTF8(z, c);
      }
    }else{
      /* UTF-16 Big-endian -> UTF-8 */
      while( zIn<zTerm ){
        READ_UTF16BE(zIn, zIn<zTerm, c); 
        WRITE_UTF8(z, c);
      }
    }
    pMem->n = (int)(z - zOut);
  }
  *z = 0;
  assert( (pMem->n+(desiredEnc==SQLITE4_UTF8?1:2))<=len );

  sqlite4VdbeMemRelease(pMem);
  pMem->flags &= ~(MEM_Static|MEM_Dyn|MEM_Ephem);
  pMem->enc = desiredEnc;
  pMem->flags |= (MEM_Term|MEM_Dyn);
  pMem->z = (char*)zOut;
  pMem->zMalloc = pMem->z;

translate_out:
#if defined(TRANSLATE_TRACE) && defined(SQLITE4_DEBUG)
  {
    char zBuf[100];
    sqlite4VdbeMemPrettyPrint(pMem, zBuf);
    fprintf(stderr, "OUTPUT: %s\n", zBuf);
  }
#endif
  return SQLITE4_OK;
}

/*
** This routine checks for a byte-order mark at the beginning of the 
** UTF-16 string stored in *pMem. If one is present, it is removed and
** the encoding of the Mem adjusted. This routine does not do any
** byte-swapping, it just sets Mem.enc appropriately.
**
** The allocation (static, dynamic etc.) and encoding of the Mem may be
** changed by this function.
*/
int sqlite4VdbeMemHandleBom(Mem *pMem){
  int rc = SQLITE4_OK;
  u8 bom = 0;

  assert( pMem->n>=0 );
  if( pMem->n>1 ){
    u8 b1 = *(u8 *)pMem->z;
    u8 b2 = *(((u8 *)pMem->z) + 1);
    if( b1==0xFE && b2==0xFF ){
      bom = SQLITE4_UTF16BE;
    }
    if( b1==0xFF && b2==0xFE ){
      bom = SQLITE4_UTF16LE;
    }
  }
  
  if( bom ){
    rc = sqlite4VdbeMemMakeWriteable(pMem);
    if( rc==SQLITE4_OK ){
      pMem->n -= 2;
      memmove(pMem->z, &pMem->z[2], pMem->n);
      pMem->z[pMem->n] = '\0';
      pMem->z[pMem->n+1] = '\0';
      pMem->flags |= MEM_Term;
      pMem->enc = bom;
    }
  }
  return rc;
}
#endif /* SQLITE4_OMIT_UTF16 */

/*
** pZ is a UTF-8 encoded unicode string. If nByte is less than zero,
** return the number of unicode characters in pZ up to (but not including)
** the first 0x00 byte. If nByte is not less than zero, return the
** number of unicode characters in the first nByte of pZ (or up to 
** the first 0x00, whichever comes first).
*/
int sqlite4Utf8CharLen(const char *zIn, int nByte){
  int r = 0;
  const u8 *z = (const u8*)zIn;
  const u8 *zTerm;
  if( nByte>=0 ){
    zTerm = &z[nByte];
  }else{
    zTerm = (const u8*)(-1);
  }
  assert( z<=zTerm );
  while( *z!=0 && z<zTerm ){
    SQLITE4_SKIP_UTF8(z);
    r++;
  }
  return r;
}

/* This test function is not currently used by the automated test-suite. 
** Hence it is only available in debug builds.
*/
#if defined(SQLITE4_TEST) && defined(SQLITE4_DEBUG)
/*
** Translate UTF-8 to UTF-8.
**
** This has the effect of making sure that the string is well-formed
** UTF-8.  Miscoded characters are removed.
**
** The translation is done in-place and aborted if the output
** overruns the input.
*/
int sqlite4Utf8To8(unsigned char *zIn){
  unsigned char *zOut = zIn;
  unsigned char *zStart = zIn;
  u32 c;

  while( zIn[0] && zOut<=zIn ){
    c = sqlite4Utf8Read(zIn, (const u8**)&zIn);
    if( c!=0xfffd ){
      WRITE_UTF8(zOut, c);
    }
  }
  *zOut = 0;
  return (int)(zOut - zStart);
}
#endif

#ifndef SQLITE4_OMIT_UTF16
/*
** Convert a UTF-16 string in the native encoding into a UTF-8 string.
** Memory to hold the UTF-8 string is obtained from sqlite4_malloc and must
** be freed by the calling function.
**
** NULL is returned if there is an allocation error.
*/
char *sqlite4Utf16to8(sqlite4 *db, const void *z, int nByte, u8 enc){
  Mem m;
  memset(&m, 0, sizeof(m));
  m.db = db;
  sqlite4VdbeMemSetStr(&m, z, nByte, enc, SQLITE4_STATIC);
  sqlite4VdbeChangeEncoding(&m, SQLITE4_UTF8);
  if( db->mallocFailed ){
    sqlite4VdbeMemRelease(&m);
    m.z = 0;
  }
  assert( (m.flags & MEM_Term)!=0 || db->mallocFailed );
  assert( (m.flags & MEM_Str)!=0 || db->mallocFailed );
  assert( (m.flags & MEM_Dyn)!=0 || db->mallocFailed );
  assert( m.z || db->mallocFailed );
  return m.z;
}

/*
** Convert a UTF-8 string to the UTF-16 encoding specified by parameter
** enc. A pointer to the new string is returned, and the value of *pnOut
** is set to the length of the returned string in bytes. The call should
** arrange to call sqlite4DbFree() on the returned pointer when it is
** no longer required.
** 
** If a malloc failure occurs, NULL is returned and the db.mallocFailed
** flag set.
*/
#ifdef SQLITE4_ENABLE_STAT3
char *sqlite4Utf8to16(sqlite4 *db, u8 enc, char *z, int n, int *pnOut){
  Mem m;
  memset(&m, 0, sizeof(m));
  m.db = db;
  sqlite4VdbeMemSetStr(&m, z, n, SQLITE4_UTF8, SQLITE4_STATIC);
  if( sqlite4VdbeMemTranslate(&m, enc) ){
    assert( db->mallocFailed );
    return 0;
  }
  assert( m.z==m.zMalloc );
  *pnOut = m.n;
  return m.z;
}
#endif

/*
** zIn is a UTF-16 encoded unicode string at least nChar characters long.
** Return the number of bytes in the first nChar unicode characters
** in pZ.  nChar must be non-negative.
*/
int sqlite4Utf16ByteLen(const void *zIn, int nChar){
  int c;
  unsigned char const *z = zIn;
  int n = 0;
  
  if( SQLITE4_UTF16NATIVE==SQLITE4_UTF16BE ){
    while( n<nChar ){
      READ_UTF16BE(z, 1, c);
      n++;
    }
  }else{
    while( n<nChar ){
      READ_UTF16LE(z, 1, c);
      n++;
    }
  }
  return (int)(z-(unsigned char const *)zIn);
}

#if defined(SQLITE4_TEST)
/*
** This routine is called from the TCL test function "translate_selftest".
** It checks that the primitives for serializing and deserializing
** characters in each encoding are inverses of each other.
*/
void sqlite4UtfSelfTest(void){
  unsigned int i, t;
  unsigned char zBuf[20];
  unsigned char *z;
  int n;
  unsigned int c;

  for(i=0; i<0x00110000; i++){
    z = zBuf;
    WRITE_UTF8(z, i);
    n = (int)(z-zBuf);
    assert( n>0 && n<=4 );
    z[0] = 0;
    z = zBuf;
    c = sqlite4Utf8Read(z, (const u8**)&z);
    t = i;
    if( i>=0xD800 && i<=0xDFFF ) t = 0xFFFD;
    if( (i&0xFFFFFFFE)==0xFFFE ) t = 0xFFFD;
    assert( c==t );
    assert( (z-zBuf)==n );
  }
  for(i=0; i<0x00110000; i++){
    if( i>=0xD800 && i<0xE000 ) continue;
    z = zBuf;
    WRITE_UTF16LE(z, i);
    n = (int)(z-zBuf);
    assert( n>0 && n<=4 );
    z[0] = 0;
    z = zBuf;
    READ_UTF16LE(z, 1, c);
    assert( c==i );
    assert( (z-zBuf)==n );
  }
  for(i=0; i<0x00110000; i++){
    if( i>=0xD800 && i<0xE000 ) continue;
    z = zBuf;
    WRITE_UTF16BE(z, i);
    n = (int)(z-zBuf);
    assert( n>0 && n<=4 );
    z[0] = 0;
    z = zBuf;
    READ_UTF16BE(z, 1, c);
    assert( c==i );
    assert( (z-zBuf)==n );
  }
}
#endif /* SQLITE4_TEST */
#endif /* SQLITE4_OMIT_UTF16 */


/*
** Return true if the argument corresponds to a unicode codepoint
** classified as either a letter or a number. Otherwise false.
**
** The results are undefined if the value passed to this function
** is less than zero.
*/
int sqlite4_isalnum(int c){
  /* Each unsigned integer in the following array corresponds to a contiguous
  ** range of unicode codepoints that are not either letters or numbers (i.e.
  ** codepoints for which this function should return 0).
  **
  ** The most significant 22 bits in each 32-bit value contain the first 
  ** codepoint in the range. The least significant 10 bits are used to store
  ** the size of the range (always at least 1). In other words, the value 
  ** ((C<<22) + N) represents a range of N codepoints starting with codepoint 
  ** C. It is not possible to represent a range larger than 1023 codepoints 
  ** using this format.
  */
  const static unsigned int aEntry[] = {
    0x00000030, 0x0000E807, 0x00016C06, 0x0001EC2F, 0x0002AC07,
    0x0002D001, 0x0002D803, 0x0002EC01, 0x0002FC01, 0x00035C01,
    0x0003DC01, 0x000B0804, 0x000B480E, 0x000B9407, 0x000BB401,
    0x000BBC81, 0x000DD401, 0x000DF801, 0x000E1002, 0x000E1C01,
    0x000FD801, 0x00120808, 0x00156806, 0x00162402, 0x00163C01,
    0x00164437, 0x0017CC02, 0x00180005, 0x00181816, 0x00187802,
    0x00192C15, 0x0019A804, 0x0019C001, 0x001B5001, 0x001B580F,
    0x001B9C07, 0x001BF402, 0x001C000E, 0x001C3C01, 0x001C4401,
    0x001CC01B, 0x001E980B, 0x001FAC09, 0x001FD804, 0x00205804,
    0x00206C09, 0x00209403, 0x0020A405, 0x0020C00F, 0x00216403,
    0x00217801, 0x0023901B, 0x00240004, 0x0024E803, 0x0024F812,
    0x00254407, 0x00258804, 0x0025C001, 0x00260403, 0x0026F001,
    0x0026F807, 0x00271C02, 0x00272C03, 0x00275C01, 0x00278802,
    0x0027C802, 0x0027E802, 0x00280403, 0x0028F001, 0x0028F805,
    0x00291C02, 0x00292C03, 0x00294401, 0x0029C002, 0x0029D401,
    0x002A0403, 0x002AF001, 0x002AF808, 0x002B1C03, 0x002B2C03,
    0x002B8802, 0x002BC002, 0x002C0403, 0x002CF001, 0x002CF807,
    0x002D1C02, 0x002D2C03, 0x002D5802, 0x002D8802, 0x002DC001,
    0x002E0801, 0x002EF805, 0x002F1803, 0x002F2804, 0x002F5C01,
    0x002FCC08, 0x00300403, 0x0030F807, 0x00311803, 0x00312804,
    0x00315402, 0x00318802, 0x0031FC01, 0x00320802, 0x0032F001,
    0x0032F807, 0x00331803, 0x00332804, 0x00335402, 0x00338802,
    0x00340802, 0x0034F807, 0x00351803, 0x00352804, 0x00355C01,
    0x00358802, 0x0035E401, 0x00360802, 0x00372801, 0x00373C06,
    0x00375801, 0x00376008, 0x0037C803, 0x0038C401, 0x0038D007,
    0x0038FC01, 0x00391C09, 0x00396802, 0x003AC401, 0x003AD006,
    0x003AEC02, 0x003B2006, 0x003C041F, 0x003CD00C, 0x003DC417,
    0x003E340B, 0x003E6424, 0x003EF80F, 0x003F380D, 0x0040AC14,
    0x00412806, 0x00415804, 0x00417803, 0x00418803, 0x00419C07,
    0x0041C404, 0x0042080C, 0x00423C01, 0x00426806, 0x0043EC01,
    0x004D740C, 0x004E400A, 0x00500001, 0x0059B402, 0x005A0001,
    0x005A6C02, 0x005BAC03, 0x005C4803, 0x005CC805, 0x005D4802,
    0x005DC802, 0x005ED023, 0x005F6004, 0x005F7401, 0x0060000F,
    0x0062A401, 0x0064800C, 0x0064C00C, 0x00650001, 0x00651002,
    0x0066C011, 0x00672002, 0x00677822, 0x00685C05, 0x00687802,
    0x0069540A, 0x0069801D, 0x0069FC01, 0x006A8007, 0x006AA006,
    0x006C0005, 0x006CD011, 0x006D6823, 0x006E0003, 0x006E840D,
    0x006F980E, 0x006FF004, 0x00709014, 0x0070EC05, 0x0071F802,
    0x00730008, 0x00734019, 0x0073B401, 0x0073C803, 0x00770027,
    0x0077F004, 0x007EF401, 0x007EFC03, 0x007F3403, 0x007F7403,
    0x007FB403, 0x007FF402, 0x00800065, 0x0081A806, 0x0081E805,
    0x00822805, 0x0082801A, 0x00834021, 0x00840002, 0x00840C04,
    0x00842002, 0x00845001, 0x00845803, 0x00847806, 0x00849401,
    0x00849C01, 0x0084A401, 0x0084B801, 0x0084E802, 0x00850005,
    0x00852804, 0x00853C01, 0x00864264, 0x00900027, 0x0091000B,
    0x0092704E, 0x00940200, 0x009C0475, 0x009E53B9, 0x00AD400A,
    0x00B39406, 0x00B3BC03, 0x00B3E404, 0x00B3F802, 0x00B5C001,
    0x00B5FC01, 0x00B7804F, 0x00B8C00C, 0x00BA001A, 0x00BA6C59,
    0x00BC00D6, 0x00BFC00C, 0x00C00005, 0x00C02019, 0x00C0A807,
    0x00C0D802, 0x00C0F403, 0x00C26404, 0x00C28001, 0x00C3EC01,
    0x00C64002, 0x00C6580A, 0x00C70024, 0x00C8001F, 0x00C8A81E,
    0x00C94001, 0x00C98020, 0x00CA2827, 0x00CB003F, 0x00CC0100,
    0x01370040, 0x02924037, 0x0293F802, 0x02983403, 0x0299BC10,
    0x029A7C01, 0x029BC008, 0x029C0017, 0x029C8002, 0x029E2402,
    0x02A00801, 0x02A01801, 0x02A02C01, 0x02A08C09, 0x02A0D804,
    0x02A1D004, 0x02A20002, 0x02A2D011, 0x02A33802, 0x02A38012,
    0x02A3E003, 0x02A4980A, 0x02A51C0D, 0x02A57C01, 0x02A60004,
    0x02A6CC1B, 0x02A77802, 0x02A8A40E, 0x02A90C01, 0x02A93002,
    0x02A97004, 0x02A9DC03, 0x02A9EC01, 0x02AAC001, 0x02AAC803,
    0x02AADC02, 0x02AAF802, 0x02AB0401, 0x02AB7802, 0x02ABAC07,
    0x02ABD402, 0x02AF8C0B, 0x03600001, 0x036DFC02, 0x036FFC02,
    0x037FFC02, 0x03E3FC01, 0x03EC7801, 0x03ECA401, 0x03EEC810,
    0x03F4F802, 0x03F7F002, 0x03F8001A, 0x03F88007, 0x03F8C023,
    0x03F95013, 0x03F9A004, 0x03FBFC01, 0x03FC040F, 0x03FC6807,
    0x03FCEC06, 0x03FD6C0B, 0x03FF8007, 0x03FFA007, 0x03FFE405,
    0x04040003, 0x0404DC09, 0x0405E411, 0x0406400C, 0x0407402E,
    0x040E7C01, 0x040F4001, 0x04215C01, 0x04247C01, 0x0424FC01,
    0x04280403, 0x04281402, 0x04283004, 0x0428E003, 0x0428FC01,
    0x04294009, 0x0429FC01, 0x042CE407, 0x04400003, 0x0440E016,
    0x04420003, 0x0442C012, 0x04440003, 0x04449C0E, 0x04450004,
    0x04460003, 0x0446CC0E, 0x04471404, 0x045AAC0D, 0x0491C004,
    0x05BD442E, 0x05BE3C04, 0x074000F6, 0x07440027, 0x0744A4B5,
    0x07480046, 0x074C0057, 0x075B0401, 0x075B6C01, 0x075BEC01,
    0x075C5401, 0x075CD401, 0x075D3C01, 0x075DBC01, 0x075E2401,
    0x075EA401, 0x075F0C01, 0x07BBC002, 0x07C0002C, 0x07C0C064,
    0x07C2800F, 0x07C2C40E, 0x07C3040F, 0x07C3440F, 0x07C4401F,
    0x07C4C03C, 0x07C5C02B, 0x07C7981D, 0x07C8402B, 0x07C90009,
    0x07C94002, 0x07CC0021, 0x07CCC006, 0x07CCDC46, 0x07CE0014,
    0x07CE8025, 0x07CF1805, 0x07CF8011, 0x07D0003F, 0x07D10001,
    0x07D108B6, 0x07D3E404, 0x07D4003E, 0x07D50004, 0x07D54018,
    0x07D7EC46, 0x07D9140B, 0x07DA0046, 0x07DC0074, 0x38000401,
    0x38008060, 0x380400F0, 0x3C000001, 0x3FFFF401, 0x40000001,
    0x43FFF401,
  };
  static const unsigned int aAscii[4] = {
    0xFFFFFFFF, 0xFC00FFFF, 0xF8000001, 0xF8000001,
  };

  if( c<128 ){
    return ( (aAscii[c >> 5] & (1 << (c & 0x001F)))==0 );
  }else if( c<(1<<22) ){
    unsigned int key = (((unsigned int)c)<<10) | 0x000003FF;
    int iRes;
    int iHi = sizeof(aEntry)/sizeof(aEntry[0]) - 1;
    int iLo = 0;
    while( iHi>=iLo ){
      int iTest = (iHi + iLo) / 2;
      if( key >= aEntry[iTest] ){
        iRes = iTest;
        iLo = iTest+1;
      }else{
        iHi = iTest-1;
      }
    }
    assert( aEntry[0]<key );
    assert( key>=aEntry[iRes] );
    return (((unsigned int)c) >= ((aEntry[iRes]>>10) + (aEntry[iRes]&0x3FF)));
  }
  return 1;
}

/*
** Interpret the argument as a unicode codepoint. If the codepoint
** is an upper case character that has a lower case equivalent,
** return the codepoint corresponding to the lower case version.
** Otherwise, return a copy of the argument.
**
** The results are undefined if the value passed to this function
** is less than zero.
*/
int sqlite4_tolower(int c){
  /* Each entry in the following array defines a rule for folding a range
  ** of codepoints to lower case. The rule applies to a range of nRange
  ** codepoints starting at codepoint iCode.
  **
  ** If the least significant bit in flags is clear, then the rule applies
  ** to all nRange codepoints (i.e. all nRange codepoints are upper case and
  ** need to be folded). Or, if it is set, then the rule only applies to
  ** every second codepoint in the range, starting with codepoint C.
  **
  ** The 7 most significant bits in flags are an index into the aiOff[]
  ** array. If a specific codepoint C does require folding, then its lower
  ** case equivalent is ((C + aiOff[flags>>1]) & 0xFFFF).
  **
  ** The contents of this array are generated by parsing the CaseFolding.txt
  ** file distributed as part of the "Unicode Character Database". See
  ** http://www.unicode.org for details.
  */
  static const struct TableEntry {
    unsigned short iCode;
    unsigned char flags;
    unsigned char nRange;
  } aEntry[] = {
    {65, 14, 26},          {181, 64, 1},          {192, 14, 23},
    {216, 14, 7},          {256, 1, 48},          {306, 1, 6},
    {313, 1, 16},          {330, 1, 46},          {376, 116, 1},
    {377, 1, 6},           {383, 104, 1},         {385, 50, 1},
    {386, 1, 4},           {390, 44, 1},          {391, 0, 1},
    {393, 42, 2},          {395, 0, 1},           {398, 32, 1},
    {399, 38, 1},          {400, 40, 1},          {401, 0, 1},
    {403, 42, 1},          {404, 46, 1},          {406, 52, 1},
    {407, 48, 1},          {408, 0, 1},           {412, 52, 1},
    {413, 54, 1},          {415, 56, 1},          {416, 1, 6},
    {422, 60, 1},          {423, 0, 1},           {425, 60, 1},
    {428, 0, 1},           {430, 60, 1},          {431, 0, 1},
    {433, 58, 2},          {435, 1, 4},           {439, 62, 1},
    {440, 0, 1},           {444, 0, 1},           {452, 2, 1},
    {453, 0, 1},           {455, 2, 1},           {456, 0, 1},
    {458, 2, 1},           {459, 1, 18},          {478, 1, 18},
    {497, 2, 1},           {498, 1, 4},           {502, 122, 1},
    {503, 134, 1},         {504, 1, 40},          {544, 110, 1},
    {546, 1, 18},          {570, 70, 1},          {571, 0, 1},
    {573, 108, 1},         {574, 68, 1},          {577, 0, 1},
    {579, 106, 1},         {580, 28, 1},          {581, 30, 1},
    {582, 1, 10},          {837, 36, 1},          {880, 1, 4},
    {886, 0, 1},           {902, 18, 1},          {904, 16, 3},
    {908, 26, 1},          {910, 24, 2},          {913, 14, 17},
    {931, 14, 9},          {962, 0, 1},           {975, 4, 1},
    {976, 140, 1},         {977, 142, 1},         {981, 146, 1},
    {982, 144, 1},         {984, 1, 24},          {1008, 136, 1},
    {1009, 138, 1},        {1012, 130, 1},        {1013, 128, 1},
    {1015, 0, 1},          {1017, 152, 1},        {1018, 0, 1},
    {1021, 110, 3},        {1024, 34, 16},        {1040, 14, 32},
    {1120, 1, 34},         {1162, 1, 54},         {1216, 6, 1},
    {1217, 1, 14},         {1232, 1, 88},         {1329, 22, 38},
    {4256, 66, 38},        {4295, 66, 1},         {4301, 66, 1},
    {7680, 1, 150},        {7835, 132, 1},        {7838, 96, 1},
    {7840, 1, 96},         {7944, 150, 8},        {7960, 150, 6},
    {7976, 150, 8},        {7992, 150, 8},        {8008, 150, 6},
    {8025, 151, 8},        {8040, 150, 8},        {8072, 150, 8},
    {8088, 150, 8},        {8104, 150, 8},        {8120, 150, 2},
    {8122, 126, 2},        {8124, 148, 1},        {8126, 100, 1},
    {8136, 124, 4},        {8140, 148, 1},        {8152, 150, 2},
    {8154, 120, 2},        {8168, 150, 2},        {8170, 118, 2},
    {8172, 152, 1},        {8184, 112, 2},        {8186, 114, 2},
    {8188, 148, 1},        {8486, 98, 1},         {8490, 92, 1},
    {8491, 94, 1},         {8498, 12, 1},         {8544, 8, 16},
    {8579, 0, 1},          {9398, 10, 26},        {11264, 22, 47},
    {11360, 0, 1},         {11362, 88, 1},        {11363, 102, 1},
    {11364, 90, 1},        {11367, 1, 6},         {11373, 84, 1},
    {11374, 86, 1},        {11375, 80, 1},        {11376, 82, 1},
    {11378, 0, 1},         {11381, 0, 1},         {11390, 78, 2},
    {11392, 1, 100},       {11499, 1, 4},         {11506, 0, 1},
    {42560, 1, 46},        {42624, 1, 24},        {42786, 1, 14},
    {42802, 1, 62},        {42873, 1, 4},         {42877, 76, 1},
    {42878, 1, 10},        {42891, 0, 1},         {42893, 74, 1},
    {42896, 1, 4},         {42912, 1, 10},        {42922, 72, 1},
    {65313, 14, 26},       
  };
  static const unsigned short aiOff[] = {
   1,     2,     8,     15,    16,    26,    28,    32,    
   37,    38,    40,    48,    63,    64,    69,    71,    
   79,    80,    116,   202,   203,   205,   206,   207,   
   209,   210,   211,   213,   214,   217,   218,   219,   
   775,   7264,  10792, 10795, 23228, 23256, 30204, 54721, 
   54753, 54754, 54756, 54787, 54793, 54809, 57153, 57274, 
   57921, 58019, 58363, 61722, 65268, 65341, 65373, 65406, 
   65408, 65410, 65415, 65424, 65436, 65439, 65450, 65462, 
   65472, 65476, 65478, 65480, 65482, 65488, 65506, 65511, 
   65514, 65521, 65527, 65528, 65529, 
  };

  int ret = c;

  assert( c>=0 );
  assert( sizeof(unsigned short)==2 && sizeof(unsigned char)==1 );

  if( c<128 ){
    if( c>='A' && c<='Z' ) ret = c + ('a' - 'A');
  }else if( c<65536 ){
    int iHi = sizeof(aEntry)/sizeof(aEntry[0]) - 1;
    int iLo = 0;
    int iRes = -1;

    while( iHi>=iLo ){
      int iTest = (iHi + iLo) / 2;
      int cmp = (c - aEntry[iTest].iCode);
      if( cmp>=0 ){
        iRes = iTest;
        iLo = iTest+1;
      }else{
        iHi = iTest-1;
      }
    }
    assert( iRes<0 || c>=aEntry[iRes].iCode );

    if( iRes>=0 ){
      const struct TableEntry *p = &aEntry[iRes];
      if( c<(p->iCode + p->nRange) && 0==(0x01 & p->flags & (p->iCode ^ c)) ){
        ret = (c + (aiOff[p->flags>>1])) & 0x0000FFFF;
        assert( ret>0 );
      }
    }
  }
  
  else if( c>=66560 && c<66600 ){
    ret = c + 40;
  }

  return ret;
}
