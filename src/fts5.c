/*
** 2012 December 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
*/

#include "sqliteInt.h"
#include "vdbeInt.h"

/* 
** Stream numbers must be lower than this.
**
** For optimization purposes, it is assumed that a given tokenizer uses
** a set of contiguous stream numbers starting with 0. And that most
** tokens belong to stream 0.
**
** The hard limit is 63 (due to the format of "row size" records).
*/
#define SQLITE4_FTS5_NSTREAM 32

/*
** Records stored within the index:
**
** Row size record:
**   There is one "row size" record in the index for each row in the
**   indexed table. The "row size" record contains the number of tokens
**   in the associated row for each combination of a stream and column
**   number (i.e. contains the data required to find the number of
**   tokens associated with stream S present in column C of the row for
**   all S and C).
**
**   The key for the row size record is a single 0x00 byte followed by
**   a copy of the PK blob for the table row. 
**
**   The value is a series of varints. Each column of the table is
**   represented by one or more varints packed into the array.
**
**   If a column contains only stream 0 tokens, then it is represented
**   by a single varint - (nToken << 1), where nToken is the number of
**   stream 0 tokens stored in the column.
**
**   Or, if the column contains tokens from multiple streams, the first
**   varint contains a bitmask indicating which of the streams are present
**   (stored as ((bitmask << 1) | 0x01)). Following the bitmask is a
**   varint containing the number of tokens for each stream present, in
**   ascending order of stream number.
**
** Global size record:
**   There is a single "global size" record stored in the database. The
**   database key for this record is a single byte - 0x00.
**
**   The data for this record is a series of varint values. The first 
**   varint is the total number of rows in the table. The subsequent
**   varints make up a "row size" record containing the total number of
**   tokens for each S/C combination in all rows of the table.
**
** FTS index records:
**   The FTS index records implement the following mapping:
**
**       (token, document-pk) -> (list of instances)
**
**   The key for each index record is in the same format as the keys for
**   regular text indexes. An 0x24 byte, followed by the utf-8 representation
**   of the token, followed by 0x00, followed by the PK blob for the table
**   row.
**
**   TODO: Describe value format.
*/

/*
** Default distance value for NEAR operators.
*/
#define FTS5_DEFAULT_NEAR 10

/*
** Token types used by expression parser.
*/
#define TOKEN_EOF       0         /* end of expression - no more tokens */
#define TOKEN_PRIMITIVE 1         /* quoted string or non-keyword */
#define TOKEN_STAR      2         /* * */
#define TOKEN_PLUS      3         /* + */
#define TOKEN_NEAR      4         /* NEAR/nnn */
#define TOKEN_COLON     5         /* : */
#define TOKEN_NOT       6         /* NOT */
#define TOKEN_AND       7         /* AND */
#define TOKEN_OR        8         /* OR */
#define TOKEN_LP        9         /* ( */
#define TOKEN_RP       10         /* ) */

/*
** Each tokenizer registered with a database handle is stored as an object
** of the following type. All objects associated with a single database
** connection are stored in the singly-linked list starting at 
** sqlite4.pTokenizer and connected by Fts5Tokenizer.pNext.
*/
struct Fts5Tokenizer {
  char *zName;                   /* Name of tokenizer (nul-terminated) */
  void *pCtx;
  int (*xCreate)(void*, const char**, int, sqlite4_tokenizer**);
  int (*xTokenize)(void*, sqlite4_tokenizer*,
      const char*, int, int(*x)(void*, int, int, const char*, int, int, int)
  );
  int (*xDestroy)(sqlite4_tokenizer *);
  Fts5Tokenizer *pNext;
};

/*
** FTS5 specific index data.
**
** This object is part of a database schema, so it may be shared between
** multiple connections.
*/
struct Fts5Index {
  int nTokenizer;                 /* Elements in azTokenizer[] array */
  char **azTokenizer;             /* Name and arguments for tokenizer */
};

/*
** Expression grammar:
**
**   phrase := PRIMITIVE
**   phrase := PRIMITIVE *
**   phrase := phrase + phrase
**   phrase := phrase NEAR phrase
**
**   expr := phrase
**   expr := PRIMITIVE COLON phrase
**
**   expr := expr NOT expr
**   expr := expr AND expr
**   expr := expr OR  expr
**   expr := LP expr RP
*/

/*
** Structure types used by this module.
*/
typedef struct Fts5Expr Fts5Expr;
typedef struct Fts5ExprNode Fts5ExprNode;
typedef struct Fts5List Fts5List;
typedef struct Fts5MatchIter Fts5MatchIter;
typedef struct Fts5Parser Fts5Parser;
typedef struct Fts5ParserToken Fts5ParserToken;
typedef struct Fts5Phrase Fts5Phrase;
typedef struct Fts5Prefix Fts5Prefix;
typedef struct Fts5Size Fts5Size;
typedef struct Fts5Str Fts5Str;
typedef struct Fts5Token Fts5Token;


struct Fts5ParserToken {
  int eType;                      /* Token type */
  int n;                          /* Size of z[] in bytes */
  const char *z;                  /* Token value */
};

struct Fts5Parser {
  Fts5Tokenizer *pTokenizer;
  sqlite4_tokenizer *p;
  sqlite4 *db;                    /* Database handle */

  char *zErr;                     /* Error message (or NULL) */

  const char *zExpr;              /* Pointer to expression text (nul-term) */
  int iExpr;                      /* Current offset in zExpr */
  Fts5ParserToken next;           /* Next token */

  char **azCol;                   /* Column names of indexed table */
  int nCol;                       /* Size of azCol[] in bytes */
  int iRoot;                      /* Root page number of FTS index */

  /* Space for dequoted copies of strings */
  char *aSpace;
  int iSpace;
  int nSpace;                     /* Total size of aSpace in bytes */
};

struct Fts5List {
  u8 *aData;
  int nData;
};

struct Fts5Prefix {
  u8 *aPk;                        /* Buffer containing PK */
  int nPk;                        /* Size of PK in bytes */
  Fts5Prefix *pNext;              /* Next entry in query-time list */
  u8 *aList;
  int nList;
  int nAlloc;
};

struct Fts5Token {
  /* TODO: The first three members are redundant in some senses, since the
  ** same information is encoded in the aPrefix[]/nPrefix key. */
  int bPrefix;                    /* True for a prefix search */
  int n;                          /* Size of z[] in bytes */
  char *z;                        /* Token value */

  KVByteArray *aPrefix;           /* KV prefix to iterate through */
  KVSize nPrefix;                 /* Size of aPrefix in bytes */
  KVCursor *pCsr;                 /* Cursor to iterate thru entries for token */
  Fts5Prefix *pPrefix;            /* Head of prefix list */
};

struct Fts5Str {
  Fts5Token *aToken;
  int nToken;
  u8 *aList;
  int nList;
  int nListAlloc;
};

struct Fts5Phrase {
  int iCol;                       /* Column of table to search (-1 -> all) */
  int nStr;
  Fts5Str *aStr;
  int *aiNear;
};

struct Fts5ExprNode {
  int eType;
  Fts5Phrase *pPhrase;
  Fts5ExprNode *pLeft;
  Fts5ExprNode *pRight;
  const u8 *aPk;                  /* Primary key of current entry (or null) */
  int nPk;                        /* Size of aPk[] in bytes */
};

struct Fts5Expr {
  Fts5ExprNode *pRoot;            /* Root node of expression */
  int nPhrase;                    /* Number of Fts5Str objects in query */
  Fts5Str **apPhrase;             /* All Fts5Str objects */
};

/*
** FTS5 specific cursor data.
*/
struct Fts5Cursor {
  sqlite4 *db;
  Fts5Info *pInfo;
  Fts5Expr *pExpr;                /* MATCH expression for this cursor */
  char *zExpr;                    /* Full text of MATCH expression */
  KVByteArray *aKey;              /* Buffer for primary key */
  int nKeyAlloc;                  /* Bytes allocated at aKey[] */

  KVCursor *pCsr;                 /* Cursor used to retrive values */
  Mem *aMem;                      /* Array of column values */
  int bMemValid;                  /* True if contents of aMem[] are valid */

  Fts5Size *pSz;                  /* Local size data */
  Fts5Size *pGlobal;              /* Global size data */
  i64 nGlobal;                    /* Total number of rows in table */
  int *anRow;

  Fts5MatchIter *pIter;           /* Used by mi_match_detail() */
};

/*
** A deserialized 'size record' (see above).
*/
struct Fts5Size {
  int nCol;                       /* Number of columns in indexed table */
  int nStream;                    /* Number of streams */
  i64 *aSz;                       /* Token count for each C/S */
};

/*
** This type is used when reading (decoding) an instance-list.
*/
typedef struct InstanceList InstanceList;
struct InstanceList {
  u8 *aList;
  int nList;
  int iList;

  /* The current entry */
  int iCol;
  int iStream;
  int iOff;
};

/*
** An instance of this structure is used by the sqlite4_mi_match_detail()
** API to iterate through matches. 
*/
struct Fts5MatchIter {
  int bValid;                     /* True if aList[] is current row */
  int iCurrent;                   /* Current index in aList[] (or -1) */
  int iMatch;                     /* Current iMatch value */
  InstanceList *aList;            /* One iterator for each phrase in expr */
};

/*
** Return true for EOF, or false if the next entry is valid.
*/
static int fts5InstanceListNext(InstanceList *p){
  int i = p->iList;
  int bRet = 1;
  
  while( bRet && i<p->nList ){
    u32 iVal;
    i += getVarint32(&p->aList[i], iVal);
    if( (iVal & 0x03)==0x01 ){
      p->iCol = (iVal>>2);
      p->iOff = 0;
    }
    else if( (iVal & 0x03)==0x03 ){
      p->iStream = (iVal>>2);
    }
    else{
      p->iOff += (iVal>>1);
      bRet = 0;
    }
  }
  if( bRet ){
    p->aList = 0;
  }

  p->iList = i;
  return bRet;
}

static int fts5InstanceListEof(InstanceList *p){
  return (p->aList==0);
}

static void fts5InstanceListAppend(
  InstanceList *p,                /* Instance list to append to */
  int iCol,                       /* Column of new entry */
  int iStream,                    /* Weight of new entry */
  int iOff                        /* Offset of new entry */
){
  assert( iCol>=p->iCol );
  assert( iCol>p->iCol || iOff>=p->iOff );

  if( iCol!=p->iCol ){
    p->iList += putVarint32(&p->aList[p->iList], (iCol<<2)|0x01);
    p->iCol = iCol;
    p->iOff = 0;
  }

  if( iStream!=p->iStream ){
    p->iList += putVarint32(&p->aList[p->iList], (iStream<<2)|0x03);
    p->iStream = iStream;
  }

  p->iList += putVarint32(&p->aList[p->iList], (iOff-p->iOff)<<1);
  p->iOff = iOff;

  assert( p->iList<=p->nList );
}

static void fts5InstanceListInit(u8 *aList, int nList, InstanceList *p){
  memset(p, 0, sizeof(InstanceList));
  p->aList = aList;
  p->nList = nList;
}

/*
** Return true if argument c is one of the special non-whitespace 
** characters that ends an unquoted expression token. 
*/
static int fts5IsSpecial(char c){
  return (c==':' || c=='(' || c==')' || c=='+' || c=='"' || c=='*');
}

static int fts5NextToken(
  Fts5Parser *pParse,             /* Parser context */
  Fts5ParserToken *p              /* OUT: Populate this object */
){
  const char *z = pParse->zExpr;
  char c;

  memset(p, 0, sizeof(Fts5ParserToken));

  /* Skip past any whitespace */
  while( sqlite4Isspace(z[pParse->iExpr]) ) pParse->iExpr++;

  c = z[pParse->iExpr];
  if( c=='\0' ){
    p->eType = TOKEN_EOF;
  }

  else if( c=='(' ){
    pParse->iExpr++;
    p->eType = TOKEN_LP;
  }

  else if( c==')' ){
    pParse->iExpr++;
    p->eType = TOKEN_RP;
  }

  else if( c==':' ){
    pParse->iExpr++;
    p->eType = TOKEN_COLON;
  }

  else if( c=='+' ){
    pParse->iExpr++;
    p->eType = TOKEN_PLUS;
  }

  else if( c=='*' ){
    pParse->iExpr++;
    p->eType = TOKEN_STAR;
  }

  else if( c=='"' ){
    char *zOut = &pParse->aSpace[pParse->iSpace];
    const char *zPrimitive = zOut;
    int i = pParse->iExpr+1;

    while( z[i] ){
      if( z[i]=='"' ){
        if( z[i+1]=='"' ){
          i++;
        }else{
          break;
        }
      }
      *zOut++ = z[i];
      i++;
    }
    if( z[i]!='"' ){
      /* Mismatched quotation mark */
      return SQLITE4_ERROR;
    }

    pParse->iExpr = i+1;
    p->eType = TOKEN_PRIMITIVE;
    p->z = zPrimitive;
    p->n = (zOut - zPrimitive);
    pParse->iSpace += (zOut - zPrimitive);
  }

  else{
    const char *zPrimitive = &z[pParse->iExpr];
    int n = 0;
    while( zPrimitive[n] 
        && fts5IsSpecial(zPrimitive[n])==0
        && sqlite4Isspace(zPrimitive[n])==0 
    ){
      n++;
    }
    pParse->iExpr += n;

    if( n>=4 && memcmp(zPrimitive, "NEAR", 4)==0 ){
      int nNear = FTS5_DEFAULT_NEAR;
      if( n>4 ){
        int i;
        nNear = 0;
        for(i=5; i<n; i++){
          if( !sqlite4Isdigit(zPrimitive[i]) ) break;
          nNear = nNear*10 + zPrimitive[i]-'0';
        }
        if( n<6 || zPrimitive[4]!='/' || i<n ){
          return SQLITE4_ERROR;
        }
      }
      p->eType = TOKEN_NEAR;
      p->n = nNear;
      p->z = 0;
    }else if( n==3 && memcmp(zPrimitive, "NOT", 3)==0 ){
      p->eType = TOKEN_NOT;
    }
    else if( n==2 && memcmp(zPrimitive, "OR", 2)==0 ){
      p->eType = TOKEN_OR;
    }
    else if( n==3 && memcmp(zPrimitive, "AND", 3)==0 ){
      p->eType = TOKEN_AND;
    }else{
      p->eType = TOKEN_PRIMITIVE;
      p->z = zPrimitive;
      p->n = n;
    }
  }

  return SQLITE4_OK;
}

static int fts5NextToken2(
  Fts5Parser *pParse,
  Fts5ParserToken *p
){
  int rc = SQLITE4_OK;
  if( pParse->iExpr==0 ){
    rc = fts5NextToken(pParse, p);
  }else{
    *p = pParse->next;
  }

  if( rc==SQLITE4_OK && p->eType!=TOKEN_EOF ){
    rc = fts5NextToken(pParse, &pParse->next);
  }

  return rc;
}

static int fts5PhraseNewStr(
  Fts5Parser *pParse,             /* Expression parsing context */
  Fts5Phrase *pPhrase,            /* Phrase to add a new Fts5Str to */
  int nNear                       /* Value of nnn in NEAR/nnn operator */
){
  const int nIncr = 4;

  if( (pPhrase->nStr % nIncr)==0 ){
    Fts5Str *aNew;
    aNew = (Fts5Str *)sqlite4DbRealloc(pParse->db, 
        pPhrase->aStr, (pPhrase->nStr+nIncr)*sizeof(Fts5Str)
    );
    if( !aNew ) return SQLITE4_NOMEM;
    memset(&aNew[pPhrase->nStr], 0, nIncr*sizeof(Fts5Str));
    pPhrase->aStr = aNew;
  }
  if( pPhrase->nStr>0 ){
    if( ((pPhrase->nStr-1) % nIncr)==0 ){
      int *aNew;
      aNew = (int *)sqlite4DbRealloc(pParse->db, 
        pPhrase->aiNear, (pPhrase->nStr+nIncr-1)*sizeof(int)
      );
      if( !aNew ) return SQLITE4_NOMEM;
      pPhrase->aiNear = aNew;
    }
    pPhrase->aiNear[pPhrase->nStr-1] = nNear;
  }

  pPhrase->nStr++;
  return SQLITE4_OK;
}

/*
** Callback for fts5CountTokens().
*/
static int fts5CountTokensCb(
  void *pCtx, 
  int iStream, 
  int iOff, 
  const char *z, int n,
  int iSrc, int nSrc
){
  (*((int *)pCtx))++;
  return 0;
}

/*
** Count the number of tokens in document zDoc/nDoc using the tokenizer and
** tokenizer instance supplied as the first two arguments. Set *pnToken to
** the result before returning.
*/
static int fts5CountTokens(
  Fts5Tokenizer *pTokenizer,
  sqlite4_tokenizer *p,
  const char *zDoc,
  int nDoc,
  int *pnToken
){
  int nToken = 0;
  int rc;
  rc = pTokenizer->xTokenize((void *)&nToken, p, zDoc, nDoc, fts5CountTokensCb);
  *pnToken = nToken;
  return rc;
}

struct AppendTokensCtx {
  Fts5Parser *pParse;
  Fts5Str *pStr;
};

static int fts5AppendTokensCb(
  void *pCtx, 
  int iStream, 
  int iOff, 
  const char *z, int n, 
  int iSrc, int nSrc
){
  struct AppendTokensCtx *p = (struct AppendTokensCtx *)pCtx;
  Fts5Parser *pParse = p->pParse;
  Fts5Token *pToken;
  char *zSpace;
  int nUsed;

  pToken = &p->pStr->aToken[p->pStr->nToken];

  zSpace = &pParse->aSpace[pParse->iSpace];
  nUsed = putVarint32((u8 *)zSpace, pParse->iRoot);
  zSpace[nUsed++] = 0x24;
  pToken->bPrefix = 0;
  pToken->z = &zSpace[nUsed];
  pToken->n = n;
  memcpy(pToken->z, z, n);
  pToken->z[n] = '\0';

  nUsed += (n+1);
  pToken->aPrefix = (u8 *)zSpace;
  pToken->nPrefix = nUsed;
  pToken->pCsr = 0;
  pParse->iSpace += nUsed;
  p->pStr->nToken++;

  assert( pParse->iSpace<=pParse->nSpace );
  return 0;
}

static int fts5AppendTokens( 
  Fts5Parser *pParse,
  Fts5Str *pStr, const char *zPrim,
  int nPrim
){
  struct AppendTokensCtx ctx;
  ctx.pParse = pParse;
  ctx.pStr = pStr;

  return pParse->pTokenizer->xTokenize(
      (void *)&ctx, pParse->p , zPrim, nPrim, fts5AppendTokensCb
  );
}

/*
** Append a new token to the current phrase.
*/
static int fts5PhraseAppend(
  Fts5Parser *pParse,
  Fts5Phrase *pPhrase,
  const char *zPrim,
  int nPrim
){
  Fts5Tokenizer *pTok = pParse->pTokenizer;
  int nToken;
  int rc;

  rc = fts5CountTokens(pTok, pParse->p, zPrim, nPrim, &nToken);
  if( rc==SQLITE4_OK && nToken>0 ){
    /* Extend the size of the token array by nToken entries */
    Fts5Str *pStr = &pPhrase->aStr[pPhrase->nStr-1];

    pStr->aToken = sqlite4DbReallocOrFree(pParse->db, pStr->aToken,
        (pStr->nToken + nToken) * sizeof(Fts5Token)
    );
    if( !pStr->aToken ){
      rc = SQLITE4_NOMEM;
    }else{
      rc = fts5AppendTokens(pParse, pStr, zPrim, nPrim);
    }
  }

  return rc;
}

static int fts5PhraseAppendStar(
  Fts5Parser *pParse,
  Fts5Phrase *pPhrase
){
  Fts5Str *pStr = &pPhrase->aStr[pPhrase->nStr-1];
  Fts5Token *p = &pStr->aToken[pStr->nToken-1];

  if( p->bPrefix ){
    return SQLITE4_ERROR;
  }
  p->bPrefix = 1;
  p->nPrefix--;
  return SQLITE4_OK;
}

static void fts5PhraseFree(sqlite4 *db, Fts5Phrase *p){
  if( p ){
    int i;
    for(i=0; i<p->nStr; i++){
      int iTok;
      for(iTok=0; iTok<p->aStr[i].nToken; iTok++){
        sqlite4KVCursorClose(p->aStr[i].aToken[iTok].pCsr);
      }
      sqlite4DbFree(db, p->aStr[i].aToken);
      sqlite4DbFree(db, p->aStr[i].aList);
    }
    sqlite4DbFree(db, p->aiNear);
    sqlite4DbFree(db, p->aStr);
    sqlite4DbFree(db, p);
  }
}

static int fts5NextTokenOrPhrase(
  Fts5Parser *pParse,             /* Parser context */
  int *peType,                    /* OUT: Token type */
  Fts5Phrase **ppPhrase           /* OUT: New phrase object */
){
  int rc;
  Fts5Phrase *pPhrase = 0;
  Fts5ParserToken t;

  rc = fts5NextToken2(pParse, &t);
  *peType = t.eType;
  if( rc==SQLITE4_OK && t.eType==TOKEN_PRIMITIVE ){

    /* Allocate the Fts5Phrase object */
    pPhrase = sqlite4DbMallocZero(pParse->db, sizeof(Fts5Phrase));
    if( pPhrase==0 ){
      rc = SQLITE4_NOMEM;
      goto token_or_phrase_out;
    }
    pPhrase->iCol = -1;

    /* Check if this first primitive is a column name or not. */
    if( pParse->next.eType==TOKEN_COLON ){
      int iCol;
      for(iCol=0; iCol<pParse->nCol; iCol++){
        if( sqlite4StrNICmp(pParse->azCol[iCol], t.z, t.n)==0 ) break;
      }
      if( iCol==pParse->nCol ){
        pParse->zErr = sqlite4MPrintf(pParse->db, 
            "fts5: no such column: %.*s", t.n, t.z
        );
        rc = SQLITE4_ERROR;
        goto token_or_phrase_out;
      }
      pPhrase->iCol = iCol;

      rc = fts5NextToken2(pParse, &t);
      if( rc==SQLITE4_OK ) rc = fts5NextToken2(pParse, &t);
      if( rc==SQLITE4_OK && t.eType!=TOKEN_PRIMITIVE ){
        rc = SQLITE4_ERROR;
      }
      if( rc!=SQLITE4_OK ) goto token_or_phrase_out;
    }

    /* Add the first Fts5Str to the new phrase object. Populate it with the
    ** results of tokenizing t.z/t.n. */
    rc = fts5PhraseNewStr(pParse, pPhrase, 0);
    if( rc==SQLITE4_OK ){
      rc = fts5PhraseAppend(pParse, pPhrase, t.z, t.n);
    }
    if( rc==SQLITE4_OK && pParse->next.eType==TOKEN_STAR ){
      fts5NextToken2(pParse, &t);
      rc = fts5PhraseAppendStar(pParse, pPhrase);
    }

    /* Add any further primitives connected by "+" or NEAR operators. */
    while( rc==SQLITE4_OK && 
        (pParse->next.eType==TOKEN_PLUS || pParse->next.eType==TOKEN_NEAR)
    ){
      rc = fts5NextToken2(pParse, &t);
      if( rc==SQLITE4_OK ){
        if( t.eType==TOKEN_NEAR ){
          rc = fts5PhraseNewStr(pParse, pPhrase, t.n);
          if( rc!=SQLITE4_OK ) goto token_or_phrase_out;
        }
        rc = fts5NextToken2(pParse, &t);
        if( rc!=SQLITE4_OK ) goto token_or_phrase_out;
        if( t.eType!=TOKEN_PRIMITIVE ){
          rc = SQLITE4_ERROR;
        }else{
          rc = fts5PhraseAppend(pParse, pPhrase, t.z, t.n);
          if( rc==SQLITE4_OK && pParse->next.eType==TOKEN_STAR ){
            fts5NextToken2(pParse, &t);
            rc = fts5PhraseAppendStar(pParse, pPhrase);
          }
        }
      }
    }
  }

 token_or_phrase_out:
  if( rc!=SQLITE4_OK ){
    fts5PhraseFree(pParse->db, pPhrase);
  }else{
    *ppPhrase = pPhrase;
  }
  return rc;
}

static void fts5FreeExprNode(sqlite4 *db, Fts5ExprNode *pNode){
  if( pNode ){
    fts5PhraseFree(db, pNode->pPhrase);
    fts5FreeExprNode(db, pNode->pLeft);
    fts5FreeExprNode(db, pNode->pRight);
    sqlite4DbFree(db, pNode);
  }
}

static void fts5ExpressionFree(sqlite4 *db, Fts5Expr *pExpr){
  if( pExpr ){
    fts5FreeExprNode(db, pExpr->pRoot);
    sqlite4DbFree(db, pExpr->apPhrase);
    sqlite4DbFree(db, pExpr);
  }
}

typedef struct ExprHier ExprHier;
struct ExprHier {
  Fts5ExprNode **ppNode;
  int nOpen;
};

static int fts5GrowExprHier(
  sqlite4 *db, 
  int *pnAlloc, 
  ExprHier **paHier, 
  int nReq
){
  int rc = SQLITE4_OK;
  int nAlloc = *pnAlloc;
  if( nAlloc<nReq ){
    ExprHier *aNew;
    nAlloc += 8;
    aNew = (ExprHier *)sqlite4DbReallocOrFree(
        db, *paHier, nAlloc*sizeof(ExprHier)
    );
    if( aNew==0 ) rc = SQLITE4_NOMEM;
    *paHier = aNew;
  }
  return rc;
}

static int fts5AddBinary(
  sqlite4 *db, 
  int eType,
  int *pnHier, 
  int *pnHierAlloc, 
  ExprHier **paHier
){
  Fts5ExprNode *pNode;
  Fts5ExprNode **pp;
  int rc;

  rc = fts5GrowExprHier(db, pnHierAlloc, paHier, *pnHier+1);
  if( rc!=SQLITE4_OK ) return rc;
  pNode = sqlite4DbMallocZero(db, sizeof(Fts5ExprNode));
  if( !pNode ) return SQLITE4_NOMEM;
  pNode->eType = eType;

  pp = (*paHier)[*pnHier-1].ppNode;
  pNode->pLeft = *pp;
  *pp = pNode;
  (*paHier)[*pnHier].ppNode = &pNode->pRight;
  (*paHier)[*pnHier].nOpen = 0;
  (*pnHier)++;

  return SQLITE4_OK;
}

static void fts5FindStrings(Fts5ExprNode *p, Fts5Str ***papStr){
  if( p ){
    if( p->eType==TOKEN_PRIMITIVE ){
      int i;
      Fts5Str *aStr = p->pPhrase->aStr;
      for(i=0; i<p->pPhrase->nStr; i++){
        **papStr = &aStr[i];
        (*papStr)++;
      }
    }
    fts5FindStrings(p->pLeft, papStr);
    fts5FindStrings(p->pRight, papStr);
  }
}

static int fts5ParseExpression(
  sqlite4 *db,                    /* Database handle */
  Fts5Tokenizer *pTokenizer,      /* Tokenizer module */
  sqlite4_tokenizer *p,           /* Tokenizer instance */
  int iRoot,                      /* Root page number of FTS index */
  char **azCol,                   /* Array of column names (nul-term'd) */
  int nCol,                       /* Size of array azCol[] */
  const char *zExpr,              /* FTS expression text */
  Fts5Expr **ppExpr,              /* OUT: Expression object */
  char **pzErr                    /* OUT: Error message */
){
  int rc = SQLITE4_OK;
  Fts5Parser sParse;
  int nStr = 0;
  int nExpr;
  int i;
  Fts5Expr *pExpr;

  int nHier = 0;
  int nHierAlloc = 0;
  ExprHier *aHier = 0;

  nExpr = sqlite4Strlen30(zExpr);
  memset(&sParse, 0, sizeof(Fts5Parser));
  sParse.zExpr = zExpr;
  sParse.azCol = azCol;
  sParse.nCol = nCol;
  sParse.pTokenizer = pTokenizer;
  sParse.p = p;
  sParse.db = db;
  sParse.iRoot = iRoot;

  pExpr = sqlite4DbMallocZero(db, sizeof(Fts5Expr) + nExpr*4);
  if( !pExpr ) return SQLITE4_NOMEM;
  sParse.aSpace = (char *)&pExpr[1];
  sParse.nSpace = nExpr*4;

  rc = fts5GrowExprHier(db, &nHierAlloc, &aHier, 1);
  if( rc==SQLITE4_OK ){
    aHier[0].ppNode = &pExpr->pRoot;
    aHier[0].nOpen = 0;
    nHier = 1;
  }

  while( rc==SQLITE4_OK ){
    int eType = 0;
    Fts5Phrase *pPhrase = 0;
    Fts5ExprNode *pNode = 0;

    rc = fts5NextTokenOrPhrase(&sParse, &eType, &pPhrase);
    if( rc!=SQLITE4_OK || eType==TOKEN_EOF ) break;

    switch( eType ){
      case TOKEN_PRIMITIVE: {
        Fts5ExprNode **pp = aHier[nHier-1].ppNode;
        if( *pp ){
          rc = fts5AddBinary(db, TOKEN_AND, &nHier, &nHierAlloc, &aHier);
          pp = aHier[nHier-1].ppNode;
        }
        if( rc==SQLITE4_OK ){
          pNode = sqlite4DbMallocZero(db, sizeof(Fts5ExprNode));
          if( pNode==0 ){
            rc = SQLITE4_NOMEM;
          }else{
            pNode->eType = TOKEN_PRIMITIVE;
            pNode->pPhrase = pPhrase;
            *pp = pNode;
          }
        }
        nStr++;
        break;
      }

      case TOKEN_AND:
      case TOKEN_OR:
      case TOKEN_NOT: {
        Fts5ExprNode **pp = aHier[nHier-1].ppNode;

        if( *pp==0 ){
          rc = SQLITE4_ERROR;
        }else{
          while( nHier>1 
             && aHier[nHier-1].nOpen==0 
             && (*aHier[nHier-2].ppNode)->eType  < eType 
          ){
            nHier--;
          }

          rc = fts5AddBinary(db, eType, &nHier, &nHierAlloc, &aHier);
        }
        break;
      }

      case TOKEN_LP: {
        Fts5ExprNode **pp = aHier[nHier-1].ppNode;
        if( *pp ){
          rc = SQLITE4_ERROR;
        }else{
          aHier[nHier-1].nOpen++;
        }
        break;
      }

      case TOKEN_RP: {
        Fts5ExprNode **pp = aHier[nHier-1].ppNode;
        if( *pp==0 ){
          rc = SQLITE4_ERROR;
        }else{
          for(i=nHier-1; i>=0; i--){
            if( aHier[i].nOpen>0 ) break;
          }
          if( i<0 ){
            rc = SQLITE4_ERROR;
          }else{
            aHier[i].nOpen--;
            nHier = i+1;
          }
        }
        break;
      }

      default:
        rc = SQLITE4_ERROR;
        break;
    }

    if( rc!=SQLITE4_OK ){
      sqlite4DbFree(db, pNode);
      break;
    }
  }

  if( rc==SQLITE4_OK && *aHier[nHier-1].ppNode==0 ){
    rc = SQLITE4_ERROR;
  }
  for(i=0; rc==SQLITE4_OK && i<nHier; i++){
    if( aHier[i].nOpen>0 ) rc = SQLITE4_ERROR;
  }

  if( rc==SQLITE4_OK ){
    pExpr->nPhrase = nStr;
    pExpr->apPhrase = (Fts5Str**)sqlite4DbMallocZero(db, sizeof(Fts5Str*)*nStr);
    if( pExpr->apPhrase==0 ){
      rc = SQLITE4_NOMEM;
    }else{
      Fts5Str **a = pExpr->apPhrase;
      fts5FindStrings(pExpr->pRoot, &a);
    }
  }

  if( rc!=SQLITE4_OK ){
    fts5ExpressionFree(db, pExpr);
    *pzErr = sParse.zErr;
    pExpr = 0;
  }
  *ppExpr = pExpr;
  sqlite4DbFree(db, aHier);
  return rc;
}

/*
** Search for the Fts5Tokenizer object named zName. Return a pointer to it
** if it exists, or NULL otherwise.
*/
static Fts5Tokenizer *fts5FindTokenizer(sqlite4 *db, const char *zName){
  Fts5Tokenizer *p;
  for(p=db->pTokenizer; p; p=p->pNext){
    if( 0==sqlite4StrICmp(zName, p->zName) ) break;
  }
  return p;
}

static void fts5TokenizerCreate(
  Parse *pParse, 
  Fts5Index *pFts, 
  Fts5Tokenizer **ppTokenizer,
  sqlite4_tokenizer **pp
){
  Fts5Tokenizer *pTok;
  char *zTok;                     /* Tokenizer name */
  const char **azArg;             /* Tokenizer arguments */
  int nArg;                       /* Number of elements in azArg */

  if( pFts->nTokenizer ){
    zTok = pFts->azTokenizer[0];
    azArg = (const char **)&pFts->azTokenizer[1];
    nArg = pFts->nTokenizer-1;
  }else{
    zTok = "simple";
    azArg = 0;
    nArg = 0;
  }
 
  *ppTokenizer = pTok = fts5FindTokenizer(pParse->db, zTok);
  if( !pTok ){
    sqlite4ErrorMsg(pParse, "no such tokenizer: \"%s\"", zTok);
  }else{
    int rc = pTok->xCreate(pTok->pCtx, azArg, nArg, pp);
    if( rc!=SQLITE4_OK ){
      assert( *pp==0 );
      sqlite4ErrorMsg(pParse, "error creating tokenizer");
    }
  }
}

static void fts5TokenizerDestroy(Fts5Tokenizer *pTok, sqlite4_tokenizer *p){
  if( p ) pTok->xDestroy(p);
}

void sqlite4ShutdownFts5(sqlite4 *db){
  Fts5Tokenizer *p;
  Fts5Tokenizer *pNext;
  for(p=db->pTokenizer; p; p=pNext){
    pNext = p->pNext;
    sqlite4DbFree(db, p);
  }
}

/*
** This function is used to install custom FTS tokenizers.
*/
int sqlite4_create_tokenizer(
  sqlite4 *db,
  const char *zName,
  void *pCtx,
  int (*xCreate)(void*, const char**, int, sqlite4_tokenizer**),
  int (*xTokenize)(void*, sqlite4_tokenizer*,
      const char*, int, int(*x)(void*, int, int, const char*, int, int, int)
  ),
  int (*xDestroy)(sqlite4_tokenizer *)
){
  int rc = SQLITE4_OK;
  sqlite4_mutex_enter(db->mutex);

  /* It is not possible to override an existing tokenizer */
  if( fts5FindTokenizer(db, zName) ){
    rc = SQLITE4_ERROR;
  }else{
    int nName = sqlite4Strlen30(zName);
    Fts5Tokenizer *pTokenizer = (Fts5Tokenizer *)sqlite4DbMallocZero(db, 
        sizeof(Fts5Tokenizer) + nName+1
    );
    if( !pTokenizer ){
      rc = SQLITE4_NOMEM;
    }else{
      pTokenizer->pCtx = pCtx;
      pTokenizer->xCreate = xCreate;
      pTokenizer->xTokenize = xTokenize;
      pTokenizer->xDestroy = xDestroy;
      pTokenizer->zName = (char *)&pTokenizer[1];
      memcpy(pTokenizer->zName, zName, nName+1);

      pTokenizer->pNext = db->pTokenizer;
      db->pTokenizer = pTokenizer;
    }
  }

  rc = sqlite4ApiExit(db, rc);
  sqlite4_mutex_leave(db->mutex);
  return rc;
}

/*
** Return the size of an Fts5Index structure, in bytes.
*/
int sqlite4Fts5IndexSz(void){ 
  return sizeof(Fts5Index); 
}

/*
** Initialize the fts5 specific part of the index object passed as the
** second argument.
*/
void sqlite4Fts5IndexInit(Parse *pParse, Index *pIdx, ExprList *pArgs){
  Fts5Index *pFts = pIdx->pFts;

  if( pArgs ){
    int i;
    for(i=0; pParse->nErr==0 && i<pArgs->nExpr; i++){
      char *zArg = pArgs->a[i].zName;
      char *zVal = pArgs->a[i].pExpr->u.zToken;

      if( zArg && sqlite4StrICmp(zArg, "tokenizer")==0 ){
        /* zVal is the name of the tokenizer to use. Any subsequent arguments
         ** that do not contain assignment operators (=) are also passed to
         ** the tokenizer. Figure out how many bytes of space are required for
         ** all.  */
        int j;
        char *pSpace;
        int nByte = sqlite4Strlen30(zVal) + 1;
        for(j=i+1; j<pArgs->nExpr; j++){
          ExprListItem *pItem = &pArgs->a[j];
          if( pItem->zName ) break;
          nByte += sqlite4Strlen30(pItem->pExpr->u.zToken) + 1;
        }
        nByte += sizeof(char *) * (j-i);
        pFts->azTokenizer = (char **)sqlite4DbMallocZero(pParse->db, nByte);
        if( pFts->azTokenizer==0 ) return;

        pSpace = (char *)&pFts->azTokenizer[j-i];
        for(j=i; j<pArgs->nExpr; j++){
          ExprListItem *pItem = &pArgs->a[j];
          if( pItem->zName && j>i ){
            break;
          }else{
            int nToken = sqlite4Strlen30(pItem->pExpr->u.zToken);
            memcpy(pSpace, pItem->pExpr->u.zToken, nToken+1);
            pFts->azTokenizer[j-i] = pSpace;
            pSpace += nToken+1;
          }
        }

        /* If this function is being called as part of a CREATE INDEX statement
        ** issued by the user (to create a new index) check if the tokenizer
        ** is valid. If not, return an error. Do not do this if this function
        ** is being called as part of parsing an existing database schema.
        */
        if( pParse->db->init.busy==0 ){
          Fts5Tokenizer *pTok = 0;
          sqlite4_tokenizer *t = 0;

          fts5TokenizerCreate(pParse, pFts, &pTok, &t);
          fts5TokenizerDestroy(pTok, t);
        }
      }
      else{
        sqlite4ErrorMsg(pParse,"unrecognized argument: \"%s\"", zArg?zArg:zVal);
      }
    }
  }
}

void sqlite4Fts5IndexFree(sqlite4 *db, Index *pIdx){
  if( pIdx->pFts ){
    sqlite4DbFree(db, pIdx->pFts->azTokenizer);
  }
}


/*
** Context structure passed to tokenizer callback when tokenizing a document.
**
** The hash table maps between tokens and TokenizeTerm structures.
**
** TokenizeTerm structures are allocated using sqlite4DbMalloc(). Immediately
** following the structure in memory is the token itself (TokenizeTerm.nToken
** bytes of data). Following this is the list of token instances in the same
** format as it is stored in the database. 
**
** All of the above is a single allocation, size TokenizeTerm.nAlloc bytes.
** If the initial allocation is too small, it is extended using
** sqlite4DbRealloc().
*/
typedef struct TokenizeCtx TokenizeCtx;
typedef struct TokenizeTerm TokenizeTerm;
struct TokenizeCtx {
  int rc;
  int iCol;
  int nCol;                       /* Number of columns in table */
  sqlite4 *db;
  int nMax;
  i64 *aSz;                       /* Number of tokens in each column/stream */
  int nStream;                    /* Number of streams in document */
  Hash hash;
};
struct TokenizeTerm {
  int iStream;                    /* Weight of previous entry */
  int iCol;                       /* Column containing previous entry */
  int iOff;                       /* Token offset of previous entry */
  int nToken;                     /* Size of token in bytes */
  int nData;                      /* Bytes of data in value */
  int nAlloc;                     /* Bytes of data allocated */
};

TokenizeTerm *fts5TokenizeAppendInt(
  TokenizeCtx *p, 
  TokenizeTerm *pTerm, 
  int iVal
){
  unsigned char *a;
  int nSpace = pTerm->nAlloc-pTerm->nData-pTerm->nToken-sizeof(TokenizeTerm);

  if( nSpace < 5 ){
    int nAlloc = (pTerm->nAlloc<256) ? 256 : pTerm->nAlloc * 2;
    pTerm = sqlite4DbReallocOrFree(p->db, pTerm, nAlloc);
    if( !pTerm ) return 0;
    pTerm->nAlloc = sqlite4DbMallocSize(p->db, pTerm);
  }

  a = &(((unsigned char *)&pTerm[1])[pTerm->nToken+pTerm->nData]);
  pTerm->nData += putVarint32(a, iVal);
  return pTerm;
}

static int fts5TokenizeCb(
  void *pCtx, 
  int iStream, 
  int iOff,
  const char *zToken, 
  int nToken, 
  int iSrc, 
  int nSrc
){
  TokenizeCtx *p = (TokenizeCtx *)pCtx;
  sqlite4 *db = p->db;
  TokenizeTerm *pTerm = 0;
  TokenizeTerm *pOrig = 0;

  /* TODO: Error here if iStream is out of range */

  if( nToken>p->nMax ) p->nMax = nToken;

  if( iStream>=p->nStream ){
    int nOld = p->nStream;
    int nNew = 4;
    while( nNew<=iStream ) nNew = nNew*2;
    p->aSz = (i64*)sqlite4DbReallocOrFree(db, p->aSz, nNew*p->nCol*sizeof(i64));
    if( p->aSz==0 ) goto tokenize_cb_out;
    memset(&p->aSz[p->nStream * p->nCol], 0, (nNew-nOld)*p->nCol*sizeof(i64));
  }
  p->aSz[iStream*p->nCol + p->iCol]++;

  pTerm = (TokenizeTerm *)sqlite4HashFind(&p->hash, zToken, nToken);
  if( pTerm==0 ){
    /* Size the initial allocation so that it fits in the lookaside buffer */
    int nAlloc = sizeof(TokenizeTerm) + nToken + 32;

    pTerm = sqlite4DbMallocZero(p->db, nAlloc);
    if( pTerm ){
      void *pFree;
      pTerm->nAlloc = sqlite4DbMallocSize(p->db, pTerm);
      pTerm->nToken = nToken;
      memcpy(&pTerm[1], zToken, nToken);
      pFree = sqlite4HashInsert(&p->hash, (char *)&pTerm[1], nToken, pTerm);
      if( pFree ){
        sqlite4DbFree(p->db, pFree);
        pTerm = 0;
      }
      if( pTerm==0 ) goto tokenize_cb_out;
    }
  }
  pOrig = pTerm;

  if( iStream!=pTerm->iStream ){
    pTerm = fts5TokenizeAppendInt(p, pTerm, (iStream << 2) | 0x00000003);
    if( !pTerm ) goto tokenize_cb_out;
    pTerm->iStream = iStream;
  }

  if( pTerm && p->iCol!=pTerm->iCol ){
    pTerm = fts5TokenizeAppendInt(p, pTerm, (p->iCol << 2) | 0x00000001);
    if( !pTerm ) goto tokenize_cb_out;
    pTerm->iCol = p->iCol;
    pTerm->iOff = 0;
  }

  pTerm = fts5TokenizeAppendInt(p, pTerm, (iOff-pTerm->iOff) << 1);
  if( !pTerm ) goto tokenize_cb_out;
  pTerm->iOff = iOff;

tokenize_cb_out:
  if( pTerm!=pOrig ){
    sqlite4HashInsert(&p->hash, (char *)&pTerm[1], nToken, pTerm);
  }
  if( !pTerm ){
    p->rc = SQLITE4_NOMEM;
    return 1;
  }

  return 0;
}

static int fts5LoadSizeRecord(
  sqlite4 *db,                    /* Database handle */
  u8 *aKey, int nKey,             /* KVStore key */
  int nMinStream,                 /* Space for at least this many streams */
  Fts5Info *pInfo,                /* Info record */
  i64 *pnRow,                     /* non-NULL when reading global record */
  Fts5Size **ppSz                 /* OUT: Loaded size record */
){
  Fts5Size *pSz = 0;              /* Size object */
  KVCursor *pCsr = 0;             /* Cursor used to read global record */
  int rc;

  rc = sqlite4KVStoreOpenCursor(db->aDb[pInfo->iDb].pKV, &pCsr);
  if( rc==SQLITE4_OK ){
    rc = sqlite4KVCursorSeek(pCsr, aKey, nKey, 0);
    if( rc==SQLITE4_NOTFOUND ){
      rc = SQLITE4_CORRUPT_BKPT;
    }else if( rc==SQLITE4_OK ){
      const u8 *aData = 0;
      int nData = 0;
      rc = sqlite4KVCursorData(pCsr, 0, -1, &aData, &nData);
      if( rc==SQLITE4_OK ){
        int iOff = 0;
        int nStream = 0;
        int nAlloc;

        /* If pnRow is not NULL, then this is the global record. Read the
        ** number of documents in the table from the start of the record. */
        if( pnRow ){
          iOff += sqlite4GetVarint(&aData[iOff], (u64 *)pnRow);
        }
        iOff += getVarint32(&aData[iOff], nStream);
        nAlloc = (nStream < nMinStream ? nMinStream : nStream);

        pSz = sqlite4DbMallocZero(db, 
            sizeof(Fts5Size) + sizeof(i64) * pInfo->nCol * nAlloc
        );
        if( pSz==0 ){
          rc = SQLITE4_NOMEM;
        }else{
          int iCol = 0;
          pSz->nCol = pInfo->nCol;
          pSz->nStream = nAlloc;
          while( iOff<nData ){
            int i;
            i64 *aSz = &pSz->aSz[iCol*nAlloc];
            for(i=0; i<nStream; i++){
              iOff += sqlite4GetVarint(&aData[iOff], (u64*)&aSz[i]);
            }
            iCol++;
          }
        }
      }
    }
    sqlite4KVCursorClose(pCsr);
  }

  *ppSz = pSz;
  return rc;
}

static int fts5StoreSizeRecord(
  KVStore *p,
  u8 *aKey, int nKey,
  Fts5Size *pSz, 
  i64 nRow, 
  u8 *a                           /* Space to serialize record in */
){
  int iOff = 0;
  int iCol;

  if( nRow>=0 ){
    iOff += sqlite4PutVarint(&a[iOff], nRow);
  }
  iOff += sqlite4PutVarint(&a[iOff], pSz->nStream);
  for(iCol=0; iCol<pSz->nCol; iCol++){
    int i;
    for(i=0; i<pSz->nStream; i++){
      iOff += sqlite4PutVarint(&a[iOff], pSz->aSz[iCol*pSz->nCol+i]);
    }
  }

  return sqlite4KVStoreReplace(p, aKey, nKey, a, iOff);
}

static int fts5CsrLoadGlobal(Fts5Cursor *pCsr){
  int rc = SQLITE4_OK;
  if( pCsr->pGlobal==0 ){
    int nKey;
    u8 aKey[10];
    nKey = putVarint32(aKey, pCsr->pInfo->iRoot);
    aKey[nKey++] = 0x00;
    rc = fts5LoadSizeRecord(
        pCsr->db, aKey, nKey, 0, pCsr->pInfo, &pCsr->nGlobal, &pCsr->pGlobal
    );
  }
  return rc;
}

static int fts5CsrLoadSz(Fts5Cursor *pCsr){
  int rc = SQLITE4_OK;
  if( pCsr->pSz==0 ){
    sqlite4 *db = pCsr->db;
    Fts5Info *pInfo = pCsr->pInfo;
    u8 *aKey;
    int nKey = 0;
    int nPk = pCsr->pExpr->pRoot->nPk;

    aKey = (u8 *)sqlite4DbMallocZero(db, 10 + nPk);
    if( !aKey ) return SQLITE4_NOMEM;

    nKey = putVarint32(aKey, pInfo->iRoot);
    aKey[nKey++] = 0x00;
    memcpy(&aKey[nKey], pCsr->pExpr->pRoot->aPk, nPk);
    nKey += nPk;

    rc = fts5LoadSizeRecord(pCsr->db, aKey, nKey, 0, pInfo, 0, &pCsr->pSz);
    sqlite4DbFree(db, aKey);
  }

  return rc;
}


/*
** Update an fts index.
*/
int sqlite4Fts5Update(
  sqlite4 *db,                    /* Database handle */
  Fts5Info *pInfo,                /* Description of fts index to update */
  Mem *pKey,                      /* Primary key blob */
  Mem *aArg,                      /* Array of arguments (see above) */
  int bDel,                       /* True for a delete, false for insert */
  char **pzErr                    /* OUT: Error message */
){
  int i;
  int rc = SQLITE4_OK;
  KVStore *pStore;
  TokenizeCtx sCtx;
  int nTnum = 0;
  u32 dummy = 0;

  u8 *aSpace = 0;
  int nSpace = 0;

  const u8 *pPK;
  int nPK;
  HashElem *pElem;

  pStore = db->aDb[pInfo->iDb].pKV;

  memset(&sCtx, 0, sizeof(sCtx));
  sCtx.db = db;
  sCtx.nCol = pInfo->nCol;
  sqlite4HashInit(db->pEnv, &sCtx.hash, 1);

  pPK = (const u8 *)sqlite4_value_blob(pKey);
  nPK = sqlite4_value_bytes(pKey);
  
  nTnum = getVarint32(pPK, dummy);
  nPK -= nTnum;
  pPK += nTnum;

  for(i=0; rc==SQLITE4_OK && i<pInfo->nCol; i++){
    sqlite4_value *pArg = (sqlite4_value *)(&aArg[i]);
    if( pArg->flags & MEM_Str ){
      const char *zText;
      int nText;

      zText = (const char *)sqlite4_value_text(pArg);
      nText = sqlite4_value_bytes(pArg); 
      sCtx.iCol = i;
      rc = pInfo->pTokenizer->xTokenize(
          &sCtx, pInfo->p, zText, nText, fts5TokenizeCb
      );
    }
  }

  /* Allocate enough space to serialize all the stuff that needs to
  ** be inserted into the database. Specifically:
  **
  **   * Space for index record keys,
  **   * space for the size record and key for this document, and
  **   * space for the updated global size record for the document set.
  **
  ** To make it easier, the below allocates enough space to simultaneously
  ** store the largest index record key and the largest possible global
  ** size record.
  */
  nSpace = (sqlite4VarintLen(pInfo->iRoot) + 2 + sCtx.nMax + nPK) + 
           (9 * (2 + pInfo->nCol * sCtx.nStream));
  aSpace = sqlite4DbMallocRaw(db, nSpace);
  if( aSpace==0 ) rc = SQLITE4_NOMEM;

  for(pElem=sqliteHashFirst(&sCtx.hash); pElem; pElem=sqliteHashNext(pElem)){
    TokenizeTerm *pTerm = (TokenizeTerm *)sqliteHashData(pElem);
    if( rc==SQLITE4_OK ){
      int nToken = sqliteHashKeysize(pElem);
      char *zToken = (char *)sqliteHashKey(pElem);
      u8 *aKey = aSpace;
      int nKey;

      nKey = putVarint32(aKey, pInfo->iRoot);
      aKey[nKey++] = 0x24;
      memcpy(&aKey[nKey], zToken, nToken);
      nKey += nToken;
      aKey[nKey++] = 0x00;
      memcpy(&aKey[nKey], pPK, nPK);
      nKey += nPK;

      if( bDel ){
        /* delete key aKey/nKey from the index */
        rc = sqlite4KVStoreReplace(pStore, aKey, nKey, 0, -1);
      }else{
        /* Insert a new entry for aKey/nKey into the fts index */
        const KVByteArray *aData = (const KVByteArray *)&pTerm[1];
        aData += pTerm->nToken;
        rc = sqlite4KVStoreReplace(pStore, aKey, nKey, aData, pTerm->nData);
      }
    }
    sqlite4DbFree(db, pTerm);
  }

  /* Write the size record into the db */
  if( rc==SQLITE4_OK ){
    u8 *aKey = aSpace;
    int nKey;

    nKey = putVarint32(aKey, pInfo->iRoot);
    aKey[nKey++] = 0x00;
    memcpy(&aKey[nKey], pPK, nPK);
    nKey += nPK;

    if( bDel==0 ){
      Fts5Size sSz;
      sSz.nCol = pInfo->nCol;
      sSz.nStream = sCtx.nStream;
      sSz.aSz = sCtx.aSz;
      rc = fts5StoreSizeRecord(pStore, aKey, nKey, &sSz, -1, &aKey[nKey]);
    }else{
      rc = sqlite4KVStoreReplace(pStore, aKey, nKey, 0, -1);
    }
  }

  /* Update the global record */
  if( rc==SQLITE4_OK ){
    Fts5Size *pSz;                /* Deserialized global size record */
    i64 nRow;                     /* Number of rows in indexed table */
    u8 *aKey = aSpace;            /* Space to format the global record key */
    int nKey;                     /* Size of global record key in bytes */

    nKey = putVarint32(aKey, pInfo->iRoot);
    aKey[nKey++] = 0x00;
    rc = fts5LoadSizeRecord(db, aKey, nKey, sCtx.nStream, pInfo, &nRow, &pSz);
    assert( rc!=SQLITE4_OK || pSz->nStream>=sCtx.nStream );

    if( rc==SQLITE4_OK ){
      int iCol;
      for(iCol=0; iCol<pSz->nCol; iCol++){
        int iStr;
        i64 *aIn = &sCtx.aSz[iCol * sCtx.nStream];
        i64 *aOut = &pSz->aSz[iCol * pSz->nStream];
        for(iStr=0; iStr<sCtx.nStream; iStr++){
          aOut[iStr] += (aIn[iStr] * (bDel?-1:1));
        }
      }
      nRow += (bDel?-1:1);
      rc = fts5StoreSizeRecord(pStore, aKey, nKey, pSz, nRow, &aKey[nKey]);
    }

    sqlite4DbFree(db, pSz);
  }
  
  sqlite4DbFree(db, aSpace);
  sqlite4DbFree(db, sCtx.aSz);
  sqlite4HashClear(&sCtx.hash);
  return rc;
}

static Fts5Info *fts5InfoCreate(Parse *pParse, Index *pIdx, int bCol){
  sqlite4 *db = pParse->db;
  Fts5Info *pInfo;                /* p4 argument for FtsUpdate opcode */
  int nByte;

  nByte = sizeof(Fts5Info);
  if( bCol ){
    int i;
    int nCol = pIdx->pTable->nCol;
    for(i=0; i<nCol; i++){
      const char *zCol = pIdx->pTable->aCol[i].zName;
      nByte += sqlite4Strlen30(zCol) + 1;
    }
    nByte += nCol * sizeof(char *);
  }

  pInfo = sqlite4DbMallocZero(db, nByte);
  if( pInfo ){
    pInfo->iDb = sqlite4SchemaToIndex(db, pIdx->pSchema);
    pInfo->iRoot = pIdx->tnum;
    sqlite4FindPrimaryKey(pIdx->pTable, &pInfo->iTbl);
    pInfo->nCol = pIdx->pTable->nCol;
    fts5TokenizerCreate(pParse, pIdx->pFts, &pInfo->pTokenizer, &pInfo->p);

    if( pInfo->p==0 ){
      assert( pParse->nErr );
      sqlite4DbFree(db, pInfo);
      pInfo = 0;
    }
    else if( bCol ){
      int i;
      char *p;
      int nCol = pIdx->pTable->nCol;

      pInfo->azCol = (char **)(&pInfo[1]);
      p = (char *)(&pInfo->azCol[nCol]);
      for(i=0; i<nCol; i++){
        const char *zCol = pIdx->pTable->aCol[i].zName;
        int n = sqlite4Strlen30(zCol) + 1;
        pInfo->azCol[i] = p;
        memcpy(p, zCol, n);
        p += n;
      }
    }
  }

  return pInfo;
}

void sqlite4Fts5CodeUpdate(
  Parse *pParse, 
  Index *pIdx, 
  int iRegPk, 
  int iRegData,
  int bDel
){
  Vdbe *v;
  Fts5Info *pInfo;                /* p4 argument for FtsUpdate opcode */

  if( 0==(pInfo = fts5InfoCreate(pParse, pIdx, 0)) ) return;

  v = sqlite4GetVdbe(pParse);
  sqlite4VdbeAddOp3(v, OP_FtsUpdate, iRegPk, 0, iRegData);
  sqlite4VdbeChangeP4(v, -1, (const char *)pInfo, P4_FTS5INFO);
  sqlite4VdbeChangeP5(v, (u8)bDel);
}

void sqlite4Fts5CodeQuery(
  Parse *pParse,
  Index *pIdx,
  int iCsr,
  int iJump,
  int iRegMatch
){
  Vdbe *v;
  Fts5Info *pInfo;                /* p4 argument for FtsOpen opcode */

  if( 0==(pInfo = fts5InfoCreate(pParse, pIdx, 1)) ) return;

  v = sqlite4GetVdbe(pParse);
  sqlite4VdbeAddOp3(v, OP_FtsOpen, iCsr, iJump, iRegMatch);
  sqlite4VdbeChangeP4(v, -1, (const char *)pInfo, P4_FTS5INFO);
}

void sqlite4Fts5FreeInfo(sqlite4 *db, Fts5Info *p){
  if( db->pnBytesFreed==0 ){
    if( p->p ) p->pTokenizer->xDestroy(p->p);
    sqlite4DbFree(db, p);
  }
}

void sqlite4Fts5CodeCksum(
  Parse *pParse, 
  Index *pIdx, 
  int iCksum, 
  int iReg,
  int bIdx                        /* True for fts index, false for table */
){
  Vdbe *v;
  Fts5Info *pInfo;                /* p4 argument for FtsCksum opcode */

  if( 0==(pInfo = fts5InfoCreate(pParse, pIdx, 0)) ) return;

  v = sqlite4GetVdbe(pParse);
  sqlite4VdbeAddOp3(v, OP_FtsCksum, iCksum, 0, iReg);
  sqlite4VdbeChangeP4(v, -1, (const char *)pInfo, P4_FTS5INFO);
  sqlite4VdbeChangeP5(v, bIdx);
}

/*
** Calculate a 64-bit checksum for a term instance. The index checksum is
** the XOR of the checksum for each term instance in the table. A term
** instance checksum is calculated based on:
**
**   * the term itself,
**   * the pk of the row the instance appears in,
**   * the weight assigned to the instance,
**   * the column number, and
**   * the term offset.
*/
static i64 fts5TermInstanceCksum(
  const u8 *aTerm, int nTerm,
  const u8 *aPk, int nPk,
  int iStream,
  int iCol,
  int iOff
){
  int i;
  i64 cksum = 0;

  /* Add the term to the checksum */
  for(i=0; i<nTerm; i++){
    cksum += (cksum << 3) + aTerm[i];
  }

  /* Add the primary key blob to the checksum */
  for(i=0; i<nPk; i++){
    cksum += (cksum << 3) + aPk[i];
  }

  /* Add the weight, column number and offset (in that order) to the checksum */
  cksum += (cksum << 3) + iStream;
  cksum += (cksum << 3) + iCol;
  cksum += (cksum << 3) + iOff;

  return cksum;
}


int sqlite4Fts5EntryCksum(
  sqlite4 *db,                    /* Database handle */
  Fts5Info *p,                    /* Index description */
  Mem *pKey,                      /* Database key */
  Mem *pVal,                      /* Database value */
  i64 *piCksum                    /* OUT: Checksum value */
){
  i64 cksum = 0;
  u8 const *aKey; int nKey;       /* Key blob */
  u8 const *aVal; int nVal;       /* List of token instances */
  u8 const *aToken; int nToken;   /* Token for this entry */
  u8 const *aPk; int nPk;         /* Entry primary key blob */
  InstanceList sList;             /* Used to iterate through pVal */
  int nTnum;
  u32 tnum;


  aKey = (const u8 *)sqlite4_value_blob(pKey);
  nKey = sqlite4_value_bytes(pKey);
  aVal = (const u8 *)sqlite4_value_blob(pVal);
  nVal = sqlite4_value_bytes(pVal);

  /* Find the token and primary key blobs for this entry. */
  nTnum = getVarint32(aKey, tnum);
  aToken = &aKey[nTnum+1];
  nToken = sqlite4Strlen30((const char *)aToken);
  aPk = &aToken[nToken+1];
  nPk = (&aKey[nKey] - aPk);

  fts5InstanceListInit((u8 *)aVal, nVal, &sList);
  while( 0==fts5InstanceListNext(&sList) ){
    i64 v = fts5TermInstanceCksum(
        aPk, nPk, aToken, nToken, sList.iStream, sList.iCol, sList.iOff
    );
    cksum = cksum ^ v;
  }

  *piCksum = cksum;
  return SQLITE4_OK;
}

typedef struct CksumCtx CksumCtx;
struct CksumCtx {
  const u8 *pPK;
  int nPK;
  int iCol;
  i64 cksum;
};

static int fts5CksumCb(
  void *pCtx, 
  int iStream, 
  int iOff,
  const char *zToken, 
  int nToken, 
  int iSrc, 
  int nSrc
){
  CksumCtx *p = (CksumCtx *)pCtx;
  i64 cksum;

  cksum = fts5TermInstanceCksum(p->pPK, p->nPK, 
      (const u8 *)zToken, nToken, iStream, p->iCol, iOff
  );

  p->cksum = (p->cksum ^ cksum);
  return 0;
}

int sqlite4Fts5RowCksum(
  sqlite4 *db,                    /* Database handle */
  Fts5Info *pInfo,                /* Index description */
  Mem *pKey,                      /* Primary key blob */
  Mem *aArg,                      /* Array of column values */
  i64 *piCksum                    /* OUT: Checksum value */
){
  int i;
  int rc = SQLITE4_OK;
  CksumCtx sCtx;
  int nTnum = 0;
  u32 dummy = 0;

  sCtx.cksum = 0;

  sCtx.pPK = (const u8 *)sqlite4_value_blob(pKey);
  sCtx.nPK = sqlite4_value_bytes(pKey);
  nTnum = getVarint32(sCtx.pPK, dummy);
  sCtx.nPK -= nTnum;
  sCtx.pPK += nTnum;

  for(i=0; rc==SQLITE4_OK && i<pInfo->nCol; i++){
    sqlite4_value *pArg = (sqlite4_value *)(&aArg[i]);
    if( pArg->flags & MEM_Str ){
      const char *zText;
      int nText;

      zText = (const char *)sqlite4_value_text(pArg);
      nText = sqlite4_value_bytes(pArg);
      sCtx.iCol = i;
      rc = pInfo->pTokenizer->xTokenize(
          &sCtx, pInfo->p, zText, nText, fts5CksumCb
      );
    }
  }

  *piCksum = sCtx.cksum;
  return rc;
}

/*
** Obtain the primary key value from the entry cursor pToken->pCsr currently
** points to. Set *paPk to point to a buffer containing the PK, and *pnPk
** to the size of the buffer in bytes before returning.
**
** Return SQLITE4_OK if everything goes according to plan, or an error code
** if an error occurs. If an error occurs the final values of *paPk and *pnPk
** are undefined.
*/
static int fts5TokenPk(Fts5Token *p, const u8 **paPk, int *pnPk){
  int rc;

  if( p->pCsr ){
    const u8 *aKey;
    int nKey;

    rc = sqlite4KVCursorKey(p->pCsr, &aKey, &nKey);
    if( rc==SQLITE4_OK ){
      if( nKey<p->nPrefix || memcmp(p->aPrefix, aKey, p->nPrefix) ){
        rc = SQLITE4_NOTFOUND;
      }else if( p->bPrefix==0 ){
        *paPk = &aKey[p->nPrefix];
        *pnPk = nKey - p->nPrefix;
      }else{
        const u8 *z = &aKey[p->nPrefix];
        while( *(z++)!='\0' );
        *paPk = z;
        *pnPk = nKey - (z-aKey);
      }
    }
  }else{
    if( p->pPrefix ){
      *paPk = p->pPrefix->aPk;
      *pnPk = p->pPrefix->nPk;
      rc = SQLITE4_OK;
    }else{
      rc = SQLITE4_NOTFOUND;
    }
  }

  return rc;
}

static int fts5TokenAdvance(sqlite4 *db, Fts5Token *pToken){
  int rc;
  if( pToken->pCsr ){
    rc = sqlite4KVCursorNext(pToken->pCsr);
  }else if( pToken->pPrefix ){
    Fts5Prefix *pDel = pToken->pPrefix;
    pToken->pPrefix = pDel->pNext;
    sqlite4DbFree(db, pDel->aList);
    sqlite4DbFree(db, pDel);
    rc = SQLITE4_OK;
  }else{
    rc = SQLITE4_NOTFOUND;
  }
  return rc;
}

static int fts5TokenData(Fts5Token *pToken, const u8 **paData, int *pnData){
  int rc;
  if( pToken->pCsr ){
    rc = sqlite4KVCursorData(pToken->pCsr, 0, -1, paData, pnData);
  }else if( pToken->pPrefix ){
    *paData = pToken->pPrefix->aList;
    *pnData = pToken->pPrefix->nList;
    rc = SQLITE4_OK;
  }else{
    rc = SQLITE4_NOTFOUND;
  }

  return rc;
}

/*
** Compare keys (aLeft/nLeft) and (aRight/nRight) using the ordinary memcmp()
** method. Except, if either aLeft or aRight are NULL, consider them larger
** than all other values.
*/
static int fts5KeyCompare(
  const u8 *aLeft, int nLeft, 
  const u8 *aRight, int nRight
){
  int res;
  int nMin;

  res = (aLeft==0) - (aRight==0);
  if( res==0 ){
    nMin = (nLeft > nRight) ? nRight : nLeft;
    res = memcmp(aLeft, aRight, nMin);
  }
  return (res ? res : (nLeft-nRight));
}

static int fts5ListMerge(sqlite4 *db, Fts5List *p1, Fts5List *p2, int bFree){
  InstanceList in1;
  InstanceList in2;
  InstanceList out;

  memset(&out, 0, sizeof(InstanceList));
  if( p1->nData==0 && p2->nData==0 ) return SQLITE4_OK;
  out.nList = p1->nData+p2->nData;
  out.aList = sqlite4DbMallocRaw(db, out.nList);
  if( !out.aList ) return SQLITE4_NOMEM;
  fts5InstanceListInit(p1->aData, p1->nData, &in1);
  fts5InstanceListInit(p2->aData, p2->nData, &in2);

  fts5InstanceListNext(&in1);
  fts5InstanceListNext(&in2);

  while( fts5InstanceListEof(&in1)==0 || fts5InstanceListEof(&in2)==0 ){
    InstanceList *pAdv;

    if( fts5InstanceListEof(&in1) ){
      pAdv = &in2;
    }else if( fts5InstanceListEof(&in2) ){
      pAdv = &in1;
    }else if( in1.iCol==in2.iCol && in1.iOff==in2.iOff ){
      pAdv = &in1;
      fts5InstanceListNext(&in2);
    }else if( in1.iCol<in2.iCol || (in1.iCol==in2.iCol && in1.iOff<in2.iOff) ){
      pAdv = &in1;
    }else{
      pAdv = &in2;
    }

    fts5InstanceListAppend(&out, pAdv->iCol, pAdv->iStream, pAdv->iOff);
    fts5InstanceListNext(pAdv);
  }

  if( bFree ){
    sqlite4DbFree(db, p1->aData);
    sqlite4DbFree(db, p2->aData);
  }
  memset(p2, 0, sizeof(Fts5List));
  p1->aData = out.aList;
  p1->nData = out.iList;
  return SQLITE4_OK;
}

static void fts5PrefixMerge(Fts5Prefix **pp, Fts5Prefix *p2){
  Fts5Prefix *p1 = *pp;
  Fts5Prefix *pRet = 0;
  Fts5Prefix **ppWrite = &pRet;

  while( p1 || p2 ){
    Fts5Prefix **ppAdv = 0;
    if( p1==0 ){
      ppAdv = &p2;
    }else if( p2==0 ){
      ppAdv = &p1;
    }else{
      int res = fts5KeyCompare(p1->aPk, p1->nPk, p2->aPk, p2->nPk);
      assert( res!=0 );
      if( res<0 ){
        ppAdv = &p1;
      }else{
        ppAdv = &p2;
      }
    }

    *ppWrite = *ppAdv;
    ppWrite = &((*ppWrite)->pNext);
    *ppAdv = (*ppAdv)->pNext;
    *ppWrite = 0;
  }

  *pp = pRet;
}

static int fts5FindPrefixes(sqlite4 *db, Fts5Info *pInfo, Fts5Token *pToken){
  int rc = SQLITE4_OK;
  HashElem *pElem;
  Hash hash;

  assert( pToken->bPrefix );
  assert( pToken->aPrefix[pToken->nPrefix-1]!='\0' );
  sqlite4HashInit(db->pEnv, &hash, 1);

  do {
    const u8 *aData;
    int nData;
    const u8 *aPk;
    int nPk;

    rc = fts5TokenPk(pToken, &aPk, &nPk);
    if( rc==SQLITE4_OK ){
      rc = fts5TokenData(pToken, &aData, &nData);
    }
    if( rc==SQLITE4_OK ){
      Fts5Prefix *p;

      p = (Fts5Prefix *)sqlite4HashFind(&hash, (const char *)aPk, nPk);
      if( !p ){
        p = (Fts5Prefix *)sqlite4DbMallocZero(db, sizeof(Fts5Prefix) + nPk);
        if( !p ){
          rc = SQLITE4_NOMEM;
        }else{
          void *pFree;
          p->aPk = (u8 *)&p[1];
          p->nPk = nPk;
          memcpy(p->aPk, aPk, nPk);
          pFree = sqlite4HashInsert(&hash, (const char *)p->aPk, p->nPk, p);
          if( pFree ){
            assert( pFree==(void *)p );
            rc = SQLITE4_NOMEM;
            sqlite4DbFree(db, pFree);
          }
        }
      }

      if( rc==SQLITE4_OK ){
        int nReq = nData + sqlite4VarintLen(nData);
        while( (p->nList + nReq) > p->nAlloc ){
          int nAlloc = (p->nAlloc ? p->nAlloc*2 : 64);
          p->aList = sqlite4DbReallocOrFree(db, p->aList, nAlloc);
          if( !p->aList ){
            rc = SQLITE4_NOMEM;
            break;
          }
          p->nAlloc = nAlloc;
        }
      }

      if( rc==SQLITE4_OK ){
        p->nList += putVarint32(&p->aList[p->nList], nData);
        memcpy(&p->aList[p->nList], aData, nData);
        p->nList += nData;
      }
      
      if( rc==SQLITE4_OK ){
        rc = fts5TokenAdvance(db, pToken);
      }
    }
  }while( rc==SQLITE4_OK );
  if( rc==SQLITE4_NOTFOUND ) rc = SQLITE4_OK;

  if( rc==SQLITE4_OK ){
    Fts5List *aMerge;
    aMerge = (Fts5List *)sqlite4DbMallocZero(db, sizeof(Fts5List) * 32);
    if( !aMerge ) rc = SQLITE4_NOMEM;

    for(pElem=sqliteHashFirst(&hash); pElem; pElem=sqliteHashNext(pElem)){
      Fts5Prefix *p = (Fts5Prefix *)sqliteHashData(pElem);
      Fts5List list = {0, 0};
      int i = 0;
      int iLevel;

      memset(aMerge, 0, sizeof(Fts5List)*32);
      while( i<p->nList && rc==SQLITE4_OK ){
        u32 n;
        i += getVarint32(&p->aList[i], n);
        list.aData = &p->aList[i];
        list.nData = n;
        i += n;

        for(iLevel=0; rc==SQLITE4_OK && iLevel<32; iLevel++){
          if( aMerge[iLevel].aData==0 ){
            aMerge[iLevel] = list;
            break;
          }else{
            rc = fts5ListMerge(db, &list, &aMerge[iLevel], (iLevel>0));
          }
        }
        assert( iLevel<32 );
      }

      list.aData = 0;
      list.nData = 0;
      for(iLevel=0; rc==SQLITE4_OK && iLevel<32; iLevel++){
        rc = fts5ListMerge(db, &list, &aMerge[iLevel], (iLevel>0));
      }

      if( rc==SQLITE4_OK ){
        sqlite4DbFree(db, p->aList);
        p->aList = list.aData;
        p->nAlloc = p->nList = list.nData;
      }else{
        sqlite4DbFree(db, list.aData);
      }
    }

    sqlite4DbFree(db, aMerge);
  }

  if( rc==SQLITE4_OK ){
    Fts5Prefix **aMerge;
    Fts5Prefix *pPrefix = 0;

    aMerge = (Fts5Prefix **)sqlite4DbMallocZero(db, sizeof(Fts5List) * 32);
    if( !aMerge ){
      rc = SQLITE4_NOMEM;
    }else{
      int iLevel;
      for(pElem=sqliteHashFirst(&hash); pElem; pElem=sqliteHashNext(pElem)){
        pPrefix = (Fts5Prefix *)sqliteHashData(pElem);
        for(iLevel=0; iLevel<32; iLevel++){
          if( aMerge[iLevel] ){
            fts5PrefixMerge(&pPrefix, aMerge[iLevel]);
            aMerge[iLevel] = 0;
          }else{
            aMerge[iLevel] = pPrefix;
            break;
          }
        }
        assert( iLevel<32 );
      }
      pPrefix = 0;
      for(iLevel=0; iLevel<32; iLevel++){
        fts5PrefixMerge(&pPrefix, aMerge[iLevel]);
      }
      sqlite4HashClear(&hash);
      sqlite4DbFree(db, aMerge);
    }
    pToken->pPrefix = pPrefix;
  }

  for(pElem=sqliteHashFirst(&hash); pElem; pElem=sqliteHashNext(pElem)){
    Fts5Prefix *pPrefix = (Fts5Prefix *)sqliteHashData(pElem);
    sqlite4DbFree(db, pPrefix->aList);
    sqlite4DbFree(db, pPrefix);
  }
  sqlite4KVCursorClose(pToken->pCsr);
  pToken->pCsr = 0;

  return rc;
}

static int fts5OpenExprCursors(sqlite4 *db, Fts5Info *pInfo, Fts5ExprNode *p){
  int rc = SQLITE4_OK;
  if( p ){
    if( p->eType==TOKEN_PRIMITIVE ){
      KVStore *pStore = db->aDb[pInfo->iDb].pKV;
      Fts5Phrase *pPhrase = p->pPhrase;
      int iStr;

      for(iStr=0; rc==SQLITE4_OK && iStr<pPhrase->nStr; iStr++){
        Fts5Str *pStr = &pPhrase->aStr[iStr];
        int i;
        for(i=0; rc==SQLITE4_OK && i<pStr->nToken; i++){
          Fts5Token *pToken = &pStr->aToken[i];
          rc = sqlite4KVStoreOpenCursor(pStore, &pToken->pCsr);
          rc = sqlite4KVCursorSeek(
              pToken->pCsr, pToken->aPrefix, pToken->nPrefix, 1
          );
          if( rc==SQLITE4_INEXACT ) rc = SQLITE4_OK;
          if( rc==SQLITE4_OK && pToken->bPrefix ){
            rc = fts5FindPrefixes(db, pInfo, pToken);
          }
        }
      }
    }
    if( rc==SQLITE4_OK ) rc = fts5OpenExprCursors(db, pInfo, p->pLeft);
    if( rc==SQLITE4_OK ) rc = fts5OpenExprCursors(db, pInfo, p->pRight);
  }

  return rc;
}

/*
** Open a cursor for each token in the expression.
*/
static int fts5OpenCursors(sqlite4 *db, Fts5Info *pInfo, Fts5Cursor *pCsr){
  return fts5OpenExprCursors(db, pInfo, pCsr->pExpr->pRoot);
}

void sqlite4Fts5Close(sqlite4 *db, Fts5Cursor *pCsr){
  if( pCsr ){
    fts5ExpressionFree(db, pCsr->pExpr);
    sqlite4DbFree(db, pCsr->pIter);
    sqlite4DbFree(db, pCsr->aKey);
    sqlite4DbFree(db, pCsr->anRow);
    sqlite4DbFree(db, pCsr);
  }
}

static int fts5TokenAdvanceToMatch(
  InstanceList *p,
  InstanceList *pFirst,
  int iOff,
  int *pbEof
){
  int iReq = pFirst->iOff + iOff;

  while( p->iCol<pFirst->iCol || (p->iCol==pFirst->iCol && p->iOff < iReq) ){
    int bEof = fts5InstanceListNext(p);
    if( bEof ){
      *pbEof = 1;
      return 0;
    }
  }

  return (p->iCol==pFirst->iCol && p->iOff==iReq);
}

static int fts5StringFindInstances(sqlite4 *db, int iCol, Fts5Str *pStr){
  int i;
  int rc = SQLITE4_OK;
  int bEof = 0;
  int nByte = sizeof(InstanceList) * pStr->nToken;
  InstanceList *aIn;
  InstanceList out;

  pStr->nList = 0;
  memset(&out, 0, sizeof(InstanceList));

  aIn = (InstanceList *)sqlite4DbMallocZero(db, nByte);
  if( !aIn ) rc = SQLITE4_NOMEM;
  for(i=0; rc==SQLITE4_OK && i<pStr->nToken; i++){
    const u8 *aData;
    int nData;
    rc = fts5TokenData(&pStr->aToken[i], &aData, &nData);
    if( rc==SQLITE4_OK ){
      fts5InstanceListInit((u8 *)aData, nData, &aIn[i]);
      fts5InstanceListNext(&aIn[i]);
    }
  }

  /* Allocate the output list */
  if( rc==SQLITE4_OK ){
    int nReq = aIn[0].nList;
    if( nReq<=pStr->nListAlloc ){
      out.aList = pStr->aList;
      out.nList = pStr->nListAlloc;
    }else{
      pStr->aList = out.aList = sqlite4DbReallocOrFree(db, pStr->aList, nReq*2);
      pStr->nListAlloc = out.nList = nReq*2;
      if( out.aList==0 ) rc = SQLITE4_NOMEM;
    }
  }

  while( rc==SQLITE4_OK && bEof==0 ){
    for(i=1; i<pStr->nToken; i++){
      int bMatch = fts5TokenAdvanceToMatch(&aIn[i], &aIn[0], i, &bEof);
      if( bMatch==0 || bEof ) break;
    }
    if( i==pStr->nToken && (iCol<0 || aIn[0].iCol==iCol) ){
      /* Record a match here */
      fts5InstanceListAppend(&out, aIn[0].iCol, aIn[0].iStream, aIn[0].iOff);
    }
    bEof = fts5InstanceListNext(&aIn[0]);
  }

  pStr->nList = out.iList;
  sqlite4DbFree(db, aIn);

  return rc;
}

static int fts5IsNear(InstanceList *p1, InstanceList *p2, int nNear){
  if( p1->iCol==p2->iCol && p1->iOff<p2->iOff && (p1->iOff+nNear)>=p2->iOff ){
    return 1;
  }
  return 0;
}

static int fts5StringNearTrim(
  Fts5Str *pTrim,                 /* Trim this instance list */
  Fts5Str *pNext,                 /* According to this one */
  int nNear
){
  if( pNext->nList==0 ){
    pTrim->nList = 0;
  }else{
    int bEof = 0;
    int nTrail = nNear + (pNext->nToken-1) + 1;
    int nLead = nNear + (pTrim->nToken-1) + 1;

    InstanceList near;
    InstanceList in;
    InstanceList out;

    fts5InstanceListInit(pNext->aList, pNext->nList, &near);
    fts5InstanceListInit(pTrim->aList, pTrim->nList, &in);
    fts5InstanceListInit(pTrim->aList, pTrim->nList, &out);
    fts5InstanceListNext(&near);
    fts5InstanceListNext(&in);

    while( bEof==0 ){
      if( fts5IsNear(&near, &in, nTrail) 
       || fts5IsNear(&in, &near, nLead)
      ){
        /* The current position is a match. Append an entry to the output
        ** and advance the input cursor. */
        fts5InstanceListAppend(&out, in.iCol, in.iStream, in.iOff);
        bEof = fts5InstanceListNext(&in);
      }else{
        if( near.iCol<in.iCol || (near.iCol==in.iCol && near.iOff<in.iOff) ){
          bEof = fts5InstanceListNext(&near);
        }else if( near.iCol==in.iCol && near.iOff==in.iOff ){
          bEof = fts5InstanceListNext(&in);
          if( fts5IsNear(&near, &in, nTrail) ){
            fts5InstanceListAppend(&out, near.iCol, near.iStream, near.iOff);
          }
        }else{
          bEof = fts5InstanceListNext(&in);
        }
      }
    }

    pTrim->nList = out.iList;
  }
  return SQLITE4_OK;
}

/*
** This function tests if the cursors embedded in the Fts5Phrase object
** currently point to a match for the entire phrase. If so, *pbMatch
** is set to true before returning.
**
** If the cursors do not point to a match, then *ppAdvance is set to
** the token of the individual cursor that should be advanced before
** retrying this function.
*/
static int fts5PhraseIsMatch(
  sqlite4 *db,                    /* Database handle */
  Fts5Phrase *pPhrase,            /* Phrase to test */
  int *pbMatch,                   /* OUT: True for a match, false otherwise */
  Fts5Token **ppAdvance           /* OUT: Token to advance before retrying */
){
  const u8 *aPk1 = 0;
  int nPk1 = 0;
  int rc = SQLITE4_OK;
  int i;

  *pbMatch = 0;
  *ppAdvance = &pPhrase->aStr[0].aToken[0];

  rc = fts5TokenPk(*ppAdvance, &aPk1, &nPk1);
  for(i=0; rc==SQLITE4_OK && i<pPhrase->nStr; i++){
    int j;
    for(j=(i==0); j<pPhrase->aStr[i].nToken; j++){
      const u8 *aPk = 0;
      int nPk = 0;
      Fts5Token *pToken = &pPhrase->aStr[i].aToken[j];
      rc = fts5TokenPk(pToken, &aPk, &nPk);
      if( rc==SQLITE4_OK ){
        int res = fts5KeyCompare(aPk1, nPk1, aPk, nPk);
        if( res<0 ){
          return SQLITE4_OK;
        }
        if( res>0 ){
          *ppAdvance = pToken;
          return SQLITE4_OK;
        }
      }
    }
  }

  /* At this point, it is established that all of the token cursors in the
  ** phrase point to an entry with the same primary key. Now figure out if
  ** the various string constraints are met. Along the way, synthesize a 
  ** position list for each Fts5Str object.  */
  for(i=0; rc==SQLITE4_OK && i<pPhrase->nStr; i++){
    Fts5Str *pStr = &pPhrase->aStr[i];
    rc = fts5StringFindInstances(db, pPhrase->iCol, pStr);
  }

  /* Trim the instance lists according to any NEAR constraints.  */
  for(i=1; rc==SQLITE4_OK && i<pPhrase->nStr; i++){
    int n = pPhrase->aiNear[i-1];
    rc = fts5StringNearTrim(&pPhrase->aStr[i], &pPhrase->aStr[i-1], n);
  }
  for(i=pPhrase->nStr-1; rc==SQLITE4_OK && i>0; i--){
    int n = pPhrase->aiNear[i-1];
    rc = fts5StringNearTrim(&pPhrase->aStr[i-1], &pPhrase->aStr[i], n);
  }

  *pbMatch = (pPhrase->aStr[0].nList>0);
  return rc;
}

static int fts5PhraseAdvanceToMatch(sqlite4 *db, Fts5Phrase *pPhrase){
  int rc;
  do {
    int bMatch;
    Fts5Token *pAdvance = 0;
    rc = fts5PhraseIsMatch(db, pPhrase, &bMatch, &pAdvance);
    if( rc!=SQLITE4_OK || bMatch ) break;
    rc = fts5TokenAdvance(db, pAdvance);
  }while( rc==SQLITE4_OK );
  return rc;
}

static int fts5ExprAdvance(sqlite4 *db, Fts5ExprNode *p, int bFirst){
  int rc = SQLITE4_OK;

  switch( p->eType ){
    case TOKEN_PRIMITIVE: {
      Fts5Phrase *pPhrase = p->pPhrase;
      if( bFirst==0 ){
        rc = fts5TokenAdvance(db, &pPhrase->aStr[0].aToken[0]);
      }
      if( rc==SQLITE4_OK ) rc = fts5PhraseAdvanceToMatch(db, pPhrase);
      if( rc==SQLITE4_OK ){
        rc = fts5TokenPk(&pPhrase->aStr[0].aToken[0], &p->aPk, &p->nPk);
      }else{
        p->aPk = 0;
        p->nPk = 0;
        if( rc==SQLITE4_NOTFOUND ) rc = SQLITE4_OK;
      }
      break;
    }

    case TOKEN_AND:
      p->aPk = 0;
      p->nPk = 0;
      rc = fts5ExprAdvance(db, p->pLeft, bFirst);
      if( rc==SQLITE4_OK ) rc = fts5ExprAdvance(db, p->pRight, bFirst);
      while( rc==SQLITE4_OK && p->pLeft->aPk && p->pRight->aPk ){
        int res = fts5KeyCompare(
            p->pLeft->aPk, p->pLeft->nPk, p->pRight->aPk, p->pRight->nPk
        );
        if( res<0 ){
          rc = fts5ExprAdvance(db, p->pLeft, 0);
        }else if( res>0 ){
          rc = fts5ExprAdvance(db, p->pRight, 0);
        }else{
          p->aPk = p->pLeft->aPk;
          p->nPk = p->pLeft->nPk;
          break;
        }
      }
      break;

    case TOKEN_OR: {
      int res = 0;
      if( bFirst==0 ){
        res = fts5KeyCompare(
            p->pLeft->aPk, p->pLeft->nPk, p->pRight->aPk, p->pRight->nPk
        );
      }
        
      if( res<=0 ) rc = fts5ExprAdvance(db, p->pLeft, bFirst);
      if( rc==SQLITE4_OK && res>=0 ){
        rc = fts5ExprAdvance(db, p->pRight, bFirst);
      }

      res = fts5KeyCompare(
          p->pLeft->aPk, p->pLeft->nPk, p->pRight->aPk, p->pRight->nPk
      );
      if( res>0 ){
        p->aPk = p->pRight->aPk;
        p->nPk = p->pRight->nPk;
      }else{
        p->aPk = p->pLeft->aPk;
        p->nPk = p->pLeft->nPk;
      }
      assert( p->aPk!=0 || (p->pLeft->aPk==0 && p->pRight->aPk==0) );
      break;
    }


    default: assert( p->eType==TOKEN_NOT );

      p->aPk = 0;
      p->nPk = 0;

      rc = fts5ExprAdvance(db, p->pLeft, bFirst);
      if( bFirst && rc==SQLITE4_OK ){
        rc = fts5ExprAdvance(db, p->pRight, bFirst);
      }

      while( rc==SQLITE4_OK && p->pLeft->aPk && p->pRight->aPk ){
        int res = fts5KeyCompare(
            p->pLeft->aPk, p->pLeft->nPk, p->pRight->aPk, p->pRight->nPk
        );
        if( res<0 ){
          break;
        }else if( res>0 ){
          rc = fts5ExprAdvance(db, p->pRight, 0);
        }else{
          rc = fts5ExprAdvance(db, p->pLeft, 0);
        }
      }

      p->aPk = p->pLeft->aPk;
      p->nPk = p->pLeft->nPk;
      break;
  }

  assert( rc!=SQLITE4_NOTFOUND );
  return rc;
}

int sqlite4Fts5Next(Fts5Cursor *pCsr){
  sqlite4DbFree(pCsr->db, pCsr->pSz);
  pCsr->pSz = 0;
  pCsr->bMemValid = 0;
  return fts5ExprAdvance(pCsr->db, pCsr->pExpr->pRoot, 0);
}

int sqlite4Fts5Open(
  sqlite4 *db,                    /* Database handle */
  Fts5Info *pInfo,                /* Index description */
  const char *zMatch,             /* Match expression */
  int bDesc,                      /* True to iterate in desc. order of PK */
  Fts5Cursor **ppCsr,             /* OUT: New FTS cursor object */
  char **pzErr                    /* OUT: Error message */
){
  int rc = SQLITE4_OK;
  Fts5Cursor *pCsr;
  int nMatch = sqlite4Strlen30(zMatch);

  pCsr = sqlite4DbMallocZero(db, sizeof(Fts5Cursor) + nMatch + 1);

  if( !pCsr ){
    rc = SQLITE4_NOMEM;
  }else{
    pCsr->zExpr = (char *)&pCsr[1];
    memcpy(pCsr->zExpr, zMatch, nMatch);
    pCsr->pInfo = pInfo;
    pCsr->db = db;
    rc = fts5ParseExpression(db, pInfo->pTokenizer, pInfo->p, 
        pInfo->iRoot, pInfo->azCol, pInfo->nCol, zMatch, &pCsr->pExpr, pzErr
    );
  }

  if( rc==SQLITE4_OK ){
    /* Open a KV cursor for each term in the expression. Set each cursor
    ** to point to the first entry in the range it will scan.  */
    rc = fts5OpenCursors(db, pInfo, pCsr);
  }
  if( rc!=SQLITE4_OK ){
    sqlite4Fts5Close(db, pCsr);
    pCsr = 0;
  }else{
    rc = fts5ExprAdvance(db, pCsr->pExpr->pRoot, 1);
  }
  *ppCsr = pCsr;
  return rc;
}

/*
** Return true if the cursor passed as the second argument currently points
** to a valid entry, or false otherwise.
*/
int sqlite4Fts5Valid(Fts5Cursor *pCsr){
  return( pCsr->pExpr->pRoot->aPk!=0 );
}

int sqlite4Fts5Pk(
  Fts5Cursor *pCsr, 
  int iTbl,
  KVByteArray **paKey,
  KVSize *pnKey
){
  int i;
  int nReq;
  const u8 *aPk;
  int nPk;

  aPk = pCsr->pExpr->pRoot->aPk;
  nPk = pCsr->pExpr->pRoot->nPk;

  nReq = sqlite4VarintLen(iTbl) + nPk;
  if( nReq>pCsr->nKeyAlloc ){
    pCsr->aKey = sqlite4DbReallocOrFree(pCsr->db, pCsr->aKey, nReq*2);
    if( !pCsr->aKey ) return SQLITE4_NOMEM;
    pCsr->nKeyAlloc = nReq*2;
  }

  i = putVarint32(pCsr->aKey, iTbl);
  memcpy(&pCsr->aKey[i], aPk, nPk);

  *paKey = pCsr->aKey;
  *pnKey = nReq;
  return SQLITE4_OK;
}

int sqlite4_mi_column_count(sqlite4_context *pCtx, int *pn){
  int rc = SQLITE4_OK;
  if( pCtx->pFts ){
    *pn = pCtx->pFts->pInfo->nCol;
  }else{
    rc = SQLITE4_MISUSE;
  }
  return rc;
}

int sqlite4_mi_phrase_count(sqlite4_context *pCtx, int *pn){
  int rc = SQLITE4_OK;
  if( pCtx->pFts ){
    *pn = pCtx->pFts->pExpr->nPhrase;
  }else{
    rc = SQLITE4_MISUSE;
  }
  return rc;
}

int sqlite4_mi_stream_count(sqlite4_context *pCtx, int *pn){
  int rc = SQLITE4_OK;
  Fts5Cursor *pCsr = pCtx->pFts;
  if( pCsr ){
    rc = fts5CsrLoadGlobal(pCtx->pFts);
    if( rc==SQLITE4_OK ) *pn = pCsr->pGlobal->nStream;
  }else{
    rc = SQLITE4_MISUSE;
  }
  return rc;
}

static int fts5GetSize(Fts5Size *pSz, int iC, int iS){
  int nToken = 0;
  int i;

  if( iC<0 && iS<0 ){
    int nFin = pSz->nCol * pSz->nStream;
    for(i=0; i<nFin; i++) nToken += pSz->aSz[i];
  }else if( iC<0 ){
    for(i=0; i<pSz->nCol; i++) nToken += pSz->aSz[i*pSz->nStream + iS];
  }else if( iS<0 ){
    for(i=0; i<pSz->nStream; i++) nToken += pSz->aSz[pSz->nStream*iC + iS];
  }else if( iC<pSz->nCol && iS<pSz->nStream ){
    nToken = pSz->aSz[iC * pSz->nStream + iS];
  }

  return nToken;
}

int sqlite4_mi_size(sqlite4_context *pCtx, int iC, int iS, int *pn){
  int rc = SQLITE4_OK;
  Fts5Cursor *pCsr = pCtx->pFts;

  if( pCsr==0 ){
    rc = SQLITE4_MISUSE;
  }else{
    rc = fts5CsrLoadSz(pCsr);
    if( rc==SQLITE4_OK ){
      *pn = fts5GetSize(pCsr->pSz, iC, iS);
    }
  }
  return rc;
}

int sqlite4_mi_total_size(sqlite4_context *pCtx, int iC, int iS, int *pn){
  int rc = SQLITE4_OK;
  Fts5Cursor *pCsr = pCtx->pFts;

  if( pCsr==0 ){
    rc = SQLITE4_MISUSE;
  }else{
    rc = fts5CsrLoadGlobal(pCsr);
    if( rc==SQLITE4_OK ){
      *pn = fts5GetSize(pCsr->pGlobal, iC, iS);
    }
  }
  return rc;
}

int sqlite4_mi_total_rows(sqlite4_context *pCtx, int *pn){
  int rc = SQLITE4_OK;
  Fts5Cursor *pCsr = pCtx->pFts;
  if( pCsr==0 ){
    rc = SQLITE4_MISUSE;
  }else{
    rc = fts5CsrLoadGlobal(pCsr);
    if( rc==SQLITE4_OK ) *pn = pCsr->nGlobal;
  }
  return rc;
}

int sqlite4_mi_column_value(
  sqlite4_context *pCtx, 
  int iCol, 
  sqlite4_value **ppVal
){
  int rc = SQLITE4_OK;
  Fts5Cursor *pCsr = pCtx->pFts;
  if( pCsr==0 ){
    rc = SQLITE4_MISUSE;
  }else{
    if( pCsr->bMemValid==0 ){
      sqlite4 *db = pCsr->db;

      Fts5Info *pInfo = pCsr->pInfo;
      if( pCsr->aMem==0 ){
        int nByte = sizeof(Mem) * pInfo->nCol;
        pCsr->aMem = (Mem *)sqlite4DbMallocZero(db, nByte);
        if( pCsr->aMem==0 ){
          rc = SQLITE4_NOMEM;
        }else{
          int i;
          for(i=0; i<pInfo->nCol; i++){
            pCsr->aMem[i].db = db;
          }
        }
      }

      if( pCsr->pCsr==0 && rc==SQLITE4_OK ){
        KVStore *pStore = db->aDb[pInfo->iDb].pKV;
        rc = sqlite4KVStoreOpenCursor(pStore, &pCsr->pCsr);
      }

      if( rc==SQLITE4_OK ){
        u8 *aKey = 0; int nKey;     /* Primary key for current row */
        const u8 *aData; int nData; /* Data record for current row */

        rc = sqlite4Fts5Pk(pCsr, pInfo->iTbl, &aKey, &nKey);
        if( rc==SQLITE4_OK ){
          rc = sqlite4KVCursorSeek(pCsr->pCsr, aKey, nKey, 0);
          if( rc==SQLITE4_NOTFOUND ){
            rc = SQLITE4_CORRUPT_BKPT;
          }
        }

        if( rc==SQLITE4_OK ){
          rc = sqlite4KVCursorData(pCsr->pCsr, 0, -1, &aData, &nData);
        }

        if( rc==SQLITE4_OK ){
          int i;
          ValueDecoder *pCodec;   /* The decoder object */

          rc = sqlite4VdbeCreateDecoder(db, aData, nData, pInfo->nCol, &pCodec);
          for(i=0; rc==SQLITE4_OK && i<pInfo->nCol; i++){
            rc = sqlite4VdbeDecodeValue(pCodec, i, 0, &pCsr->aMem[i]);
          }
          sqlite4VdbeDestroyDecoder(pCodec);
        }

        if( rc==SQLITE4_OK ) pCsr->bMemValid = 1;
      }
    }

    if( rc==SQLITE4_OK ){
      assert( pCsr->bMemValid );
      *ppVal = &pCsr->aMem[iCol];
    }
  }

  return rc;
}

int sqlite4_mi_tokenize(
  sqlite4_context *pCtx,
  const char *zText,
  int nText,
  void *p,
  int(*x)(void *, int, int, const char *, int, int, int)
){
  int rc = SQLITE4_OK;
  Fts5Cursor *pCsr = pCtx->pFts;

  if( pCsr==0 ){
    rc = SQLITE4_MISUSE;
  }else{
    Fts5Info *pInfo = pCsr->pInfo;
    rc = pInfo->pTokenizer->xTokenize(p, pInfo->p, zText, nText, x);
  }
  return rc;
}

static Fts5Str *fts5FindStr(Fts5ExprNode *p, int *piStr){
  Fts5Str *pRet = 0;
  if( p->eType==TOKEN_PRIMITIVE ){
    int iStr = *piStr;
    if( iStr<p->pPhrase->nStr ){
      pRet = &p->pPhrase->aStr[iStr];
    }else{
      *piStr = iStr - p->pPhrase->nStr;
    }
  }else{
    pRet = fts5FindStr(p->pLeft, piStr);
    if( pRet==0 ) pRet = fts5FindStr(p->pRight, piStr);
  }
  return pRet;
}

int sqlite4_mi_match_count(
  sqlite4_context *pCtx, 
  int iC,
  int iS,
  int iPhrase,
  int *pnMatch
){
  int rc = SQLITE4_OK;
  Fts5Cursor *pCsr = pCtx->pFts;
  if( pCsr ){
    int nMatch = 0;
    Fts5Str *pStr;
    int iCopy = iPhrase;
    InstanceList sList;

    pStr = fts5FindStr(pCsr->pExpr->pRoot, &iCopy);
    assert( pStr );

    fts5InstanceListInit(pStr->aList, pStr->nList, &sList);
    while( 0==fts5InstanceListNext(&sList) ){
      if( (iC<0 || sList.iCol==iC) && (iS<0 || sList.iStream==iS) ) nMatch++;
    }
    *pnMatch = nMatch;
  }else{
    rc = SQLITE4_MISUSE;
  }
  return rc;
}

int sqlite4_mi_match_offset(
  sqlite4_context *pCtx, 
  int iCol, 
  int iPhrase, 
  int iMatch, 
  int *piOff
){
  return SQLITE4_OK;
}

int sqlite4_mi_total_match_count(
  sqlite4_context *pCtx,
  int iCol,
  int iPhrase,
  int *pnMatch,
  int *pnDoc,
  int *pnRelevant
){
  return SQLITE4_OK;
}

static void fts5StrLoadRowcounts(Fts5Str *pStr, int nStream, int *anRow){
  u32 mask = 0;
  int iPrevCol = 0;
  InstanceList sList;

  fts5InstanceListInit(pStr->aList, pStr->nList, &sList);
  while( 0==fts5InstanceListNext(&sList) ){
    if( sList.iCol!=iPrevCol ) mask = 0;
    if( (mask & (1<<sList.iStream))==0 ){
      anRow[sList.iCol * nStream + sList.iStream]++;
      mask |= (1<<sList.iStream);
      iPrevCol = sList.iCol;
    }
  }
}

static int fts5ExprLoadRowcounts(
  sqlite4 *db, 
  Fts5Info *pInfo,
  int nStream,
  Fts5ExprNode *pNode, 
  int **panRow
){
  int rc = SQLITE4_OK;

  if( pNode ){
    if( pNode->eType==TOKEN_PRIMITIVE ){
      int *anRow = *panRow;
      Fts5Phrase *pPhrase = pNode->pPhrase;

      rc = fts5ExprAdvance(db, pNode, 1);
      while( rc==SQLITE4_OK ){
        int nIncr =  pInfo->nCol * nStream;      /* Values for each Fts5Str */
        int i;
        for(i=0; i<pPhrase->nStr; i++){
          fts5StrLoadRowcounts(&pPhrase->aStr[i], nStream, &anRow[i*nIncr]);
        }
        rc = fts5ExprAdvance(db, pNode, 0);
      }

      *panRow = &anRow[pInfo->nCol * nStream * pPhrase->nStr];
    }

    if( rc==SQLITE4_OK ){
      rc = fts5ExprLoadRowcounts(db, pInfo, nStream, pNode->pLeft, panRow);
    }
    if( rc==SQLITE4_OK ){
      rc = fts5ExprLoadRowcounts(db, pInfo, nStream, pNode->pRight, panRow);
    }
  }

  return rc;
}

static int fts5CsrLoadRowcounts(Fts5Cursor *pCsr){
  int rc = SQLITE4_OK;

  if( pCsr->anRow==0 ){
    int nStream = pCsr->pGlobal->nStream;
    sqlite4 *db = pCsr->db;
    Fts5Expr *pCopy;
    Fts5Expr *pExpr = pCsr->pExpr;
    Fts5Info *pInfo = pCsr->pInfo;
    int *anRow;

    pCsr->anRow = anRow = (int *)sqlite4DbMallocZero(db, 
        pExpr->nPhrase * pInfo->nCol * pCsr->pGlobal->nStream * sizeof(int)
    );
    if( !anRow ) return SQLITE4_NOMEM;

    rc = fts5ParseExpression(db, pInfo->pTokenizer, pInfo->p, 
        pInfo->iRoot, pInfo->azCol, pInfo->nCol, pCsr->zExpr, &pCopy, 0
    );
    if( rc==SQLITE4_OK ){
      rc = fts5OpenExprCursors(db, pInfo, pExpr->pRoot);
    }
    if( rc==SQLITE4_OK ){
      rc = fts5ExprLoadRowcounts(db, pInfo, nStream, pCopy->pRoot, &anRow);
    }

    fts5ExpressionFree(db, pCopy);
  }

  return rc;
}

int sqlite4_mi_row_count(
  sqlite4_context *pCtx,          /* Context object passed to mi function */
  int iC,                         /* Specific column (or -ve for all columns) */
  int iS,                         /* Specific stream (or -ve for all streams) */
  int iP,                         /* Specific phrase */
  int *pn                         /* Total number of rows containing C/S/P */
){
  int rc = SQLITE4_OK;
  Fts5Cursor *pCsr = pCtx->pFts;
  if( pCsr==0 ){
    rc = SQLITE4_MISUSE;
  }else{
    rc = fts5CsrLoadGlobal(pCsr);
    if( rc==SQLITE4_OK ) rc = fts5CsrLoadRowcounts(pCsr);

    if( rc==SQLITE4_OK ){
      int i;
      int nRow = 0;
      int nStream = pCsr->pGlobal->nStream;
      int nCol = pCsr->pInfo->nCol;
      int *aRow = &pCsr->anRow[iP * nStream * nCol];

      if( iC<0 && iS<0 ){
        int nFin = nCol * nStream;
        for(i=0; i<nFin; i++) nRow += aRow[i];
      }else if( iC<0 ){
        for(i=0; i<nCol; i++) nRow += aRow[i*nStream + iS];
      }else if( iS<0 ){
        for(i=0; i<nStream; i++) nRow += aRow[nStream*iC + iS];
      }else if( iC<nCol && iS<nStream ){
        nRow = aRow[iC * nStream + iS];
      }

      *pn = nRow;
    }
  }
  return rc;
}

static void fts5IterSetCurrent(Fts5MatchIter *pIter, int nList){
  InstanceList *pBest = 0;
  int i;

  for(i=0; i<nList; i++){
    InstanceList *p = &pIter->aList[i];
    if( fts5InstanceListEof(p)==0 ){
      if( (pBest==0)
       || (p->iCol<pBest->iCol)
       || (p->iCol==pBest->iCol && p->iOff<pBest->iOff)
      ){
        pBest = p;
      }
    }
  }

  if( pBest==0 ){
    pIter->iCurrent = -1;
  }else{
    pIter->iCurrent = pBest - pIter->aList;
  }
}

int sqlite4_mi_match_detail(
  sqlite4_context *pCtx,          /* Context object passed to mi function */
  int iMatch,                     /* Index of match */
  int *piOff,                     /* OUT: Token offset of match */
  int *piC,                       /* OUT: Column number of match iMatch */
  int *piS,                       /* OUT: Stream number of match iMatch */
  int *piP                        /* OUT: Phrase number of match iMatch */
){
  int rc = SQLITE4_OK;
  Fts5Cursor *pCsr = pCtx->pFts;
  if( pCsr==0 ){
    rc = SQLITE4_MISUSE;
  }else{
    int nPhrase = pCsr->pExpr->nPhrase;
    Fts5MatchIter *pIter = pCsr->pIter;
    if( pIter==0 ){
      pCsr->pIter = pIter = (Fts5MatchIter *)sqlite4DbMallocZero(
          pCsr->db, sizeof(Fts5MatchIter) + sizeof(InstanceList)*nPhrase
      );
      if( pIter ){
        pIter->aList = (InstanceList *)&pIter[1];
      }else{
        rc = SQLITE4_NOMEM;
      }
    }

    if( rc==SQLITE4_OK && (pIter->bValid==0 || iMatch<pIter->iMatch) ){
      int i;
      for(i=0; i<pCsr->pExpr->nPhrase; i++){
        Fts5Str *pStr = pCsr->pExpr->apPhrase[i];
        fts5InstanceListInit(pStr->aList, pStr->nList, &pIter->aList[i]);
        fts5InstanceListNext(&pIter->aList[i]);
      }

      pIter->iMatch = 0;
      fts5IterSetCurrent(pIter, pCsr->pExpr->nPhrase);
    }

    if( rc==SQLITE4_OK ){
      assert( pIter->iMatch<=iMatch );
      while( pIter->iCurrent>=0 && pIter->iMatch<iMatch ){
        fts5InstanceListNext(&pIter->aList[pIter->iCurrent]);
        fts5IterSetCurrent(pIter, pCsr->pExpr->nPhrase);
      }
      if( pIter->iCurrent<0 ){
        rc = SQLITE4_NOTFOUND;
      }else{
        InstanceList *p = &pIter->aList[pIter->iCurrent];
        *piOff = p->iOff;
        *piC = p->iCol;
        *piS = p->iStream;
        *piP = pIter->iCurrent;
      }
    }
  }
  return rc;
}

/**************************************************************************
***************************************************************************
** Below this point is test code.
*/
#ifdef SQLITE4_TEST
static int fts5PrintExprNode(sqlite4 *, const char **, Fts5ExprNode *, char **);
static int fts5PrintExprNodeParen(
  sqlite4 *db, const char **azCol,
  Fts5ExprNode *pNode, 
  char **pzRet
){
  int bParen = (pNode->eType!=TOKEN_PRIMITIVE || pNode->pPhrase->nStr>1);
  sqlite4_env *pEnv = sqlite4_db_env(db);
  char *zRet = *pzRet;

  if( bParen ) zRet = sqlite4_mprintf(pEnv, "%z(", zRet);
  fts5PrintExprNode(db, azCol, pNode, &zRet);
  if( bParen ) zRet = sqlite4_mprintf(pEnv, "%z)", zRet);

  *pzRet = zRet;
  return SQLITE4_OK;
}
static int fts5PrintExprNode(
  sqlite4 *db, 
  const char **azCol,
  Fts5ExprNode *pNode, 
  char **pzRet
){
  sqlite4_env *pEnv = sqlite4_db_env(db);
  char *zRet = *pzRet;

  assert(
      pNode->eType==TOKEN_AND || pNode->eType==TOKEN_OR
   || pNode->eType==TOKEN_NOT || pNode->eType==TOKEN_PRIMITIVE
  );
  assert( (pNode->eType==TOKEN_PRIMITIVE)==(pNode->pPhrase!=0) );

  if( pNode->eType==TOKEN_PRIMITIVE ){
    int iStr;
    Fts5Phrase *pPhrase = pNode->pPhrase;
    if( pPhrase->iCol>=0 ){
        zRet = sqlite4_mprintf(pEnv, "%z\"%s\":", zRet, azCol[pPhrase->iCol]);
    }
    for(iStr=0; iStr<pPhrase->nStr; iStr++){
      int iToken;
      Fts5Str *pStr = &pPhrase->aStr[iStr];
      if( iStr>0 ){
        zRet = sqlite4_mprintf(
            pEnv, "%z NEAR/%d ", zRet, pPhrase->aiNear[iStr-1]
        );
      }
      for(iToken=0; iToken<pStr->nToken; iToken++){
        int nRet = sqlite4Strlen30(zRet);
        const char *z = pStr->aToken[iToken].z;
        int n = pStr->aToken[iToken].n;
        int i;

        zRet = (char *)sqlite4_realloc(pEnv, zRet, nRet + n*2+4);
        if( iToken>0 ) zRet[nRet++] = '+';
        zRet[nRet++] = '"';

        for(i=0; i<n; i++){
          if( z[i]=='"' ) zRet[nRet++] = '"';
          zRet[nRet++] = z[i];
        }
        zRet[nRet++] = '"';
        if( pStr->aToken[iToken].bPrefix ){
          zRet[nRet++] = '*';
        }
        zRet[nRet++] = '\0';
      }
    }
  }else{
    fts5PrintExprNodeParen(db, azCol, pNode->pLeft, &zRet);
    switch( pNode->eType ){
      case TOKEN_AND:
        zRet = sqlite4_mprintf(pEnv, "%z AND ", zRet);
        break;
      case TOKEN_OR:
        zRet = sqlite4_mprintf(pEnv, "%z OR ", zRet);
        break;
      case TOKEN_NOT:
        zRet = sqlite4_mprintf(pEnv, "%z NOT ", zRet);
        break;
    }
    fts5PrintExprNodeParen(db, azCol, pNode->pRight, &zRet);
  }

  *pzRet = zRet;
  return SQLITE4_OK;
}
static int fts5PrintExpr(
  sqlite4 *db, 
  const char **azCol, 
  Fts5Expr *pExpr, 
  char **pzRet
){
  return fts5PrintExprNode(db, azCol, pExpr->pRoot, pzRet);
}

/*
** A user defined function used to test the fts5 expression parser. As follows:
**
**   fts5_parse_expr(<tokenizer>, <expr>);
*/
static void fts5_parse_expr(
  sqlite4_context *pCtx, 
  int nVal, 
  sqlite4_value **aVal
){
  int rc;
  Fts5Expr *pExpr = 0;
  Fts5Tokenizer *pTok;
  sqlite4_tokenizer *p = 0;
  sqlite4 *db;

  const char *zTokenizer;
  const char *zExpr;
  const char *zTbl;
  char *zErr = 0;
  char *zRet = 0;
  const char **azCol = 0;
  int nCol = 0;
  sqlite4_stmt *pStmt = 0;

  db = sqlite4_context_db_handle(pCtx);
  assert( nVal==3 );
  zTokenizer = (const char *)sqlite4_value_text(aVal[0]);
  zExpr = (const char *)sqlite4_value_text(aVal[1]);
  zTbl = (const char *)sqlite4_value_text(aVal[2]);

  if( sqlite4Strlen30(zTbl)>0 ){
    int i;
    char *zSql = sqlite4MPrintf(db, "SELECT * FROM '%q'", zTbl);
    rc = sqlite4_prepare(db, zSql, -1, &pStmt, 0);
    sqlite4DbFree(db, zSql);
    if( rc!=SQLITE4_OK ){
      sqlite4_result_error(pCtx, sqlite4_errmsg(db), -1);
      sqlite4_result_error_code(pCtx, rc);
      return;
    }
    nCol = sqlite4_column_count(pStmt);
    azCol = sqlite4DbMallocZero(db, sizeof(char *)*nCol);
    for(i=0; i<nCol; i++){
      azCol[i] = sqlite4_column_name(pStmt, i);
    }
  }

  pTok = fts5FindTokenizer(db, zTokenizer);
  if( pTok==0 ){
    zErr = sqlite4MPrintf(db, "no such tokenizer: %s", zTokenizer);
    goto fts5_parse_expr_out;
  }else{
    rc = pTok->xCreate(pTok->pCtx, 0, 0, &p);
    if( rc!=SQLITE4_OK ){
      zErr = sqlite4MPrintf(db, "error creating tokenizer: %d", rc);
      goto fts5_parse_expr_out;
    }
  }

  rc = fts5ParseExpression(
      db, pTok, p, 0, (char **)azCol, nCol, zExpr, &pExpr, &zErr);
  if( rc!=SQLITE4_OK ){
    if( zErr==0 ){
      zErr = sqlite4MPrintf(db, "error parsing expression: %d", rc);
    }
    goto fts5_parse_expr_out;
  }

  fts5PrintExpr(db, azCol, pExpr, &zRet);
  sqlite4_result_text(pCtx, zRet, -1, SQLITE4_TRANSIENT);
  fts5ExpressionFree(db, pExpr);
  sqlite4_free(sqlite4_db_env(db), zRet);

 fts5_parse_expr_out:
  if( p ) pTok->xDestroy(p);
  sqlite4DbFree(db, azCol);
  sqlite4_finalize(pStmt);
  if( zErr ){
    sqlite4_result_error(pCtx, zErr, -1);
    sqlite4DbFree(db, zErr);
  }
}
#endif

/*
** Register the default FTS5 tokenizer and functions with handle db.
*/
int sqlite4InitFts5(sqlite4 *db){
#ifdef SQLITE4_TEST
  int rc = sqlite4_create_function(
      db, "fts5_parse_expr", 3, SQLITE4_UTF8, 0, fts5_parse_expr, 0, 0
  );
  if( rc!=SQLITE4_OK ) return rc;
#endif
  return sqlite4InitFts5Func(db);
}

