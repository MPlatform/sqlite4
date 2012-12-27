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
** Context object used by expression parser.
*/
typedef struct Fts5Parser Fts5Parser;
typedef struct Fts5ParserToken Fts5ParserToken;

typedef struct Fts5Token Fts5Token;
typedef struct Fts5Str Fts5Str;
typedef struct Fts5Phrase Fts5Phrase;
typedef struct Fts5ExprNode Fts5ExprNode;
typedef struct Fts5Expr Fts5Expr;


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

struct Fts5Token {
  /* TODO: The first three members are redundant, since they can be encoded
  ** in the aPrefix[]/nPrefix key.  */
  int bPrefix;                    /* True for a prefix search */
  int n;                          /* Size of z[] in bytes */
  char *z;                        /* Token value */

  KVCursor *pCsr;                 /* Cursor to iterate thru entries for token */
  KVByteArray *aPrefix;           /* KV prefix to iterate through */
  KVSize nPrefix;                 /* Size of aPrefix in bytes */
};

struct Fts5Str {
  Fts5Token *aToken;
  int nToken;
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
};

struct Fts5Expr {
  Fts5ExprNode *pRoot;
};


/*
** FTS5 specific cursor data.
*/
struct Fts5Cursor {
  sqlite4 *db;
  Fts5Info *pInfo;
  Fts5Expr *pExpr;                /* MATCH expression for this cursor */

  KVByteArray *aKey;              /* Buffer for primary key */
  int nKeyAlloc;                  /* Bytes allocated at aKey[] */
};

/*
** Return true if argument c is one of the special non-whitespace 
** characters that ends an unquoted expression token. 
*/
static int fts5IsSpecial(char c){
  return (c==':' || c=='(' || c==')' || c=='+' || c=='"');
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
  int iWeight, 
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
  int iWeight, 
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

static void fts5PhraseFree(sqlite4 *db, Fts5Phrase *p){
  if( p ){
    int i;
    for(i=0; i<p->nStr; i++){
      int iTok;
      for(iTok=0; iTok<p->aStr[i].nToken; iTok++){
        sqlite4KVCursorClose(p->aStr[i].aToken[iTok].pCsr);
      }
      sqlite4DbFree(db, p->aStr[i].aToken);
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

  if( rc!=SQLITE4_OK ){
    fts5ExpressionFree(db, pExpr);
    *pzErr = sParse.zErr;
  }else{
    *ppExpr = pExpr;
  }
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
  sqlite4 *db;
  int nMax;
  Hash hash;
};
struct TokenizeTerm {
  int iWeight;                    /* Weight of previous entry */
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
  int iWeight, 
  int iOff,
  const char *zToken, 
  int nToken, 
  int iSrc, 
  int nSrc
){
  TokenizeCtx *p = (TokenizeCtx *)pCtx;
  TokenizeTerm *pTerm = 0;
  TokenizeTerm *pOrig = 0;

  if( nToken>p->nMax ) p->nMax = nToken;

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

  if( iWeight!=pTerm->iWeight ){
    pTerm = fts5TokenizeAppendInt(p, pTerm, (iWeight << 2) | 0x00000003);
    if( !pTerm ) goto tokenize_cb_out;
    pTerm->iWeight = iWeight;
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
  u8 *aKey = 0;
  int nKey = 0;
  int nTnum = 0;
  u32 dummy = 0;

  const u8 *pPK;
  int nPK;
  HashElem *pElem;

  pStore = db->aDb[pInfo->iDb].pKV;
  sCtx.rc = SQLITE4_OK;
  sCtx.db = db;
  sCtx.nMax = 0;
  sqlite4HashInit(db->pEnv, &sCtx.hash);

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
      nText = sqlite4_value_bytes(pArg); sCtx.iCol = i;
      rc = pInfo->pTokenizer->xTokenize(
          &sCtx, pInfo->p, zText, nText, fts5TokenizeCb
      );
    }
  }

  nKey = sqlite4VarintLen(pInfo->iRoot) + 2 + sCtx.nMax + nPK;
  aKey = sqlite4DbMallocRaw(db, nKey);
  if( aKey==0 ) rc = SQLITE4_NOMEM;

  for(pElem=sqliteHashFirst(&sCtx.hash); pElem; pElem=sqliteHashNext(pElem)){
    TokenizeTerm *pTerm = (TokenizeTerm *)sqliteHashData(pElem);
    if( rc==SQLITE4_OK ){
      int nToken = sqliteHashKeysize(pElem);
      char *zToken = (char *)sqliteHashKey(pElem);

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
  
  sqlite4DbFree(db, aKey);
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
  int iWeight,
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
  cksum += (cksum << 3) + iWeight;
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
  int nTnum;
  u32 tnum;

  int iOff = 0;
  int iCol = 0;
  int iWeight = 0;
  int i = 0;

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

  while( i<nVal ){
    u32 iVal;
    i += getVarint32(&aVal[i], iVal);

    if( (iVal & 0x03)==0x01 ){
      iCol = (iVal>>2);
      iOff = 0;
    }
    else if( (iVal & 0x03)==0x03 ){
      iWeight = (iVal>>2);
    }
    else{
      i64 v;
      iOff += (iVal>>1);
      v = fts5TermInstanceCksum(aPk, nPk, aToken, nToken, iWeight, iCol, iOff);
      cksum = cksum ^ v;
    }
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
  int iWeight, 
  int iOff,
  const char *zToken, 
  int nToken, 
  int iSrc, 
  int nSrc
){
  CksumCtx *p = (CksumCtx *)pCtx;
  i64 cksum;

  cksum = fts5TermInstanceCksum(p->pPK, p->nPK, 
      (const u8 *)zToken, nToken, iWeight, p->iCol, iOff
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
          if( rc==SQLITE4_OK ){
            rc = sqlite4KVCursorSeek(
                pToken->pCsr, pToken->aPrefix, pToken->nPrefix, 1
            );
            if( rc==SQLITE4_INEXACT ) rc = SQLITE4_OK;
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
    sqlite4DbFree(db, pCsr);
  }
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

  pCsr = sqlite4DbMallocZero(db, sizeof(Fts5Cursor));
  if( !pCsr ){
    rc = SQLITE4_NOMEM;
  }else{
    pCsr->pInfo = pInfo;
    pCsr->db = db;
    rc = fts5ParseExpression(db, pInfo->pTokenizer, pInfo->p, 
        pInfo->iRoot, pInfo->azCol, pInfo->nCol, zMatch, &pCsr->pExpr, pzErr
    );
  }

  if( rc==SQLITE4_OK ){
    /* Open a KV cursor for each term in the expression. */
    rc = fts5OpenCursors(db, pInfo, pCsr);
  }
  if( rc!=SQLITE4_OK ){
    sqlite4Fts5Close(db, pCsr);
    pCsr = 0;
  }
  *ppCsr = pCsr;
  return rc;
}

int sqlite4Fts5Next(Fts5Cursor *pCsr){
  Fts5Token *pToken;
  KVCursor *pKVCsr;
  int rc;

  assert( pCsr->pExpr->pRoot->eType==TOKEN_PRIMITIVE );
  pToken = &pCsr->pExpr->pRoot->pPhrase->aStr[0].aToken[0];

  rc = sqlite4KVCursorNext(pToken->pCsr);
  if( rc==SQLITE4_OK ){
    const KVByteArray *aKey;
    KVSize nKey;
    rc = sqlite4KVCursorKey(pToken->pCsr, &aKey, &nKey);
    if( rc==SQLITE4_OK 
     && (nKey<pToken->nPrefix || memcmp(pToken->aPrefix, aKey, pToken->nPrefix))
    ){
      rc = SQLITE4_NOTFOUND;
    }
  }
  return rc;
}

/*
** Return true if the cursor passed as the second argument currently points
** to a valid entry, or false otherwise.
*/
int sqlite4Fts5Valid(Fts5Cursor *pCsr){
  const KVByteArray *aKey;
  KVSize nKey;
  KVCursor *pKVCsr;
  int rc;

  assert( pCsr->pExpr->pRoot->eType==TOKEN_PRIMITIVE );
  pKVCsr = pCsr->pExpr->pRoot->pPhrase->aStr[0].aToken[0].pCsr;

  rc = sqlite4KVCursorKey(pKVCsr, &aKey, &nKey);
  return (rc==SQLITE4_OK);
}

int sqlite4Fts5Pk(
  Fts5Cursor *pCsr, 
  int iTbl, 
  KVByteArray **paKey, 
  KVSize *pnKey
){
  const KVByteArray *aKey;
  KVSize nKey;
  KVCursor *pKVCsr;
  int rc;
  int i;
  int i2;
  int nReq;

  assert( pCsr->pExpr->pRoot->eType==TOKEN_PRIMITIVE );

  pKVCsr = pCsr->pExpr->pRoot->pPhrase->aStr[0].aToken[0].pCsr;
  rc = sqlite4KVCursorKey(pKVCsr, &aKey, &nKey);
  if( rc!=SQLITE4_OK ) return rc;

  i = sqlite4VarintLen(pCsr->pInfo->iRoot);
  while( aKey[i] ) i++;
  i++;

  nReq = sqlite4VarintLen(iTbl) + (nKey-i);
  if( nReq>pCsr->nKeyAlloc ){
    pCsr->aKey = sqlite4DbReallocOrFree(pCsr->db, pCsr->aKey, nReq*2);
    if( !pCsr->aKey ) return SQLITE4_NOMEM;
    pCsr->nKeyAlloc = nReq*2;
  }

  i2 = putVarint32(pCsr->aKey, iTbl);
  memcpy(&pCsr->aKey[i2], &aKey[i], nKey-i);

  *paKey = pCsr->aKey;
  *pnKey = nReq;
  return SQLITE4_OK;
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

