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
      const char*, int, int(*x)(void*, int, const char*, int)
  );
  int (*xDestroy)(void*, sqlite4_tokenizer *);
  Fts5Tokenizer *pNext;
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

  const char **azCol;             /* Column names of indexed table */
  int nCol;                       /* Size of azCol[] in bytes */

  /* Space for dequoted copies of strings */
  char *aSpace;
  int iSpace;
};

struct Fts5Token {
  int bPrefix;                    /* True for a prefix search */
  int n;                          /* Size of z[] in bytes */
  const char *z;                  /* Token value */
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
** Return true if argument c is an ASCII whitespace character.
*/
static int fts5IsWhite(char c){
  return (c==' ' || c=='\n' || c=='\r' || c=='\t');
}

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
  while( fts5IsWhite(z[pParse->iExpr]) ) pParse->iExpr++;

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
        && fts5IsWhite(zPrimitive[n])==0 
    ){
      n++;
    }
    pParse->iExpr += n;

    if( n==3 && memcmp(zPrimitive, "NOT", 3)==0 ){
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

  pPhrase->nStr++;
  return SQLITE4_OK;
}

/*
** Callback for fts5CountTokens().
*/
static int fts5CountTokensCb(void *pCtx, int iWeight, const char *z, int n){
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

static int fts5AppendTokensCb(void *pCtx, int iWeight, const char *z, int n){
  struct AppendTokensCtx *p = (struct AppendTokensCtx *)pCtx;
  Fts5Token *pToken;
  char *zSpace;

  zSpace = &p->pParse->aSpace[p->pParse->iSpace];
  p->pParse->iSpace += (n+1);
  memcpy(zSpace, z, n);
  zSpace[n] = '\0';

  pToken = &p->pStr->aToken[p->pStr->nToken];
  p->pStr->nToken++;
  pToken->bPrefix = 0;
  pToken->z = zSpace;
  pToken->n = n;

  return 0;
}

static int fts5AppendTokens(
  Fts5Parser *pParse,
  Fts5Str *pStr,
  const char *zPrim,
  int nPrim
){
  struct AppendTokensCtx ctx;
  ctx.pParse = pParse;
  ctx.pStr = pStr;

  return pParse->pTokenizer->xTokenize(
      (void *)&ctx, pParse->p , zPrim, nPrim, fts5AppendTokensCb
  );
}

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
  const char **azCol,             /* Array of column names (nul-term'd) */
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

  pExpr = sqlite4DbMallocZero(db, sizeof(Fts5Expr) + nExpr*2);
  if( !pExpr ) return SQLITE4_NOMEM;
  sParse.aSpace = (char *)&pExpr[1];

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
      const char*, int, int(*x)(void*, int, const char*, int)
  ),
  int (*xDestroy)(void*, sqlite4_tokenizer *)
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

/**************************************************************************
***************************************************************************
** Below this point is test code.
*/
#ifdef SQLITE4_TEST
static int fts5PrintExprNode(sqlite4 *, const char **, Fts5ExprNode *, char **);
static int fts5PrintExprNodeParen(
  sqlite4 *db, 
  const char **azCol,
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
  sqlite4_tokenizer *p;
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

  rc = fts5ParseExpression(db, pTok, p, azCol, nCol, zExpr, &pExpr, &zErr);
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

