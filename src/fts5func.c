/*
** 2012 December 17
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

/*
** The BM25 and BM25F implementations in this file are based on information
** found in:
**
**   Stephen Robertson and Hugo Zaragoza: "The Probablistic Relevance
**   Framework: BM25 and Beyond", 2009.
*/

#include "sqliteInt.h"
#include <math.h>                 /* temporary: For log() */

static char fts5Tolower(char c){
  if( c>='A' && c<='Z' ) c = c + ('a' - 'A');
  return c;
}

static int fts5SimpleCreate(
  void *pCtx, 
  const char **azArg, 
  int nArg, 
  sqlite4_tokenizer **pp
){
  *pp = (sqlite4_tokenizer *)pCtx;
  return SQLITE4_OK;
}

static int fts5SimpleDestroy(sqlite4_tokenizer *p){
  return SQLITE4_OK;
}

typedef struct Fts5RankCtx Fts5RankCtx;
struct Fts5RankCtx {
  sqlite4 *db;
  double avgdl;                   /* Average document size in tokens */
  int nPhrase;                    /* Number of phrases in query */
  double *aIdf;                   /* IDF weights for each phrase in query */
};

static void fts5RankFreeCtx(void *pCtx){
  if( pCtx ){
    Fts5RankCtx *p = (Fts5RankCtx *)pCtx;
    sqlite4DbFree(p->db, p);
  }
}

/*
** A BM25 based ranking function for fts5.
**
** This is based on the information in the Robertson/Zaragoza paper 
** referenced above. As there is no way to provide relevance feedback 
** IDF weights (equation 3.3 in R/Z) are used instead of RSJ for each phrase.
** The rest of the implementation is as presented in equation 3.15.
**
** R and Z observe that the experimental evidence suggests that reasonable
** values for free parameters "b" and "k1" are often in the ranges 
** (0.5 < b < 0.8) and (1.2 < k1 < 2), although the optimal values depend
** on the nature of both the documents and queries. The implementation
** below sets each parameter to the midpoint of the suggested range.
*/
static void fts5Rank(sqlite4_context *pCtx, int nArg, sqlite4_value **apArg){
  const double b = 0.65;
  const double k1 = 1.6;

  int rc = SQLITE4_OK;            /* Error code */
  Fts5RankCtx *p;                 /* Structure to store reusable values */
  int i;                          /* Used to iterate through phrases */
  double rank = 0.0;              /* UDF return value */

  p = sqlite4_get_auxdata(pCtx, 0);
  if( p==0 ){
    sqlite4 *db = sqlite4_context_db_handle(pCtx);
    int nPhrase;                  /* Number of phrases in query expression */
    int nByte;                    /* Number of bytes of data to allocate */

    sqlite4_mi_phrase_count(pCtx, &nPhrase);
    nByte = sizeof(Fts5RankCtx) + nPhrase * sizeof(double);
    p = (Fts5RankCtx *)sqlite4DbMallocZero(db, nByte);
    sqlite4_set_auxdata(pCtx, 0, (void *)p, fts5RankFreeCtx);
    p = sqlite4_get_auxdata(pCtx, 0);

    if( !p ){
      rc = SQLITE4_NOMEM;
    }else{
      int N;                      /* Total number of docs in collection */
      int ni;                     /* Number of docs with phrase i */

      p->db = db;
      p->nPhrase = nPhrase;
      p->aIdf = (double *)&p[1];

      /* Determine the IDF weight for each phrase in the query. */
      rc = sqlite4_mi_total_rows(pCtx, &N);
      for(i=0; rc==SQLITE4_OK && i<nPhrase; i++){
        rc = sqlite4_mi_row_count(pCtx, -1, -1, i, &ni);
        if( rc==SQLITE4_OK ){
          p->aIdf[i] = log((0.5 + N - ni) / (0.5 + ni));
        }
      }

      /* Determine the average document length */
      if( rc==SQLITE4_OK ){
        int nTotal;
        rc = sqlite4_mi_total_size(pCtx, -1, -1, &nTotal);
        if( rc==SQLITE4_OK ){
          p->avgdl = (double)nTotal / (double)N;
        }
      }
    }
  }

  for(i=0; rc==SQLITE4_OK && i<p->nPhrase; i++){
    int tf;                     /* Occurences of phrase i in row (term freq.) */
    int dl;                     /* Tokens in this row (document length) */
    double L;                   /* Normalized document length */
    double prank;               /* Contribution to rank of this phrase */

    /* Set variable tf to the total number of occurrences of phrase iPhrase
    ** in this row (within any column). And dl to the number of tokens in
    ** the current row (again, in any column).  */
    rc = sqlite4_mi_match_count(pCtx, -1, -1, i, &tf); 
    if( rc==SQLITE4_OK ) rc = sqlite4_mi_size(pCtx, -1, -1, &dl); 

    /* Calculate the normalized document length */
    L = (double)dl / p->avgdl;

    /* Calculate the contribution to the rank made by this phrase. Then
    ** add it to variable rank.  */
    prank = (p->aIdf[i] * tf) / (k1 * ( (1.0 - b) + b * L) + tf);
    rank += prank;
  }

  if( rc==SQLITE4_OK ){
    sqlite4_result_double(pCtx, rank);
  }else{
    sqlite4_result_error_code(pCtx, rc);
  }
}

typedef struct Snippet Snippet;
typedef struct SnippetText SnippetText;

struct Snippet {
  int iCol;
  int iOff;
  u64 hlmask;
};

struct SnippetText {
  char *zOut;                     /* Pointer to snippet text */
  int nOut;                       /* Size of zOut in bytes */
  int nAlloc;                     /* Bytes of space allocated at zOut */
};

typedef struct SnippetCtx SnippetCtx;
struct SnippetCtx {
  sqlite4 *db;                    /* Database handle */
  int nToken;                     /* Number of tokens in snippet */
  int iOff;                       /* First token in snippet */
  u64 mask;                       /* Snippet mask. Highlight these terms */
  const char *zStart;
  const char *zEnd;
  const char *zEllipses;

  SnippetText *pOut;

  int iFrom;
  int iTo;
  const char *zText;              /* Document to extract snippet from */
  int rc;                         /* Set to NOMEM if OOM is encountered */
};

static void fts5SnippetAppend(SnippetCtx *p, const char *z, int n){
  if( p->rc==SQLITE4_OK ){
    SnippetText *pOut = p->pOut;
    if( n<0 ) n = strlen(z);
    if( (pOut->nOut + n) > pOut->nAlloc ){
      int nNew = (pOut->nOut+n) * 2;

      pOut->zOut = sqlite4DbReallocOrFree(p->db, pOut->zOut, nNew);
      if( pOut->zOut==0 ){
        p->rc = SQLITE4_NOMEM;
        return;
      }
      pOut->nAlloc = sqlite4DbMallocSize(p->db, pOut->zOut);
    }

    memcpy(&pOut->zOut[pOut->nOut], z, n);
    pOut->nOut += n;
  }
}

static int fts5SnippetCb(
  void *pCtx, 
  int iStream, 
  int iOff, 
  const char *z, int n,
  int iSrc, int nSrc
){
  SnippetCtx *p = (SnippetCtx *)pCtx;

  if( iOff<p->iOff ){
    return 0;
  }else if( iOff>=(p->iOff + p->nToken) ){
    fts5SnippetAppend(p, &p->zText[p->iFrom], p->iTo - p->iFrom);
    fts5SnippetAppend(p, "...", 3);
    p->iFrom = -1;
    return 1;
  }else{
    int bHighlight;               /* True to highlight term */

    bHighlight = (p->mask & (1 << (iOff-p->iOff)));

    if( p->iFrom==0 && p->iOff!=0 ){
      p->iFrom = iSrc;
      if( p->pOut->nOut==0 ) fts5SnippetAppend(p, p->zEllipses, -1);
    }

    if( bHighlight ){
      fts5SnippetAppend(p, &p->zText[p->iFrom], iSrc - p->iFrom);
      fts5SnippetAppend(p, p->zStart, -1);
      fts5SnippetAppend(p, &p->zText[iSrc], nSrc);
      fts5SnippetAppend(p, p->zEnd, -1);
      p->iTo = p->iFrom = iSrc+nSrc;
    }else{
      p->iTo = iSrc + nSrc;
    }
  }

  return 0;
}

static int fts5SnippetText(
  sqlite4_context *pCtx, 
  Snippet *pSnip,
  SnippetText *pText,
  int nToken,
  const char *zStart,
  const char *zEnd,
  const char *zEllipses
){
  int rc;
  sqlite4_value *pVal = 0;

  u64 mask = pSnip->hlmask;
  int iOff = pSnip->iOff;
  int iCol = pSnip->iCol;

  rc = sqlite4_mi_column_value(pCtx, iCol, &pVal);
  if( rc==SQLITE4_OK ){
    SnippetCtx sCtx;
    int nText;

    nText = sqlite4_value_bytes(pVal);
    memset(&sCtx, 0, sizeof(sCtx));
    sCtx.zText = (const char *)sqlite4_value_text(pVal);
    sCtx.db = sqlite4_context_db_handle(pCtx);
    sCtx.nToken = nToken;
    sCtx.iOff = iOff;
    sCtx.mask = mask;
    sCtx.zStart = zStart;
    sCtx.zEnd = zEnd;
    sCtx.zEllipses = zEllipses;
    sCtx.pOut = pText;

    sqlite4_mi_tokenize(pCtx, sCtx.zText, nText, &sCtx, fts5SnippetCb);
    if( sCtx.rc==SQLITE4_OK && sCtx.iFrom>0 ){
      fts5SnippetAppend(&sCtx, &sCtx.zText[sCtx.iFrom], nText - sCtx.iFrom);
    }
    rc = sCtx.rc;
  }

  return rc;
}

static int fts5BestSnippet(
  sqlite4_context *pCtx,          /* Context snippet() was called in */
  int iColumn,                    /* In this column (-1 means any column) */
  u64 *pMask,                     /* IN/OUT: Mask of high-priority phrases */
  int nToken,                     /* Number of tokens in requested snippet */
  Snippet *pSnip                  /* Populate this object */
){
  sqlite4 *db = sqlite4_context_db_handle(pCtx);
  int nPhrase;
  int rc = SQLITE4_OK;
  int i;
  int iPrev = 0;
  int iPrevCol = 0;
  u64 *aMask;
  u64 mask = *pMask;
  u64 allmask = 0;

  int iBestOff = nToken-1;
  int iBestCol = (iColumn >= 0 ? iColumn : 0);
  int nBest = 0;
  u64 hlmask = 0;                 /* Highlight mask associated with iBestOff */
  u64 missmask = 0;               /* Mask of missing terms in iBestOff snip. */

  sqlite4_mi_phrase_count(pCtx, &nPhrase);
  aMask = sqlite4DbMallocZero(db, sizeof(u64) * nPhrase);
  if( !aMask ) return SQLITE4_NOMEM;

  /* Iterate through all matches for all phrases */
  for(i=0; rc==SQLITE4_OK; i++){
    int iOff;
    int iCol;
    int iStream;
    int iPhrase;

    rc = sqlite4_mi_match_detail(pCtx, i, &iOff, &iCol, &iStream, &iPhrase);
    if( rc==SQLITE4_OK ){
      u64 tmask = 0;
      u64 miss = 0;
      int iMask;
      int nShift; 
      int nScore = 0;

      int nPTok;
      int iPTok;

      if( iColumn>=0 && iColumn!=iCol ) continue;

      allmask |= (1 << iPhrase);

      nShift = ((iPrevCol==iCol) ? (iOff-iPrev) : 100);

      for(iMask=0; iMask<nPhrase; iMask++){
        if( nShift<64){
          aMask[iMask] = aMask[iMask] >> nShift;
        }else{
          aMask[iMask] = 0;
        }
      }
      sqlite4_mi_phrase_token_count(pCtx, iPhrase, &nPTok);
      for(iPTok=0; iPTok<nPTok; iPTok++){
        aMask[iPhrase] = aMask[iPhrase] | (1<<(nToken-1+iPTok));
      }

      for(iMask=0; iMask<nPhrase; iMask++){
        if( aMask[iMask] ){
          nScore += (((1 << iMask) & mask) ? 100 : 1);
        }else{
          miss |= (1 << iMask);
        }
        tmask = tmask | aMask[iMask];
      }

      if( nScore>nBest ){
        hlmask = tmask;
        missmask = miss;
        nBest = nScore;
        iBestOff = iOff;
        iBestCol = iCol;
      }

      iPrev = iOff;
      iPrevCol = iCol;
    }
  }
  if( rc==SQLITE4_NOTFOUND ) rc = SQLITE4_OK;

  pSnip->iOff = iBestOff-nToken+1;
  pSnip->iCol = iBestCol;
  pSnip->hlmask = hlmask;
  *pMask = mask & missmask & allmask;

  sqlite4DbFree(db, aMask);
  return rc;
}

static void fts5SnippetImprove(
  sqlite4_context *pCtx, 
  int nToken,                     /* Size of required snippet */
  int nSz,                        /* Total size of column in tokens */
  Snippet *pSnip
){
  int i;
  int nLead = 0;
  int nShift = 0;

  u64 mask = pSnip->hlmask;
  int iOff = pSnip->iOff;

  if( mask==0 ) return;
  assert( mask & (1 << (nToken-1)) );

  for(i=0; (mask & (1<<i))==0; i++);
  nLead = i;

  nShift = (nLead/2);
  if( iOff+nShift > nSz-nToken ) nShift = (nSz-nToken) - iOff;
  if( iOff+nShift < 0 ) nShift = -1 * iOff;

  iOff += nShift;
  mask = mask >> nShift;

  pSnip->iOff = iOff;
  pSnip->hlmask = mask;
}

static void fts5Snippet(sqlite4_context *pCtx, int nArg, sqlite4_value **apArg){
  Snippet aSnip[4];
  int nSnip;
  int iCol = -1;
  int nToken = -15;
  int rc;
  int nPhrase;

  const char *zStart = "<b>";
  const char *zEnd = "</b>";
  const char *zEllipses = "...";

  if( nArg>0 ) zStart = (const char *)sqlite4_value_text(apArg[0]);
  if( nArg>1 ) zEnd = (const char *)sqlite4_value_text(apArg[1]);
  if( nArg>2 ) zEllipses = (const char *)sqlite4_value_text(apArg[2]);
  if( nArg>3 ) iCol = sqlite4_value_int(apArg[3]);
  if( nArg>4 ) nToken = sqlite4_value_int(apArg[4]);

  rc = sqlite4_mi_phrase_count(pCtx, &nPhrase);
  for(nSnip=1; rc==SQLITE4_OK && nSnip<5; nSnip = ((nSnip==2) ? 3 : (nSnip+1))){
    int nTok;
    int i;
    u64 mask = ((u64)1 << nPhrase) - 1;

    if( nToken<0 ){
      nTok = nToken * -1;
    }else{
      nTok = (nToken + (nSnip-1)) / nSnip;
    }

    memset(aSnip, 0, sizeof(aSnip));
    for(i=0; rc==SQLITE4_OK && i<nSnip; i++){
      rc = fts5BestSnippet(pCtx, iCol, &mask, nTok, &aSnip[i]);
    }
    if( mask==0 || nSnip==4 ){
      SnippetText text = {0, 0, 0};
      for(i=0; rc==SQLITE4_OK && i<nSnip; i++){
        int nSz;
        rc = sqlite4_mi_size(pCtx, aSnip[i].iCol, -1, &nSz);
        if( rc==SQLITE4_OK ){
          fts5SnippetImprove(pCtx, nTok, nSz, &aSnip[i]);
          rc = fts5SnippetText(
              pCtx, &aSnip[i], &text, nTok, zStart, zEnd, zEllipses
          );
        }
      }
      sqlite4_result_text(pCtx, text.zOut, text.nOut, SQLITE4_TRANSIENT);
      sqlite4DbFree(sqlite4_context_db_handle(pCtx), text.zOut);
      break;
    }
  }

  if( rc!=SQLITE4_OK ){
    sqlite4_result_error_code(pCtx, rc);
  }
}

static int fts5SimpleTokenize(
  void *pCtx, sqlite4_tokenizer *p,
  const char *zDoc,
  int nDoc,
  int(*x)(void*, int, int, const char*, int, int, int)
){
  sqlite4_env *pEnv = (sqlite4_env *)p;
  char *aBuf;
  int nBuf;
  int iBuf;
  int i;
  int brk = 0;
  int iOff = 0;

  nBuf = 128;
  aBuf = (char *)sqlite4_malloc(pEnv, nBuf);
  if( !aBuf ) return SQLITE4_NOMEM;

  iBuf = 0;
  for(i=0; brk==0 && i<nDoc; i++){
    if( sqlite4Isalnum(zDoc[i]) ){
      aBuf[iBuf++] = fts5Tolower(zDoc[i]);
    }else if( iBuf>0 ){
      brk = x(pCtx, 0, iOff++, aBuf, iBuf, i-iBuf, iBuf);
      iBuf = 0;
    }
  }
  if( iBuf>0 ) x(pCtx, 0, iOff++, aBuf, iBuf, i-iBuf, iBuf);

  sqlite4_free(pEnv, aBuf);
  return SQLITE4_OK;
}

int sqlite4InitFts5Func(sqlite4 *db){
  int rc;
  sqlite4_env *pEnv = sqlite4_db_env(db);

  rc = sqlite4_create_tokenizer(db, "simple", (void *)pEnv, 
      fts5SimpleCreate, fts5SimpleTokenize, fts5SimpleDestroy
  );
  if( rc!=SQLITE4_OK ) return rc;

  rc = sqlite4_create_mi_function(db, "rank", 0, SQLITE4_UTF8, 0, fts5Rank, 0);
  if( rc!=SQLITE4_OK ) return rc;

  rc = sqlite4_create_mi_function(
      db, "snippet", -1, SQLITE4_UTF8, 0, fts5Snippet, 0
  );
  return rc;
}

