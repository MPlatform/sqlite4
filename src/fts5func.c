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
** BM25 and BM25F references:
**
**   Stephen Robertson and Hugo Zaragoza: "The Probablistic Relevance
**   Framework: BM25 and Beyond", 2009.
**
**   http://xapian.org/docs/bm25.html
**
**   http://en.wikipedia.org/wiki/Okapi_BM25
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

typedef struct SnippetCtx SnippetCtx;
struct SnippetCtx {
  sqlite4 *db;                    /* Database handle */
  int nToken;                     /* Number of tokens in snippet */
  int iOff;                       /* First token in snippet */
  u64 mask;                       /* Snippet mask. Highlight these terms */

  char *zOut;                     /* Pointer to snippet text */
  int nOut;                       /* Size of zOut in bytes */
  int nAlloc;                     /* Bytes of space allocated at zOut */

  int iFrom;
  const char *zText;              /* Document to extract snippet from */

  int rc;                         /* Set to NOMEM if OOM is encountered */
};

static void fts5SnippetAppend(SnippetCtx *p, const char *z, int n){
  if( p->rc==SQLITE4_OK ){
    if( (p->nOut + n) > p->nAlloc ){
      int nNew = (p->nOut+n) * 2;

      p->zOut = sqlite4DbReallocOrFree(p->db, p->zOut, nNew);
      if( p->zOut==0 ){
        p->rc = SQLITE4_NOMEM;
        return;
      }
      p->nAlloc = sqlite4DbMallocSize(p->db, p->zOut);
    }

    memcpy(&p->zOut[p->nOut], z, n);
    p->nOut += n;
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
  }else if( iOff>(p->iOff + p->nToken) ){
    fts5SnippetAppend(p, &p->zText[p->iFrom], iSrc - p->iFrom);
    fts5SnippetAppend(p, "...", 3);
    p->iFrom = -1;
    return 1;
  }else{
    int bHighlight;               /* True to highlight term */

    bHighlight = (p->mask & (1 << (p->iOff+p->nToken - iOff - 1))) ? 1 : 0;

    if( p->iFrom==0 && p->iOff!=0 ){
      p->iFrom = iSrc;
      fts5SnippetAppend(p, "...", 3);
    }

    if( bHighlight ){
      fts5SnippetAppend(p, &p->zText[p->iFrom], iSrc - p->iFrom);
      fts5SnippetAppend(p, "[", 1);
      fts5SnippetAppend(p, &p->zText[iSrc], nSrc);
      fts5SnippetAppend(p, "]", 1);
      p->iFrom = iSrc+nSrc;
    }
  }

  return 0;
}

static int fts5SnippetText(
  sqlite4_context *pCtx, 
  int iCol,
  int iOff,
  int nToken,
  u64 mask
){
  int rc;
  sqlite4_value *pVal = 0;

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

    sqlite4_mi_tokenize(pCtx, sCtx.zText, nText, &sCtx, fts5SnippetCb);
    if( sCtx.rc==SQLITE4_OK && sCtx.iFrom>0 ){
      fts5SnippetAppend(&sCtx, &sCtx.zText[sCtx.iFrom], nText - sCtx.iFrom);
    }
    rc = sCtx.rc;

    sqlite4_result_text(pCtx, sCtx.zOut, sCtx.nOut, SQLITE4_TRANSIENT);
    sqlite4DbFree(sCtx.db, sCtx.zOut);
  }

  return rc;
}

static int fts5BestSnippet(
  sqlite4_context *pCtx, 
  u64 mask,                       /* Mask of high-priority phrases */
  int nToken,
  int *piOff,
  int *piCol,
  u64 *pMask
){
  sqlite4 *db = sqlite4_context_db_handle(pCtx);
  int nPhrase;
  int rc = SQLITE4_OK;
  int i;
  int iPrev = 0;
  int iPrevCol = 0;
  u64 *aMask;
  u64 lmask = (((u64)1) << nToken) - 1;

  int iBestOff = 0;
  int iBestCol = 0;
  int nBest = 0;
  u64 bmask = 0;

  sqlite4_mi_phrase_count(pCtx, &nPhrase);
  aMask = sqlite4DbMallocZero(db, sizeof(u64) * nPhrase);
  if( !aMask ) return SQLITE4_NOMEM;

  /* Iterate through all matches for all phrases */
  for(i=0; rc==SQLITE4_OK; i++){
    int iOff;
    int iCol;
    int iStream;
    int iPhrase;
    u64 tmask = 0;

    rc = sqlite4_mi_match_detail(pCtx, i, &iOff, &iCol, &iStream, &iPhrase);
    if( rc==SQLITE4_OK ){
      int iMask;
      int nShift; 
      int nScore = 0;

      nShift = ((iPrevCol==iCol) ? (iOff-iPrev) : 100);

      for(iMask=0; iMask<nPhrase; iMask++){
        if( nShift<64){
          aMask[iMask] = aMask[iMask] << nShift;
        }else{
          aMask[iMask] = 0;
        }
      }
      aMask[iPhrase] = aMask[iMask] | 0x0001;

      for(iMask=0; iMask<nPhrase; iMask++){
        if( (aMask[iMask] & lmask) ){
          nScore += ((aMask[iMask] & mask) ? 100 : 1);
        }
        tmask = tmask | aMask[iMask];
      }

      if( nScore>nBest ){
        bmask = (tmask & lmask);
        nBest = nScore;
        iBestOff = iOff;
        iBestCol = iCol;
      }

      iPrev = iOff;
      iPrevCol = iCol;
    }
  }

  *piOff = iBestOff;
  *piCol = iBestCol;
  *pMask = bmask;

  sqlite4DbFree(db, aMask);
  return rc;
}

static void fts5Snippet(sqlite4_context *pCtx, int nArg, sqlite4_value **apArg){
  int nToken = 15;
  u64 hlmask = 0;
  u64 mask = 0;
  int iOff = 0;
  int iCol = 0;
  int rc;

  rc = fts5BestSnippet(pCtx, mask, nToken, &iOff, &iCol, &hlmask);
  if( rc==SQLITE4_OK ){
    rc = fts5SnippetText(pCtx, iCol, iOff, nToken, hlmask);
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

