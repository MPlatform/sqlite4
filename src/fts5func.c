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
  double *aAvgdl;                 /* Average document size of each field */
  int nPhrase;                    /* Number of phrases in query */
  double *aIdf;                   /* IDF weights for each phrase in query */
};

static void fts5RankFreeCtx(void *pCtx){
  if( pCtx ){
    Fts5RankCtx *p = (Fts5RankCtx *)pCtx;
    sqlite4DbFree(p->db, p);
  }
}

#define BM25_EXPLAIN  0x01
#define BM25_FCOLUMNS 0x02
#define BM25_FSTREAMS 0x04

static int fts5GetSizeFreqScale(
  sqlite4_context *pCtx,
  int nArg, sqlite4_value **apArg,/* Function arguments */
  int bm25mask,                   /* bm25 configuration mask */
  int iPhrase,                    /* Phrase number */
  int iField,                     /* Field number */
  int *pnSize,                    /* OUT: Size of field in tokens */
  int *pnFreq,                    /* OUT: Occurences of phrase in field */
  double *pdScale                 /* OUT: Scale to use with this field */
){
  int rc;
  double scale = 1.0;
  int nSize = 0;
  int nFreq = 0;

  if( bm25mask & BM25_FCOLUMNS ){
    rc = sqlite4_mi_match_count(pCtx, iField, -1, iPhrase, &nFreq); 
    if( rc==SQLITE4_OK ) rc = sqlite4_mi_size(pCtx, iField, -1, &nSize); 
    if( nArg>iField ) scale = sqlite4_value_double(apArg[iField]);
  }else if( bm25mask & BM25_FSTREAMS ){
    rc = sqlite4_mi_match_count(pCtx, -1, iField, iPhrase, &nFreq); 
    if( rc==SQLITE4_OK ) rc = sqlite4_mi_size(pCtx, -1, iField, &nSize); 
    if( nArg>iField ) scale = sqlite4_value_double(apArg[iField]);
  }else{
    rc = sqlite4_mi_match_count(pCtx, -1, -1, iPhrase, &nFreq); 
    if( rc==SQLITE4_OK ) rc = sqlite4_mi_size(pCtx, -1, -1, &nSize); 
  }

  *pnSize = nSize;
  *pnFreq = nFreq;
  *pdScale = scale;
  return rc;
}

/*
** A BM25(F) based ranking function for fts5.
**
** This is based on the information in the Robertson/Zaragoza paper 
** referenced above. As there is no way to provide relevance feedback 
** IDF weights (equation 3.3 in R/Z) are used instead of RSJ for each phrase.
** The rest of the implementation is as presented in equations 3.19-21.
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

  sqlite4 *db = sqlite4_context_db_handle(pCtx);
  int rc = SQLITE4_OK;            /* Error code */
  Fts5RankCtx *p;                 /* Structure to store reusable values */
  int i;                          /* Used to iterate through phrases */
  double rank = 0.0;              /* UDF return value */

  int bExplain;                   /* True to run in explain mode */
  char *zExplain = 0;             /* String to return in explain mode */
  int nField = 1;                 /* Number of fields in collection */

  int bm25mask = SQLITE4_PTR_TO_INT(sqlite4_user_data(pCtx));
  bExplain = (bm25mask & BM25_EXPLAIN);

  if( bm25mask & BM25_FCOLUMNS ) sqlite4_mi_column_count(pCtx, &nField);
  if( bm25mask & BM25_FSTREAMS ) sqlite4_mi_stream_count(pCtx, &nField);

  p = sqlite4_get_auxdata(pCtx, 0);
  if( p==0 ){
    int nPhrase;                  /* Number of phrases in query expression */
    int nByte;                    /* Number of bytes of data to allocate */

    sqlite4_mi_phrase_count(pCtx, &nPhrase);
    nByte = sizeof(Fts5RankCtx) + (nPhrase+nField) * sizeof(double);
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
      p->aAvgdl = &p->aIdf[nPhrase];

      /* Determine the IDF weight for each phrase in the query. */
      rc = sqlite4_mi_total_rows(pCtx, &N);
      for(i=0; rc==SQLITE4_OK && i<nPhrase; i++){
        rc = sqlite4_mi_row_count(pCtx, -1, -1, i, &ni);
        if( rc==SQLITE4_OK ){
          assert( ni<=N );
          p->aIdf[i] = log((0.5 + N - ni) / (0.5 + ni));
        }
      }

      /* Determine the average document length. For bm25f, determine the
      ** average length of each field.  */
      if( rc==SQLITE4_OK ){
        int iField;
        for(iField=0; iField<nField; iField++){
          int nTotal;
          if( bm25mask & BM25_FCOLUMNS ){
            rc = sqlite4_mi_total_size(pCtx, iField, -1, &nTotal);
          }else if( bm25mask & BM25_FSTREAMS ){
            rc = sqlite4_mi_total_size(pCtx, -1, iField, &nTotal);
          }else{
            rc = sqlite4_mi_total_size(pCtx, -1, -1, &nTotal);
          }
          if( rc==SQLITE4_OK ){
            p->aAvgdl[iField] = (double)nTotal / (double)N;
          }
        }
      }
    }
  }

  if( bExplain ){
    int iField;
    zExplain = sqlite4MAppendf(
        db, zExplain, "%s<table><tr><th>Stream<th>Scale<th>avgsl<th>sl", 
        zExplain
    );
    for(i=0; i<p->nPhrase; i++){
      zExplain = sqlite4MAppendf(
        db, zExplain, "%s<th>tf</span><sub>%d</sub>", zExplain, i
      );
    }
    for(iField=0; rc==SQLITE4_OK && iField<nField; iField++){
      int dl, tf;
      double scale;
      rc = fts5GetSizeFreqScale(
          pCtx, nArg, apArg, bm25mask, 0, iField, &dl, &tf, &scale
      );
      zExplain = sqlite4MAppendf(
          db, zExplain, "%s<tr><td>%d<td>%.2f<td>%.2f<td>%d", 
          zExplain, iField, scale, p->aAvgdl[iField], dl
      );
      for(i=0; rc==SQLITE4_OK && i<p->nPhrase; i++){
        rc = fts5GetSizeFreqScale(
            pCtx, nArg, apArg, bm25mask, i, iField, &dl, &tf, &scale
        );
        zExplain = sqlite4MAppendf(db, zExplain, "%s<td>%d", zExplain, tf);
      }
    }
    zExplain = sqlite4MAppendf(
        db, zExplain, "%s</table><table><tr><th>Phrase<th>IDF", zExplain
    );
    for(i=0; i<nField; i++){
      zExplain = sqlite4MAppendf(
        db, zExplain, "%s<th><span style=text-decoration:overline>"
        "tf</span><sub>s%d</sub>", zExplain, i
      );
    }
    zExplain = sqlite4MAppendf(db, zExplain, "%s<th>rank", zExplain);
  }

  for(i=0; rc==SQLITE4_OK && i<p->nPhrase; i++){
    int iField;
    double tfns = 0.0;            /* Sum of tfn for all fields */
    double prank;                 /* Contribution to rank of this phrase */

    if( bExplain ){
      zExplain = sqlite4MAppendf(
        db, zExplain, "%s<tr><td>%d<td>%.2f", zExplain, i, p->aIdf[i]
      );
    }

    for(iField = 0; iField<nField; iField++){
      double scale = 1.0;
      int tf;                       /* Count of phrase i in row (term freq.) */
      double tfn;                   /* Normalized term frequency */
      int dl;                       /* Tokens in this row (document length) */
      double B;                     /* B from formula 3.20 */

      /* Set variable tf to the total number of occurrences of phrase iPhrase
      ** in this row/field. And dl to the number of tokens in the current 
      ** row/field. */
      rc = fts5GetSizeFreqScale(
          pCtx, nArg, apArg, bm25mask, i, iField, &dl, &tf, &scale
      );

      B = (1.0 - b) + b * (double)dl / p->aAvgdl[iField];    /* 3.20 */
      tfn = scale * (double)tf / B;
      tfns += tfn;                                           /* 3.19 */


      if( bExplain ){
        zExplain = sqlite4MAppendf(db, zExplain, "%s<td>%.2f", zExplain, tfn);
      }
    }

    prank = p->aIdf[i] * tfns / (k1 + tfns);                 /* 3.21 */
    if( bExplain ){
      zExplain = sqlite4MAppendf(db, zExplain, "%s<td>%.2f", zExplain, prank);
    }

    /* Add it to the overall rank */
    rank += prank;
  }

  if( rc==SQLITE4_OK ){
    if( bExplain ){
      zExplain = sqlite4MAppendf(
          db, zExplain, "%s</table><b>overall rank=%.2f</b>", zExplain, rank
      );
      sqlite4_result_text(pCtx, zExplain, -1, SQLITE4_TRANSIENT);
    }else{
      sqlite4_result_double(pCtx, rank);
    }
  }else{
    sqlite4_result_error_code(pCtx, rc);
  }
  sqlite4DbFree(db, zExplain);
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
    fts5SnippetAppend(p, p->zEllipses, -1);
    p->iFrom = -1;
    return 1;
  }else{
    int bHighlight;               /* True to highlight term */

    bHighlight = (p->mask & ((u64)1 << (iOff-p->iOff))) ? 1 : 0;

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

      allmask |= ((u64)1 << iPhrase);

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
        aMask[iPhrase] = aMask[iPhrase] | ((u64)1 << (nToken-1+iPTok));
      }

      for(iMask=0; iMask<nPhrase; iMask++){
        int iBit;
        if( aMask[iMask] ){
          nScore += ((((u64)1 << iMask) & mask) ? 100 : 1);
        }else{
          miss |= ((u64)1 << iMask);
        }
        tmask = tmask | aMask[iMask];
        /* TODO: This is the Hamming Weight. There are much more efficient
        ** ways to calculate it. */
        for(iBit=0; iBit<nToken; iBit++){
          if( tmask & ((u64)1 << iBit) ) nScore++;
        }
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
  assert( mask & ((u64)1 << (nToken-1)) );

  for(i=0; (mask & ((u64)1 << i))==0; i++);
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
  int i;
  sqlite4_env *pEnv = sqlite4_db_env(db);

  struct RankFunction {
    const char *zName;
    int mask;
  } aRank[] = {
    { "rank",  0 },
    { "erank",  BM25_EXPLAIN },
    { "rankc",  BM25_FCOLUMNS },
    { "erankc", BM25_FCOLUMNS|BM25_EXPLAIN },
    { "ranks",  BM25_FSTREAMS },
    { "eranks", BM25_FSTREAMS|BM25_EXPLAIN }
  };

  rc = sqlite4_create_tokenizer(db, "simple", (void *)pEnv, 
      fts5SimpleCreate, fts5SimpleTokenize, fts5SimpleDestroy
  );
  if( rc==SQLITE4_OK ){
    rc = sqlite4_create_mi_function(
        db, "snippet", -1, SQLITE4_UTF8, 0, fts5Snippet, 0);
  }

  for(i=0; rc==SQLITE4_OK && i<ArraySize(aRank); i++){
    void *p = SQLITE4_INT_TO_PTR(aRank[i].mask);
    const char *z = aRank[i].zName;
    rc = sqlite4_create_mi_function(db, z, -1, SQLITE4_UTF8, p, fts5Rank, 0);
  }

  return rc;
}

