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
      rc = sqlite4_mi_row_count(pCtx, -1, -1, &N);
      for(i=0; rc==SQLITE4_OK && i<nPhrase; i++){
        rc = sqlite4_mi_row_count(pCtx, -1, i, &ni);
        if( rc==SQLITE4_OK ){
          p->aIdf[i] = log((0.5 + N - ni) / (0.5 + ni));
        }
      }

      /* Determine the average document length */
      if( rc==SQLITE4_OK ){
        int nTotal;
        rc = sqlite4_mi_total_size(pCtx, -1, &nTotal);
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
    rc = sqlite4_mi_match_count(pCtx, -1, i, &tf); 
    if( rc==SQLITE4_OK ) rc = sqlite4_mi_column_size(pCtx, -1, &dl); 

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

static void fts5Snippet(sqlite4_context *pCtx, int nArg, sqlite4_value **apArg){
}

static int fts5SimpleTokenize(
  void *pCtx, 
  sqlite4_tokenizer *p,
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

