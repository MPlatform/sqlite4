/*
** 2009 November 25
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains code used to insert the values of host parameters
** (aka "wildcards") into the SQL text output by sqlite4_trace().
**
** The Vdbe parse-tree explainer is also found here.
*/
#include "sqliteInt.h"
#include "vdbeInt.h"

#ifndef SQLITE_OMIT_TRACE

/*
** zSql is a zero-terminated string of UTF-8 SQL text.  Return the number of
** bytes in this text up to but excluding the first character in
** a host parameter.  If the text contains no host parameters, return
** the total number of bytes in the text.
*/
static int findNextHostParameter(const char *zSql, int *pnToken){
  int tokenType;
  int nTotal = 0;
  int n;

  *pnToken = 0;
  while( zSql[0] ){
    n = sqlite4GetToken((u8*)zSql, &tokenType);
    assert( n>0 && tokenType!=TK_ILLEGAL );
    if( tokenType==TK_VARIABLE ){
      *pnToken = n;
      break;
    }
    nTotal += n;
    zSql += n;
  }
  return nTotal;
}

/*
** This function returns a pointer to a nul-terminated string in memory
** obtained from sqlite4DbMalloc(). If sqlite4.vdbeExecCnt is 1, then the
** string contains a copy of zRawSql but with host parameters expanded to 
** their current bindings. Or, if sqlite4.vdbeExecCnt is greater than 1, 
** then the returned string holds a copy of zRawSql with "-- " prepended
** to each line of text.
**
** The calling function is responsible for making sure the memory returned
** is eventually freed.
**
** ALGORITHM:  Scan the input string looking for host parameters in any of
** these forms:  ?, ?N, $A, @A, :A.  Take care to avoid text within
** string literals, quoted identifier names, and comments.  For text forms,
** the host parameter index is found by scanning the perpared
** statement for the corresponding OP_Variable opcode.  Once the host
** parameter index is known, locate the value in p->aVar[].  Then render
** the value as a literal in place of the host parameter name.
*/
char *sqlite4VdbeExpandSql(
  Vdbe *p,                 /* The prepared statement being evaluated */
  const char *zRawSql      /* Raw text of the SQL statement */
){
  sqlite4 *db;             /* The database connection */
  int idx = 0;             /* Index of a host parameter */
  int nextIndex = 1;       /* Index of next ? host parameter */
  int n;                   /* Length of a token prefix */
  int nToken;              /* Length of the parameter token */
  int i;                   /* Loop counter */
  Mem *pVar;               /* Value of a host parameter */
  StrAccum out;            /* Accumulate the output here */
  char zBase[100];         /* Initial working space */

  db = p->db;
  sqlite4StrAccumInit(&out, zBase, sizeof(zBase), 
                      db->aLimit[SQLITE_LIMIT_LENGTH]);
  out.db = db;
  out.pEnv = db->pEnv;
  if( db->vdbeExecCnt>1 ){
    while( *zRawSql ){
      const char *zStart = zRawSql;
      while( *(zRawSql++)!='\n' && *zRawSql );
      sqlite4StrAccumAppend(&out, "-- ", 3);
      sqlite4StrAccumAppend(&out, zStart, (int)(zRawSql-zStart));
    }
  }else{
    while( zRawSql[0] ){
      n = findNextHostParameter(zRawSql, &nToken);
      assert( n>0 );
      sqlite4StrAccumAppend(&out, zRawSql, n);
      zRawSql += n;
      assert( zRawSql[0] || nToken==0 );
      if( nToken==0 ) break;
      if( zRawSql[0]=='?' ){
        if( nToken>1 ){
          assert( sqlite4Isdigit(zRawSql[1]) );
          sqlite4GetInt32(&zRawSql[1], &idx);
        }else{
          idx = nextIndex;
        }
      }else{
        assert( zRawSql[0]==':' || zRawSql[0]=='$' || zRawSql[0]=='@' );
        testcase( zRawSql[0]==':' );
        testcase( zRawSql[0]=='$' );
        testcase( zRawSql[0]=='@' );
        idx = sqlite4VdbeParameterIndex(p, zRawSql, nToken);
        assert( idx>0 );
      }
      zRawSql += nToken;
      nextIndex = idx + 1;
      assert( idx>0 && idx<=p->nVar );
      pVar = &p->aVar[idx-1];
      if( pVar->flags & MEM_Null ){
        sqlite4StrAccumAppend(&out, "NULL", 4);
      }else if( pVar->flags & MEM_Int ){
        sqlite4XPrintf(&out, "%lld", pVar->u.i);
      }else if( pVar->flags & MEM_Real ){
        sqlite4XPrintf(&out, "%!.16g", pVar->r);
      }else if( pVar->flags & MEM_Str ){
#ifndef SQLITE_OMIT_UTF16
        u8 enc = ENC(db);
        if( enc!=SQLITE_UTF8 ){
          Mem utf8;
          memset(&utf8, 0, sizeof(utf8));
          utf8.db = db;
          sqlite4VdbeMemSetStr(&utf8, pVar->z, pVar->n, enc, SQLITE_STATIC);
          sqlite4VdbeChangeEncoding(&utf8, SQLITE_UTF8);
          sqlite4XPrintf(&out, "'%.*q'", utf8.n, utf8.z);
          sqlite4VdbeMemRelease(&utf8);
        }else
#endif
        {
          sqlite4XPrintf(&out, "'%.*q'", pVar->n, pVar->z);
        }
      }else if( pVar->flags & MEM_Zero ){
        sqlite4XPrintf(&out, "zeroblob(%d)", pVar->u.nZero);
      }else{
        assert( pVar->flags & MEM_Blob );
        sqlite4StrAccumAppend(&out, "x'", 2);
        for(i=0; i<pVar->n; i++){
          sqlite4XPrintf(&out, "%02x", pVar->z[i]&0xff);
        }
        sqlite4StrAccumAppend(&out, "'", 1);
      }
    }
  }
  return sqlite4StrAccumFinish(&out);
}

#endif /* #ifndef SQLITE_OMIT_TRACE */

/*****************************************************************************
** The following code implements the data-structure explaining logic
** for the Vdbe.
*/

#if defined(SQLITE_ENABLE_TREE_EXPLAIN)

/*
** Allocate a new Explain object
*/
void sqlite4ExplainBegin(Vdbe *pVdbe){
  if( pVdbe ){
    Explain *p;
    sqlite4 *db = pVdbe->db;
    sqlite4_env *pEnv = db->pEnv;
    sqlite4BeginBenignMalloc(pEnv);
    p = sqlite4_malloc(pEnv, sizeof(Explain) );
    if( p ){
      memset(p, 0, sizeof(*p));
      p->pVdbe = pVdbe;
      sqlite4_free(pEnv, pVdbe->pExplain);
      pVdbe->pExplain = p;
      sqlite4StrAccumInit(&p->str, p->zBase, sizeof(p->zBase),
                          SQLITE_MAX_LENGTH);
      p->str.useMalloc = 2;
      p->str.pEnv = pEnv;
    }else{
      sqlite4EndBenignMalloc(pEnv);
    }
  }
}

/*
** Return true if the Explain ends with a new-line.
*/
static int endsWithNL(Explain *p){
  return p && p->str.zText && p->str.nChar
           && p->str.zText[p->str.nChar-1]=='\n';
}
    
/*
** Append text to the indentation
*/
void sqlite4ExplainPrintf(Vdbe *pVdbe, const char *zFormat, ...){
  Explain *p;
  if( pVdbe && (p = pVdbe->pExplain)!=0 ){
    va_list ap;
    if( p->nIndent && endsWithNL(p) ){
      int n = p->nIndent;
      if( n>ArraySize(p->aIndent) ) n = ArraySize(p->aIndent);
      sqlite4AppendSpace(&p->str, p->aIndent[n-1]);
    }   
    va_start(ap, zFormat);
    sqlite4VXPrintf(&p->str, 1, zFormat, ap);
    va_end(ap);
  }
}

/*
** Append a '\n' if there is not already one.
*/
void sqlite4ExplainNL(Vdbe *pVdbe){
  Explain *p;
  if( pVdbe && (p = pVdbe->pExplain)!=0 && !endsWithNL(p) ){
    sqlite4StrAccumAppend(&p->str, "\n", 1);
  }
}

/*
** Push a new indentation level.  Subsequent lines will be indented
** so that they begin at the current cursor position.
*/
void sqlite4ExplainPush(Vdbe *pVdbe){
  Explain *p;
  if( pVdbe && (p = pVdbe->pExplain)!=0 ){
    if( p->str.zText && p->nIndent<ArraySize(p->aIndent) ){
      const char *z = p->str.zText;
      int i = p->str.nChar-1;
      int x;
      while( i>=0 && z[i]!='\n' ){ i--; }
      x = (p->str.nChar - 1) - i;
      if( p->nIndent && x<p->aIndent[p->nIndent-1] ){
        x = p->aIndent[p->nIndent-1];
      }
      p->aIndent[p->nIndent] = x;
    }
    p->nIndent++;
  }
}

/*
** Pop the indentation stack by one level.
*/
void sqlite4ExplainPop(Vdbe *p){
  if( p && p->pExplain ) p->pExplain->nIndent--;
}

/*
** Free the indentation structure
*/
void sqlite4ExplainFinish(Vdbe *pVdbe){
  if( pVdbe && pVdbe->pExplain ){
    sqlite4_env *pEnv = pVdbe->db->pEnv;
    sqlite4_free(pEnv, pVdbe->zExplain);
    sqlite4ExplainNL(pVdbe);
    pVdbe->zExplain = sqlite4StrAccumFinish(&pVdbe->pExplain->str);
    sqlite4_free(pEnv, pVdbe->pExplain);
    pVdbe->pExplain = 0;
    sqlite4EndBenignMalloc(pEnv);
  }
}

/*
** Return the explanation of a virtual machine.
*/
const char *sqlite4VdbeExplanation(Vdbe *pVdbe){
  return (pVdbe && pVdbe->zExplain) ? pVdbe->zExplain : 0;
}
#endif /* defined(SQLITE_DEBUG) */
