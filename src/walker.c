/*
** 2008 August 16
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains routines used for walking the parser tree for
** an SQL statement.
*/
#include "sqliteInt.h"
#include <stdlib.h>
#include <string.h>


/*
** Walk an expression tree.  Invoke the callback once for each node
** of the expression, while decending.  (In other words, the callback
** is invoked before visiting children.)
**
** The return value from the callback should be one of the WRC_*
** constants to specify how to proceed with the walk.
**
**    WRC_Continue      Continue descending down the tree.
**
**    WRC_Prune         Do not descend into child nodes.  But allow
**                      the walk to continue with sibling nodes.
**
**    WRC_Abort         Do no more callbacks.  Unwind the stack and
**                      return the top-level walk call.
**
** The return value from this routine is WRC_Abort to abandon the tree walk
** and WRC_Continue to continue.
*/
int sqlite4WalkExpr(Walker *pWalker, Expr *pExpr){
  int rc;
  if( pExpr==0 ) return WRC_Continue;
  testcase( ExprHasProperty(pExpr, EP_TokenOnly) );
  testcase( ExprHasProperty(pExpr, EP_Reduced) );
  rc = pWalker->xExprCallback(pWalker, pExpr);
  if( rc==WRC_Continue
              && !ExprHasAnyProperty(pExpr,EP_TokenOnly) ){
    if( sqlite4WalkExpr(pWalker, pExpr->pLeft) ) return WRC_Abort;
    if( sqlite4WalkExpr(pWalker, pExpr->pRight) ) return WRC_Abort;
    if( ExprHasProperty(pExpr, EP_xIsSelect) ){
      if( sqlite4WalkSelect(pWalker, pExpr->x.pSelect) ) return WRC_Abort;
    }else{
      if( sqlite4WalkExprList(pWalker, pExpr->x.pList) ) return WRC_Abort;
    }
  }
  return rc & WRC_Abort;
}

/*
** Call sqlite4WalkExpr() for every expression in list p or until
** an abort request is seen.
*/
int sqlite4WalkExprList(Walker *pWalker, ExprList *p){
  int i;
  ExprListItem *pItem;
  if( p ){
    for(i=p->nExpr, pItem=p->a; i>0; i--, pItem++){
      if( sqlite4WalkExpr(pWalker, pItem->pExpr) ) return WRC_Abort;
    }
  }
  return WRC_Continue;
}

/*
** Walk all expressions associated with SELECT statement p.  Do
** not invoke the SELECT callback on p, but do (of course) invoke
** any expr callbacks and SELECT callbacks that come from subqueries.
** Return WRC_Abort or WRC_Continue.
*/
int sqlite4WalkSelectExpr(Walker *pWalker, Select *p){
  if( sqlite4WalkExprList(pWalker, p->pEList) ) return WRC_Abort;
  if( sqlite4WalkExpr(pWalker, p->pWhere) ) return WRC_Abort;
  if( sqlite4WalkExprList(pWalker, p->pGroupBy) ) return WRC_Abort;
  if( sqlite4WalkExpr(pWalker, p->pHaving) ) return WRC_Abort;
  if( sqlite4WalkExprList(pWalker, p->pOrderBy) ) return WRC_Abort;
  if( sqlite4WalkExpr(pWalker, p->pLimit) ) return WRC_Abort;
  if( sqlite4WalkExpr(pWalker, p->pOffset) ) return WRC_Abort;
  return WRC_Continue;
}

/*
** Walk the parse trees associated with all subqueries in the
** FROM clause of SELECT statement p.  Do not invoke the select
** callback on p, but do invoke it on each FROM clause subquery
** and on any subqueries further down in the tree.  Return 
** WRC_Abort or WRC_Continue;
*/
int sqlite4WalkSelectFrom(Walker *pWalker, Select *p){
  SrcList *pSrc;
  int i;
  SrcListItem *pItem;

  pSrc = p->pSrc;
  if( ALWAYS(pSrc) ){
    for(i=pSrc->nSrc, pItem=pSrc->a; i>0; i--, pItem++){
      if( sqlite4WalkSelect(pWalker, pItem->pSelect) ){
        return WRC_Abort;
      }
    }
  }
  return WRC_Continue;
} 

/*
** Call sqlite4WalkExpr() for every expression in Select statement p.
** Invoke sqlite4WalkSelect() for subqueries in the FROM clause and
** on the compound select chain, p->pPrior.
**
** Return WRC_Continue under normal conditions.  Return WRC_Abort if
** there is an abort request.
**
** If the Walker does not have an xSelectCallback() then this routine
** is a no-op returning WRC_Continue.
*/
int sqlite4WalkSelect(Walker *pWalker, Select *p){
  int rc;
  if( p==0 || pWalker->xSelectCallback==0 ) return WRC_Continue;
  rc = WRC_Continue;
  while( p  ){
    rc = pWalker->xSelectCallback(pWalker, p);
    if( rc ) break;
    if( sqlite4WalkSelectExpr(pWalker, p) ) return WRC_Abort;
    if( sqlite4WalkSelectFrom(pWalker, p) ) return WRC_Abort;
    p = p->pPrior;
  }
  return rc & WRC_Abort;
}
