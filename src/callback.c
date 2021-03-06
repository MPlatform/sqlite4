/*
** 2005 May 23 
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
** This file contains functions used to access the internal hash tables
** of user defined functions and collation sequences.
*/

#include "sqliteInt.h"

/*
** Invoke the 'collation needed' callback to request a collation sequence
** in the encoding enc of name zName, length nName.
*/
static void callCollNeeded(sqlite4 *db, int enc, const char *zName){
  assert( !db->xCollNeeded || !db->xCollNeeded16 );
  if( db->xCollNeeded ){
    char *zExternal = sqlite4DbStrDup(db, zName);
    if( !zExternal ) return;
    db->xCollNeeded(db->pCollNeededArg, db, enc, zExternal);
    sqlite4DbFree(db, zExternal);
  }
#ifndef SQLITE4_OMIT_UTF16
  if( db->xCollNeeded16 ){
    char const *zExternal;
    sqlite4_value *pTmp = sqlite4ValueNew(db);
    sqlite4ValueSetStr(pTmp, -1, zName, SQLITE4_UTF8, SQLITE4_STATIC, 0);
    zExternal = sqlite4ValueText(pTmp, SQLITE4_UTF16NATIVE);
    if( zExternal ){
      db->xCollNeeded16(db->pCollNeededArg, db, (int)ENC(db), zExternal);
    }
    sqlite4ValueFree(pTmp);
  }
#endif
}

/*
** This routine is called if the collation factory fails to deliver a
** collation function in the best encoding but there may be other versions
** of this collation function (for other text encodings) available. Use one
** of these instead if they exist. Avoid a UTF-8 <-> UTF-16 conversion if
** possible.
*/
static int synthCollSeq(sqlite4 *db, CollSeq *pColl){
  CollSeq *pColl2;
  char *z = pColl->zName;
  int i;
  static const u8 aEnc[] = { SQLITE4_UTF16BE, SQLITE4_UTF16LE, SQLITE4_UTF8 };
  for(i=0; i<3; i++){
    pColl2 = sqlite4FindCollSeq(db, aEnc[i], z, 0);
    if( pColl2->xCmp!=0 ){
      memcpy(pColl, pColl2, sizeof(CollSeq));
      pColl->xDel = 0;         /* Do not copy the destructor */
      return SQLITE4_OK;
    }
  }
  return SQLITE4_ERROR;
}

/*
** This function is responsible for invoking the collation factory callback
** or substituting a collation sequence of a different encoding when the
** requested collation sequence is not available in the desired encoding.
** 
** If it is not NULL, then pColl must point to the database native encoding 
** collation sequence with name zName, length nName.
**
** The return value is either the collation sequence to be used in database
** db for collation type name zName, length nName, or NULL, if no collation
** sequence can be found.
**
** See also: sqlite4LocateCollSeq(), sqlite4FindCollSeq()
*/
CollSeq *sqlite4GetCollSeq(
  sqlite4* db,          /* The database connection */
  u8 enc,               /* The desired encoding for the collating sequence */
  CollSeq *pColl,       /* Collating sequence with native encoding, or NULL */
  const char *zName     /* Collating sequence name */
){
  CollSeq *p;

  p = pColl;
  if( !p ){
    p = sqlite4FindCollSeq(db, enc, zName, 0);
  }
  if( !p || !p->xCmp ){
    /* No collation sequence of this type for this encoding is registered.
    ** Call the collation factory to see if it can supply us with one.
    */
    callCollNeeded(db, enc, zName);
    p = sqlite4FindCollSeq(db, enc, zName, 0);
  }
  if( p && !p->xCmp && synthCollSeq(db, p) ){
    p = 0;
  }
  assert( !p || p->xCmp );
  return p;
}

/*
** This routine is called on a collation sequence before it is used to
** check that it is defined. An undefined collation sequence exists when
** a database is loaded that contains references to collation sequences
** that have not been defined by sqlite4_create_collation() etc.
**
** If required, this routine calls the 'collation needed' callback to
** request a definition of the collating sequence. If this doesn't work, 
** an equivalent collating sequence that uses a text encoding different
** from the main database is substituted, if one is available.
*/
int sqlite4CheckCollSeq(Parse *pParse, CollSeq *pColl){
  if( pColl ){
    const char *zName = pColl->zName;
    sqlite4 *db = pParse->db;
    CollSeq *p = sqlite4GetCollSeq(db, ENC(db), pColl, zName);
    if( !p ){
      sqlite4ErrorMsg(pParse, "no such collation sequence: %s", zName);
      pParse->nErr++;
      return SQLITE4_ERROR;
    }
    assert( p==pColl );
  }
  return SQLITE4_OK;
}



/*
** Locate and return an entry from the db.aCollSeq hash table. If the entry
** specified by zName and nName is not found and parameter 'create' is
** true, then create a new entry. Otherwise return NULL.
**
** Each pointer stored in the sqlite4.aCollSeq hash table contains an
** array of three CollSeq structures. The first is the collation sequence
** prefferred for UTF-8, the second UTF-16le, and the third UTF-16be.
**
** Stored immediately after the three collation sequences is a copy of
** the collation sequence name. A pointer to this string is stored in
** each collation sequence structure.
*/
static CollSeq *findCollSeqEntry(
  sqlite4 *db,          /* Database connection */
  const char *zName,    /* Name of the collating sequence */
  int create            /* Create a new entry if true */
){
  CollSeq *pColl;
  int nName = sqlite4Strlen30(zName);
  pColl = sqlite4HashFind(&db->aCollSeq, zName, nName);

  if( 0==pColl && create ){
    pColl = sqlite4DbMallocZero(db, 3*sizeof(*pColl) + nName + 1 );
    if( pColl ){
      CollSeq *pDel = 0;
      pColl[0].zName = (char*)&pColl[3];
      pColl[0].enc = SQLITE4_UTF8;
      pColl[1].zName = (char*)&pColl[3];
      pColl[1].enc = SQLITE4_UTF16LE;
      pColl[2].zName = (char*)&pColl[3];
      pColl[2].enc = SQLITE4_UTF16BE;
      memcpy(pColl[0].zName, zName, nName);
      pColl[0].zName[nName] = 0;
      pDel = sqlite4HashInsert(&db->aCollSeq, pColl[0].zName, nName, pColl);

      /* If a malloc() failure occurred in sqlite4HashInsert(), it will 
      ** return the pColl pointer to be deleted (because it wasn't added
      ** to the hash table).
      */
      assert( pDel==0 || pDel==pColl );
      if( pDel!=0 ){
        db->mallocFailed = 1;
        sqlite4DbFree(db, pDel);
        pColl = 0;
      }
    }
  }
  return pColl;
}

/*
** Parameter zName points to a UTF-8 encoded string nName bytes long.
** Return the CollSeq* pointer for the collation sequence named zName
** for the encoding 'enc' from the database 'db'.
**
** If the entry specified is not found and 'create' is true, then create a
** new entry.  Otherwise return NULL.
**
** A separate function sqlite4LocateCollSeq() is a wrapper around
** this routine.  sqlite4LocateCollSeq() invokes the collation factory
** if necessary and generates an error message if the collating sequence
** cannot be found.
**
** See also: sqlite4LocateCollSeq(), sqlite4GetCollSeq()
*/
CollSeq *sqlite4FindCollSeq(
  sqlite4 *db,
  u8 enc,
  const char *zName,
  int create
){
  CollSeq *pColl;
  if( zName ){
    pColl = findCollSeqEntry(db, zName, create);
  }else{
    pColl = db->pDfltColl;
  }
  assert( SQLITE4_UTF8==1 && SQLITE4_UTF16LE==2 && SQLITE4_UTF16BE==3 );
  assert( enc>=SQLITE4_UTF8 && enc<=SQLITE4_UTF16BE );
  if( pColl ) pColl += enc-1;
  return pColl;
}

/* During the search for the best function definition, this procedure
** is called to test how well the function passed as the first argument
** matches the request for a function with nArg arguments in a system
** that uses encoding enc. The value returned indicates how well the
** request is matched. A higher value indicates a better match.
**
** The returned value is always between 0 and 6, as follows:
**
** 0: Not a match, or if nArg<0 and the function is has no implementation.
** 1: A variable arguments function that prefers UTF-8 when a UTF-16
**    encoding is requested, or vice versa.
** 2: A variable arguments function that uses UTF-16BE when UTF-16LE is
**    requested, or vice versa.
** 3: A variable arguments function using the same text encoding.
** 4: A function with the exact number of arguments requested that
**    prefers UTF-8 when a UTF-16 encoding is requested, or vice versa.
** 5: A function with the exact number of arguments requested that
**    prefers UTF-16LE when UTF-16BE is requested, or vice versa.
** 6: An exact match.
**
*/
static int matchQuality(FuncDef *p, int nArg, u8 enc){
  int match = 0;
  if( p->nArg==-1 || p->nArg==nArg 
   || (nArg==-1 && (p->xFunc!=0 || p->xStep!=0))
  ){
    match = 1;
    if( p->nArg==nArg || nArg==-1 ){
      match = 4;
    }
    if( enc==p->iPrefEnc ){
      match += 2;
    }
    else if( (enc==SQLITE4_UTF16LE && p->iPrefEnc==SQLITE4_UTF16BE) ||
             (enc==SQLITE4_UTF16BE && p->iPrefEnc==SQLITE4_UTF16LE) ){
      match += 1;
    }
  }
  return match;
}

/*
** Search a FuncDefTable for a function with the given name.  Return
** a pointer to the matching FuncDef if found, or 0 if there is no match.
*/
static FuncDef *functionSearch(
  FuncDefTable *pFuncTab,  /* Lookup table to search */
  const char *zFunc,       /* Name of function */
  int nFunc                /* Number of bytes in zFunc */
){
  FuncDef *p;
  if( nFunc<0 ) nFunc = sqlite4Strlen30(zFunc);
  for(p=pFuncTab->pFirst; p; p=p->pNextName){
    if( sqlite4_strnicmp(p->zName, zFunc, nFunc)==0 && p->zName[nFunc]==0 ){
      return p;
    }
  }
  return 0;
}

/*
** Insert a new FuncDef into a FuncDefTable.
**
** The pDef is private to a single database connection if isBuiltIn==0 but
** is a global public function if isBuiltIn==1.  In the case of isBuiltIn==1,
** any changes to pDef are made in a way that is threadsafe, so that if two
** threads attempt to build the global function table at the same time, the
** trailing thread will perform harmless no-op assignments.
*/
void sqlite4FuncDefInsert(
  FuncDefTable *pFuncTab,  /* The lookup table into which to insert */
  FuncDef *pDef,           /* The function definition to insert */
  int isBuiltIn            /* True if pDef is one of the built-in functions */
){
  FuncDef *pOther;
  assert( pDef->pSameName==0 || isBuiltIn );
  assert( pDef->pNextName==0 || isBuiltIn );
  if( pFuncTab->pFirst==0 ){
    pFuncTab->pFirst = pDef;
    pFuncTab->pLast = pDef;
    pFuncTab->pSame = pDef;
  }else if( isBuiltIn
            && sqlite4_stricmp(pDef->zName, pFuncTab->pLast->zName)==0 ){
    assert( pFuncTab->pSame->pSameName==0 || pFuncTab->pSame->pSameName==pDef );
    pFuncTab->pSame->pSameName = pDef;
    pFuncTab->pSame = pDef;
  }else if( !isBuiltIn && (pOther=functionSearch(pFuncTab,pDef->zName,-1))!=0 ){
    pDef->pSameName = pOther->pSameName;
    pOther->pSameName = pDef;
  }else{
    assert( pFuncTab->pLast->pNextName==0 || pFuncTab->pLast->pNextName==pDef );
    pFuncTab->pLast->pNextName = pDef;
    pFuncTab->pLast = pDef;
    pFuncTab->pSame = pDef;
  }
}
  
  

/*
** Locate a user function given a name, a number of arguments and a flag
** indicating whether the function prefers UTF-16 over UTF-8.  Return a
** pointer to the FuncDef structure that defines that function, or return
** NULL if the function does not exist.
**
** If the createFlag argument is true, then a new (blank) FuncDef
** structure is created and liked into the "db" structure if a
** no matching function previously existed.  When createFlag is true
** and the nArg parameter is -1, then only a function that accepts
** any number of arguments will be returned.
**
** If createFlag is false and nArg is -1, then the first valid
** function found is returned.  A function is valid if either xFunc
** or xStep is non-zero.
**
** If createFlag is false, then a function with the required name and
** number of arguments may be returned even if the eTextRep flag does not
** match that requested.
*/
FuncDef *sqlite4FindFunction(
  sqlite4 *db,       /* An open database */
  const char *zName, /* Name of the function.  Not null-terminated */
  int nName,         /* Number of characters in the name */
  int nArg,          /* Number of arguments.  -1 means any number */
  u8 enc,            /* Preferred text encoding */
  int createFlag     /* Create new entry if true and does not otherwise exist */
){
  FuncDef *p;         /* Iterator variable */
  FuncDef *pBest = 0; /* Best match found so far */
  int bestScore = 0;  /* Score of best match */


  assert( enc==SQLITE4_UTF8 || enc==SQLITE4_UTF16LE || enc==SQLITE4_UTF16BE );

  /* First search for a match amongst the application-defined functions.
  */
  p = functionSearch(&db->aFunc, zName, nName);
  while( p ){
    int score = matchQuality(p, nArg, enc);
    if( score>bestScore ){
      pBest = p;
      bestScore = score;
    }
    p = p->pSameName;
  }

  /* If no match is found, search the built-in functions.
  **
  ** If the SQLITE4_PreferBuiltin flag is set, then search the built-in
  ** functions even if a prior app-defined function was found.  And give
  ** priority to built-in functions.
  **
  ** Except, if createFlag is true, that means that we are trying to
  ** install a new function.  Whatever FuncDef structure is returned it will
  ** have fields overwritten with new information appropriate for the
  ** new function.  But the FuncDefs for built-in functions are read-only.
  ** So we must not search for built-ins when creating a new function.
  */ 
  if( !createFlag && (pBest==0 || (db->flags & SQLITE4_PreferBuiltin)!=0) ){
    FuncDefTable *pFuncTab = &db->pEnv->aGlobalFuncs;
    bestScore = 0;
    p = functionSearch(pFuncTab, zName, nName);
    while( p ){
      int score = matchQuality(p, nArg, enc);
      if( score>bestScore ){
        pBest = p;
        bestScore = score;
      }
      p = p->pSameName;
    }
  }

  /* If the createFlag parameter is true and the search did not reveal an
  ** exact match for the name, number of arguments and encoding, then add a
  ** new entry to the hash table and return it.
  */
  if( createFlag && (bestScore<6 || pBest->nArg!=nArg) && 
      (pBest = sqlite4DbMallocZero(db, sizeof(*pBest)+nName+1))!=0 ){
    pBest->zName = (char *)&pBest[1];
    pBest->nArg = (u16)nArg;
    pBest->iPrefEnc = enc;
    memcpy(pBest->zName, zName, nName);
    pBest->zName[nName] = 0;
    sqlite4FuncDefInsert(&db->aFunc, pBest, 0);
  }

  if( pBest && (pBest->xStep || pBest->xFunc || createFlag) ){
    return pBest;
  }
  return 0;
}

/*
** Free all resources held by the schema structure. The void* argument points
** at a Schema struct. This function does not call sqlite4DbFree(db, ) on the 
** pointer itself, it just cleans up subsidiary resources (i.e. the contents
** of the schema hash tables).
**
** The Schema.cache_size variable is not cleared.
*/
void sqlite4SchemaClear(sqlite4_env *pEnv, Schema *pSchema){
  Hash temp1;
  Hash temp2;
  HashElem *pElem;

  temp1 = pSchema->tblHash;
  temp2 = pSchema->trigHash;
  sqlite4HashInit(pEnv, &pSchema->trigHash, 0);
  sqlite4HashClear(&pSchema->idxHash);
  for(pElem=sqliteHashFirst(&temp2); pElem; pElem=sqliteHashNext(pElem)){
    sqlite4DeleteTrigger(0, (Trigger*)sqliteHashData(pElem));
  }
  sqlite4HashClear(&temp2);
  sqlite4HashInit(pEnv, &pSchema->tblHash, 0);
  for(pElem=sqliteHashFirst(&temp1); pElem; pElem=sqliteHashNext(pElem)){
    Table *pTab = sqliteHashData(pElem);
    sqlite4DeleteTable(0, pTab);
  }
  sqlite4HashClear(&temp1);
  sqlite4HashClear(&pSchema->fkeyHash);
  pSchema->pSeqTab = 0;
  if( pSchema->flags & DB_SchemaLoaded ){
    pSchema->iGeneration++;
    pSchema->flags &= ~DB_SchemaLoaded;
  }
}

/*
** Find and return the schema associated with a BTree.  Create
** a new one if necessary.
*/
Schema *sqlite4SchemaGet(sqlite4 *db){
  Schema * p;
  p = (Schema *)sqlite4DbMallocZero(0, sizeof(Schema));
  if( !p ){
    db->mallocFailed = 1;
  }else if ( 0==p->file_format ){
    sqlite4HashInit(db->pEnv, &p->tblHash, 0);
    sqlite4HashInit(db->pEnv, &p->idxHash, 0);
    sqlite4HashInit(db->pEnv, &p->trigHash, 0);
    sqlite4HashInit(db->pEnv, &p->fkeyHash, 0);
    p->enc = SQLITE4_UTF8;
  }
  return p;
}
