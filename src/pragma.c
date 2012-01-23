/*
** 2003 April 6
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code used to implement the PRAGMA command.
*/
#include "sqliteInt.h"

/*
** Interpret the given string as a safety level.  Return 0 for OFF,
** 1 for ON or NORMAL and 2 for FULL.  Return 1 for an empty or 
** unrecognized string argument.
**
** Note that the values returned are one less that the values that
** should be passed into sqlite4BtreeSetSafetyLevel().  The is done
** to support legacy SQL code.  The safety level used to be boolean
** and older scripts may have used numbers 0 for OFF and 1 for ON.
*/
static u8 getSafetyLevel(const char *z){
                             /* 123456789 123456789 */
  static const char zText[] = "onoffalseyestruefull";
  static const u8 iOffset[] = {0, 1, 2, 4, 9, 12, 16};
  static const u8 iLength[] = {2, 2, 3, 5, 3, 4, 4};
  static const u8 iValue[] =  {1, 0, 0, 0, 1, 1, 2};
  int i, n;
  if( sqlite4Isdigit(*z) ){
    return (u8)sqlite4Atoi(z);
  }
  n = sqlite4Strlen30(z);
  for(i=0; i<ArraySize(iLength); i++){
    if( iLength[i]==n && sqlite4StrNICmp(&zText[iOffset[i]],z,n)==0 ){
      return iValue[i];
    }
  }
  return 1;
}

/*
** Interpret the given string as a boolean value.
*/
u8 sqlite4GetBoolean(const char *z){
  return getSafetyLevel(z)&1;
}

/* The sqlite4GetBoolean() function is used by other modules but the
** remainder of this file is specific to PRAGMA processing.  So omit
** the rest of the file if PRAGMAs are omitted from the build.
*/
#if !defined(SQLITE_OMIT_PRAGMA)

/*
** Interpret the given string as a locking mode value.
*/
static int getLockingMode(const char *z){
  if( z ){
    if( 0==sqlite4StrICmp(z, "exclusive") ) return PAGER_LOCKINGMODE_EXCLUSIVE;
    if( 0==sqlite4StrICmp(z, "normal") ) return PAGER_LOCKINGMODE_NORMAL;
  }
  return PAGER_LOCKINGMODE_QUERY;
}

#ifndef SQLITE_OMIT_AUTOVACUUM
/*
** Interpret the given string as an auto-vacuum mode value.
**
** The following strings, "none", "full" and "incremental" are 
** acceptable, as are their numeric equivalents: 0, 1 and 2 respectively.
*/
static int getAutoVacuum(const char *z){
  int i;
  if( 0==sqlite4StrICmp(z, "none") ) return BTREE_AUTOVACUUM_NONE;
  if( 0==sqlite4StrICmp(z, "full") ) return BTREE_AUTOVACUUM_FULL;
  if( 0==sqlite4StrICmp(z, "incremental") ) return BTREE_AUTOVACUUM_INCR;
  i = sqlite4Atoi(z);
  return (u8)((i>=0&&i<=2)?i:0);
}
#endif /* ifndef SQLITE_OMIT_AUTOVACUUM */

#ifndef SQLITE_OMIT_PAGER_PRAGMAS
/*
** Interpret the given string as a temp db location. Return 1 for file
** backed temporary databases, 2 for the Red-Black tree in memory database
** and 0 to use the compile-time default.
*/
static int getTempStore(const char *z){
  if( z[0]>='0' && z[0]<='2' ){
    return z[0] - '0';
  }else if( sqlite4StrICmp(z, "file")==0 ){
    return 1;
  }else if( sqlite4StrICmp(z, "memory")==0 ){
    return 2;
  }else{
    return 0;
  }
}
#endif /* SQLITE_PAGER_PRAGMAS */

#ifndef SQLITE_OMIT_PAGER_PRAGMAS
/*
** Invalidate temp storage, either when the temp storage is changed
** from default, or when 'file' and the temp_store_directory has changed
*/
static int invalidateTempStorage(Parse *pParse){
  sqlite4 *db = pParse->db;
  if( db->aDb[1].pBt!=0 ){
    if( !db->autoCommit || sqlite4BtreeIsInReadTrans(db->aDb[1].pBt) ){
      sqlite4ErrorMsg(pParse, "temporary storage cannot be changed "
        "from within a transaction");
      return SQLITE_ERROR;
    }
    sqlite4BtreeClose(db->aDb[1].pBt);
    db->aDb[1].pBt = 0;
    sqlite4KVStoreClose(db->aDb[1].pKV);
    db->aDb[1].pKV = 0;
    sqlite4ResetInternalSchema(db, -1);
  }
  return SQLITE_OK;
}
#endif /* SQLITE_PAGER_PRAGMAS */

#ifndef SQLITE_OMIT_PAGER_PRAGMAS
/*
** If the TEMP database is open, close it and mark the database schema
** as needing reloading.  This must be done when using the SQLITE_TEMP_STORE
** or DEFAULT_TEMP_STORE pragmas.
*/
static int changeTempStorage(Parse *pParse, const char *zStorageType){
  int ts = getTempStore(zStorageType);
  sqlite4 *db = pParse->db;
  if( db->temp_store==ts ) return SQLITE_OK;
  if( invalidateTempStorage( pParse ) != SQLITE_OK ){
    return SQLITE_ERROR;
  }
  db->temp_store = (u8)ts;
  return SQLITE_OK;
}
#endif /* SQLITE_PAGER_PRAGMAS */

/*
** Generate code to return a single integer value.
*/
static void returnSingleInt(Parse *pParse, const char *zLabel, i64 value){
  Vdbe *v = sqlite4GetVdbe(pParse);
  int mem = ++pParse->nMem;
  i64 *pI64 = sqlite4DbMallocRaw(pParse->db, sizeof(value));
  if( pI64 ){
    memcpy(pI64, &value, sizeof(value));
  }
  sqlite4VdbeAddOp4(v, OP_Int64, 0, mem, 0, (char*)pI64, P4_INT64);
  sqlite4VdbeSetNumCols(v, 1);
  sqlite4VdbeSetColName(v, 0, COLNAME_NAME, zLabel, SQLITE_STATIC);
  sqlite4VdbeAddOp2(v, OP_ResultRow, mem, 1);
}

#ifndef SQLITE_OMIT_FLAG_PRAGMAS
/*
** Check to see if zRight and zLeft refer to a pragma that queries
** or changes one of the flags in db->flags.  Return 1 if so and 0 if not.
** Also, implement the pragma.
*/
static int flagPragma(Parse *pParse, const char *zLeft, const char *zRight){
  static const struct sPragmaType {
    const char *zName;  /* Name of the pragma */
    int mask;           /* Mask for the db->flags value */
  } aPragma[] = {
    { "full_column_names",        SQLITE_FullColNames  },
    { "short_column_names",       SQLITE_ShortColNames },
    { "count_changes",            SQLITE_CountRows     },
    { "empty_result_callbacks",   SQLITE_NullCallback  },
    { "legacy_file_format",       SQLITE_LegacyFileFmt },
    { "fullfsync",                SQLITE_FullFSync     },
    { "checkpoint_fullfsync",     SQLITE_CkptFullFSync },
    { "reverse_unordered_selects", SQLITE_ReverseOrder  },
#ifndef SQLITE_OMIT_AUTOMATIC_INDEX
    { "automatic_index",          SQLITE_AutoIndex     },
#endif
#ifdef SQLITE_DEBUG
    { "sql_trace",                SQLITE_SqlTrace      },
    { "vdbe_listing",             SQLITE_VdbeListing   },
    { "vdbe_trace",               SQLITE_VdbeTrace     },
#endif
#ifndef SQLITE_OMIT_CHECK
    { "ignore_check_constraints", SQLITE_IgnoreChecks  },
#endif
    /* The following is VERY experimental */
    { "writable_schema",          SQLITE_WriteSchema|SQLITE_RecoveryMode },

    /* TODO: Maybe it shouldn't be possible to change the ReadUncommitted
    ** flag if there are any active statements. */
    { "read_uncommitted",         SQLITE_ReadUncommitted },
    { "recursive_triggers",       SQLITE_RecTriggers },

    /* This flag may only be set if both foreign-key and trigger support
    ** are present in the build.  */
#if !defined(SQLITE_OMIT_FOREIGN_KEY) && !defined(SQLITE_OMIT_TRIGGER)
    { "foreign_keys",             SQLITE_ForeignKeys },
#endif
  };
  int i;
  const struct sPragmaType *p;
  for(i=0, p=aPragma; i<ArraySize(aPragma); i++, p++){
    if( sqlite4StrICmp(zLeft, p->zName)==0 ){
      sqlite4 *db = pParse->db;
      Vdbe *v;
      v = sqlite4GetVdbe(pParse);
      assert( v!=0 );  /* Already allocated by sqlite4Pragma() */
      if( ALWAYS(v) ){
        if( zRight==0 ){
          returnSingleInt(pParse, p->zName, (db->flags & p->mask)!=0 );
        }else{
          int mask = p->mask;          /* Mask of bits to set or clear. */
          if( db->autoCommit==0 ){
            /* Foreign key support may not be enabled or disabled while not
            ** in auto-commit mode.  */
            mask &= ~(SQLITE_ForeignKeys);
          }

          if( sqlite4GetBoolean(zRight) ){
            db->flags |= mask;
          }else{
            db->flags &= ~mask;
          }

          /* Many of the flag-pragmas modify the code generated by the SQL 
          ** compiler (eg. count_changes). So add an opcode to expire all
          ** compiled SQL statements after modifying a pragma value.
          */
          sqlite4VdbeAddOp2(v, OP_Expire, 0, 0);
        }
      }

      return 1;
    }
  }
  return 0;
}
#endif /* SQLITE_OMIT_FLAG_PRAGMAS */

/*
** Return a human-readable name for a constraint resolution action.
*/
#ifndef SQLITE_OMIT_FOREIGN_KEY
static const char *actionName(u8 action){
  const char *zName;
  switch( action ){
    case OE_SetNull:  zName = "SET NULL";        break;
    case OE_SetDflt:  zName = "SET DEFAULT";     break;
    case OE_Cascade:  zName = "CASCADE";         break;
    case OE_Restrict: zName = "RESTRICT";        break;
    default:          zName = "NO ACTION";  
                      assert( action==OE_None ); break;
  }
  return zName;
}
#endif


/*
** Parameter eMode must be one of the PAGER_JOURNALMODE_XXX constants
** defined in pager.h. This function returns the associated lowercase
** journal-mode name.
*/
const char *sqlite4JournalModename(int eMode){
  static char * const azModeName[] = {
    "delete", "persist", "off", "truncate", "memory"
#ifndef SQLITE_OMIT_WAL
     , "wal"
#endif
  };
  assert( PAGER_JOURNALMODE_DELETE==0 );
  assert( PAGER_JOURNALMODE_PERSIST==1 );
  assert( PAGER_JOURNALMODE_OFF==2 );
  assert( PAGER_JOURNALMODE_TRUNCATE==3 );
  assert( PAGER_JOURNALMODE_MEMORY==4 );
  assert( PAGER_JOURNALMODE_WAL==5 );
  assert( eMode>=0 && eMode<=ArraySize(azModeName) );

  if( eMode==ArraySize(azModeName) ) return 0;
  return azModeName[eMode];
}

/*
** Process a pragma statement.  
**
** Pragmas are of this form:
**
**      PRAGMA [database.]id [= value]
**
** The identifier might also be a string.  The value is a string, and
** identifier, or a number.  If minusFlag is true, then the value is
** a number that was preceded by a minus sign.
**
** If the left side is "database.id" then pId1 is the database name
** and pId2 is the id.  If the left side is just "id" then pId1 is the
** id and pId2 is any empty string.
*/
void sqlite4Pragma(
  Parse *pParse, 
  Token *pId1,        /* First part of [database.]id field */
  Token *pId2,        /* Second part of [database.]id field, or NULL */
  Token *pValue,      /* Token for <value>, or NULL */
  int minusFlag       /* True if a '-' sign preceded <value> */
){
  char *zLeft = 0;       /* Nul-terminated UTF-8 string <id> */
  char *zRight = 0;      /* Nul-terminated UTF-8 string <value>, or NULL */
  const char *zDb = 0;   /* The database name */
  Token *pId;            /* Pointer to <id> token */
  int iDb;               /* Database index for <database> */
  sqlite4 *db = pParse->db;
  Db *pDb;
  Vdbe *v = pParse->pVdbe = sqlite4VdbeCreate(db);
  if( v==0 ) return;
  sqlite4VdbeRunOnlyOnce(v);
  pParse->nMem = 2;

  /* Interpret the [database.] part of the pragma statement. iDb is the
  ** index of the database this pragma is being applied to in db.aDb[]. */
  iDb = sqlite4TwoPartName(pParse, pId1, pId2, &pId);
  if( iDb<0 ) return;
  pDb = &db->aDb[iDb];

  /* If the temp database has been explicitly named as part of the 
  ** pragma, make sure it is open. 
  */
  if( iDb==1 && sqlite4OpenTempDatabase(pParse) ){
    return;
  }

  zLeft = sqlite4NameFromToken(db, pId);
  if( !zLeft ) return;
  if( minusFlag ){
    zRight = sqlite4MPrintf(db, "-%T", pValue);
  }else{
    zRight = sqlite4NameFromToken(db, pValue);
  }

  assert( pId2 );
  zDb = pId2->n>0 ? pDb->zName : 0;
  if( sqlite4AuthCheck(pParse, SQLITE_PRAGMA, zLeft, zRight, zDb) ){
    goto pragma_out;
  }
 
#if !defined(SQLITE_OMIT_PAGER_PRAGMAS) && !defined(SQLITE_OMIT_DEPRECATED)
  /*
  **  PRAGMA [database.]default_cache_size
  **  PRAGMA [database.]default_cache_size=N
  **
  ** The first form reports the current persistent setting for the
  ** page cache size.  The value returned is the maximum number of
  ** pages in the page cache.  The second form sets both the current
  ** page cache size value and the persistent page cache size value
  ** stored in the database file.
  **
  ** Older versions of SQLite would set the default cache size to a
  ** negative number to indicate synchronous=OFF.  These days, synchronous
  ** is always on by default regardless of the sign of the default cache
  ** size.  But continue to take the absolute value of the default cache
  ** size of historical compatibility.
  */
  if( sqlite4StrICmp(zLeft,"default_cache_size")==0 ){
    static const VdbeOpList getCacheSize[] = {
      { OP_Transaction, 0, 0,        0},                         /* 0 */
      { OP_ReadCookie,  0, 1,        BTREE_DEFAULT_CACHE_SIZE},  /* 1 */
      { OP_IfPos,       1, 7,        0},
      { OP_Integer,     0, 2,        0},
      { OP_Subtract,    1, 2,        1},
      { OP_IfPos,       1, 7,        0},
      { OP_Integer,     0, 1,        0},                         /* 6 */
      { OP_ResultRow,   1, 1,        0},
    };
    int addr;
    if( sqlite4ReadSchema(pParse) ) goto pragma_out;
    sqlite4VdbeUsesBtree(v, iDb);
    if( !zRight ){
      sqlite4VdbeSetNumCols(v, 1);
      sqlite4VdbeSetColName(v, 0, COLNAME_NAME, "cache_size", SQLITE_STATIC);
      pParse->nMem += 2;
      addr = sqlite4VdbeAddOpList(v, ArraySize(getCacheSize), getCacheSize);
      sqlite4VdbeChangeP1(v, addr, iDb);
      sqlite4VdbeChangeP1(v, addr+1, iDb);
      sqlite4VdbeChangeP1(v, addr+6, SQLITE_DEFAULT_CACHE_SIZE);
    }else{
      int size = sqlite4AbsInt32(sqlite4Atoi(zRight));
      sqlite4BeginWriteOperation(pParse, 0, iDb);
      sqlite4VdbeAddOp2(v, OP_Integer, size, 1);
      sqlite4VdbeAddOp3(v, OP_SetCookie, iDb, BTREE_DEFAULT_CACHE_SIZE, 1);
      assert( sqlite4SchemaMutexHeld(db, iDb, 0) );
      pDb->pSchema->cache_size = size;
      sqlite4BtreeSetCacheSize(pDb->pBt, pDb->pSchema->cache_size);
    }
  }else
#endif /* !SQLITE_OMIT_PAGER_PRAGMAS && !SQLITE_OMIT_DEPRECATED */

#if !defined(SQLITE_OMIT_PAGER_PRAGMAS)
  /*
  **  PRAGMA [database.]page_size
  **  PRAGMA [database.]page_size=N
  **
  ** The first form reports the current setting for the
  ** database page size in bytes.  The second form sets the
  ** database page size value.  The value can only be set if
  ** the database has not yet been created.
  */
  if( sqlite4StrICmp(zLeft,"page_size")==0 ){
    Btree *pBt = pDb->pBt;
    assert( pBt!=0 );
    if( !zRight ){
      int size = ALWAYS(pBt) ? sqlite4BtreeGetPageSize(pBt) : 0;
      returnSingleInt(pParse, "page_size", size);
    }else{
      /* Malloc may fail when setting the page-size, as there is an internal
      ** buffer that the pager module resizes using sqlite4_realloc().
      */
      db->nextPagesize = sqlite4Atoi(zRight);
      if( SQLITE_NOMEM==sqlite4BtreeSetPageSize(pBt, db->nextPagesize,-1,0) ){
        db->mallocFailed = 1;
      }
    }
  }else

  /*
  **  PRAGMA [database.]secure_delete
  **  PRAGMA [database.]secure_delete=ON/OFF
  **
  ** The first form reports the current setting for the
  ** secure_delete flag.  The second form changes the secure_delete
  ** flag setting and reports thenew value.
  */
  if( sqlite4StrICmp(zLeft,"secure_delete")==0 ){
    Btree *pBt = pDb->pBt;
    int b = -1;
    assert( pBt!=0 );
    if( zRight ){
      b = sqlite4GetBoolean(zRight);
    }
    if( pId2->n==0 && b>=0 ){
      int ii;
      for(ii=0; ii<db->nDb; ii++){
        sqlite4BtreeSecureDelete(db->aDb[ii].pBt, b);
      }
    }
    b = sqlite4BtreeSecureDelete(pBt, b);
    returnSingleInt(pParse, "secure_delete", b);
  }else

  /*
  **  PRAGMA [database.]max_page_count
  **  PRAGMA [database.]max_page_count=N
  **
  ** The first form reports the current setting for the
  ** maximum number of pages in the database file.  The 
  ** second form attempts to change this setting.  Both
  ** forms return the current setting.
  **
  ** The absolute value of N is used.  This is undocumented and might
  ** change.  The only purpose is to provide an easy way to test
  ** the sqlite4AbsInt32() function.
  **
  **  PRAGMA [database.]page_count
  **
  ** Return the number of pages in the specified database.
  */
  if( sqlite4StrICmp(zLeft,"page_count")==0
   || sqlite4StrICmp(zLeft,"max_page_count")==0
  ){
    int iReg;
    if( sqlite4ReadSchema(pParse) ) goto pragma_out;
    sqlite4CodeVerifySchema(pParse, iDb);
    iReg = ++pParse->nMem;
    if( sqlite4Tolower(zLeft[0])=='p' ){
      sqlite4VdbeAddOp2(v, OP_Pagecount, iDb, iReg);
    }else{
      sqlite4VdbeAddOp3(v, OP_MaxPgcnt, iDb, iReg, 
                        sqlite4AbsInt32(sqlite4Atoi(zRight)));
    }
    sqlite4VdbeAddOp2(v, OP_ResultRow, iReg, 1);
    sqlite4VdbeSetNumCols(v, 1);
    sqlite4VdbeSetColName(v, 0, COLNAME_NAME, zLeft, SQLITE_TRANSIENT);
  }else

  /*
  **  PRAGMA [database.]locking_mode
  **  PRAGMA [database.]locking_mode = (normal|exclusive)
  */
  if( sqlite4StrICmp(zLeft,"locking_mode")==0 ){
    const char *zRet = "normal";
    int eMode = getLockingMode(zRight);

    if( pId2->n==0 && eMode==PAGER_LOCKINGMODE_QUERY ){
      /* Simple "PRAGMA locking_mode;" statement. This is a query for
      ** the current default locking mode (which may be different to
      ** the locking-mode of the main database).
      */
      eMode = db->dfltLockMode;
    }else{
      Pager *pPager;
      if( pId2->n==0 ){
        /* This indicates that no database name was specified as part
        ** of the PRAGMA command. In this case the locking-mode must be
        ** set on all attached databases, as well as the main db file.
        **
        ** Also, the sqlite4.dfltLockMode variable is set so that
        ** any subsequently attached databases also use the specified
        ** locking mode.
        */
        int ii;
        assert(pDb==&db->aDb[0]);
        for(ii=2; ii<db->nDb; ii++){
          pPager = sqlite4BtreePager(db->aDb[ii].pBt);
          sqlite4PagerLockingMode(pPager, eMode);
        }
        db->dfltLockMode = (u8)eMode;
      }
      pPager = sqlite4BtreePager(pDb->pBt);
      eMode = sqlite4PagerLockingMode(pPager, eMode);
    }

    assert(eMode==PAGER_LOCKINGMODE_NORMAL||eMode==PAGER_LOCKINGMODE_EXCLUSIVE);
    if( eMode==PAGER_LOCKINGMODE_EXCLUSIVE ){
      zRet = "exclusive";
    }
    sqlite4VdbeSetNumCols(v, 1);
    sqlite4VdbeSetColName(v, 0, COLNAME_NAME, "locking_mode", SQLITE_STATIC);
    sqlite4VdbeAddOp4(v, OP_String8, 0, 1, 0, zRet, 0);
    sqlite4VdbeAddOp2(v, OP_ResultRow, 1, 1);
  }else

  /*
  **  PRAGMA [database.]journal_mode
  **  PRAGMA [database.]journal_mode =
  **                      (delete|persist|off|truncate|memory|wal|off)
  */
  if( sqlite4StrICmp(zLeft,"journal_mode")==0 ){
    int eMode;        /* One of the PAGER_JOURNALMODE_XXX symbols */
    int ii;           /* Loop counter */

    /* Force the schema to be loaded on all databases.  This causes all
    ** database files to be opened and the journal_modes set.  This is
    ** necessary because subsequent processing must know if the databases
    ** are in WAL mode. */
    if( sqlite4ReadSchema(pParse) ){
      goto pragma_out;
    }

    sqlite4VdbeSetNumCols(v, 1);
    sqlite4VdbeSetColName(v, 0, COLNAME_NAME, "journal_mode", SQLITE_STATIC);

    if( zRight==0 ){
      /* If there is no "=MODE" part of the pragma, do a query for the
      ** current mode */
      eMode = PAGER_JOURNALMODE_QUERY;
    }else{
      const char *zMode;
      int n = sqlite4Strlen30(zRight);
      for(eMode=0; (zMode = sqlite4JournalModename(eMode))!=0; eMode++){
        if( sqlite4StrNICmp(zRight, zMode, n)==0 ) break;
      }
      if( !zMode ){
        /* If the "=MODE" part does not match any known journal mode,
        ** then do a query */
        eMode = PAGER_JOURNALMODE_QUERY;
      }
    }
    if( eMode==PAGER_JOURNALMODE_QUERY && pId2->n==0 ){
      /* Convert "PRAGMA journal_mode" into "PRAGMA main.journal_mode" */
      iDb = 0;
      pId2->n = 1;
    }
    for(ii=db->nDb-1; ii>=0; ii--){
      if( db->aDb[ii].pBt && (ii==iDb || pId2->n==0) ){
        sqlite4VdbeUsesBtree(v, ii);
        sqlite4VdbeAddOp3(v, OP_JournalMode, ii, 1, eMode);
      }
    }
    sqlite4VdbeAddOp2(v, OP_ResultRow, 1, 1);
  }else

  /*
  **  PRAGMA [database.]journal_size_limit
  **  PRAGMA [database.]journal_size_limit=N
  **
  ** Get or set the size limit on rollback journal files.
  */
  if( sqlite4StrICmp(zLeft,"journal_size_limit")==0 ){
    Pager *pPager = sqlite4BtreePager(pDb->pBt);
    i64 iLimit = -2;
    if( zRight ){
      sqlite4Atoi64(zRight, &iLimit, 1000000, SQLITE_UTF8);
      if( iLimit<-1 ) iLimit = -1;
    }
    iLimit = sqlite4PagerJournalSizeLimit(pPager, iLimit);
    returnSingleInt(pParse, "journal_size_limit", iLimit);
  }else

#endif /* SQLITE_OMIT_PAGER_PRAGMAS */

  /*
  **  PRAGMA [database.]auto_vacuum
  **  PRAGMA [database.]auto_vacuum=N
  **
  ** Get or set the value of the database 'auto-vacuum' parameter.
  ** The value is one of:  0 NONE 1 FULL 2 INCREMENTAL
  */
#ifndef SQLITE_OMIT_AUTOVACUUM
  if( sqlite4StrICmp(zLeft,"auto_vacuum")==0 ){
    Btree *pBt = pDb->pBt;
    assert( pBt!=0 );
    if( sqlite4ReadSchema(pParse) ){
      goto pragma_out;
    }
    if( !zRight ){
      int auto_vacuum;
      if( ALWAYS(pBt) ){
         auto_vacuum = sqlite4BtreeGetAutoVacuum(pBt);
      }else{
         auto_vacuum = SQLITE_DEFAULT_AUTOVACUUM;
      }
      returnSingleInt(pParse, "auto_vacuum", auto_vacuum);
    }else{
      int eAuto = getAutoVacuum(zRight);
      assert( eAuto>=0 && eAuto<=2 );
      db->nextAutovac = (u8)eAuto;
      if( ALWAYS(eAuto>=0) ){
        /* Call SetAutoVacuum() to set initialize the internal auto and
        ** incr-vacuum flags. This is required in case this connection
        ** creates the database file. It is important that it is created
        ** as an auto-vacuum capable db.
        */
        int rc = sqlite4BtreeSetAutoVacuum(pBt, eAuto);
        if( rc==SQLITE_OK && (eAuto==1 || eAuto==2) ){
          /* When setting the auto_vacuum mode to either "full" or 
          ** "incremental", write the value of meta[6] in the database
          ** file. Before writing to meta[6], check that meta[3] indicates
          ** that this really is an auto-vacuum capable database.
          */
          static const VdbeOpList setMeta6[] = {
            { OP_Transaction,    0,         1,                 0},    /* 0 */
            { OP_ReadCookie,     0,         1,         BTREE_LARGEST_ROOT_PAGE},
            { OP_If,             1,         0,                 0},    /* 2 */
            { OP_Halt,           SQLITE_OK, OE_Abort,          0},    /* 3 */
            { OP_Integer,        0,         1,                 0},    /* 4 */
            { OP_SetCookie,      0,         BTREE_INCR_VACUUM, 1},    /* 5 */
          };
          int iAddr;
          iAddr = sqlite4VdbeAddOpList(v, ArraySize(setMeta6), setMeta6);
          sqlite4VdbeChangeP1(v, iAddr, iDb);
          sqlite4VdbeChangeP1(v, iAddr+1, iDb);
          sqlite4VdbeChangeP2(v, iAddr+2, iAddr+4);
          sqlite4VdbeChangeP1(v, iAddr+4, eAuto-1);
          sqlite4VdbeChangeP1(v, iAddr+5, iDb);
          sqlite4VdbeUsesBtree(v, iDb);
        }
      }
    }
  }else
#endif

  /*
  **  PRAGMA [database.]incremental_vacuum(N)
  **
  ** Do N steps of incremental vacuuming on a database.
  */
#ifndef SQLITE_OMIT_AUTOVACUUM
  if( sqlite4StrICmp(zLeft,"incremental_vacuum")==0 ){
    int iLimit, addr;
    if( sqlite4ReadSchema(pParse) ){
      goto pragma_out;
    }
    if( zRight==0 || !sqlite4GetInt32(zRight, &iLimit) || iLimit<=0 ){
      iLimit = 0x7fffffff;
    }
    sqlite4BeginWriteOperation(pParse, 0, iDb);
    sqlite4VdbeAddOp2(v, OP_Integer, iLimit, 1);
    addr = sqlite4VdbeAddOp1(v, OP_IncrVacuum, iDb);
    sqlite4VdbeAddOp1(v, OP_ResultRow, 1);
    sqlite4VdbeAddOp2(v, OP_AddImm, 1, -1);
    sqlite4VdbeAddOp2(v, OP_IfPos, 1, addr);
    sqlite4VdbeJumpHere(v, addr);
  }else
#endif

#ifndef SQLITE_OMIT_PAGER_PRAGMAS
  /*
  **  PRAGMA [database.]cache_size
  **  PRAGMA [database.]cache_size=N
  **
  ** The first form reports the current local setting for the
  ** page cache size. The second form sets the local
  ** page cache size value.  If N is positive then that is the
  ** number of pages in the cache.  If N is negative, then the
  ** number of pages is adjusted so that the cache uses -N kibibytes
  ** of memory.
  */
  if( sqlite4StrICmp(zLeft,"cache_size")==0 ){
    if( sqlite4ReadSchema(pParse) ) goto pragma_out;
    assert( sqlite4SchemaMutexHeld(db, iDb, 0) );
    if( !zRight ){
      returnSingleInt(pParse, "cache_size", pDb->pSchema->cache_size);
    }else{
      int size = sqlite4Atoi(zRight);
      pDb->pSchema->cache_size = size;
      sqlite4BtreeSetCacheSize(pDb->pBt, pDb->pSchema->cache_size);
    }
  }else

  /*
  **   PRAGMA temp_store
  **   PRAGMA temp_store = "default"|"memory"|"file"
  **
  ** Return or set the local value of the temp_store flag.  Changing
  ** the local value does not make changes to the disk file and the default
  ** value will be restored the next time the database is opened.
  **
  ** Note that it is possible for the library compile-time options to
  ** override this setting
  */
  if( sqlite4StrICmp(zLeft, "temp_store")==0 ){
    if( !zRight ){
      returnSingleInt(pParse, "temp_store", db->temp_store);
    }else{
      changeTempStorage(pParse, zRight);
    }
  }else

  /*
  **   PRAGMA temp_store_directory
  **   PRAGMA temp_store_directory = ""|"directory_name"
  **
  ** Return or set the local value of the temp_store_directory flag.  Changing
  ** the value sets a specific directory to be used for temporary files.
  ** Setting to a null string reverts to the default temporary directory search.
  ** If temporary directory is changed, then invalidateTempStorage.
  **
  */
  if( sqlite4StrICmp(zLeft, "temp_store_directory")==0 ){
    if( !zRight ){
      if( sqlite4_temp_directory ){
        sqlite4VdbeSetNumCols(v, 1);
        sqlite4VdbeSetColName(v, 0, COLNAME_NAME, 
            "temp_store_directory", SQLITE_STATIC);
        sqlite4VdbeAddOp4(v, OP_String8, 0, 1, 0, sqlite4_temp_directory, 0);
        sqlite4VdbeAddOp2(v, OP_ResultRow, 1, 1);
      }
    }else{
#ifndef SQLITE_OMIT_WSD
      if( zRight[0] ){
        int rc;
        int res;
        rc = sqlite4OsAccess(db->pVfs, zRight, SQLITE_ACCESS_READWRITE, &res);
        if( rc!=SQLITE_OK || res==0 ){
          sqlite4ErrorMsg(pParse, "not a writable directory");
          goto pragma_out;
        }
      }
      if( SQLITE_TEMP_STORE==0
       || (SQLITE_TEMP_STORE==1 && db->temp_store<=1)
       || (SQLITE_TEMP_STORE==2 && db->temp_store==1)
      ){
        invalidateTempStorage(pParse);
      }
      sqlite4_free(sqlite4_temp_directory);
      if( zRight[0] ){
        sqlite4_temp_directory = sqlite4_mprintf("%s", zRight);
      }else{
        sqlite4_temp_directory = 0;
      }
#endif /* SQLITE_OMIT_WSD */
    }
  }else

#if !defined(SQLITE_ENABLE_LOCKING_STYLE)
#  if defined(__APPLE__)
#    define SQLITE_ENABLE_LOCKING_STYLE 1
#  else
#    define SQLITE_ENABLE_LOCKING_STYLE 0
#  endif
#endif
#if SQLITE_ENABLE_LOCKING_STYLE
  /*
   **   PRAGMA [database.]lock_proxy_file
   **   PRAGMA [database.]lock_proxy_file = ":auto:"|"lock_file_path"
   **
   ** Return or set the value of the lock_proxy_file flag.  Changing
   ** the value sets a specific file to be used for database access locks.
   **
   */
  if( sqlite4StrICmp(zLeft, "lock_proxy_file")==0 ){
    if( !zRight ){
      Pager *pPager = sqlite4BtreePager(pDb->pBt);
      char *proxy_file_path = NULL;
      sqlite4_file *pFile = sqlite4PagerFile(pPager);
      sqlite4OsFileControlHint(pFile, SQLITE_GET_LOCKPROXYFILE, 
                           &proxy_file_path);
      
      if( proxy_file_path ){
        sqlite4VdbeSetNumCols(v, 1);
        sqlite4VdbeSetColName(v, 0, COLNAME_NAME, 
                              "lock_proxy_file", SQLITE_STATIC);
        sqlite4VdbeAddOp4(v, OP_String8, 0, 1, 0, proxy_file_path, 0);
        sqlite4VdbeAddOp2(v, OP_ResultRow, 1, 1);
      }
    }else{
      Pager *pPager = sqlite4BtreePager(pDb->pBt);
      sqlite4_file *pFile = sqlite4PagerFile(pPager);
      int res;
      if( zRight[0] ){
        res=sqlite4OsFileControl(pFile, SQLITE_SET_LOCKPROXYFILE, 
                                     zRight);
      } else {
        res=sqlite4OsFileControl(pFile, SQLITE_SET_LOCKPROXYFILE, 
                                     NULL);
      }
      if( res!=SQLITE_OK ){
        sqlite4ErrorMsg(pParse, "failed to set lock proxy file");
        goto pragma_out;
      }
    }
  }else
#endif /* SQLITE_ENABLE_LOCKING_STYLE */      
    
  /*
  **   PRAGMA [database.]synchronous
  **   PRAGMA [database.]synchronous=OFF|ON|NORMAL|FULL
  **
  ** Return or set the local value of the synchronous flag.  Changing
  ** the local value does not make changes to the disk file and the
  ** default value will be restored the next time the database is
  ** opened.
  */
  if( sqlite4StrICmp(zLeft,"synchronous")==0 ){
    if( sqlite4ReadSchema(pParse) ) goto pragma_out;
    if( !zRight ){
      returnSingleInt(pParse, "synchronous", pDb->safety_level-1);
    }else{
      if( !db->autoCommit ){
        sqlite4ErrorMsg(pParse, 
            "Safety level may not be changed inside a transaction");
      }else{
        pDb->safety_level = getSafetyLevel(zRight)+1;
      }
    }
  }else
#endif /* SQLITE_OMIT_PAGER_PRAGMAS */

#ifndef SQLITE_OMIT_FLAG_PRAGMAS
  if( flagPragma(pParse, zLeft, zRight) ){
    /* The flagPragma() subroutine also generates any necessary code
    ** there is nothing more to do here */
  }else
#endif /* SQLITE_OMIT_FLAG_PRAGMAS */

#ifndef SQLITE_OMIT_SCHEMA_PRAGMAS
  /*
  **   PRAGMA table_info(<table>)
  **
  ** Return a single row for each column of the named table. The columns of
  ** the returned data set are:
  **
  ** cid:        Column id (numbered from left to right, starting at 0)
  ** name:       Column name
  ** type:       Column declaration type.
  ** notnull:    True if 'NOT NULL' is part of column declaration
  ** dflt_value: The default value for the column, if any.
  */
  if( sqlite4StrICmp(zLeft, "table_info")==0 && zRight ){
    Table *pTab;
    if( sqlite4ReadSchema(pParse) ) goto pragma_out;
    pTab = sqlite4FindTable(db, zRight, zDb);
    if( pTab ){
      int i;
      int nHidden = 0;
      Column *pCol;
      sqlite4VdbeSetNumCols(v, 6);
      pParse->nMem = 6;
      sqlite4VdbeSetColName(v, 0, COLNAME_NAME, "cid", SQLITE_STATIC);
      sqlite4VdbeSetColName(v, 1, COLNAME_NAME, "name", SQLITE_STATIC);
      sqlite4VdbeSetColName(v, 2, COLNAME_NAME, "type", SQLITE_STATIC);
      sqlite4VdbeSetColName(v, 3, COLNAME_NAME, "notnull", SQLITE_STATIC);
      sqlite4VdbeSetColName(v, 4, COLNAME_NAME, "dflt_value", SQLITE_STATIC);
      sqlite4VdbeSetColName(v, 5, COLNAME_NAME, "pk", SQLITE_STATIC);
      sqlite4ViewGetColumnNames(pParse, pTab);
      for(i=0, pCol=pTab->aCol; i<pTab->nCol; i++, pCol++){
        if( IsHiddenColumn(pCol) ){
          nHidden++;
          continue;
        }
        sqlite4VdbeAddOp2(v, OP_Integer, i-nHidden, 1);
        sqlite4VdbeAddOp4(v, OP_String8, 0, 2, 0, pCol->zName, 0);
        sqlite4VdbeAddOp4(v, OP_String8, 0, 3, 0,
           pCol->zType ? pCol->zType : "", 0);
        sqlite4VdbeAddOp2(v, OP_Integer, (pCol->notNull ? 1 : 0), 4);
        if( pCol->zDflt ){
          sqlite4VdbeAddOp4(v, OP_String8, 0, 5, 0, (char*)pCol->zDflt, 0);
        }else{
          sqlite4VdbeAddOp2(v, OP_Null, 0, 5);
        }
        sqlite4VdbeAddOp2(v, OP_Integer, pCol->isPrimKey, 6);
        sqlite4VdbeAddOp2(v, OP_ResultRow, 1, 6);
      }
    }
  }else

  if( sqlite4StrICmp(zLeft, "index_info")==0 && zRight ){
    Index *pIdx;
    Table *pTab;
    if( sqlite4ReadSchema(pParse) ) goto pragma_out;
    pIdx = sqlite4FindIndex(db, zRight, zDb);
    if( pIdx ){
      int i;
      pTab = pIdx->pTable;
      sqlite4VdbeSetNumCols(v, 3);
      pParse->nMem = 3;
      sqlite4VdbeSetColName(v, 0, COLNAME_NAME, "seqno", SQLITE_STATIC);
      sqlite4VdbeSetColName(v, 1, COLNAME_NAME, "cid", SQLITE_STATIC);
      sqlite4VdbeSetColName(v, 2, COLNAME_NAME, "name", SQLITE_STATIC);
      for(i=0; i<pIdx->nColumn; i++){
        int cnum = pIdx->aiColumn[i];
        sqlite4VdbeAddOp2(v, OP_Integer, i, 1);
        sqlite4VdbeAddOp2(v, OP_Integer, cnum, 2);
        assert( pTab->nCol>cnum );
        sqlite4VdbeAddOp4(v, OP_String8, 0, 3, 0, pTab->aCol[cnum].zName, 0);
        sqlite4VdbeAddOp2(v, OP_ResultRow, 1, 3);
      }
    }
  }else

  if( sqlite4StrICmp(zLeft, "index_list")==0 && zRight ){
    Index *pIdx;
    Table *pTab;
    if( sqlite4ReadSchema(pParse) ) goto pragma_out;
    pTab = sqlite4FindTable(db, zRight, zDb);
    if( pTab ){
      v = sqlite4GetVdbe(pParse);
      pIdx = pTab->pIndex;
      if( pIdx ){
        int i = 0; 
        sqlite4VdbeSetNumCols(v, 3);
        pParse->nMem = 3;
        sqlite4VdbeSetColName(v, 0, COLNAME_NAME, "seq", SQLITE_STATIC);
        sqlite4VdbeSetColName(v, 1, COLNAME_NAME, "name", SQLITE_STATIC);
        sqlite4VdbeSetColName(v, 2, COLNAME_NAME, "unique", SQLITE_STATIC);
        while(pIdx){
          sqlite4VdbeAddOp2(v, OP_Integer, i, 1);
          sqlite4VdbeAddOp4(v, OP_String8, 0, 2, 0, pIdx->zName, 0);
          sqlite4VdbeAddOp2(v, OP_Integer, pIdx->onError!=OE_None, 3);
          sqlite4VdbeAddOp2(v, OP_ResultRow, 1, 3);
          ++i;
          pIdx = pIdx->pNext;
        }
      }
    }
  }else

  if( sqlite4StrICmp(zLeft, "database_list")==0 ){
    int i;
    if( sqlite4ReadSchema(pParse) ) goto pragma_out;
    sqlite4VdbeSetNumCols(v, 3);
    pParse->nMem = 3;
    sqlite4VdbeSetColName(v, 0, COLNAME_NAME, "seq", SQLITE_STATIC);
    sqlite4VdbeSetColName(v, 1, COLNAME_NAME, "name", SQLITE_STATIC);
    sqlite4VdbeSetColName(v, 2, COLNAME_NAME, "file", SQLITE_STATIC);
    for(i=0; i<db->nDb; i++){
      if( db->aDb[i].pBt==0 ) continue;
      assert( db->aDb[i].zName!=0 );
      sqlite4VdbeAddOp2(v, OP_Integer, i, 1);
      sqlite4VdbeAddOp4(v, OP_String8, 0, 2, 0, db->aDb[i].zName, 0);
      sqlite4VdbeAddOp4(v, OP_String8, 0, 3, 0,
           sqlite4BtreeGetFilename(db->aDb[i].pBt), 0);
      sqlite4VdbeAddOp2(v, OP_ResultRow, 1, 3);
    }
  }else

  if( sqlite4StrICmp(zLeft, "collation_list")==0 ){
    int i = 0;
    HashElem *p;
    sqlite4VdbeSetNumCols(v, 2);
    pParse->nMem = 2;
    sqlite4VdbeSetColName(v, 0, COLNAME_NAME, "seq", SQLITE_STATIC);
    sqlite4VdbeSetColName(v, 1, COLNAME_NAME, "name", SQLITE_STATIC);
    for(p=sqliteHashFirst(&db->aCollSeq); p; p=sqliteHashNext(p)){
      CollSeq *pColl = (CollSeq *)sqliteHashData(p);
      sqlite4VdbeAddOp2(v, OP_Integer, i++, 1);
      sqlite4VdbeAddOp4(v, OP_String8, 0, 2, 0, pColl->zName, 0);
      sqlite4VdbeAddOp2(v, OP_ResultRow, 1, 2);
    }
  }else
#endif /* SQLITE_OMIT_SCHEMA_PRAGMAS */

#ifndef SQLITE_OMIT_FOREIGN_KEY
  if( sqlite4StrICmp(zLeft, "foreign_key_list")==0 && zRight ){
    FKey *pFK;
    Table *pTab;
    if( sqlite4ReadSchema(pParse) ) goto pragma_out;
    pTab = sqlite4FindTable(db, zRight, zDb);
    if( pTab ){
      v = sqlite4GetVdbe(pParse);
      pFK = pTab->pFKey;
      if( pFK ){
        int i = 0; 
        sqlite4VdbeSetNumCols(v, 8);
        pParse->nMem = 8;
        sqlite4VdbeSetColName(v, 0, COLNAME_NAME, "id", SQLITE_STATIC);
        sqlite4VdbeSetColName(v, 1, COLNAME_NAME, "seq", SQLITE_STATIC);
        sqlite4VdbeSetColName(v, 2, COLNAME_NAME, "table", SQLITE_STATIC);
        sqlite4VdbeSetColName(v, 3, COLNAME_NAME, "from", SQLITE_STATIC);
        sqlite4VdbeSetColName(v, 4, COLNAME_NAME, "to", SQLITE_STATIC);
        sqlite4VdbeSetColName(v, 5, COLNAME_NAME, "on_update", SQLITE_STATIC);
        sqlite4VdbeSetColName(v, 6, COLNAME_NAME, "on_delete", SQLITE_STATIC);
        sqlite4VdbeSetColName(v, 7, COLNAME_NAME, "match", SQLITE_STATIC);
        while(pFK){
          int j;
          for(j=0; j<pFK->nCol; j++){
            char *zCol = pFK->aCol[j].zCol;
            char *zOnDelete = (char *)actionName(pFK->aAction[0]);
            char *zOnUpdate = (char *)actionName(pFK->aAction[1]);
            sqlite4VdbeAddOp2(v, OP_Integer, i, 1);
            sqlite4VdbeAddOp2(v, OP_Integer, j, 2);
            sqlite4VdbeAddOp4(v, OP_String8, 0, 3, 0, pFK->zTo, 0);
            sqlite4VdbeAddOp4(v, OP_String8, 0, 4, 0,
                              pTab->aCol[pFK->aCol[j].iFrom].zName, 0);
            sqlite4VdbeAddOp4(v, zCol ? OP_String8 : OP_Null, 0, 5, 0, zCol, 0);
            sqlite4VdbeAddOp4(v, OP_String8, 0, 6, 0, zOnUpdate, 0);
            sqlite4VdbeAddOp4(v, OP_String8, 0, 7, 0, zOnDelete, 0);
            sqlite4VdbeAddOp4(v, OP_String8, 0, 8, 0, "NONE", 0);
            sqlite4VdbeAddOp2(v, OP_ResultRow, 1, 8);
          }
          ++i;
          pFK = pFK->pNextFrom;
        }
      }
    }
  }else
#endif /* !defined(SQLITE_OMIT_FOREIGN_KEY) */

#ifndef NDEBUG
  if( sqlite4StrICmp(zLeft, "parser_trace")==0 ){
    if( zRight ){
      if( sqlite4GetBoolean(zRight) ){
        sqlite4ParserTrace(stderr, "parser: ");
      }else{
        sqlite4ParserTrace(0, 0);
      }
    }
  }else
#endif

  /* Reinstall the LIKE and GLOB functions.  The variant of LIKE
  ** used will be case sensitive or not depending on the RHS.
  */
  if( sqlite4StrICmp(zLeft, "case_sensitive_like")==0 ){
    if( zRight ){
      sqlite4RegisterLikeFunctions(db, sqlite4GetBoolean(zRight));
    }
  }else

#ifndef SQLITE_INTEGRITY_CHECK_ERROR_MAX
# define SQLITE_INTEGRITY_CHECK_ERROR_MAX 100
#endif

#ifndef SQLITE_OMIT_INTEGRITY_CHECK
  /* Pragma "quick_check" is an experimental reduced version of 
  ** integrity_check designed to detect most database corruption
  ** without most of the overhead of a full integrity-check.
  */
  if( sqlite4StrICmp(zLeft, "integrity_check")==0
   || sqlite4StrICmp(zLeft, "quick_check")==0 
  ){
    int i, j, addr, mxErr;

    /* Code that appears at the end of the integrity check.  If no error
    ** messages have been generated, output OK.  Otherwise output the
    ** error message
    */
    static const VdbeOpList endCode[] = {
      { OP_AddImm,      1, 0,        0},    /* 0 */
      { OP_IfNeg,       1, 0,        0},    /* 1 */
      { OP_String8,     0, 3,        0},    /* 2 */
      { OP_ResultRow,   3, 1,        0},
    };

    int isQuick = (sqlite4Tolower(zLeft[0])=='q');

    /* Initialize the VDBE program */
    if( sqlite4ReadSchema(pParse) ) goto pragma_out;
    pParse->nMem = 6;
    sqlite4VdbeSetNumCols(v, 1);
    sqlite4VdbeSetColName(v, 0, COLNAME_NAME, "integrity_check", SQLITE_STATIC);

    /* Set the maximum error count */
    mxErr = SQLITE_INTEGRITY_CHECK_ERROR_MAX;
    if( zRight ){
      sqlite4GetInt32(zRight, &mxErr);
      if( mxErr<=0 ){
        mxErr = SQLITE_INTEGRITY_CHECK_ERROR_MAX;
      }
    }
    sqlite4VdbeAddOp2(v, OP_Integer, mxErr, 1);  /* reg[1] holds errors left */

    /* Do an integrity check on each database file */
    for(i=0; i<db->nDb; i++){
      HashElem *x;
      Hash *pTbls;
      int cnt = 0;

      if( OMIT_TEMPDB && i==1 ) continue;

      sqlite4CodeVerifySchema(pParse, i);
      addr = sqlite4VdbeAddOp1(v, OP_IfPos, 1); /* Halt if out of errors */
      sqlite4VdbeAddOp2(v, OP_Halt, 0, 0);
      sqlite4VdbeJumpHere(v, addr);

      /* Do an integrity check of the B-Tree
      **
      ** Begin by filling registers 2, 3, ... with the root pages numbers
      ** for all tables and indices in the database.
      */
      assert( sqlite4SchemaMutexHeld(db, iDb, 0) );
      pTbls = &db->aDb[i].pSchema->tblHash;
      for(x=sqliteHashFirst(pTbls); x; x=sqliteHashNext(x)){
        Table *pTab = sqliteHashData(x);
        Index *pIdx;
        sqlite4VdbeAddOp2(v, OP_Integer, pTab->tnum, 2+cnt);
        cnt++;
        for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
          sqlite4VdbeAddOp2(v, OP_Integer, pIdx->tnum, 2+cnt);
          cnt++;
        }
      }

      /* Make sure sufficient number of registers have been allocated */
      if( pParse->nMem < cnt+4 ){
        pParse->nMem = cnt+4;
      }

      /* Do the b-tree integrity checks */
      sqlite4VdbeAddOp3(v, OP_IntegrityCk, 2, cnt, 1);
      sqlite4VdbeChangeP5(v, (u8)i);
      addr = sqlite4VdbeAddOp1(v, OP_IsNull, 2);
      sqlite4VdbeAddOp4(v, OP_String8, 0, 3, 0,
         sqlite4MPrintf(db, "*** in database %s ***\n", db->aDb[i].zName),
         P4_DYNAMIC);
      sqlite4VdbeAddOp3(v, OP_Move, 2, 4, 1);
      sqlite4VdbeAddOp3(v, OP_Concat, 4, 3, 2);
      sqlite4VdbeAddOp2(v, OP_ResultRow, 2, 1);
      sqlite4VdbeJumpHere(v, addr);

      /* Make sure all the indices are constructed correctly.
      */
      for(x=sqliteHashFirst(pTbls); x && !isQuick; x=sqliteHashNext(x)){
        Table *pTab = sqliteHashData(x);
        Index *pIdx;
        int loopTop;

        if( pTab->pIndex==0 ) continue;
        addr = sqlite4VdbeAddOp1(v, OP_IfPos, 1);  /* Stop if out of errors */
        sqlite4VdbeAddOp2(v, OP_Halt, 0, 0);
        sqlite4VdbeJumpHere(v, addr);
        sqlite4OpenTableAndIndices(pParse, pTab, 1, OP_OpenRead);
        sqlite4VdbeAddOp2(v, OP_Integer, 0, 2);  /* reg(2) will count entries */
        loopTop = sqlite4VdbeAddOp2(v, OP_Rewind, 1, 0);
        sqlite4VdbeAddOp2(v, OP_AddImm, 2, 1);   /* increment entry count */
        for(j=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, j++){
          int jmp2;
          int r1;
          static const VdbeOpList idxErr[] = {
            { OP_AddImm,      1, -1,  0},
            { OP_String8,     0,  3,  0},    /* 1 */
            { OP_Rowid,       1,  4,  0},
            { OP_String8,     0,  5,  0},    /* 3 */
            { OP_String8,     0,  6,  0},    /* 4 */
            { OP_Concat,      4,  3,  3},
            { OP_Concat,      5,  3,  3},
            { OP_Concat,      6,  3,  3},
            { OP_ResultRow,   3,  1,  0},
            { OP_IfPos,       1,  0,  0},    /* 9 */
            { OP_Halt,        0,  0,  0},
          };
          r1 = sqlite4GenerateIndexKey(pParse, pIdx, 1, 3, 0);
          jmp2 = sqlite4VdbeAddOp4Int(v, OP_Found, j+2, 0, r1, pIdx->nColumn+1);
          addr = sqlite4VdbeAddOpList(v, ArraySize(idxErr), idxErr);
          sqlite4VdbeChangeP4(v, addr+1, "rowid ", P4_STATIC);
          sqlite4VdbeChangeP4(v, addr+3, " missing from index ", P4_STATIC);
          sqlite4VdbeChangeP4(v, addr+4, pIdx->zName, P4_TRANSIENT);
          sqlite4VdbeJumpHere(v, addr+9);
          sqlite4VdbeJumpHere(v, jmp2);
        }
        sqlite4VdbeAddOp2(v, OP_Next, 1, loopTop+1);
        sqlite4VdbeJumpHere(v, loopTop);
        for(j=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, j++){
          static const VdbeOpList cntIdx[] = {
             { OP_Integer,      0,  3,  0},
             { OP_Rewind,       0,  0,  0},  /* 1 */
             { OP_AddImm,       3,  1,  0},
             { OP_Next,         0,  0,  0},  /* 3 */
             { OP_Eq,           2,  0,  3},  /* 4 */
             { OP_AddImm,       1, -1,  0},
             { OP_String8,      0,  2,  0},  /* 6 */
             { OP_String8,      0,  3,  0},  /* 7 */
             { OP_Concat,       3,  2,  2},
             { OP_ResultRow,    2,  1,  0},
          };
          addr = sqlite4VdbeAddOp1(v, OP_IfPos, 1);
          sqlite4VdbeAddOp2(v, OP_Halt, 0, 0);
          sqlite4VdbeJumpHere(v, addr);
          addr = sqlite4VdbeAddOpList(v, ArraySize(cntIdx), cntIdx);
          sqlite4VdbeChangeP1(v, addr+1, j+2);
          sqlite4VdbeChangeP2(v, addr+1, addr+4);
          sqlite4VdbeChangeP1(v, addr+3, j+2);
          sqlite4VdbeChangeP2(v, addr+3, addr+2);
          sqlite4VdbeJumpHere(v, addr+4);
          sqlite4VdbeChangeP4(v, addr+6, 
                     "wrong # of entries in index ", P4_STATIC);
          sqlite4VdbeChangeP4(v, addr+7, pIdx->zName, P4_TRANSIENT);
        }
      } 
    }
    addr = sqlite4VdbeAddOpList(v, ArraySize(endCode), endCode);
    sqlite4VdbeChangeP2(v, addr, -mxErr);
    sqlite4VdbeJumpHere(v, addr+1);
    sqlite4VdbeChangeP4(v, addr+2, "ok", P4_STATIC);
  }else
#endif /* SQLITE_OMIT_INTEGRITY_CHECK */

#ifndef SQLITE_OMIT_UTF16
  /*
  **   PRAGMA encoding
  **   PRAGMA encoding = "utf-8"|"utf-16"|"utf-16le"|"utf-16be"
  **
  ** In its first form, this pragma returns the encoding of the main
  ** database. If the database is not initialized, it is initialized now.
  **
  ** The second form of this pragma is a no-op if the main database file
  ** has not already been initialized. In this case it sets the default
  ** encoding that will be used for the main database file if a new file
  ** is created. If an existing main database file is opened, then the
  ** default text encoding for the existing database is used.
  ** 
  ** In all cases new databases created using the ATTACH command are
  ** created to use the same default text encoding as the main database. If
  ** the main database has not been initialized and/or created when ATTACH
  ** is executed, this is done before the ATTACH operation.
  **
  ** In the second form this pragma sets the text encoding to be used in
  ** new database files created using this database handle. It is only
  ** useful if invoked immediately after the main database i
  */
  if( sqlite4StrICmp(zLeft, "encoding")==0 ){
    static const struct EncName {
      char *zName;
      u8 enc;
    } encnames[] = {
      { "UTF8",     SQLITE_UTF8        },
      { "UTF-8",    SQLITE_UTF8        },  /* Must be element [1] */
      { "UTF-16le", SQLITE_UTF16LE     },  /* Must be element [2] */
      { "UTF-16be", SQLITE_UTF16BE     },  /* Must be element [3] */
      { "UTF16le",  SQLITE_UTF16LE     },
      { "UTF16be",  SQLITE_UTF16BE     },
      { "UTF-16",   0                  }, /* SQLITE_UTF16NATIVE */
      { "UTF16",    0                  }, /* SQLITE_UTF16NATIVE */
      { 0, 0 }
    };
    const struct EncName *pEnc;
    if( !zRight ){    /* "PRAGMA encoding" */
      if( sqlite4ReadSchema(pParse) ) goto pragma_out;
      sqlite4VdbeSetNumCols(v, 1);
      sqlite4VdbeSetColName(v, 0, COLNAME_NAME, "encoding", SQLITE_STATIC);
      sqlite4VdbeAddOp2(v, OP_String8, 0, 1);
      assert( encnames[SQLITE_UTF8].enc==SQLITE_UTF8 );
      assert( encnames[SQLITE_UTF16LE].enc==SQLITE_UTF16LE );
      assert( encnames[SQLITE_UTF16BE].enc==SQLITE_UTF16BE );
      sqlite4VdbeChangeP4(v, -1, encnames[ENC(pParse->db)].zName, P4_STATIC);
      sqlite4VdbeAddOp2(v, OP_ResultRow, 1, 1);
    }else{                        /* "PRAGMA encoding = XXX" */
      /* Only change the value of sqlite.enc if the database handle is not
      ** initialized. If the main database exists, the new sqlite.enc value
      ** will be overwritten when the schema is next loaded. If it does not
      ** already exists, it will be created to use the new encoding value.
      */
      if( 
        !(DbHasProperty(db, 0, DB_SchemaLoaded)) || 
        DbHasProperty(db, 0, DB_Empty) 
      ){
        for(pEnc=&encnames[0]; pEnc->zName; pEnc++){
          if( 0==sqlite4StrICmp(zRight, pEnc->zName) ){
            ENC(pParse->db) = pEnc->enc ? pEnc->enc : SQLITE_UTF16NATIVE;
            break;
          }
        }
        if( !pEnc->zName ){
          sqlite4ErrorMsg(pParse, "unsupported encoding: %s", zRight);
        }
      }
    }
  }else
#endif /* SQLITE_OMIT_UTF16 */

#ifndef SQLITE_OMIT_SCHEMA_VERSION_PRAGMAS
  /*
  **   PRAGMA [database.]schema_version
  **   PRAGMA [database.]schema_version = <integer>
  **
  **   PRAGMA [database.]user_version
  **   PRAGMA [database.]user_version = <integer>
  **
  ** The pragma's schema_version and user_version are used to set or get
  ** the value of the schema-version and user-version, respectively. Both
  ** the schema-version and the user-version are 32-bit signed integers
  ** stored in the database header.
  **
  ** The schema-cookie is usually only manipulated internally by SQLite. It
  ** is incremented by SQLite whenever the database schema is modified (by
  ** creating or dropping a table or index). The schema version is used by
  ** SQLite each time a query is executed to ensure that the internal cache
  ** of the schema used when compiling the SQL query matches the schema of
  ** the database against which the compiled query is actually executed.
  ** Subverting this mechanism by using "PRAGMA schema_version" to modify
  ** the schema-version is potentially dangerous and may lead to program
  ** crashes or database corruption. Use with caution!
  **
  ** The user-version is not used internally by SQLite. It may be used by
  ** applications for any purpose.
  */
  if( sqlite4StrICmp(zLeft, "schema_version")==0 
   || sqlite4StrICmp(zLeft, "user_version")==0 
   || sqlite4StrICmp(zLeft, "freelist_count")==0 
  ){
    int iCookie;   /* Cookie index. 1 for schema-cookie, 6 for user-cookie. */
    sqlite4VdbeUsesBtree(v, iDb);
    switch( zLeft[0] ){
      case 'f': case 'F':
        iCookie = BTREE_FREE_PAGE_COUNT;
        break;
      case 's': case 'S':
        iCookie = BTREE_SCHEMA_VERSION;
        break;
      default:
        iCookie = BTREE_USER_VERSION;
        break;
    }

    if( zRight && iCookie!=BTREE_FREE_PAGE_COUNT ){
      /* Write the specified cookie value */
      static const VdbeOpList setCookie[] = {
        { OP_Transaction,    0,  1,  0},    /* 0 */
        { OP_Integer,        0,  1,  0},    /* 1 */
        { OP_SetCookie,      0,  0,  1},    /* 2 */
      };
      int addr = sqlite4VdbeAddOpList(v, ArraySize(setCookie), setCookie);
      sqlite4VdbeChangeP1(v, addr, iDb);
      sqlite4VdbeChangeP1(v, addr+1, sqlite4Atoi(zRight));
      sqlite4VdbeChangeP1(v, addr+2, iDb);
      sqlite4VdbeChangeP2(v, addr+2, iCookie);
    }else{
      /* Read the specified cookie value */
      static const VdbeOpList readCookie[] = {
        { OP_Transaction,     0,  0,  0},    /* 0 */
        { OP_ReadCookie,      0,  1,  0},    /* 1 */
        { OP_ResultRow,       1,  1,  0}
      };
      int addr = sqlite4VdbeAddOpList(v, ArraySize(readCookie), readCookie);
      sqlite4VdbeChangeP1(v, addr, iDb);
      sqlite4VdbeChangeP1(v, addr+1, iDb);
      sqlite4VdbeChangeP3(v, addr+1, iCookie);
      sqlite4VdbeSetNumCols(v, 1);
      sqlite4VdbeSetColName(v, 0, COLNAME_NAME, zLeft, SQLITE_TRANSIENT);
    }
  }else
#endif /* SQLITE_OMIT_SCHEMA_VERSION_PRAGMAS */

#ifndef SQLITE_OMIT_COMPILEOPTION_DIAGS
  /*
  **   PRAGMA compile_options
  **
  ** Return the names of all compile-time options used in this build,
  ** one option per row.
  */
  if( sqlite4StrICmp(zLeft, "compile_options")==0 ){
    int i = 0;
    const char *zOpt;
    sqlite4VdbeSetNumCols(v, 1);
    pParse->nMem = 1;
    sqlite4VdbeSetColName(v, 0, COLNAME_NAME, "compile_option", SQLITE_STATIC);
    while( (zOpt = sqlite4_compileoption_get(i++))!=0 ){
      sqlite4VdbeAddOp4(v, OP_String8, 0, 1, 0, zOpt, 0);
      sqlite4VdbeAddOp2(v, OP_ResultRow, 1, 1);
    }
  }else
#endif /* SQLITE_OMIT_COMPILEOPTION_DIAGS */

#ifndef SQLITE_OMIT_WAL
  /*
  **   PRAGMA [database.]wal_checkpoint = passive|full|restart
  **
  ** Checkpoint the database.
  */
  if( sqlite4StrICmp(zLeft, "wal_checkpoint")==0 ){
    int iBt = (pId2->z?iDb:SQLITE_MAX_ATTACHED);
    int eMode = SQLITE_CHECKPOINT_PASSIVE;
    if( zRight ){
      if( sqlite4StrICmp(zRight, "full")==0 ){
        eMode = SQLITE_CHECKPOINT_FULL;
      }else if( sqlite4StrICmp(zRight, "restart")==0 ){
        eMode = SQLITE_CHECKPOINT_RESTART;
      }
    }
    if( sqlite4ReadSchema(pParse) ) goto pragma_out;
    sqlite4VdbeSetNumCols(v, 3);
    pParse->nMem = 3;
    sqlite4VdbeSetColName(v, 0, COLNAME_NAME, "busy", SQLITE_STATIC);
    sqlite4VdbeSetColName(v, 1, COLNAME_NAME, "log", SQLITE_STATIC);
    sqlite4VdbeSetColName(v, 2, COLNAME_NAME, "checkpointed", SQLITE_STATIC);

    sqlite4VdbeAddOp3(v, OP_Checkpoint, iBt, eMode, 1);
    sqlite4VdbeAddOp2(v, OP_ResultRow, 1, 3);
  }else

  /*
  **   PRAGMA wal_autocheckpoint
  **   PRAGMA wal_autocheckpoint = N
  **
  ** Configure a database connection to automatically checkpoint a database
  ** after accumulating N frames in the log. Or query for the current value
  ** of N.
  */
  if( sqlite4StrICmp(zLeft, "wal_autocheckpoint")==0 ){
    if( zRight ){
      sqlite4_wal_autocheckpoint(db, sqlite4Atoi(zRight));
    }
    returnSingleInt(pParse, "wal_autocheckpoint", 
       db->xWalCallback==sqlite4WalDefaultHook ? 
           SQLITE_PTR_TO_INT(db->pWalArg) : 0);
  }else
#endif

  /*
  **  PRAGMA shrink_memory
  **
  ** This pragma attempts to free as much memory as possible from the
  ** current database connection.
  */
  if( sqlite4StrICmp(zLeft, "shrink_memory")==0 ){
    sqlite4_db_release_memory(db);
  }else

#if defined(SQLITE_DEBUG) || defined(SQLITE_TEST)
  /*
  ** Report the current state of file logs for all databases
  */
  if( sqlite4StrICmp(zLeft, "lock_status")==0 ){
    static const char *const azLockName[] = {
      "unlocked", "shared", "reserved", "pending", "exclusive"
    };
    int i;
    sqlite4VdbeSetNumCols(v, 2);
    pParse->nMem = 2;
    sqlite4VdbeSetColName(v, 0, COLNAME_NAME, "database", SQLITE_STATIC);
    sqlite4VdbeSetColName(v, 1, COLNAME_NAME, "status", SQLITE_STATIC);
    for(i=0; i<db->nDb; i++){
      Btree *pBt;
      Pager *pPager;
      const char *zState = "unknown";
      int j;
      if( db->aDb[i].zName==0 ) continue;
      sqlite4VdbeAddOp4(v, OP_String8, 0, 1, 0, db->aDb[i].zName, P4_STATIC);
      pBt = db->aDb[i].pBt;
      if( pBt==0 || (pPager = sqlite4BtreePager(pBt))==0 ){
        zState = "closed";
      }else if( sqlite4_file_control(db, i ? db->aDb[i].zName : 0, 
                                     SQLITE_FCNTL_LOCKSTATE, &j)==SQLITE_OK ){
         zState = azLockName[j];
      }
      sqlite4VdbeAddOp4(v, OP_String8, 0, 2, 0, zState, P4_STATIC);
      sqlite4VdbeAddOp2(v, OP_ResultRow, 1, 2);
    }

  }else
#endif

#ifdef SQLITE_HAS_CODEC
  if( sqlite4StrICmp(zLeft, "key")==0 && zRight ){
    sqlite4_key(db, zRight, sqlite4Strlen30(zRight));
  }else
  if( sqlite4StrICmp(zLeft, "rekey")==0 && zRight ){
    sqlite4_rekey(db, zRight, sqlite4Strlen30(zRight));
  }else
  if( zRight && (sqlite4StrICmp(zLeft, "hexkey")==0 ||
                 sqlite4StrICmp(zLeft, "hexrekey")==0) ){
    int i, h1, h2;
    char zKey[40];
    for(i=0; (h1 = zRight[i])!=0 && (h2 = zRight[i+1])!=0; i+=2){
      h1 += 9*(1&(h1>>6));
      h2 += 9*(1&(h2>>6));
      zKey[i/2] = (h2 & 0x0f) | ((h1 & 0xf)<<4);
    }
    if( (zLeft[3] & 0xf)==0xb ){
      sqlite4_key(db, zKey, i/2);
    }else{
      sqlite4_rekey(db, zKey, i/2);
    }
  }else
#endif
#if defined(SQLITE_HAS_CODEC) || defined(SQLITE_ENABLE_CEROD)
  if( sqlite4StrICmp(zLeft, "activate_extensions")==0 ){
#ifdef SQLITE_HAS_CODEC
    if( sqlite4StrNICmp(zRight, "see-", 4)==0 ){
      sqlite4_activate_see(&zRight[4]);
    }
#endif
#ifdef SQLITE_ENABLE_CEROD
    if( sqlite4StrNICmp(zRight, "cerod-", 6)==0 ){
      sqlite4_activate_cerod(&zRight[6]);
    }
#endif
  }else
#endif

 
  {/* Empty ELSE clause */}

  /*
  ** Reset the safety level, in case the fullfsync flag or synchronous
  ** setting changed.
  */
#ifndef SQLITE_OMIT_PAGER_PRAGMAS
  if( db->autoCommit ){
    sqlite4BtreeSetSafetyLevel(pDb->pBt, pDb->safety_level,
               (db->flags&SQLITE_FullFSync)!=0,
               (db->flags&SQLITE_CkptFullFSync)!=0);
  }
#endif
pragma_out:
  sqlite4DbFree(db, zLeft);
  sqlite4DbFree(db, zRight);
}

#endif /* SQLITE_OMIT_PRAGMA */
