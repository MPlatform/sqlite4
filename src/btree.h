/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This header file defines the interface that the sqlite B-Tree file
** subsystem.  See comments in the source code for a detailed description
** of what each interface routine does.
*/
#ifndef _BTREE_H_
#define _BTREE_H_

/* TODO: This definition is just included so other modules compile. It
** needs to be revisited.
*/
#define SQLITE_N_BTREE_META 10

/*
** If defined as non-zero, auto-vacuum is enabled by default. Otherwise
** it must be turned on for each database using "PRAGMA auto_vacuum = 1".
*/
#ifndef SQLITE_DEFAULT_AUTOVACUUM
  #define SQLITE_DEFAULT_AUTOVACUUM 0
#endif

#define BTREE_AUTOVACUUM_NONE 0        /* Do not do auto-vacuum */
#define BTREE_AUTOVACUUM_FULL 1        /* Do full auto-vacuum */
#define BTREE_AUTOVACUUM_INCR 2        /* Incremental vacuum */

/*
** Forward declarations of structure
*/
typedef struct Btree Btree;
typedef struct BtCursor BtCursor;
typedef struct BtShared BtShared;


int sqlite4BtreeOpen(
  sqlite4_vfs *pVfs,       /* VFS to use with this b-tree */
  const char *zFilename,   /* Name of database file to open */
  sqlite4 *db,             /* Associated database connection */
  Btree **ppBtree,         /* Return open Btree* here */
  int flags,               /* Flags */
  int vfsFlags             /* Flags passed through to VFS open */
);

/* The flags parameter to sqlite4BtreeOpen can be the bitwise or of the
** following values.
**
** NOTE:  These values must match the corresponding PAGER_ values in
** pager.h.
*/
#define BTREE_OMIT_JOURNAL  1  /* Do not create or use a rollback journal */
#define BTREE_MEMORY        2  /* This is an in-memory DB */
#define BTREE_SINGLE        4  /* The file contains at most 1 b-tree */
#define BTREE_UNORDERED     8  /* Use of a hash implementation is OK */

int sqlite4BtreeClose(Btree*);
int sqlite4BtreeSetCacheSize(Btree*,int);
int sqlite4BtreeSetSafetyLevel(Btree*,int,int,int);
int sqlite4BtreeSyncDisabled(Btree*);
int sqlite4BtreeSetPageSize(Btree *p, int nPagesize, int nReserve, int eFix);
int sqlite4BtreeGetPageSize(Btree*);
int sqlite4BtreeMaxPageCount(Btree*,int);
u32 sqlite4BtreeLastPage(Btree*);
int sqlite4BtreeSecureDelete(Btree*,int);
int sqlite4BtreeGetReserve(Btree*);
int sqlite4BtreeSetAutoVacuum(Btree *, int);
int sqlite4BtreeGetAutoVacuum(Btree *);
int sqlite4BtreeBeginTrans(Btree*,int);
int sqlite4BtreeCommitPhaseOne(Btree*, const char *zMaster);
int sqlite4BtreeCommitPhaseTwo(Btree*, int);
int sqlite4BtreeCommit(Btree*);
int sqlite4BtreeRollback(Btree*);
int sqlite4BtreeBeginStmt(Btree*,int);
int sqlite4BtreeCreateTable(Btree*, int*, int flags);
int sqlite4BtreeIsInTrans(Btree*);
int sqlite4BtreeIsInReadTrans(Btree*);
int sqlite4BtreeIsInBackup(Btree*);
void *sqlite4BtreeSchema(Btree *, int, void(*)(void *));
int sqlite4BtreeSchemaLocked(Btree *pBtree);
int sqlite4BtreeLockTable(Btree *pBtree, int iTab, u8 isWriteLock);
int sqlite4BtreeSavepoint(Btree *, int, int);

const char *sqlite4BtreeGetFilename(Btree *);
const char *sqlite4BtreeGetJournalname(Btree *);
int sqlite4BtreeCopyFile(Btree *, Btree *);

int sqlite4BtreeIncrVacuum(Btree *);

/* The flags parameter to sqlite4BtreeCreateTable can be the bitwise OR
** of the flags shown below.
**
** Every SQLite table must have either BTREE_INTKEY or BTREE_BLOBKEY set.
** With BTREE_INTKEY, the table key is a 64-bit integer and arbitrary data
** is stored in the leaves.  (BTREE_INTKEY is used for SQL tables.)  With
** BTREE_BLOBKEY, the key is an arbitrary BLOB and no content is stored
** anywhere - the key is the content.  (BTREE_BLOBKEY is used for SQL
** indices.)
*/
#define BTREE_INTKEY     1    /* Table has only 64-bit signed integer keys */
#define BTREE_BLOBKEY    2    /* Table has keys only - no data */

int sqlite4BtreeDropTable(Btree*, int, int*);
int sqlite4BtreeClearTable(Btree*, int, int*);
void sqlite4BtreeTripAllCursors(Btree*, int);

void sqlite4BtreeGetMeta(Btree *pBtree, int idx, u32 *pValue);
int sqlite4BtreeUpdateMeta(Btree*, int idx, u32 value);

/*
** The second parameter to sqlite4BtreeGetMeta or sqlite4BtreeUpdateMeta
** should be one of the following values. The integer values are assigned 
** to constants so that the offset of the corresponding field in an
** SQLite database header may be found using the following formula:
**
**   offset = 36 + (idx * 4)
**
** For example, the free-page-count field is located at byte offset 36 of
** the database file header. The incr-vacuum-flag field is located at
** byte offset 64 (== 36+4*7).
*/
#define BTREE_FREE_PAGE_COUNT     0
#define BTREE_SCHEMA_VERSION      1
#define BTREE_FILE_FORMAT         2
#define BTREE_DEFAULT_CACHE_SIZE  3
#define BTREE_LARGEST_ROOT_PAGE   4
#define BTREE_TEXT_ENCODING       5
#define BTREE_USER_VERSION        6
#define BTREE_INCR_VACUUM         7

int sqlite4BtreeCursor(
  Btree*,                              /* BTree containing table to open */
  int iTable,                          /* Index of root page */
  int wrFlag,                          /* 1 for writing.  0 for read-only */
  struct KeyInfo*,                     /* First argument to compare function */
  BtCursor *pCursor                    /* Space to write cursor structure */
);
int sqlite4BtreeCursorSize(void);
void sqlite4BtreeCursorZero(BtCursor*);

int sqlite4BtreeCloseCursor(BtCursor*);
int sqlite4BtreeMovetoUnpacked(
  BtCursor*,
  UnpackedRecord *pUnKey,
  i64 intKey,
  int bias,
  int *pRes
);
int sqlite4BtreeCursorHasMoved(BtCursor*, int*);
int sqlite4BtreeDelete(BtCursor*);
int sqlite4BtreeInsert(BtCursor*, const void *pKey, i64 nKey,
                                  const void *pData, int nData,
                                  int nZero, int bias, int seekResult);
int sqlite4BtreeFirst(BtCursor*, int *pRes);
int sqlite4BtreeLast(BtCursor*, int *pRes);
int sqlite4BtreeNext(BtCursor*, int *pRes);
int sqlite4BtreeEof(BtCursor*);
int sqlite4BtreePrevious(BtCursor*, int *pRes);
int sqlite4BtreeKeySize(BtCursor*, i64 *pSize);
int sqlite4BtreeKey(BtCursor*, u32 offset, u32 amt, void*);
const void *sqlite4BtreeKeyFetch(BtCursor*, int *pAmt);
const void *sqlite4BtreeDataFetch(BtCursor*, int *pAmt);
int sqlite4BtreeDataSize(BtCursor*, u32 *pSize);
int sqlite4BtreeData(BtCursor*, u32 offset, u32 amt, void*);
void sqlite4BtreeSetCachedRowid(BtCursor*, sqlite4_int64);
sqlite4_int64 sqlite4BtreeGetCachedRowid(BtCursor*);

char *sqlite4BtreeIntegrityCheck(Btree*, int *aRoot, int nRoot, int, int*);
struct Pager *sqlite4BtreePager(Btree*);

int sqlite4BtreePutData(BtCursor*, u32 offset, u32 amt, void*);
void sqlite4BtreeCacheOverflow(BtCursor *);
void sqlite4BtreeClearCursor(BtCursor *);

int sqlite4BtreeSetVersion(Btree *pBt, int iVersion);

#ifndef NDEBUG
int sqlite4BtreeCursorIsValid(BtCursor*);
#endif

#ifndef SQLITE_OMIT_BTREECOUNT
int sqlite4BtreeCount(BtCursor *, i64 *);
#endif

#ifdef SQLITE_TEST
int sqlite4BtreeCursorInfo(BtCursor*, int*, int);
void sqlite4BtreeCursorList(Btree*);
#endif

#ifndef SQLITE_OMIT_WAL
  int sqlite4BtreeCheckpoint(Btree*, int, int *, int *);
#endif

/*
** If we are not using shared cache, then there is no need to
** use mutexes to access the BtShared structures.  So make the
** Enter and Leave procedures no-ops.
*/
#ifndef SQLITE_OMIT_SHARED_CACHE
  void sqlite4BtreeEnter(Btree*);
  void sqlite4BtreeEnterAll(sqlite4*);
#else
# define sqlite4BtreeEnter(X) 
# define sqlite4BtreeEnterAll(X)
#endif

#if !defined(SQLITE_OMIT_SHARED_CACHE) && SQLITE_THREADSAFE
  int sqlite4BtreeSharable(Btree*);
  void sqlite4BtreeLeave(Btree*);
  void sqlite4BtreeEnterCursor(BtCursor*);
  void sqlite4BtreeLeaveCursor(BtCursor*);
  void sqlite4BtreeLeaveAll(sqlite4*);
#ifndef NDEBUG
  /* These routines are used inside assert() statements only. */
  int sqlite4BtreeHoldsMutex(Btree*);
  int sqlite4BtreeHoldsAllMutexes(sqlite4*);
  int sqlite4SchemaMutexHeld(sqlite4*,int,Schema*);
#endif
#else

# define sqlite4BtreeSharable(X) 0
# define sqlite4BtreeLeave(X)
# define sqlite4BtreeEnterCursor(X)
# define sqlite4BtreeLeaveCursor(X)
# define sqlite4BtreeLeaveAll(X)

# define sqlite4BtreeHoldsMutex(X) 1
# define sqlite4BtreeHoldsAllMutexes(X) 1
# define sqlite4SchemaMutexHeld(X,Y,Z) 1
#endif


#endif /* _BTREE_H_ */
