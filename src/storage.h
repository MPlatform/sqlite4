/*
** 2012 January 20
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
** This header file defines the interface to the KV storage engine(s).
**
** Notes on the storage subsystem interface:
** 
** The storage subsystem is a key/value store.  All keys and values are
** binary with arbitrary content.  Keys are unique.  Keys compare in
** memcmp() order.  Shorter keys appear first.
** 
** The xBegin, xCommit, and xRollback methods change the transaction level
** of the store.  The transaction level is a non-negative integer that is
** initialized to zero.  The transaction level must be at least 1 in order
** for content to be read.  The transaction level must be at least 2 for 
** content to be modified.
** 
** The xBegin method increases transaction level.  The increase may only be
** by an amount of 1 unless the transaction level is initially 0 in which case
** it can be increased immediately to 2.  Increasing the transaction level
** to 1 or more makes a "snapshot" of the complete store such that changes
** made by other connections are not visible.  An xBegin call may fail
** with SQLITE_BUSY if the initial transaction level is 0 or 1.
** 
** A read-only store will fail an attempt to increase xBegin above 1.  An
** implementation that does not support nested transactions will fail any
** attempt to increase the transaction level above 2.
** 
** The xCommit method lowers the transaction level to the value given in its
** argument, and makes all the changes made at higher transaction levels
** permanent.
** 
** The xRollback method lowers the transaction level to the value given in
** its argument and reverts or undoes all changes made at higher transaction
** levels.  An xRollback to level N causes the database to revert to the state
** it was in on the most recent xBegin to level N+1.
** 
** 
** The xReplace method replaces the value for an existing entry with the
** given key, or creates a new entry with the given key and value if no
** prior entry exists with the given key.  The key and value pointers passed
** into xReplace will likely be destroyed when the call to xReplace returns
** so the xReplace routine must make its own copy of that information.
** 
** The xDelete method delets an existing entry with the given key.  If no
** such entry exists, xDelete is a no-op.
** 
** A cursor is at all times pointing to ether an entry in the store or
** to EOF.  EOF means "no entry".  Cursor operations other than xCloseCursor 
** will fail if the transaction level is less than 1.
** 
** The xSeek method moves a cursor to a point in the store that matches
** the supplied key as closely as possible.  If the dir argument is 0, then
** the match must be exact or else the seek fails and the cursor is left
** pointing to EOF.  If dir is negative, then an exact match is
** found if it is available, otherwise the cursor is positioned at the largest
** entry that is less than the search key or to EOF if the store contains no
** entry less than the search key.  If dir is positive, then an exist match
** is found if it is available, otherwise the cursor is left pointing the
** the smallest entry that is larger than the search key, or to EOF if there
** are no entries larger than the search key.
** 
** The xNext method may only be used following an xSeek with a positive dir,
** or another xNext.  The xPrev method may only be used following an xSeek with
** a negative dir or another xPrev.
** 
** Values returned by xKey and xData are guaranteed to remain stable until
** the next xSeek, xNext, xPrev, xReset, or xCloseCursor on the same cursor.  
** This is true even if the transaction level is reduced to zero, or if the
** content of the entry is changed by xInsert, xDelete, or xRollback.  The
** content returned by repeated calls to xKey and xData is allowed (but is not
** required) to change if xInsert, xDelete, or xRollback are invoked in between
** the calls, but the content returned by every call must be stable until 
** the cursor moves, or is reset or closed.
** 
** It is acceptable to xDelete an entry out from under a cursor.  Subsequent
** xNext or xPrev calls on that cursor will work the same as if the entry
** had not been deleted.
*/

/* Typedefs of datatypes */
typedef struct KVStore KVStore;
typedef struct KVStoreMethods KVStoreMethods;
typedef struct KVCursor KVCursor;
typedef unsigned char KVByteArray;
typedef int KVSize;

/*
** A Key-Value storage engine is defined by an instance of the following
** structure.
*/
struct KVStoreMethods {
  int (*xReplace)(KVStore*, const KVByteArray *pKey, KVSize nKey,
                            const KVByteArray *pData, KVSize nData);
  int (*xOpenCursor)(KVStore*, KVCursor**);
  int (*xSeek)(KVCursor*, const KVByteArray *pKey, KVSize nKey, int dir);
  int (*xNext)(KVCursor*);
  int (*xPrev)(KVCursor*);
  int (*xDelete)(KVCursor*);
  int (*xKey)(KVCursor*, const KVByteArray **ppKey, KVSize *pnKey);
  int (*xData)(KVCursor*, KVSize ofst, KVSize n,
                          const KVByteArray **ppData, KVSize *pnData);
  int (*xReset)(KVCursor*);
  int (*xCloseCursor)(KVCursor*);
  int (*xBegin)(KVStore*, int);
  int (*xCommit)(KVStore*, int);
  int (*xRollback)(KVStore*, int);
  int (*xClose)(KVStore*);
};
struct KVStore {
  const KVStoreMethods *pStoreVfunc;
  /* Subclasses will typically append additional fields */
};

/*
** Base class for cursors
*/
struct KVCursor {
  KVStore *pStore;                    /* The owner of this cursor */
  const KVStoreMethods *pStoreVfunc;  /* Methods */
  /* Subclasses will typically add additional fields */
};

int sqlite3KVStoreOpenMem(KVStore**);
int sqlite3KVStoreOpen(const char *zUri, KVStore**);
#define sqlite3KVStoreReplace(X,A,B,C,D) \
        ((X)->pStoreVfunc->xReplace((X),(A),(B),(C),(D)))
#define sqlite3KVStoreOpenCursor(X,A) \
        ((X)->pStoreVfunc->xOpenCursor((X),(A)))
#define sqlite3KVCursorSeek(X,Y,Z)  ((X)->pStoreVfunc->xSeek(X,(Y),(Z)))
#define sqlite3KVCursorNext(X)      ((X)->pStoreVfunc->xNext(X))
#define sqlite3KVCursorPrev(X)      ((X)->pStoreVfunc->xPrev(X))
#define sqlite3KVCursorDelete(X)    ((X)->pStoreVfunc->xDelete(X))
#define sqlite3KVCursorKey(X,Y,Z)   ((X)->pStoreVfunc->xKey(X,(Y),(Z)))
#define sqlite3KVCursorData(X,A,B,C,D) \
        ((X)->pStoreVfunc->xData(X,(A),(B),(C),(D))
#define sqlite3KVCursorReset(X)     ((X)->pStoreVfunc->xReset(X))
#define sqlite3KVCursorClose(X)     ((X)?(X)->pStoreVfunc->xCloseCursor(X):0)
#define sqlite3KVStoreBegin(X,A)    ((X)->pStoreVfunc->xBegin((X),(A)))
#define sqlite3KVStoreCommit(X,A)   ((X)->pStoreVfunc->xCommit((X),(A)))
#define sqlite3KVStoreRollback(X,A) ((X)->pStoreVfunc->xRollback((X),(A)))
#define sqlite3KVStoreClose(X)      ((X)?(X)->pStoreVfunc->xClose(X):SQLITE_OK)
