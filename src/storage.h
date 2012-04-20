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
** The storage subsystem is a key/value database.  All keys and values are
** binary with arbitrary content.  Keys are unique.  Keys compare in
** memcmp() order.  Shorter keys appear first.
** 
** The xBegin, xCommit, and xRollback methods change the transaction level
** of the store.  The transaction level is a non-negative integer that is
** initialized to zero.  The transaction level must be at least 1 in order
** for content to be read.  The transaction level must be at least 2 for 
** content to be modified.
** 
** The xBegin method increases transaction level.  The increase may be no
** more than 1 unless the transaction level is initially 0 in which case
** it can be increased immediately to 2.  Increasing the transaction level
** to 1 or more makes a "snapshot" of the database file such that changes
** made by other connections are not visible.  An xBegin call may fail
** with SQLITE_BUSY if the initial transaction level is 0 or 1.
** 
** A read-only database will fail an attempt to increase xBegin above 1.  An
** implementation that does not support nested transactions will fail any
** attempt to increase the transaction level above 2.
** 
** The xCommitPhaseOne and xCommitPhaseTwo methods implement a 2-phase
** commit that lowers the transaction level to the value given in the
** second argument, making all the changes made at higher transaction levels
** permanent.  A rollback is still possible following phase one.  If
** possible, errors should be reported during phase one so that a
** multiple-database transaction can still be rolled back if the
** phase one fails on a different database.  Implementations that do not
** support two-phase commit can implement xCommitPhaseOne as a no-op function
** returning SQLITE_OK.
** 
** The xRollback method lowers the transaction level to the value given in
** its argument and reverts or undoes all changes made at higher transaction
** levels.  An xRollback to level N causes the database to revert to the state
** it was in on the most recent xBegin to level N+1.
** 
** The xRevert(N) method causes the state of the database file to go back
** to what it was immediately after the most recent xCommit(N).  Higher-level
** subtransactions are cancelled.  This call is equivalent to xRollback(N-1)
** followed by xBegin(N) but is atomic and might be more efficient.
** 
** The xReplace method replaces the value for an existing entry with the
** given key, or creates a new entry with the given key and value if no
** prior entry exists with the given key.  The key and value pointers passed
** into xReplace belong to the caller will likely be destroyed when the
** call to xReplace returns so the xReplace routine must make its own
** copy of that information.
** 
** A cursor is at all times pointing to ether an entry in the database or
** to EOF.  EOF means "no entry".  Cursor operations other than xCloseCursor 
** will fail if the transaction level is less than 1.
** 
** The xSeek method moves a cursor to an entry in the database that matches
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
** The return code from xSeek might be one of the following:
**
**    SQLITE_OK        The cursor is left pointing to any entry that
**                     exactly matchings the probe key.
**
**    SQLITE_INEXACT   The cursor is left pointing to the nearest entry
**                     to the probe it could find, either before or after
**                     the probe, according to the dir argument.
**
**    SQLITE_NOTFOUND  No suitable entry could be found.  Either dir==0 and
**                     there was no exact match, or dir<0 and the probe is
**                     smaller than every entry in the database, or dir>0 and
**                     the probe is larger than every entry in the database.
**
** xSeek might also return some error code like SQLITE_IOERR or
** SQLITE_NOMEM.
** 
** The xNext method will only be called following an xSeek with a positive dir,
** or another xNext.  The xPrev method will only be called following an xSeek
** with a negative dir or another xPrev.  Both xNext and xPrev will return
** SQLITE_OK on success and SQLITE_NOTFOUND if they run off the end of the
** database.  Both routines might also return error codes such as
** SQLITE_IOERR, SQLITE_CORRUPT, or SQLITE_NOMEM.
** 
** Values returned by xKey and xData are guaranteed to remain stable until
** the next xSeek, xNext, xPrev, xReset, xDelete, or xCloseCursor on the same
** cursor.  This is true even if the transaction level is reduced to zero,
** or if the content of the entry is changed by xInsert, xDelete on a different
** cursor, or xRollback.  The content returned by repeated calls to xKey and
** xData is allowed (but is not required) to change if xInsert, xDelete, or
** xRollback are invoked in between the calls, but the content returned by
** every call must be stable until the cursor moves, or is reset or closed.
** The cursor owns the values returned by xKey and xData and will take
** responsiblity for freeing memory used to hold those values when appropriate.
** 
** The xDelete method deletes the entry that the cursor is currently
** pointing at.  However, subsequent xNext or xPrev calls behave as if the
** entries is not actually deleted until the cursor moves.  In other words
** it is acceptable to xDelete an entry out from under a cursor.  Subsequent
** xNext or xPrev calls on that cursor will work the same as if the entry
** had not been deleted.  Two cursors can be pointing to the same entry and
** one cursor can xDelete and the other cursor is expected to continue
** functioning normally, including responding correctly to subsequent
** xNext and xPrev calls.
*/

/* Typedefs of datatypes */
typedef struct KVStore KVStore;
typedef struct KVStoreMethods KVStoreMethods;
typedef struct KVCursor KVCursor;
typedef unsigned char KVByteArray;
typedef int KVSize;

/*
** A Key-Value storage engine is defined by an instance of the following
** structures:
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
  int (*xCommitPhaseOne)(KVStore*, int);
  int (*xCommitPhaseTwo)(KVStore*, int);
  int (*xRollback)(KVStore*, int);
  int (*xRevert)(KVStore*, int);
  int (*xClose)(KVStore*);
};
struct KVStore {
  const KVStoreMethods *pStoreVfunc;    /* Virtual method table */
  int iTransLevel;                      /* Current transaction level */
  u16 kvId;                             /* Unique ID used for tracing */
  u8 fTrace;                            /* True to enable tracing */
  char zKVName[12];                     /* Used for debugging */
  /* Subclasses will typically append additional fields */
};

/*
** Base class for cursors
*/
struct KVCursor {
  KVStore *pStore;                    /* The owner of this cursor */
  const KVStoreMethods *pStoreVfunc;  /* Methods */
  int iTransLevel;                    /* Current transaction level */
  u16 curId;                          /* Unique ID for tracing */
  u8 fTrace;                          /* True to enable tracing */
  /* Subclasses will typically add additional fields */
};

/*
** Valid flags for sqlite4KVStorageOpen()
*/
#define SQLITE_KVOPEN_TEMPORARY       0x0001  /* A temporary database */
#define SQLITE_KVOPEN_NO_TRANSACTIONS 0x0002  /* No transactions will be used */

int sqlite4KVStoreOpenMem(KVStore**, const char *, unsigned);
int sqlite4KVStoreOpen(
  sqlite4*,
  const char *zLabel, 
  const char *zUri,
  KVStore**,
  unsigned flags
);
int sqlite4KVStoreReplace(
 KVStore*,
 const KVByteArray *pKey, KVSize nKey,
 const KVByteArray *pData, KVSize nData
);
int sqlite4KVStoreOpenCursor(KVStore *p, KVCursor **ppKVCursor);
int sqlite4KVCursorSeek(
  KVCursor *p,
  const KVByteArray *pKey, KVSize nKey,
  int dir
);
int sqlite4KVCursorNext(KVCursor *p);
int sqlite4KVCursorPrev(KVCursor *p);
int sqlite4KVCursorDelete(KVCursor *p);
int sqlite4KVCursorReset(KVCursor *p);
int sqlite4KVCursorKey(KVCursor *p, const KVByteArray **ppKey, KVSize *pnKey);
int sqlite4KVCursorData(
  KVCursor *p,
  KVSize ofst,
  KVSize n,
  const KVByteArray **ppData,
  KVSize *pnData
);
int sqlite4KVCursorClose(KVCursor *p);
int sqlite4KVStoreBegin(KVStore *p, int iLevel);
int sqlite4KVStoreCommitPhaseOne(KVStore *p, int iLevel);
int sqlite4KVStoreCommitPhaseTwo(KVStore *p, int iLevel);
int sqlite4KVStoreCommit(KVStore *p, int iLevel);
int sqlite4KVStoreRollback(KVStore *p, int iLevel);
int sqlite4KVStoreRevert(KVStore *p, int iLevel);
int sqlite4KVStoreClose(KVStore *p);

int sqlite4KVStoreGetMeta(KVStore *p, int, int, unsigned int*);
int sqlite4KVStorePutMeta(sqlite4*, KVStore *p, int, int, unsigned int*);
#ifdef SQLITE_DEBUG
  void sqlite4KVStoreDump(KVStore *p);
#endif

#ifdef SQLITE_ENABLE_LSM
int sqlite4KVStoreOpenLsm(KVStore**, const char *, unsigned);
#endif

