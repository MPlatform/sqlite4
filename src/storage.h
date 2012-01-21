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
  int (*xDelete)(KVStore*, const KVByteArray *pKey, KVSize nKey);
  int (*xOpenCursor)(KVStore*, KVCursor**);
  int (*xSeek)(KVCursor*, const KVByteArray *pKey, KVSize nKey, int dir);
  int (*xNext)(KVCursor*);
  int (*xPrev)(KVCursor*);
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
  const KVStoreMethods *pMethods;
  /* Subclasses will typically append additional fields */
};


int sqlite4KVStoreOpen(const char *zUri, KVStore**)
#define sqlite3KVStoreClose(X)  ((X)?(X)->pMethods->xClose(X):SQLITE_OK)
