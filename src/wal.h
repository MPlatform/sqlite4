/*
** 2010 February 1
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This header file defines the interface to the write-ahead logging 
** system. Refer to the comments below and the header comment attached to 
** the implementation of each function in log.c for further details.
*/

#ifndef _WAL_H_
#define _WAL_H_

#include "sqliteInt.h"

/* Additional values that can be added to the sync_flags argument of
** sqlite4WalFrames():
*/
#define WAL_SYNC_TRANSACTIONS  0x20   /* Sync at the end of each transaction */
#define SQLITE_SYNC_MASK       0x13   /* Mask off the SQLITE_SYNC_* values */

#ifdef SQLITE_OMIT_WAL
# define sqlite4WalOpen(x,y,z)                   0
# define sqlite4WalLimit(x,y)
# define sqlite4WalClose(w,x,y,z)                0
# define sqlite4WalBeginReadTransaction(y,z)     0
# define sqlite4WalEndReadTransaction(z)
# define sqlite4WalRead(v,w,x,y,z)               0
# define sqlite4WalDbsize(y)                     0
# define sqlite4WalBeginWriteTransaction(y)      0
# define sqlite4WalEndWriteTransaction(x)        0
# define sqlite4WalUndo(x,y,z)                   0
# define sqlite4WalSavepoint(y,z)
# define sqlite4WalSavepointUndo(y,z)            0
# define sqlite4WalFrames(u,v,w,x,y,z)           0
# define sqlite4WalCheckpoint(r,s,t,u,v,w,x,y,z) 0
# define sqlite4WalCallback(z)                   0
# define sqlite4WalExclusiveMode(y,z)            0
# define sqlite4WalHeapMemory(z)                 0
#else

#define WAL_SAVEPOINT_NDATA 4

/* Connection to a write-ahead log (WAL) file. 
** There is one object of this type for each pager. 
*/
typedef struct Wal Wal;

/* Open and close a connection to a write-ahead log. */
int sqlite4WalOpen(sqlite4_vfs*, sqlite4_file*, const char *, int, i64, Wal**);
int sqlite4WalClose(Wal *pWal, int sync_flags, int, u8 *);

/* Set the limiting size of a WAL file. */
void sqlite4WalLimit(Wal*, i64);

/* Used by readers to open (lock) and close (unlock) a snapshot.  A 
** snapshot is like a read-transaction.  It is the state of the database
** at an instant in time.  sqlite4WalOpenSnapshot gets a read lock and
** preserves the current state even if the other threads or processes
** write to or checkpoint the WAL.  sqlite4WalCloseSnapshot() closes the
** transaction and releases the lock.
*/
int sqlite4WalBeginReadTransaction(Wal *pWal, int *);
void sqlite4WalEndReadTransaction(Wal *pWal);

/* Read a page from the write-ahead log, if it is present. */
int sqlite4WalRead(Wal *pWal, Pgno pgno, int *pInWal, int nOut, u8 *pOut);

/* If the WAL is not empty, return the size of the database. */
Pgno sqlite4WalDbsize(Wal *pWal);

/* Obtain or release the WRITER lock. */
int sqlite4WalBeginWriteTransaction(Wal *pWal);
int sqlite4WalEndWriteTransaction(Wal *pWal);

/* Undo any frames written (but not committed) to the log */
int sqlite4WalUndo(Wal *pWal, int (*xUndo)(void *, Pgno), void *pUndoCtx);

/* Return an integer that records the current (uncommitted) write
** position in the WAL */
void sqlite4WalSavepoint(Wal *pWal, u32 *aWalData);

/* Move the write position of the WAL back to iFrame.  Called in
** response to a ROLLBACK TO command. */
int sqlite4WalSavepointUndo(Wal *pWal, u32 *aWalData);

/* Write a frame or frames to the log. */
int sqlite4WalFrames(Wal *pWal, int, PgHdr *, Pgno, int, int);

/* Copy pages from the log to the database file */ 
int sqlite4WalCheckpoint(
  Wal *pWal,                      /* Write-ahead log connection */
  int eMode,                      /* One of PASSIVE, FULL and RESTART */
  int (*xBusy)(void*),            /* Function to call when busy */
  void *pBusyArg,                 /* Context argument for xBusyHandler */
  int sync_flags,                 /* Flags to sync db file with (or 0) */
  int nBuf,                       /* Size of buffer nBuf */
  u8 *zBuf,                       /* Temporary buffer to use */
  int *pnLog,                     /* OUT: Number of frames in WAL */
  int *pnCkpt                     /* OUT: Number of backfilled frames in WAL */
);

/* Return the value to pass to a sqlite4_wal_hook callback, the
** number of frames in the WAL at the point of the last commit since
** sqlite4WalCallback() was called.  If no commits have occurred since
** the last call, then return 0.
*/
int sqlite4WalCallback(Wal *pWal);

/* Tell the wal layer that an EXCLUSIVE lock has been obtained (or released)
** by the pager layer on the database file.
*/
int sqlite4WalExclusiveMode(Wal *pWal, int op);

/* Return true if the argument is non-NULL and the WAL module is using
** heap-memory for the wal-index. Otherwise, if the argument is NULL or the
** WAL module is using shared-memory, return false. 
*/
int sqlite4WalHeapMemory(Wal *pWal);

#endif /* ifndef SQLITE_OMIT_WAL */
#endif /* _WAL_H_ */
