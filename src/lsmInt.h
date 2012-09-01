/*
** 2011-08-18
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Internal structure definitions for the LSM module.
*/
#ifndef _LSM_INT_H
#define _LSM_INT_H

#include "lsm.h"
#include <assert.h>
#include <string.h>

#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>

#include <unistd.h>

#ifdef NDEBUG
# ifdef LSM_DEBUG_EXPENSIVE
#  undef LSM_DEBUG_EXPENSIVE
# endif
# ifdef LSM_DEBUG
#  undef LSM_DEBUG
# endif
#else
# ifndef LSM_DEBUG
#  define LSM_DEBUG
# endif
#endif

/*
** Default values for various data structure parameters. These may be
** overridden by calls to lsm_config().
*/
#define LSM_PAGE_SIZE   4096
#define LSM_BLOCK_SIZE  (2 * 1024 * 1024)
#define LSM_TREE_BYTES  (2 * 1024 * 1024)

#define LSM_DEFAULT_LOG_SIZE (128*1024)
#define LSM_DEFAULT_NMERGE   4

/* Places where a NULL needs to be changed to a real lsm_env pointer
** are marked with NEED_ENV */
#define NEED_ENV ((lsm_env*)0)

/* Initial values for log file checksums. These are only used if the 
** database file does not contain a valid checkpoint.  */
#define LSM_CKSUM0_INIT 42
#define LSM_CKSUM1_INIT 42

#define LSM_META_PAGE_SIZE 4096

/* "mmap" mode is currently only used in environments with 64-bit address 
** spaces. The following macro is used to test for this.  */
#define LSM_IS_64_BIT (sizeof(void*)==8)

#define LSM_AUTOWORK_QUANT 32

/* Minimum number of free-list entries to store in the checkpoint, assuming
** the free-list contains this many entries. i.e. if overflow is required,
** the first LSM_CKPT_MIN_FREELIST entries are stored in the checkpoint and
** the remainder in an LSM system entry.  */
#define LSM_CKPT_MIN_FREELIST     6
#define LSM_CKPT_MAX_REFREE       2
#define LSM_CKPT_MIN_NONLSM       (LSM_CKPT_MIN_FREELIST - LSM_CKPT_MAX_REFREE)

typedef struct Database Database;
typedef struct DbLog DbLog;
typedef struct FileSystem FileSystem;
typedef struct Level Level;
typedef struct LogMark LogMark;
typedef struct LogRegion LogRegion;
typedef struct LogWriter LogWriter;
typedef struct LsmString LsmString;
typedef struct Mempool Mempool;
typedef struct MetaPage MetaPage;
typedef struct MultiCursor MultiCursor;
typedef struct Page Page;
typedef struct Segment Segment;
typedef struct SegmentMerger SegmentMerger;
typedef struct Snapshot Snapshot;
typedef struct TransMark TransMark;
typedef struct Tree Tree;
typedef struct TreeMark TreeMark;
typedef struct TreeVersion TreeVersion;
typedef struct TreeCursor TreeCursor;
typedef struct Merge Merge;
typedef struct MergeInput MergeInput;

typedef struct TreeHeader TreeHeader;
typedef struct ShmHeader ShmHeader;
typedef struct ShmChunk ShmChunk;
typedef struct ShmReader ShmReader;

typedef unsigned char u8;
typedef unsigned short int u16;
typedef unsigned int u32;
typedef lsm_i64 i64;
typedef unsigned long long int u64;

/* A page number is an integer. */
typedef int Pgno;

#ifdef LSM_DEBUG
int lsmErrorBkpt(int);
#else
# define lsmErrorBkpt(x) (x)
#endif

#define LSM_IOERR_BKPT   lsmErrorBkpt(LSM_IOERR)
#define LSM_NOMEM_BKPT   lsmErrorBkpt(LSM_NOMEM)
#define LSM_CORRUPT_BKPT lsmErrorBkpt(LSM_CORRUPT)
#define LSM_MISUSE_BKPT  lsmErrorBkpt(LSM_MISUSE)

#define unused_parameter(x) (void)(x)
#define array_size(x) (sizeof(x)/sizeof(x[0]))


/* The size of each shared-memory chunk */
#define LSM_SHM_CHUNK_SIZE (32*1024)

/* The number of bytes reserved at the start of each shm chunk for MM. */
#define LSM_SHM_CHUNK_HDR  (3 * 4)

/* The number of available read locks. */
#define LSM_LOCK_NREADER   6

/* Lock definitions */
#define LSM_LOCK_DMS1         1
#define LSM_LOCK_DMS2         2
#define LSM_LOCK_WRITER       3
#define LSM_LOCK_WORKER       4
#define LSM_LOCK_CHECKPOINTER 5
#define LSM_LOCK_READER(i)    ((i) + LSM_LOCK_CHECKPOINTER + 1)

/*
** Hard limit on the number of free-list entries that may be stored in 
** a checkpoint (the remainder are stored as a system record in the LSM).
** See also LSM_CONFIG_MAX_FREELIST.
*/
#define LSM_MAX_FREELIST_ENTRIES 100

/*
** A string that can grow by appending.
*/
struct LsmString {
  lsm_env *pEnv;              /* Run-time environment */
  int n;                      /* Size of string.  -1 indicates error */
  int nAlloc;                 /* Space allocated for z[] */
  char *z;                    /* The string content */
};

/*
** An instance of the following type is used to store an ordered list of
** u32 values. 
**
** Note: This is a place-holder implementation. It should be replaced by
** a version that avoids making a single large allocation when the array
** contains a large number of values. For this reason, the internals of 
** this object should only manipulated by the intArrayXXX() functions in 
** lsm_tree.c.
*/
typedef struct IntArray IntArray;
struct IntArray {
  int nAlloc;
  int nArray;
  u32 *aArray;
};

/*
** An instance of this structure represents a point in the history of the
** tree structure to roll back to. Refer to comments in lsm_tree.c for 
** details.
*/
struct TreeMark {
  u32 iRoot;                      /* Offset of root node in shm file */
  u32 nHeight;                    /* Current height of tree structure */
  u32 iWrite;                     /* Write offset in shm file */
  u32 nChunk;                     /* Number of chunks in shared-memory file */
  u32 iFirst;                     /* First chunk in linked list */
  int iRollback;                  /* Index in lsm->rollback to revert to */
};

/*
** An instance of this structure represents a point in the database log.
*/
struct LogMark {
  i64 iOff;                       /* Offset into log (see lsm_log.c) */
  int nBuf;                       /* Size of in-memory buffer here */
  u8 aBuf[8];                     /* Bytes of content in aBuf[] */
  u32 cksum0;                     /* Checksum 0 at offset (iOff-nBuf) */
  u32 cksum1;                     /* Checksum 1 at offset (iOff-nBuf) */
};

struct TransMark {
  TreeMark tree;
  LogMark log;
};

/*
** A structure that defines the start and end offsets of a region in the
** log file. The size of the region in bytes is (iEnd - iStart), so if
** iEnd==iStart the region is zero bytes in size.
*/
struct LogRegion {
  i64 iStart;                     /* Start of region in log file */
  i64 iEnd;                       /* End of region in log file */
};

struct DbLog {
  u32 cksum0;                     /* Checksum 0 at offset iOff */
  u32 cksum1;                     /* Checksum 1 at offset iOff */
  LogRegion aRegion[3];           /* Log file regions (see docs in lsm_log.c) */
};

/*
** Tree header structure. 
*/
struct TreeHeader {
  u32 iTreeId;                    /* Current tree id */
  u32 iTransId;                   /* Current transaction id */
  u32 iRoot;                      /* Offset of root node in shm file */
  u32 nHeight;                    /* Current height of tree structure */
  u32 iWrite;                     /* Write offset in shm file */
  u32 nChunk;                     /* Number of chunks in shared-memory file */
  u32 iFirst;                     /* First chunk in linked list */
  u32 nByte;                      /* Size of current tree structure in bytes */
  DbLog log;                      /* Current layout of log file */ 
  i64 iCkpt;                      /* Id of ckpt log space is reclaimed for */
  u32 aCksum[2];                  /* Checksums 1 and 2. */
};

/*
** Database handle structure.
**
** mLock:
**   A bitmask representing the locks currently held by the connection.
**   An LSM database supports N distinct locks, where N is some number less
**   than or equal to 16. Locks are numbered starting from 1 (see the 
**   definitions for LSM_LOCK_WRITER and co.).
**
**   The least significant 16-bits in mLock represent EXCLUSIVE locks. The
**   most significant are SHARED locks. So, if a connection holds a SHARED
**   lock on lock region iLock, then the following is true:
**
**       (mLock & ((iLock+16-1) << 1))
**
**   Or for an EXCLUSIVE lock:
**
**       (mLock & ((iLock-1) << 1))
*/
struct lsm_db {

  /* Database handle configuration */
  lsm_env *pEnv;                            /* runtime environment */
  int (*xCmp)(void *, int, void *, int);    /* Compare function */

  /* Values configured by calls to lsm_config */
  int eSafety;                    /* LSM_SAFETY_OFF, NORMAL or FULL */
  int bAutowork;                  /* Configured by LSM_CONFIG_AUTOWORK */
  int nTreeLimit;                 /* Configured by LSM_CONFIG_WRITE_BUFFER */
  int nMerge;                     /* Configured by LSM_CONFIG_NMERGE */
  int nLogSz;                     /* Configured by LSM_CONFIG_LOG_SIZE */
  int bUseLog;                    /* Configured by LSM_CONFIG_USE_LOG */
  int nDfltPgsz;                  /* Configured by LSM_CONFIG_PAGE_SIZE */
  int nDfltBlksz;                 /* Configured by LSM_CONFIG_BLOCK_SIZE */
  int nMaxFreelist;               /* Configured by LSM_CONFIG_MAX_FREELIST */

  /* Sub-system handles */
  FileSystem *pFS;                /* On-disk portion of database */
  Database *pDatabase;            /* Database shared data */

  /* Client transaction context */
  Snapshot *pClient;              /* Client snapshot (non-NULL in read trans) */
  int iReader;                    /* Read lock held (-1 == unlocked) */
  MultiCursor *pCsr;              /* List of all open cursors */
  LogWriter *pLogWriter;          /* Context for writing to the log file */
  int nTransOpen;                 /* Number of opened write transactions */
  int nTransAlloc;                /* Allocated size of aTrans[] array */
  TransMark *aTrans;              /* Array of marks for transaction rollback */
  IntArray rollback;              /* List of tree-nodes to roll back */

  /* Worker context */
  Snapshot *pWorker;              /* Worker snapshot (or NULL) */

  /* Debugging message callback */
  void (*xLog)(void *, int, const char *);
  void *pLogCtx;

  /* Work done notification callback */
  void (*xWork)(lsm_db *, void *);
  void *pWorkCtx;

  u32 mLock;                      /* Mask of current locks. See lsmShmLock(). */
  lsm_db *pNext;                  /* Next connection to same database */

  int nShm;                       /* Size of apShm[] array */
  void **apShm;                   /* Shared memory chunks */
  ShmHeader *pShmhdr;             /* Live shared-memory header */
  TreeHeader treehdr;             /* Local copy of tree-header */
  u32 aSnapshot[LSM_META_PAGE_SIZE / sizeof(u32)];
};

struct Segment {
  int iFirst;                     /* First page of this run */
  int iLast;                      /* Last page of this run */
  Pgno iRoot;                     /* Root page number (if any) */
  int nSize;                      /* Size of this run in pages */
};

/*
** iSplitTopic/pSplitKey/nSplitKey:
**   If nRight>0, this buffer contains a copy of the largest key that has
**   already been written to the left-hand-side of the level.
*/
struct Level {
  Segment lhs;                    /* Left-hand (main) segment */
  int iAge;                       /* Number of times data has been written */
  int nRight;                     /* Size of apRight[] array */
  Segment *aRhs;                  /* Old segments being merged into this */
  int iSplitTopic;                /* Split key topic (if nRight>0) */
  void *pSplitKey;                /* Pointer to split-key (if nRight>0) */
  int nSplitKey;                  /* Number of bytes in split-key */
  Merge *pMerge;                  /* Merge operation currently underway */
  Level *pNext;                   /* Next level in tree */
};

/*
** A structure describing an ongoing merge. There is an instance of this
** structure for every Level currently undergoing a merge in the worker
** snapshot.
**
** It is assumed that code that uses an instance of this structure has
** access to the associated Level struct.
**
** bHierReadonly:
**   True if the b-tree hierarchy is currently read-only.
**
** iOutputOff:
**   The byte offset to write to next within the last page of the 
**   output segment.
*/
struct MergeInput {
  Pgno iPg;                       /* Page on which next input is stored */
  int iCell;                      /* Cell containing next input to merge */
};
struct Merge {
  int nInput;                     /* Number of input runs being merged */
  MergeInput *aInput;             /* Array nInput entries in size */
  MergeInput splitkey;            /* Location in file of current splitkey */
  int nSkip;                      /* Number of separators entries to skip */
  int iOutputOff;                 /* Write offset on output page */
  int bHierReadonly;              /* True if b-tree heirarchies are read-only */
};

/* 
** The first argument to this macro is a pointer to a Segment structure.
** Returns true if the structure instance indicates that the separators
** array is valid.
*/
#define segmentHasSeparators(pSegment) ((pSegment)->sep.iFirst>0)

/*
** The values that accompany the lock held by a database reader.
*/
struct ShmReader {
  i64 iTreeId;
  i64 iLsmId;
};

/*
** An instance of this structure is stored in the first shared-memory
** page. The shared-memory header.
**
** bWriter:
**   Immediately after opening a write transaction taking the WRITER lock, 
**   each writer client sets this flag. It is cleared right before the 
**   WRITER lock is relinquished. If a subsequent writer finds that this
**   flag is already set when a write transaction is opened, this indicates
**   that a previous writer failed mid-transaction.
**
** iMetaPage:
**   If the database file does not contain a valid, synced, checkpoint, this
**   value is set to 0. Otherwise, it is set to the meta-page number that
**   contains the most recently written checkpoint (either 1 or 2).
**
** hdr1, hdr2:
**   The two copies of the in-memory tree header. Two copies are required
**   in case a writer fails while updating one of them.
*/
struct ShmHeader {
  u32 aClient[LSM_META_PAGE_SIZE / 4];
  u32 aWorker[LSM_META_PAGE_SIZE / 4];
  u32 bWriter;
  u32 iMetaPage;
  TreeHeader hdr1;
  TreeHeader hdr2;
  ShmReader aReader[LSM_LOCK_NREADER];
};

/*
** An instance of this structure is stored at the start of each shared-memory
** chunk except the first (which is the header chunk - see above).
*/
struct ShmChunk {
  u32 iFirstTree;
  u32 iLastTree;
  u32 iNext;
};

#define LSM_APPLIST_SZ 4

typedef struct Freelist Freelist;
typedef struct FreelistEntry FreelistEntry;

/*
** An instance of the following structure stores the current database free
** block list. The free list is a list of blocks that are not currently
** used by the worker snapshot. Assocated with each block in the list is the
** snapshot id of the most recent snapshot that did actually use the block.
*/
struct Freelist {
  FreelistEntry *aEntry;          /* Free list entries */
  int nEntry;                     /* Number of valid slots in aEntry[] */
  int nAlloc;                     /* Allocated size of aEntry[] */
};
struct FreelistEntry {
  u32 iBlk;                       /* Block number */
  i64 iId;                        /* Largest snapshot id to use this block */
};

/*
** A snapshot of a database. A snapshot contains all the information required
** to read or write a database file on disk. See the description of struct
** Database below for futher details.
*/
struct Snapshot {
  Database *pDatabase;            /* Database this snapshot belongs to */
  Level *pLevel;                  /* Pointer to level 0 of snapshot (or NULL) */
  i64 iId;                        /* Snapshot id */

  /* Used by worker snapshots only */
  int nBlock;                     /* Number of blocks in database file */
  u32 aiAppend[LSM_APPLIST_SZ];   /* Append point list */
  Freelist freelist;              /* Free block list */
  int nFreelistOvfl;              /* Number of extra free-list entries in LSM */
};
#define LSM_INITIAL_SNAPSHOT_ID 11

/*
** Functions from file "lsm_ckpt.c".
*/
int lsmCheckpointWrite(lsm_db *);
int lsmCheckpointLevels(lsm_db *, int, void **, int *);
int lsmCheckpointLoadLevels(lsm_db *pDb, void *pVal, int nVal);

int lsmCheckpointOverflow(lsm_db *pDb, void **, int *, int *);
int lsmCheckpointOverflowRequired(lsm_db *pDb);
int lsmCheckpointOverflowLoad(lsm_db *pDb, Freelist *);

int lsmCheckpointRecover(lsm_db *);
int lsmCheckpointDeserialize(lsm_db *, int, u32 *, Snapshot **);

int lsmCheckpointLoad(lsm_db *pDb);
int lsmCheckpointLoadWorker(lsm_db *pDb);
int lsmCheckpointStore(lsm_db *pDb, int);

i64 lsmCheckpointId(u32 *, int);
i64 lsmCheckpointLogOffset(u32 *);
int lsmCheckpointPgsz(u32 *);
int lsmCheckpointBlksz(u32 *);
void lsmCheckpointLogoffset(u32 *aCkpt, DbLog *pLog);
void lsmCheckpointZeroLogoffset(lsm_db *);

int lsmCheckpointSaveWorker(lsm_db *pDb, int, int);
int lsmDatabaseFull(lsm_db *pDb);
int lsmCheckpointSynced(lsm_db *pDb, i64 *piId);


/* 
** Functions from file "lsm_tree.c".
*/
int lsmTreeNew(lsm_env *, int (*)(void *, int, void *, int), Tree **ppTree);
void lsmTreeRelease(lsm_env *, Tree *);
void lsmTreeClear(lsm_db *);
void lsmTreeInit(lsm_db *);

int lsmTreeSize(lsm_db *);
int lsmTreeIsEmpty(lsm_db *);
int lsmTreeEndTransaction(lsm_db *pDb, int bCommit);
int lsmTreeBeginTransaction(lsm_db *pDb);
int lsmTreeLoadHeader(lsm_db *pDb);

int lsmTreeInsert(lsm_db *pDb, void *pKey, int nKey, void *pVal, int nVal);
void lsmTreeRollback(lsm_db *pDb, TreeMark *pMark);
void lsmTreeMark(lsm_db *pDb, TreeMark *pMark);

int lsmTreeCursorNew(lsm_db *pDb, TreeCursor **);
void lsmTreeCursorDestroy(TreeCursor *);

int lsmTreeCursorSeek(TreeCursor *pCsr, void *pKey, int nKey, int *pRes);
int lsmTreeCursorNext(TreeCursor *pCsr);
int lsmTreeCursorPrev(TreeCursor *pCsr);
int lsmTreeCursorEnd(TreeCursor *pCsr, int bLast);
void lsmTreeCursorReset(TreeCursor *pCsr);
int lsmTreeCursorKey(TreeCursor *pCsr, void **ppKey, int *pnKey);
int lsmTreeCursorValue(TreeCursor *pCsr, void **ppVal, int *pnVal);
int lsmTreeCursorValid(TreeCursor *pCsr);
int lsmTreeCursorSave(TreeCursor *pCsr);

/* 
** Functions from file "mem.c".
*/
int lsmPoolNew(lsm_env *pEnv, Mempool **ppPool);
void lsmPoolDestroy(lsm_env *pEnv, Mempool *pPool);
void *lsmPoolMalloc(lsm_env *pEnv, Mempool *pPool, int nByte);
void *lsmPoolMallocZero(lsm_env *pEnv, Mempool *pPool, int nByte);
int lsmPoolUsed(Mempool *pPool);

void lsmPoolMark(Mempool *pPool, void **, int *);
void lsmPoolRollback(lsm_env *pEnv, Mempool *pPool, void *, int);

void *lsmMalloc(lsm_env*, size_t);
void lsmFree(lsm_env*, void *);
void *lsmRealloc(lsm_env*, void *, size_t);
void *lsmReallocOrFree(lsm_env*, void *, size_t);
void *lsmReallocOrFreeRc(lsm_env *, void *, size_t, int *);

void *lsmMallocZeroRc(lsm_env*, size_t, int *);
void *lsmMallocRc(lsm_env*, size_t, int *);

void *lsmMallocZero(lsm_env *pEnv, size_t);
char *lsmMallocStrdup(lsm_env *pEnv, const char *);

/* 
** Functions from file "lsm_mutex.c".
*/
int lsmMutexStatic(lsm_env*, int, lsm_mutex **);
int lsmMutexNew(lsm_env*, lsm_mutex **);
void lsmMutexDel(lsm_env*, lsm_mutex *);
void lsmMutexEnter(lsm_env*, lsm_mutex *);
int lsmMutexTry(lsm_env*, lsm_mutex *);
void lsmMutexLeave(lsm_env*, lsm_mutex *);

#ifndef NDEBUG
int lsmMutexHeld(lsm_env *, lsm_mutex *);
int lsmMutexNotHeld(lsm_env *, lsm_mutex *);
#endif

/**************************************************************************
** Start of functions from "lsm_file.c".
*/
int lsmFsOpen(lsm_db *, const char *);
void lsmFsClose(FileSystem *);

int lsmFsBlockSize(FileSystem *);
void lsmFsSetBlockSize(FileSystem *, int);

int lsmFsPageSize(FileSystem *);
void lsmFsSetPageSize(FileSystem *, int);

int lsmFsFileid(lsm_db *pDb, void **ppId, int *pnId);

/* Creating, populating, gobbling and deleting sorted runs. */
void lsmFsGobble(Snapshot *, Segment *, Page *);
int lsmFsSortedDelete(FileSystem *, Snapshot *, int, Segment *);
int lsmFsSortedFinish(FileSystem *, Segment *);
int lsmFsSortedAppend(FileSystem *, Snapshot *, Segment *, Page **);
int lsmFsPhantomMaterialize(FileSystem *, Snapshot *, Segment *);

/* Functions to retrieve the lsm_env pointer from a FileSystem or Page object */
lsm_env *lsmFsEnv(FileSystem *);
lsm_env *lsmPageEnv(Page *);
FileSystem *lsmPageFS(Page *);

int lsmFsSectorSize(FileSystem *);

void lsmSortedSplitkey(lsm_db *, Level *, int *);

/* Reading sorted run content. */
int lsmFsDbPageGet(FileSystem *, Pgno, Page **);
int lsmFsDbPageNext(Segment *, Page *, int eDir, Page **);

int lsmFsPageWrite(Page *);
u8 *lsmFsPageData(Page *, int *);
int lsmFsPageRelease(Page *);
int lsmFsPagePersist(Page *);
void lsmFsPageRef(Page *);
Pgno lsmFsPageNumber(Page *);

int lsmFsNRead(FileSystem *);
int lsmFsNWrite(FileSystem *);

int lsmFsMetaPageGet(FileSystem *, int, int, MetaPage **);
int lsmFsMetaPageRelease(MetaPage *);
u8 *lsmFsMetaPageData(MetaPage *, int *);

#ifdef LSM_DEBUG
int lsmFsIntegrityCheck(lsm_db *);
#endif

int lsmFsPageWritable(Page *);

/* Functions to read, write and sync the log file. */
int lsmFsWriteLog(FileSystem *pFS, i64 iOff, LsmString *pStr);
int lsmFsSyncLog(FileSystem *pFS);
int lsmFsReadLog(FileSystem *pFS, i64 iOff, int nRead, LsmString *pStr);
int lsmFsTruncateLog(FileSystem *pFS, i64 nByte);
int lsmFsCloseAndDeleteLog(FileSystem *pFS);

/* And to sync the db file */
int lsmFsSyncDb(FileSystem *);

/* Used by lsm_info(ARRAY_STRUCTURE) and lsm_config(MMAP) */
int lsmInfoArrayStructure(lsm_db *pDb, Pgno iFirst, char **pzOut);
int lsmConfigMmap(lsm_db *pDb, int *piParam);

/*
** End of functions from "lsm_file.c".
**************************************************************************/

/* 
** Functions from file "lsm_sorted.c".
*/
int lsmInfoPageDump(lsm_db *, Pgno, int, char **);
int lsmSortedFlushTree(lsm_db *, int *);
void lsmSortedCleanup(lsm_db *);
int lsmSortedAutoWork(lsm_db *, int nUnit);

void lsmSortedRemap(lsm_db *pDb);

void lsmSortedFreeLevel(lsm_env *pEnv, Level *);

int lsmSortedFlushDb(lsm_db *);
int lsmSortedAdvanceAll(lsm_db *pDb);

int lsmSortedLoadMerge(lsm_db *, Level *, u32 *, int *);
int lsmSortedLoadFreelist(lsm_db *pDb, void **, int *);

void *lsmSortedSplitKey(Level *pLevel, int *pnByte);

void lsmSortedSaveTreeCursors(lsm_db *);

int lsmMCursorNew(lsm_db *, MultiCursor **);
void lsmMCursorClose(MultiCursor *);
int lsmMCursorSeek(MultiCursor *, void *, int , int);
int lsmMCursorFirst(MultiCursor *);
int lsmMCursorPrev(MultiCursor *);
int lsmMCursorLast(MultiCursor *);
int lsmMCursorValid(MultiCursor *);
int lsmMCursorNext(MultiCursor *);
int lsmMCursorKey(MultiCursor *, void **, int *);
int lsmMCursorValue(MultiCursor *, void **, int *);
int lsmMCursorType(MultiCursor *, int *);
lsm_db *lsmMCursorDb(MultiCursor *);

int lsmSaveCursors(lsm_db *pDb);
int lsmRestoreCursors(lsm_db *pDb);

void lsmSortedDumpStructure(lsm_db *pDb, Snapshot *, int, int, const char *);
void lsmFsDumpBlocklists(lsm_db *);


void lsmPutU32(u8 *, u32);
u32 lsmGetU32(u8 *);

/*
** Functions from "lsm_varint.c".
*/
int lsmVarintPut32(u8 *, int);
int lsmVarintGet32(u8 *, int *);
int lsmVarintPut64(u8 *aData, i64 iVal);
int lsmVarintGet64(const u8 *aData, i64 *piVal);

int lsmVarintLen32(int);
int lsmVarintSize(u8 c);

/* 
** Functions from file "main.c".
*/
void lsmLogMessage(lsm_db *, int, const char *, ...);
int lsmFlushToDisk(lsm_db *);

/*
** Functions from file "lsm_log.c".
*/
int lsmLogBegin(lsm_db *pDb);
int lsmLogWrite(lsm_db *, void *, int, void *, int);
int lsmLogCommit(lsm_db *);
void lsmLogEnd(lsm_db *pDb, int bCommit);
void lsmLogTell(lsm_db *, LogMark *);
void lsmLogSeek(lsm_db *, LogMark *);

int lsmLogRecover(lsm_db *);
void lsmLogCheckpoint(lsm_db *, lsm_i64);
int lsmLogStructure(lsm_db *pDb, char **pzVal);


/**************************************************************************
** Functions from file "lsm_shared.c".
*/

int lsmDbDatabaseConnect(lsm_db*, const char *);
void lsmDbDatabaseRelease(lsm_db *);

int lsmBeginReadTrans(lsm_db *);
int lsmBeginWriteTrans(lsm_db *);
int lsmBeginFlush(lsm_db *);

int lsmBeginWork(lsm_db *);
void lsmFinishWork(lsm_db *, int, int, int *);

int lsmFinishRecovery(lsm_db *);
void lsmFinishReadTrans(lsm_db *);
int lsmFinishWriteTrans(lsm_db *, int);
int lsmFinishFlush(lsm_db *, int);

int lsmSnapshotSetFreelist(lsm_db *, int *, int);

Snapshot *lsmDbSnapshotClient(lsm_db *);
Snapshot *lsmDbSnapshotWorker(lsm_db *);

void lsmSnapshotSetCkptid(Snapshot *, i64);

Level *lsmDbSnapshotLevel(Snapshot *);
void lsmDbSnapshotSetLevel(Snapshot *, Level *);

void lsmDbRecoveryComplete(lsm_db *, int);

int lsmBlockAllocate(lsm_db *, int *);
int lsmBlockFree(lsm_db *, int);
int lsmBlockRefree(lsm_db *, int);

void lsmFreelistDeltaBegin(lsm_db *);
void lsmFreelistDeltaEnd(lsm_db *);
int lsmFreelistDelta(lsm_db *pDb);

DbLog *lsmDatabaseLog(lsm_db *pDb);

#ifdef LSM_DEBUG
  int lsmHoldingClientMutex(lsm_db *pDb);
  int lsmShmAssertLock(lsm_db *db, int iLock, int eOp);
  int lsmShmAssertWorker(lsm_db *db);
#endif

void lsmFreeSnapshot(lsm_env *, Snapshot *);


/* Candidate values for the 3rd argument to lsmShmLock() */
#define LSM_LOCK_UNLOCK 0
#define LSM_LOCK_SHARED 1
#define LSM_LOCK_EXCL   2

int lsmShmChunk(lsm_db *db, int iChunk, void **ppData);
int lsmShmLock(lsm_db *db, int iLock, int eOp, int bBlock);
void lsmShmBarrier(lsm_db *db);

#ifdef LSM_DEBUG
void lsmShmHasLock(lsm_db *db, int iLock, int eOp);
#else
# define lsmShmHasLock(x,y,z)
#endif

int lsmReadlock(lsm_db *, i64 iLsm, i64 iTree);
int lsmReleaseReadlock(lsm_db *);

int lsmLsmInUse(lsm_db *db, i64 iLsmId, int *pbInUse);
int lsmTreeInUse(lsm_db *db, u32 iLsmId, int *pbInUse);
int lsmFreelistAppend(lsm_env *pEnv, Freelist *p, int iBlk, i64 iId);


/**************************************************************************
** functions in lsm_str.c
*/
void lsmStringInit(LsmString*, lsm_env *pEnv);
int lsmStringExtend(LsmString*, int);
int lsmStringAppend(LsmString*, const char *, int);
void lsmStringVAppendf(LsmString*, const char *zFormat, va_list, va_list);
void lsmStringAppendf(LsmString*, const char *zFormat, ...);
void lsmStringClear(LsmString*);
char *lsmMallocPrintf(lsm_env*, const char*, ...);
int lsmStringBinAppend(LsmString *pStr, const u8 *a, int n);



/* 
** Round up a number to the next larger multiple of 8.  This is used
** to force 8-byte alignment on 64-bit architectures.
*/
#define ROUND8(x)     (((x)+7)&~7)

#define LSM_MIN(x,y) ((x)>(y) ? (y) : (x))
#define LSM_MAX(x,y) ((x)>(y) ? (x) : (y))

#endif
