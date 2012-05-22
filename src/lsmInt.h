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
# define LSM_DEBUG
#endif

/*
** Default values for various data structure parameters. These may be
** overridden by calls to lsm_config().
*/
#define LSM_PAGE_SIZE  4096
#define LSM_TREE_BYTES (2 * 1024 * 1024)
#define LSM_ECOLA      4

#define LSM_DEFAULT_LOG_SIZE (128*1024)

/* Places where a NULL needs to be changed to a real lsm_env pointer
** are marked with NEED_ENV */
#define NEED_ENV ((lsm_env*)0)

/* Initial values for log file checksums. These are only used if the 
** database file does not contain a valid checkpoint.  */
#define LSM_CKSUM0_INIT 42
#define LSM_CKSUM1_INIT 42

typedef struct Database Database;
typedef struct DbLog DbLog;
typedef struct FileSystem FileSystem;
typedef struct Level Level;

typedef struct LogMark LogMark;
typedef struct LogRegion LogRegion;
typedef struct LogWriter LogWriter;

typedef struct LsmString LsmString;
typedef struct Mempool Mempool;
typedef struct MultiCursor MultiCursor;
typedef struct Page Page;
typedef struct Segment Segment;
typedef struct SegmentMerger SegmentMerger;
typedef struct Snapshot Snapshot;
typedef struct SortedRun SortedRun;
typedef struct Tree Tree;
typedef struct TreeMark TreeMark;
typedef struct TreeVersion TreeVersion;
typedef struct TreeCursor TreeCursor;
typedef struct Merge Merge;
typedef struct MergeInput MergeInput;

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

typedef struct IList IList;
struct IList {
  int nAlloc;
  int n;
  int *a;
};

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
** An instance of this structure represents a point in the history of the
** tree structure to roll back to. Refer to comments in tree.c for details.
**
** Pointers pRollback and pRoot both point to structures of type TreeNode.
*/
struct TreeMark {
  void *pMpChunk;                 /* Mempool chunk to roll back to */
  int iMpOff;                     /* Mempool chunk offset to roll back to */
  void *pRollback;                /* Zero v2 information starting here */
  void *pRoot;                    /* Root node to restore */
  int nHeight;                    /* Height of tree at pRoot */
};

/*
** An instance of this structure represents a point in the database log.
*/
struct LogMark {
  i64 iOff;                       /* Offset into log (see lsm_log.c) */
  int nBuf;                       /* Size of in-memory buffer here */
  u32 cksum0;                     /* Checksum 0 at offset iOff */
  u32 cksum1;                     /* Checksum 1 at offset iOff */
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
  LsmString buf;                  /* Buffer containing data not yet written */
};

/*
** Database handle structure.
*/
struct lsm_db {

  /* Database handle configuration */
  lsm_env *pEnv;                            /* runtime environment */
  int (*xCmp)(void *, int, void *, int);    /* Compare function */
  int nTreeLimit;                 /* Maximum size of in-memory tree in bytes */
  int eCola;                      /* Max size ratio between adjacent segments */
  int bAutowork;                  /* True to do auto-work after writing */
  int eSafety;                    /* LSM_SAFETY_OFF, NORMAL or FULL */
  int nLogSz;                     /* Configured by LSM_CONFIG_LOG_SIZE */

  /* Sub-system handles */
  FileSystem *pFS;                /* On-disk portion of database */
  Database *pDatabase;            /* Database shared data */

  /* Client transaction context */
  TreeVersion *pTV;               /* In-memory tree snapshot (non-NULL in rt) */
  Snapshot *pClient;              /* Client snapshot (non-NULL in read trans) */
  lsm_cursor *pCsr;               /* List of open cursors */
  int nTransOpen;                 /* Number of opened write transactions */
  int nTransAlloc;                /* Allocated size of aTrans[] array */
  TreeMark *aTrans;               /* Array of marks for transaction rollback */
  LogWriter *pLogWriter;

  /* Worker context */
  Snapshot *pWorker;              /* Worker snapshot (or NULL) */

  /* Debugging message callback */
  void (*xLog)(void *, int, const char *);
  void *pLogCtx;

  /* Work done notification callback */
  void (*xWork)(lsm_db *, void *);
  void *pWorkCtx;
};

/*
** Cursor handle structure.
*/
struct lsm_cursor {
  MultiCursor *pMC;
  lsm_db *pDb;                    /* Database handle that owns this cursor */
  lsm_cursor *pNext;              /* Next cursor belonging to same lsm_db */
};

struct SortedRun {
  int iFirst;                     /* First page of this run */
  int iLast;                      /* Last page of this run */
  Pgno iRoot;                     /* Root page number (if any) */
  int nSize;                      /* Size of this run in pages */
};

struct Segment {
  SortedRun run;                  /* Main array */
  SortedRun sep;                  /* If sep.iFirst!=0, the separators array */
};

/*
** iSplitTopic/pSplitKey/nSplitKey:
**   If nRight>0, this buffer contains a copy of the largest key that has
**   already been written to the left-hand-side of the level.
*/
struct Level {
  Segment lhs;                    /* Left-hand (main) segment */
  int nRight;                     /* Size of apRight[] array */
  Segment *aRhs;                  /* Old segments being merged into this */
  int iSplitTopic;
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
** aiOutputOff:
**   The byte offset to write to next within the last page of the output
**   segments main run (aiOutputOff[0]) or separators run (aiOutputOff[1]).
**   If either page is read-only, then the associated aiOutputOff[] entry
**   is set to a negative value.
*/
struct Merge {
  int nInput;                     /* Number of input runs being merged */
  MergeInput *aInput;             /* Array nInput entries in size */
  int nSkip;                      /* Number of separators entries to skip */
  int aiOutputOff[2];             /* Write offsets on run output pages */
  int bHierReadonly;              /* True if b-tree heirarchy is read-only */
};
struct MergeInput {
  Pgno iPg;                       /* Page on which next input is stored */
  int iCell;                      /* Cell containing next input to merge */
};

/* 
** The first argument to this macro is a pointer to a Segment structure.
** Returns true if the structure instance indicates that the separators
** array is valid.
*/
#define segmentHasSeparators(pSegment) ((pSegment)->sep.iFirst>0)

/*
** Number of integers in the free-list delta.
*/
#define LSM_FREELIST_DELTA_SIZE 3

/* 
** Functions from file "lsm_ckpt.c".
*/
int lsmCheckpointRead(lsm_db *);
int lsmCheckpointWrite(lsm_db *);
int lsmCheckpointExport(lsm_db *, i64, int, void **, int *);
void lsmChecksumBytes(const u8 *, int, const u32 *, u32 *);
lsm_i64 lsmCheckpointLogOffset(void *pExport);


/* 
** Functions from file "lsm_tree.c".
*/
int lsmTreeNew(lsm_env *, int (*)(void *, int, void *, int), Tree **ppTree);
void lsmTreeDestroy(Tree *pTree);
int lsmTreeSize(TreeVersion *pTV);
int lsmTreeIsEmpty(Tree *pTree);

int lsmTreeInsert(TreeVersion *, void *pKey, int nKey, void *pVal, int nVal);

void lsmTreeRollback(TreeVersion *pTV, TreeMark *pMark);
void lsmTreeMark(TreeVersion *pTV, TreeMark *pMark);

int lsmTreeCursorNew(TreeVersion *, TreeCursor **);
void lsmTreeCursorDestroy(TreeCursor *);

int lsmTreeCursorSeek(TreeCursor *pCsr, void *pKey, int nKey, int *pRes);
int lsmTreeCursorNext(TreeCursor *pCsr);
int lsmTreeCursorPrev(TreeCursor *pCsr);
int lsmTreeCursorEnd(TreeCursor *pCsr, int bLast);
void lsmTreeCursorReset(TreeCursor *pCsr);
int lsmTreeCursorKey(TreeCursor *pCsr, void **ppKey, int *pnKey);
int lsmTreeCursorValue(TreeCursor *pCsr, void **ppVal, int *pnVal);
int lsmTreeCursorValid(TreeCursor *pCsr);

TreeVersion *lsmTreeReadVersion(Tree *);
TreeVersion *lsmTreeWriteVersion(Tree *, TreeVersion *);
TreeVersion *lsmTreeRecoverVersion(Tree *);

int lsmTreeReleaseWriteVersion(TreeVersion *, TreeVersion **);
void lsmTreeReleaseReadVersion(TreeVersion *);

int lsmTreeIsWriteVersion(TreeVersion *);

void lsmTreeDecrRefcount(Tree *);
void lsmTreeIncrRefcount(Tree *);

void lsmTreeRelease(Tree *);
void lsmTreeFixVersion(TreeCursor *, TreeVersion *);
void lsmTreeCursorSave(TreeCursor *);

/* 
** Functions from file "mem.c".
*/
int lsmPoolNew(lsm_env *pEnv, Mempool **ppPool);
void lsmPoolDestroy(Mempool *pPool);
void *lsmPoolMalloc(Mempool *pPool, int nByte);
void *lsmPoolMallocZero(Mempool *pPool, int nByte);
int lsmPoolUsed(Mempool *pPool);

void lsmPoolMark(Mempool *pPool, void **, int *);
void lsmPoolRollback(Mempool *pPool, void *, int);

void *lsmMalloc(lsm_env*, size_t);
void lsmFree(lsm_env*, void *);
void *lsmRealloc(lsm_env*, void *, size_t);
void *lsmReallocOrFree(lsm_env*, void *, size_t);

void *lsmMallocZeroRc(lsm_env*, size_t, int *);
void *lsmMallocRc(lsm_env*, size_t, int *);

void *lsmMallocZero(lsm_env *pEnv, size_t);
char *lsmMallocStrdup(lsm_env *pEnv, const char *);

/* 
** Functions from file "mutex.c".
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

/* 
** Functions from file "file.c".
*/
int lsmFsOpen(lsm_db *, const char *, int);
void lsmFsClose(FileSystem *);

int lsmFsBlockSize(FileSystem *);
void lsmFsSetBlockSize(FileSystem *, int);

int lsmFsPageSize(FileSystem *);
void lsmFsSetPageSize(FileSystem *, int);

int lsmFsBlockCnt(FileSystem *);
void lsmFsSetBlockCnt(FileSystem *pFS, int);

int lsmFsSetFreelist(FileSystem *, int, u32 *, int);

/* Creating and populating sorted runs. */
int lsmFsSortedPhantom(FileSystem *, SortedRun *);
void lsmFsSortedPhantomFree(FileSystem *pFS);
void lsmFsSortedSetRoot(SortedRun *, Pgno);

void lsmFsGobble(Snapshot *, SortedRun *, Page *);
int lsmFsSortedDelete(FileSystem *, Snapshot *, int, SortedRun *);
int lsmFsSortedFinish(FileSystem *, Snapshot *, SortedRun *);
int lsmFsSortedAppend(FileSystem *, Snapshot *, SortedRun *, Page **);
int lsmFsPhantomMaterialize(FileSystem *, Snapshot *, SortedRun *);

lsm_env *lsmFsEnv(FileSystem *);
lsm_env *lsmPageEnv(Page *);

void lsmSortedSplitkey(lsm_db *, Level *, int *);

/* Reading sorted run content. */
int lsmFsDbPageGet(FileSystem *, SortedRun *, Pgno, Page **);
int lsmFsDbPageNext(SortedRun *, Page *, int eDir, Page **);
int lsmFsDbPageEnd(FileSystem *, SortedRun *, int bLast, Page **);

Pgno lsmFsFirstPgno(SortedRun *);
Pgno lsmFsLastPgno(SortedRun *);
Pgno lsmFsSortedRoot(SortedRun *);
int lsmFsSortedSize(SortedRun *);

int lsmFsPageWrite(Page *);
u8 *lsmFsPageData(Page *, int *);
int lsmFsPageRelease(Page *);
int lsmFsPagePersist(Page *);
void lsmFsPageRef(Page *);
Pgno lsmFsPageNumber(Page *);

int lsmFsNRead(FileSystem *);
int lsmFsNWrite(FileSystem *);

int lsmFsDbSync(FileSystem *);
int lsmFsLogSync(FileSystem *);

int lsmFsMetaPageGet(FileSystem *, int, Page **);
int lsmFsLogPageGet(FileSystem *, int, Page **);

#ifdef LSM_EXPENSIVE_DEBUG
int lsmFsIntegrityCheck(lsm_db *);
#else
# define lsmFsIntegrityCheck(pDb) 1
#endif

int lsmFsPageWritable(Page *);

int lsmFsWriteLog(FileSystem *pFS, i64 iOff, LsmString *pStr);
int lsmFsSyncLog(FileSystem *pFS);
int lsmFsReadLog(FileSystem *pFS, i64 iOff, int nRead, LsmString *pStr);
int lsmFsTruncateLog(FileSystem *pFS, i64 nByte);

/* 
** Functions from file "sorted.c".
*/
int lsmSortedFlushTree(lsm_db *);
void lsmSortedCleanup(lsm_db *);
int lsmSortedAutoWork(lsm_db *);

void lsmSortedFreeLevel(lsm_env *pEnv, Level *);

int lsmSortedFlushDb(lsm_db *);
int lsmSortedAdvanceAll(lsm_db *pDb);

int lsmSortedLoadMerge(lsm_db *, Level *, u32 *, int *);

int lsmSortedLoadFreelist(lsm_db *pDb);

void *lsmSortedSplitKey(Level *pLevel, int *pnByte);

void lsmSortedFixTreeVersions(lsm_db *);
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

int lsmMCursorSave(MultiCursor *);
int lsmMCursorRestore(lsm_db *, MultiCursor *);

void lsmSortedDumpStructure(lsm_db *pDb, Snapshot *, int, int, const char *);
void lsmFsDumpBlockmap(lsm_db *, SortedRun *);
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
int lsmLogWrite(lsm_db *, void *, int, void *, int);
int lsmLogCommit(lsm_db *);

void lsmLogTell(lsm_db *, LogMark *);
int lsmLogSeek(lsm_db *, LogMark *);
int lsmLogRecover(lsm_db *);

int lsmLogCheckpoint(lsm_db *, DbLog *pLog, lsm_i64);
int lsmLogBegin(lsm_db *pDb, DbLog *pLog);
void lsmLogEnd(lsm_db *pDb, DbLog *pLog, int bCommit);


/**************************************************************************
** Functions from file "shared.c".
*/
int lsmDbDatabaseFind(lsm_env*, const char *, Database **);
void lsmDbDatabaseRelease(lsm_db *);

int lsmBeginRecovery(lsm_db *);
int lsmBeginReadTrans(lsm_db *);
int lsmBeginWriteTrans(lsm_db *);
int lsmBeginFlush(lsm_db *);

int lsmFinishRecovery(lsm_db *);
void lsmFinishReadTrans(lsm_db *);
int lsmFinishWriteTrans(lsm_db *, int);
int lsmFinishFlush(lsm_db *, int);

int lsmDbUpdateClient(lsm_db *);

int lsmSnapshotFreelist(Snapshot *, int **, int *);
int lsmSnapshotSetFreelist(Snapshot *pSnap, int *aElem, int nElem);

Snapshot *lsmDbSnapshotClient(Database *);
Snapshot *lsmDbSnapshotWorker(Database *);
Snapshot *lsmDbSnapshotRecover(Database *);
void lsmDbSnapshotRelease(Snapshot *);


void lsmSnapshotSetNBlock(Snapshot *, int);
int lsmSnapshotGetNBlock(Snapshot *);

void lsmSnapshotSetCkptid(Snapshot *, i64);
i64 lsmSnapshotGetCkptid(Snapshot *);

void lsmSnapshotSetSalt(Snapshot *, u32, u32);
void lsmSnapshotGetSalt(Snapshot *, u32 *, u32 *);
void lsmSnapshotSetLogpgno(Snapshot *pSnap, Pgno iLogPg);
Pgno lsmSnapshotGetLogpgno(Snapshot *pSnap);

Level *lsmDbSnapshotLevel(Snapshot *);
void lsmDbSnapshotSetLevel(Snapshot *, Level *);

#define LSM_APPEND_LIST  1
IList *lsmSnapshotList(Snapshot *, int);
int lsmIListSet(lsm_env *pEnv, IList *, int *, int);

void lsmDbRecoveryComplete(Database *, int);
void lsmDbCheckpointed(lsm_db *, i64);

int lsmBlockAllocate(lsm_db *, int *);
int lsmBlockFree(lsm_db *, int);
int lsmBlockRefree(lsm_db *, int);

void lsmFreelistDeltaBegin(lsm_db *);
void lsmFreelistDeltaEnd(lsm_db *);
void lsmFreelistDelta(lsm_db *, u32 *);
u32 *lsmFreelistDeltaPtr(lsm_db *pDb);

void lsmDatabaseDirty(Database *p);
int lsmDatabaseIsDirty(Database *p);

DbLog *lsmDatabaseLog(lsm_db *pDb);

#ifdef LSM_DEBUG
  int lsmHoldingClientMutex(lsm_db *pDb);
#endif


/**************************************************************************
** functions in lsm_str.c
*/
void lsmStringInit(LsmString*, lsm_env *pEnv);
int lsmStringExtend(LsmString*, int);
int lsmStringAppend(LsmString*, const char *, int);
void lsmStringVAppendf(LsmString*, const char *zFormat, va_list ap);
void lsmStringAppendf(LsmString*, const char *zFormat, ...);
void lsmStringClear(LsmString*);
char *lsmMallocPrintf(lsm_env*, const char*, ...);

int lsmStringBinAppend(LsmString *pStr, const u8 *a, int n);



/* 
** Round up a number to the next larger multiple of 8.  This is used
** to force 8-byte alignment on 64-bit architectures.
*/
#define ROUND8(x)     (((x)+7)&~7)

#define MIN(x,y) ((x)>(y) ? (y) : (x))
#define MAX(x,y) ((x)>(y) ? (x) : (y))

#endif
