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
** This header file defines the interface that the sqlite page cache
** subsystem.  The page cache subsystem reads and writes a file a page
** at a time and provides a journal for rollback.
*/

#ifndef _PAGER_H_
#define _PAGER_H_

/*
** Default maximum size for persistent journal files. A negative 
** value means no limit. This value may be overridden using the 
** sqlite4PagerJournalSizeLimit() API. See also "PRAGMA journal_size_limit".
*/
#ifndef SQLITE_DEFAULT_JOURNAL_SIZE_LIMIT
  #define SQLITE_DEFAULT_JOURNAL_SIZE_LIMIT -1
#endif

/*
** The type used to represent a page number.  The first page in a file
** is called page 1.  0 is used to represent "not a page".
*/
typedef u32 Pgno;

/*
** Each open file is managed by a separate instance of the "Pager" structure.
*/
typedef struct Pager Pager;

/*
** Handle type for pages.
*/
typedef struct PgHdr DbPage;

/*
** Page number PAGER_MJ_PGNO is never used in an SQLite database (it is
** reserved for working around a windows/posix incompatibility). It is
** used in the journal to signify that the remainder of the journal file 
** is devoted to storing a master journal name - there are no more pages to
** roll back. See comments for function writeMasterJournal() in pager.c 
** for details.
*/
#define PAGER_MJ_PGNO(x) ((Pgno)((PENDING_BYTE/((x)->pageSize))+1))

/*
** Allowed values for the flags parameter to sqlite4PagerOpen().
**
** NOTE: These values must match the corresponding BTREE_ values in btree.h.
*/
#define PAGER_OMIT_JOURNAL  0x0001    /* Do not use a rollback journal */
#define PAGER_MEMORY        0x0002    /* In-memory database */

/*
** Valid values for the second argument to sqlite4PagerLockingMode().
*/
#define PAGER_LOCKINGMODE_QUERY      -1
#define PAGER_LOCKINGMODE_NORMAL      0
#define PAGER_LOCKINGMODE_EXCLUSIVE   1

/*
** Numeric constants that encode the journalmode.  
*/
#define PAGER_JOURNALMODE_QUERY     (-1)  /* Query the value of journalmode */
#define PAGER_JOURNALMODE_DELETE      0   /* Commit by deleting journal file */
#define PAGER_JOURNALMODE_PERSIST     1   /* Commit by zeroing journal header */
#define PAGER_JOURNALMODE_OFF         2   /* Journal omitted.  */
#define PAGER_JOURNALMODE_TRUNCATE    3   /* Commit by truncating journal */
#define PAGER_JOURNALMODE_MEMORY      4   /* In-memory journal file */
#define PAGER_JOURNALMODE_WAL         5   /* Use write-ahead logging */

/*
** The remainder of this file contains the declarations of the functions
** that make up the Pager sub-system API. See source code comments for 
** a detailed description of each routine.
*/

/* Open and close a Pager connection. */ 
int sqlite4PagerOpen(
  sqlite4_vfs*,
  Pager **ppPager,
  const char*,
  int,
  int,
  int,
  void(*)(DbPage*)
);
int sqlite4PagerClose(Pager *pPager);
int sqlite4PagerReadFileheader(Pager*, int, unsigned char*);

/* Functions used to configure a Pager object. */
void sqlite4PagerSetBusyhandler(Pager*, int(*)(void *), void *);
int sqlite4PagerSetPagesize(Pager*, u32*, int);
int sqlite4PagerMaxPageCount(Pager*, int);
void sqlite4PagerSetCachesize(Pager*, int);
void sqlite4PagerShrink(Pager*);
void sqlite4PagerSetSafetyLevel(Pager*,int,int,int);
int sqlite4PagerLockingMode(Pager *, int);
int sqlite4PagerSetJournalMode(Pager *, int);
int sqlite4PagerGetJournalMode(Pager*);
int sqlite4PagerOkToChangeJournalMode(Pager*);
i64 sqlite4PagerJournalSizeLimit(Pager *, i64);
sqlite4_backup **sqlite4PagerBackupPtr(Pager*);

/* Functions used to obtain and release page references. */ 
int sqlite4PagerAcquire(Pager *pPager, Pgno pgno, DbPage **ppPage, int clrFlag);
#define sqlite4PagerGet(A,B,C) sqlite4PagerAcquire(A,B,C,0)
DbPage *sqlite4PagerLookup(Pager *pPager, Pgno pgno);
void sqlite4PagerRef(DbPage*);
void sqlite4PagerUnref(DbPage*);

/* Operations on page references. */
int sqlite4PagerWrite(DbPage*);
void sqlite4PagerDontWrite(DbPage*);
int sqlite4PagerMovepage(Pager*,DbPage*,Pgno,int);
int sqlite4PagerPageRefcount(DbPage*);
void *sqlite4PagerGetData(DbPage *); 
void *sqlite4PagerGetExtra(DbPage *); 

/* Functions used to manage pager transactions and savepoints. */
void sqlite4PagerPagecount(Pager*, int*);
int sqlite4PagerBegin(Pager*, int exFlag, int);
int sqlite4PagerCommitPhaseOne(Pager*,const char *zMaster, int);
int sqlite4PagerExclusiveLock(Pager*);
int sqlite4PagerSync(Pager *pPager);
int sqlite4PagerCommitPhaseTwo(Pager*);
int sqlite4PagerRollback(Pager*);
int sqlite4PagerOpenSavepoint(Pager *pPager, int n);
int sqlite4PagerSavepoint(Pager *pPager, int op, int iSavepoint);
int sqlite4PagerSharedLock(Pager *pPager);

int sqlite4PagerCheckpoint(Pager *pPager, int, int*, int*);
int sqlite4PagerWalSupported(Pager *pPager);
int sqlite4PagerWalCallback(Pager *pPager);
int sqlite4PagerOpenWal(Pager *pPager, int *pisOpen);
int sqlite4PagerCloseWal(Pager *pPager);

/* Functions used to query pager state and configuration. */
u8 sqlite4PagerIsreadonly(Pager*);
int sqlite4PagerRefcount(Pager*);
int sqlite4PagerMemUsed(Pager*);
const char *sqlite4PagerFilename(Pager*);
const sqlite4_vfs *sqlite4PagerVfs(Pager*);
sqlite4_file *sqlite4PagerFile(Pager*);
const char *sqlite4PagerJournalname(Pager*);
int sqlite4PagerNosync(Pager*);
void *sqlite4PagerTempSpace(Pager*);
int sqlite4PagerIsMemdb(Pager*);
void sqlite4PagerCacheStat(Pager *, int, int, int *);
void sqlite4PagerClearCache(Pager *);

/* Functions used to truncate the database file. */
void sqlite4PagerTruncateImage(Pager*,Pgno);

/* Functions to support testing and debugging. */
#if !defined(NDEBUG) || defined(SQLITE_TEST)
  Pgno sqlite4PagerPagenumber(DbPage*);
  int sqlite4PagerIswriteable(DbPage*);
#endif
#ifdef SQLITE_TEST
  int *sqlite4PagerStats(Pager*);
  void sqlite4PagerRefdump(Pager*);
  void disable_simulated_io_errors(void);
  void enable_simulated_io_errors(void);
#else
# define disable_simulated_io_errors()
# define enable_simulated_io_errors()
#endif

#endif /* _PAGER_H_ */
