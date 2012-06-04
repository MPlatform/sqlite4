/*
** 2011-08-26
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
** DATABASE FILE FORMAT
**
** A database file is divided into pages. The first 8KB of the file consists
** of two 4KB meta-pages. The meta-page size is not configurable. The 
** remainder of the file is made up of database pages. The default database
** page size is 4KB. Database pages are aligned to page-size boundaries,
** so if the database page size is larger than 8KB there is a gap between
** the end of the meta pages and the start of the database pages.
**
** Database pages are numbered based on their position in the file. Page N
** begins at byte offset ((N-1)*pgsz). This means that page 1 does not 
** exist - since it would always overlap with the meta pages. If the 
** page-size is (say) 512 bytes, then the first usable page in the database
** is page 33.
**
** The database file is also divided into blocks. The default block size is
** 2MB. When writing to the database file, an attempt is made to write data
** in contiguous block-sized chunks.
**
**
** TWO FILE DATABASE FORMAT:
**
** The database is stored in two files - the db file and the log file. 
** Sometimes, the log file is zero bytes in size or completely absent.
**
** The database file is broken into pages and blocks. Data is read a
** page at a time. When the database is written, at attempt is made to
** write at least one block sequentially before moving the disk head to
** write elsewhere. Plausible example sizes are 4KB for pages and 2MB 
** for blocks.
**
** Any page in the database file may be identified by its absolute 
** page number. FC pointers are absolute page numbers.
**
** Space within the file is allocated and freed on a per-block basis.
** Once an existing block is free it is eligible for reuse.
**
** Say, for now, that a checkpoint is always stored in the log file. 
** Either at the start of it or the end. Only sorted runs are stored in 
** the database file.
**
**
** BLOCK ALLOCATION AND TRACKING
**
** Each block is allocated to a single sorted run. All blocks are freed
** when the sorted run is no longer required.
**
** There are two problems with this:
**
**   * It means each short separator only run will consume a full 2MB 
**     on disk. No good at all for small databases. And there is the problem
**     of the short separator only runs sometimes created. Deal with this
**     one later - after making the changes to use partially merged runs
**     in queries.
**
**   * When a very large sorted run is finally discarded, we need to find all
**     of its blocks to free them. 
**
** A 1GB database contains 500 2MB blocks. A 1TB database contains 500,000
** blocks. If LSM uses 4 bytes for each block for space management, that is
** 4KB or 4MB respectively. Both are larger than we would like, but will
** do for the moment. So, to track blocks, there is an array of 32-bit 
** unsigned integers. One integer per block in the database file. When a 
** block is free, the array entry is set to 0. Otherwise, it is set to the
** id number of the sorted run of which it is a part.
**
** TODO: The data-structure used to track blocks can be optimized for space
** later on. Once partially complete merges can be linked into the main tree,
** it will be possible to release blocks as soon as their contents are 
** merged. Which means that all blocks in a single run could be linked 
** together in a list for tracking purposes, rather than having an external
** data structure.
**
**
** META PAGES.
**
** Meta pages are used to store checkpoint blobs and the block free-list.
** Whether or not a page is a meta page depends on its location in the 
** database file. As follows:
**
**   * The file begins with two meta pages, each 4KB in size.
**
**   * TODO: By allocating an entire block for meta data once a 
**           checkpoint blob is larger than 4KB in size? Probably...
**
** It is assumed that the first two meta pages and the data that follows
** them are located on different disk sectors. So that if a power failure 
** while writing to a meta page there is no risk of damage to the other
** meta page or any other part of the database file.
*/
#include "lsmInt.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define FS_MAX_PHANTOM_PAGES 32

typedef struct PhantomRun PhantomRun;

/* 
** A phantom-run under construction.
*/
struct PhantomRun {
  SortedRun *pRun;                /* Accompanying SortedRun object */
  int nPhantom;                   /* Number of pages in run */
  int bRunFinished;               /* True if the associated run is finished */
  Page *pFirst;                   /* First page in phantom run */
  Page *pLast;                    /* Current last page in phantom run */
};

struct FileSystem {
  lsm_db *pDb;
  lsm_env *pEnv;

  char *zDb;                      /* Database file name */
  int nMetasize;                  /* Size of meta pages in bytes */
  int nPagesize;                  /* Database page-size in bytes */
  int nBlocksize;                 /* Database block-size in bytes */

  /* r/w file descriptors for both files */
  lsm_file *fdDb;                 /* Database file */
  lsm_file *fdLog;                /* Log file */

  PhantomRun *pPhantom;           /* Phantom run currently being constructed */

  /* Statistics */
  int nWrite;                     /* Total number of pages written */
  int nRead;                      /* Total number of pages read */

  /* Page cache */
  int nOut;                       /* Number of outstanding pages */
  int nCacheMax;                  /* Configured cache size (in pages) */
  int nCacheAlloc;                /* Current cache size (in pages) */
  Page *pLruFirst;                /* Head of the LRU list */
  Page *pLruLast;                 /* Tail of the LRU list */
  int nHash;                      /* Number of hash slots in hash table */
  Page **apHash;                  /* nHash Hash slots */

  /* All meta pages are stored in the following singly linked-list. The
  ** list is connected using the Page->pHashNext pointers. */
  Page *pMeta;
};

struct Page {
  u8 *aData;                      /* Buffer containing page data */

  int iPg;                        /* Page number */
  int eType;

  int nRef;                       /* Number of outstanding references */
  int flags;                      /* Combination of PAGE_XXX flags */

  Page *pHashNext;                /* Next page in hash table slot */
  Page *pLruNext;                 /* Next page in LRU list */
  Page *pLruPrev;                 /* Previous page in LRU list */
  FileSystem *pFS;                /* File system that owns this page */
};

/* 
** Values for LsmPage.flags 
*/
#define PAGE_DIRTY 0x00000001

/*
** Values for Page.eType.
*/
#define LSM_CKPT_FILE 1
#define LSM_DB_FILE 3

/*
** Number of pgsz byte pages omitted from the start of block 1.
*/
#define BLOCK1_HDR_SIZE(pgsz)  MAX(1, 8192/(pgsz))

#define isPhantom(pFS, pSorted) (                                    \
    (pFS)->pPhantom && (pFS)->pPhantom->pRun==(pSorted)              \
)


/*
** Wrappers around the VFS methods of the lsm_env object.
*/
static int lsmEnvOpen(lsm_env *pEnv, const char *zFile, lsm_file **ppNew){
  return pEnv->xOpen(pEnv, zFile, ppNew);
}
static int lsmEnvRead(
  lsm_env *pEnv, 
  lsm_file *pFile, 
  lsm_i64 iOff, 
  void *pRead, 
  int nRead
){
  return pEnv->xRead(pFile, iOff, pRead, nRead);
}
static int lsmEnvWrite(
  lsm_env *pEnv, 
  lsm_file *pFile, 
  lsm_i64 iOff, 
  void *pWrite, 
  int nWrite
){
  return pEnv->xWrite(pFile, iOff, pWrite, nWrite);
}
static int lsmEnvSync(lsm_env *pEnv, lsm_file *pFile){
  return pEnv->xSync(pFile);
}
static int lsmEnvClose(lsm_env *pEnv, lsm_file *pFile){
  return pEnv->xClose(pFile);
}
static int lsmEnvTruncate(lsm_env *pEnv, lsm_file *pFile, lsm_i64 nByte){
  return pEnv->xTruncate(pFile, nByte);
}

/*
** Functions to read, write, sync and truncate the log file.
*/
int lsmFsWriteLog(FileSystem *pFS, i64 iOff, LsmString *pStr){
  return lsmEnvWrite(pFS->pEnv, pFS->fdLog, iOff, pStr->z, pStr->n);
}

int lsmFsSyncLog(FileSystem *pFS){
  return lsmEnvSync(pFS->pEnv, pFS->fdLog);
}

int lsmFsReadLog(FileSystem *pFS, i64 iOff, int nRead, LsmString *pStr){
  int rc;

  rc = lsmStringExtend(pStr, nRead);
  if( rc==LSM_OK ){
    rc = lsmEnvRead(pFS->pEnv, pFS->fdLog, iOff, &pStr->z[pStr->n], nRead);
    pStr->n += nRead;
  }

  return rc;
}

int lsmFsTruncateLog(FileSystem *pFS, i64 nByte){
  if( pFS->fdLog==0 ) return LSM_OK;
  return lsmEnvTruncate(pFS->pEnv, pFS->fdLog, nByte);
}

/*
** Given that there are currently nHash slots in the hash table, return 
** the hash key for file iFile, page iPg.
*/
static int fsHashKey(int nHash, int iPg){
  return (iPg % nHash);
}

/*
** This is a helper function for lsmFsOpen(). It opens a single file on
** disk (either the database or log file).
*/
static lsm_file *fsOpenFile(
  FileSystem *pFS,                /* File system object */
  int bLog,                       /* True for log, false for db */
  int *pRc                        /* IN/OUT: Error code */
){
  lsm_file *pFile = 0;
  if( *pRc==LSM_OK ){
    char *zName;
    zName = lsmMallocPrintf(pFS->pEnv, "%s%s", pFS->zDb, (bLog ? "-log" : ""));
    if( !zName ){
      *pRc = LSM_NOMEM;
    }else{
      *pRc = lsmEnvOpen(pFS->pEnv, zName, &pFile);
    }
    lsmFree(pFS->pEnv, zName);
  }
  return pFile;
}

/*
** Open a connection to a database stored within the file-system (the
** "system of files").
*/
int lsmFsOpen(lsm_db *pDb, const char *zDb, int nPgsz){
  FileSystem *pFS;
  int rc = LSM_OK;

  assert( pDb->pFS==0 );
  assert( pDb->pWorker==0 && pDb->pClient==0 );

  pFS = (FileSystem *)lsmMallocZeroRc(pDb->pEnv, sizeof(FileSystem), &rc);
  if( pFS ){
    pFS->nPagesize = nPgsz;
    pFS->nMetasize = 4 * 1024;
    pFS->nBlocksize = 2 * 1024 * 1024;
    pFS->pDb = pDb;
    pFS->pEnv = pDb->pEnv;

    /* Make a copy of the database name. */
    pFS->zDb = lsmMallocStrdup(pDb->pEnv, zDb);
    if( pFS->zDb==0 ) rc = LSM_NOMEM;

    /* Allocate the hash-table here. At some point, it should be changed
    ** so that it can grow dynamicly. */
    pFS->nCacheMax = 2048;
    pFS->nHash = 4096;
    pFS->apHash = lsmMallocZeroRc(pDb->pEnv, sizeof(Page *) * pFS->nHash, &rc);

    /* Open the files */
    pFS->fdDb = fsOpenFile(pFS, 0, &rc);
    pFS->fdLog = fsOpenFile(pFS, 1, &rc);

    if( rc!=LSM_OK ){
      lsmFsClose(pFS);
      pFS = 0;
    }
  }

  pDb->pFS = pFS;
  return rc;
}

/*
** Close and destroy a FileSystem object.
*/
void lsmFsClose(FileSystem *pFS){
  if( pFS ){
    Page *pPg;
    lsm_env *pEnv = pFS->pEnv;

    assert( pFS->nOut==0 );

    pPg = pFS->pLruFirst;
    while( pPg ){
      Page *pNext = pPg->pLruNext;
      lsmFree(pEnv, pPg->aData);
      lsmFree(pEnv, pPg);
      pPg = pNext;
    }

    pPg = pFS->pMeta;
    while( pPg ){
      Page *pNext = pPg->pHashNext;
      assert( pPg->nRef==0 );
      lsmFree(pEnv, pPg->aData);
      lsmFree(pEnv, pPg);
      pPg = pNext;
    }

    if( pFS->fdDb ) lsmEnvClose(pFS->pEnv, pFS->fdDb );
    if( pFS->fdLog ) lsmEnvClose(pFS->pEnv, pFS->fdLog );

    lsmFree(pEnv, pFS->zDb);
    lsmFree(pEnv, pFS->apHash);
    lsmFree(pEnv, pFS);
  }
}

/*
** Return the nominal page-size used by this file-system. Actual pages
** may be smaller or larger than this value.
*/
int lsmFsPageSize(FileSystem *pFS){
  return pFS->nPagesize;
}

/*
** Return the block-size used by this file-system.
*/
int lsmFsBlockSize(FileSystem *pFS){
  return pFS->nBlocksize;
}

/*
** Configure the nominal page-size used by this file-system. Actual 
** pages may be smaller or larger than this value.
*/
void lsmFsSetPageSize(FileSystem *pFS, int nPgsz){
  pFS->nPagesize = nPgsz;
}

/*
** Configure the block-size used by this file-system. Actual pages may be 
** smaller or larger than this value.
*/
void lsmFsSetBlockSize(FileSystem *pFS, int nBlocksize){
  pFS->nBlocksize = nBlocksize;
}

static Pgno fsFirstPageOnBlock(FileSystem *pFS, int iBlock){
  const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
  int iPg;
  if( iBlock==1 ){
    iPg = 1 + ((pFS->nMetasize*2 + pFS->nPagesize - 1) / pFS->nPagesize);
  }else{
    iPg = 1 + (iBlock-1) * nPagePerBlock;
  }
  return iPg;
}

static Pgno fsLastPageOnBlock(FileSystem *pFS, int iBlock){
  const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
  return iBlock * nPagePerBlock;
}

static int fsIsLast(FileSystem *pFS, Pgno iPg){
  const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
  return ( iPg && (iPg % nPagePerBlock)==0 );
}

static int fsIsFirst(FileSystem *pFS, Pgno iPg){
  const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
  return (
      (iPg % nPagePerBlock)==1
   || (iPg<nPagePerBlock && iPg==fsFirstPageOnBlock(pFS, 1))
  );
}


/*
** Given a page reference, return a pointer to the in-memory buffer of the
** pages contents. If parameter pnData is not NULL, set *pnData to the size
** of the buffer in bytes before returning.
*/
u8 *lsmFsPageData(Page *pPage, int *pnData){
  if( pnData ){
    FileSystem *pFS = pPage->pFS;
    switch( pPage->eType ){
      case LSM_CKPT_FILE:
        *pnData = pFS->nMetasize;
        break;
      default: {
        /* If this page is the first or last of its block, then it is 4
        ** bytes smaller than the usual pFS->nPagesize bytes. 
        */
        int nReserve = 0;
        assert( pPage->eType==LSM_DB_FILE );
        if( fsIsFirst(pFS, pPage->iPg) || fsIsLast(pFS, pPage->iPg) ){
          nReserve = 4;
        }
        *pnData = pFS->nPagesize - nReserve;
        break;
      }
    }
  }
  return pPage->aData;
}

/*
** Return the page number of a page.
*/
Pgno lsmFsPageNumber(Page *pPage){
  return pPage ? pPage->iPg : 0;
}

/*
** Page pPg is currently part of the LRU list belonging to pFS. Remove
** it from the list. pPg->pLruNext and pPg->pLruPrev are cleared by this
** operation.
*/
static void fsPageRemoveFromLru(FileSystem *pFS, Page *pPg){
  assert( pPg->pLruNext || pPg==pFS->pLruLast );
  assert( pPg->pLruPrev || pPg==pFS->pLruFirst );
  if( pPg->pLruNext ){
    pPg->pLruNext->pLruPrev = pPg->pLruPrev;
  }else{
    pFS->pLruLast = pPg->pLruPrev;
  }
  if( pPg->pLruPrev ){
    pPg->pLruPrev->pLruNext = pPg->pLruNext;
  }else{
    pFS->pLruFirst = pPg->pLruNext;
  }
  pPg->pLruPrev = 0;
  pPg->pLruNext = 0;
}

static void fsPageRemoveFromHash(FileSystem *pFS, Page *pPg){
  int iHash;
  Page **pp;

  iHash = fsHashKey(pFS->nHash, pPg->iPg);
  for(pp=&pFS->apHash[iHash]; *pp!=pPg; pp=&(*pp)->pHashNext);
  *pp = pPg->pHashNext;
}

static int fsPageBuffer(FileSystem *pFS, int bMeta, Page **ppOut){
  int rc = LSM_OK;
  Page *pPage = 0;
  if( pFS->pLruFirst==0 || pFS->nCacheAlloc<pFS->nCacheMax ){
    pPage = lsmMallocZero(pFS->pEnv, sizeof(Page));
    if( !pPage ){
      rc = LSM_NOMEM_BKPT;
    }else{
      int nByte;
      nByte = (bMeta ? pFS->nMetasize : pFS->nPagesize);
      pPage->aData = (u8 *)lsmMalloc(pFS->pEnv, nByte);
      if( !pPage->aData ){
        lsmFree(pFS->pEnv, pPage);
        rc = LSM_NOMEM_BKPT;
        pPage = 0;
      }
      pFS->nCacheAlloc++;
    }
  }else{
    pPage = pFS->pLruFirst;
    fsPageRemoveFromLru(pFS, pPage);
    fsPageRemoveFromHash(pFS, pPage);
  }

  *ppOut = pPage;
  return rc;
}

static void fsPageBufferFree(Page *pPg){
  lsmFree(pPg->pFS->pEnv, pPg->aData);
  lsmFree(pPg->pFS->pEnv, pPg);
}

Pgno lsmFsSortedRoot(SortedRun *p){
  return (p ? p->iRoot : 0);
}

void lsmFsSortedSetRoot(SortedRun *p, Pgno iRoot){
  p->iRoot = iRoot;
}

/*
** Return the number of pages in sorted file iFile.
*/
int lsmFsSortedSize(SortedRun *p){
  return p->nSize;
}

/*
** Return the page number of the first page in the sorted run iFile.
*/
int lsmFsFirstPgno(SortedRun *p){
  return p->iFirst;
}

/*
** Return the page number of the last page in the sorted run iFile.
*/
int lsmFsLastPgno(SortedRun *p){
  return p->iLast;
}

static i64 fsMetaOffset(
  FileSystem *pFS,                /* File-system object */
  int iMeta                       /* Meta page to return offset of */
){
  i64 iOff;                       /* Return value */
  assert( iMeta==1 || iMeta==2 );
  iOff = (iMeta-1) * pFS->nMetasize;
  return iOff;
}

int fsPageToBlock(FileSystem *pFS, Pgno iPg){
  return 1 + ((iPg-1) / (pFS->nBlocksize / pFS->nPagesize));
}


/*
** Return a handle for a database page.
*/
int fsPageGet(
  FileSystem *pFS,                /* File-system handle */
  SortedRun *pRun,       
  Pgno iPg,                       /* Page id */
  int noContent,                  /* True to not load content from disk */
  Page **ppPg                     /* OUT: New page handle */
){
  Page *p;
  int iHash;
  int rc = LSM_OK;

  assert( iPg>=fsFirstPageOnBlock(pFS, 1) );
  assert( !isPhantom(pFS, pRun) );

  /* Search the hash-table for the page */
  iHash = fsHashKey(pFS->nHash, iPg);
  for(p=pFS->apHash[iHash]; p; p=p->pHashNext){
    if( p->iPg==iPg) break;
  }

  if( p==0 ){
    rc = fsPageBuffer(pFS, 0, &p);

    if( rc==LSM_OK ){
      p->iPg = iPg;
      p->nRef = 0;
      p->flags = 0;
      p->pLruNext = 0;
      p->pLruPrev = 0;
      p->pFS = pFS;
      p->eType = LSM_DB_FILE;

#ifdef LSM_DEBUG
      memset(p->aData, 0x56, pFS->nPagesize);
#endif
      if( noContent==0 ){
        int nByte = pFS->nPagesize;
        i64 iOff;

        iOff = (i64)(iPg-1) * pFS->nPagesize;
        rc = lsmEnvRead(pFS->pEnv, pFS->fdDb, iOff, p->aData, nByte);
        pFS->nRead++;
      }

      /* If the xRead() call was successful (or not attempted), link the
      ** page into the page-cache hash-table. Otherwise, if it failed,
      ** free the buffer. */
      if( rc==LSM_OK ){
        p->pHashNext = pFS->apHash[iHash];
        pFS->apHash[iHash] = p;
      }else{
        fsPageBufferFree(p);
        p = 0;
      }
    }
  }else if( p->nRef==0 ){
    fsPageRemoveFromLru(pFS, p);
  }

  assert( (rc==LSM_OK && p) || (rc!=LSM_OK && p==0) );
  if( rc==LSM_OK ){
    pFS->nOut += (p->nRef==0);
    p->nRef++;
  }
  *ppPg = p;
  return rc;
}

static int fsBlockNext(
  FileSystem *pFS, 
  int iBlock, 
  int *piNext
){
  const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
  Page *pLast;
  int rc;
  
  rc = fsPageGet(pFS, 0, iBlock*nPagePerBlock, 0, &pLast);
  if( rc==LSM_OK ){
    *piNext = fsPageToBlock(pFS, lsmGetU32(&pLast->aData[pFS->nPagesize-4]));
    lsmFsPageRelease(pLast);
  }
  return rc;
}

static int fsRunEndsBetween(
  SortedRun *pRun, 
  SortedRun *pIgnore, 
  int iFirst, 
  int iLast
){
  return (pRun!=pIgnore && (
        (pRun->iFirst>=iFirst && pRun->iFirst<=iLast)
     || (pRun->iLast>=iFirst && pRun->iLast<=iLast)
  ));
}

static int fsLevelEndsBetween(
  Level *pLevel, 
  SortedRun *pIgnore, 
  int iFirst, 
  int iLast
){
  int i;

  if( fsRunEndsBetween(&pLevel->lhs.run, pIgnore, iFirst, iLast)
   || fsRunEndsBetween(&pLevel->lhs.sep, pIgnore, iFirst, iLast)
  ){
    return 1;
  }
  for(i=0; i<pLevel->nRight; i++){
    if( fsRunEndsBetween(&pLevel->aRhs[i].run, pIgnore, iFirst, iLast)
     || fsRunEndsBetween(&pLevel->aRhs[i].sep, pIgnore, iFirst, iLast)
    ){
      return 1;
    }
  }

  return 0;
}

static int fsFreeBlock(
  FileSystem *pFS, 
  Snapshot *pSnapshot, 
  SortedRun *pIgnore,             /* Ignore this run when searching */
  int iBlk
){
  int rc = LSM_OK;                /* Return code */
  int iFirst;                     /* First page on block iBlk */
  int iLast;                      /* Last page on block iBlk */
  int i;                          /* Used to iterate through append points */
  Level *pLevel;                  /* Used to iterate through levels */

  Pgno *aAppend;
  int nAppend;

  iFirst = fsFirstPageOnBlock(pFS, iBlk);
  iLast = fsLastPageOnBlock(pFS, iBlk);

  /* Check if any other run in the snapshot has a start or end page 
  ** within this block. If there is such a run, return early. */
  for(pLevel=lsmDbSnapshotLevel(pSnapshot); pLevel; pLevel=pLevel->pNext){
    if( fsLevelEndsBetween(pLevel, pIgnore, iFirst, iLast) ){
      return LSM_OK;
    }
  }

  aAppend = lsmSharedAppendList(pFS->pDb, &nAppend);
  for(i=0; i<nAppend; i++){
    if( aAppend[i]>=iFirst && aAppend[i]<=iLast ){
      lsmSharedAppendListRemove(pFS->pDb, i);
      break;
    }
  }

  if( rc==LSM_OK ){
    rc = lsmBlockFree(pFS->pDb, iBlk);
  }
  return rc;
}

/*
** Delete or otherwise recycle the blocks currently occupied by run pDel.
*/
int lsmFsSortedDelete(
  FileSystem *pFS, 
  Snapshot *pSnapshot,
  int bZero,                      /* True to zero the SortedRun structure */
  SortedRun *pDel
){
  if( pDel->iFirst ){
    int rc = LSM_OK;

    int iBlk;
    int iLastBlk;

    iBlk = fsPageToBlock(pFS, pDel->iFirst);
    iLastBlk = fsPageToBlock(pFS, pDel->iLast);

    /* Mark all blocks currently used by this sorted run as free */
    while( iBlk && rc==LSM_OK ){
      int iNext = 0;
      if( iBlk!=iLastBlk ){
        rc = fsBlockNext(pFS, iBlk, &iNext);
      }else if( bZero==0 && pDel->iLast!=fsLastPageOnBlock(pFS, iLastBlk) ){
        break;
      }
      rc = fsFreeBlock(pFS, pSnapshot, pDel, iBlk);
      iBlk = iNext;
    }

    if( bZero ) memset(pDel, 0, sizeof(SortedRun));
  }
  return LSM_OK;
}

/*
** The pager reference passed as the only argument must refer to a sorted
** file page (not a log or meta page). This call indicates that the argument
** page is now the first page in its sorted file - all previous pages may
** be considered free.
*/
void lsmFsGobble(
  Snapshot *pSnapshot,
  SortedRun *pRun, 
  Page *pPg
){
  FileSystem *pFS = pPg->pFS;

  if( pPg->iPg!=pRun->iFirst ){
    int rc = LSM_OK;
    int iBlk = fsPageToBlock(pFS, pRun->iFirst);
    int iFirstBlk = fsPageToBlock(pFS, pPg->iPg);

    pRun->nSize += (pRun->iFirst - fsFirstPageOnBlock(pFS, iBlk));
    pRun->iFirst = pPg->iPg;
    while( rc==LSM_OK && iBlk!=iFirstBlk ){
      int iNext = 0;
      rc = fsBlockNext(pFS, iBlk, &iNext);
      if( rc==LSM_OK ) rc = fsFreeBlock(pFS, pSnapshot, 0, iBlk);
      pRun->nSize -= (
          1 + fsLastPageOnBlock(pFS, iBlk) - fsFirstPageOnBlock(pFS, iBlk)
      );
      iBlk = iNext;
    }

    pRun->nSize -= (pRun->iFirst - fsFirstPageOnBlock(pFS, iBlk));
    assert( pRun->nSize>0 );
  }
}


/*
** The first argument to this function is a valid reference to a database
** file page that is part of a segment. This function attempts to locate and
** load the next page in the same segment. If successful, *ppNext is set to 
** point to the next page handle and LSM_OK is returned.
**
** Or, if an error occurs, *ppNext is set to NULL and and lsm error code
** returned.
**
** Page references returned by this function should be released by the 
** caller using lsmFsPageRelease().
*/
int lsmFsDbPageNext(SortedRun *pRun, Page *pPg, int eDir, Page **ppNext){
  FileSystem *pFS = pPg->pFS;
  int iPg = pPg->iPg;

  assert( eDir==1 || eDir==-1 );

  if( eDir<0 ){
    if( pRun && iPg==pRun->iFirst ){
      *ppNext = 0;
      return LSM_OK;
    }else if( fsIsFirst(pFS, iPg) ){
      iPg = lsmGetU32(&pPg->aData[pFS->nPagesize-4]);
    }else{
      iPg--;
    }
  }else{
    if( pRun && iPg==pRun->iLast ){
      *ppNext = 0;
      return LSM_OK;
    }else if( fsIsLast(pFS, iPg) ){
      iPg = lsmGetU32(&pPg->aData[pFS->nPagesize-4]);
    }else{
      iPg++;
    }
  }

  return fsPageGet(pFS, pRun, iPg, 0, ppNext);
}

static Pgno findAppendPoint(FileSystem *pFS, int nMin){
  Pgno ret = 0;
  Pgno *aAppend;
  int nAppend;
  int i;

  aAppend = lsmSharedAppendList(pFS->pDb, &nAppend);
  for(i=0; i<nAppend; i++){
    Pgno iLastOnBlock;
    iLastOnBlock = fsLastPageOnBlock(pFS, fsPageToBlock(pFS, aAppend[i]));
    if( (iLastOnBlock - aAppend[i])>=nMin ){
      ret = aAppend[i];
      lsmSharedAppendListRemove(pFS->pDb, i);
      break;
    }
  }

  return ret;
}

static void addAppendPoint(
  lsm_db *db, 
  Pgno iLast,
  int *pRc                        /* IN/OUT: Error code */
){
  if( *pRc==LSM_OK && iLast>0 ){
    FileSystem *pFS = db->pFS;

    Pgno *aPoint;
    int nPoint;
    int i;
    int iBlk;
    int bLast;

    iBlk = fsPageToBlock(pFS, iLast);
    bLast = (iLast==fsLastPageOnBlock(pFS, iBlk));

    aPoint = lsmSharedAppendList(db, &nPoint);
    for(i=0; i<nPoint; i++){
      if( iBlk==fsPageToBlock(pFS, aPoint[i]) ){
        if( bLast ){
          lsmSharedAppendListRemove(db, i);
        }else if( iLast>=aPoint[i] ){
          aPoint[i] = iLast+1;
        }
        return;
      }
    }

    if( bLast==0 ){
      *pRc = lsmSharedAppendListAdd(db, iLast+1);
    }
  }
}

static void subAppendPoint(lsm_db *db, Pgno iFirst){
  if( iFirst>0 ){
    FileSystem *pFS = db->pFS;
    Pgno *aPoint;
    int nPoint;
    int i;
    int iBlk;

    iBlk = fsPageToBlock(pFS, iFirst);
    aPoint = lsmSharedAppendList(db, &nPoint);
    for(i=0; i<nPoint; i++){
      if( iBlk==fsPageToBlock(pFS, aPoint[i]) ){
        if( iFirst>=aPoint[i] ) lsmSharedAppendListRemove(db, i);
        return;
      }
    }
  }
}

int lsmFsSetupAppendList(lsm_db *db){
  int rc = LSM_OK;
  Level *pLvl;

  assert( db->pWorker );
  for(pLvl=lsmDbSnapshotLevel(db->pWorker); 
      rc==LSM_OK && pLvl; 
      pLvl=pLvl->pNext
  ){
    if( pLvl->nRight==0 ){
      addAppendPoint(db, pLvl->lhs.sep.iLast, &rc);
      addAppendPoint(db, pLvl->lhs.run.iLast, &rc);
    }else{
      int i;
      for(i=0; i<pLvl->nRight; i++){
        addAppendPoint(db, pLvl->aRhs[i].sep.iLast, &rc);
        addAppendPoint(db, pLvl->aRhs[i].run.iLast, &rc);
      }
    }
  }

  for(pLvl=lsmDbSnapshotLevel(db->pWorker); pLvl; pLvl=pLvl->pNext){
    int i;
    subAppendPoint(db, pLvl->lhs.sep.iFirst);
    subAppendPoint(db, pLvl->lhs.run.iFirst);
    for(i=0; i<pLvl->nRight; i++){
      subAppendPoint(db, pLvl->aRhs[i].sep.iFirst);
      subAppendPoint(db, pLvl->aRhs[i].run.iFirst);
    }
  }

  return rc;
}

int lsmFsPhantomMaterialize(
  FileSystem *pFS, 
  Snapshot *pSnapshot, 
  SortedRun *p
){
  int rc = LSM_OK;
  if( isPhantom(pFS, p) ){
    PhantomRun *pPhantom = pFS->pPhantom;
    Page *pPg;
    Page *pNext;
    int i;
    Pgno iFirst = 0;

    /* Search for an existing run in the database that this run can be
    ** appended to. See comments surrounding findAppendPoint() for details. */
    iFirst = findAppendPoint(pFS, pPhantom->nPhantom);

    /* If the array can not be written into any partially used block, 
    ** allocate a new block. The first page of the materialized run will
    ** be the second page of the new block (since the first is undersized
    ** and can not be used).  */
    if( iFirst==0 ){
      int iNew;                   /* New block */
      lsmBlockAllocate(pFS->pDb, &iNew);
      iFirst = fsFirstPageOnBlock(pFS, iNew) + 1;
    }

    pFS->pPhantom = 0;
    p->iFirst = iFirst;
    p->iLast = iFirst + pPhantom->nPhantom - 1;
    assert( 0==fsIsFirst(pFS, p->iFirst) && 0==fsIsLast(pFS, p->iFirst) );
    assert( 0==fsIsFirst(pFS, p->iLast) && 0==fsIsLast(pFS, p->iLast) );
    assert( fsPageToBlock(pFS, p->iFirst)==fsPageToBlock(pFS, p->iLast) );

    i = iFirst;
    for(pPg=pPhantom->pFirst; pPg; pPg=pNext){
      int iHash;
      pNext = pPg->pHashNext;
      pPg->iPg = i++;
      pPg->nRef++;

      iHash = fsHashKey(pFS->nHash, pPg->iPg);
      pPg->pHashNext = pFS->apHash[iHash];
      pFS->apHash[iHash] = pPg;
      pFS->nOut++;
      lsmFsPageRelease(pPg);
    }
    assert( i==p->iLast+1 );

    p->nSize = pPhantom->nPhantom;
    lsmFree(pFS->pEnv, pPhantom);
  }
  return rc;
}

int lsmFsDbPageEnd(FileSystem *pFS, SortedRun *pRun, int bLast, Page **ppEnd){
  int iPg;
  int rc;

  assert( isPhantom(pFS, pRun)==0 );

  iPg = (bLast ? pRun->iLast : pRun->iFirst);
  *ppEnd = 0;
  if( iPg==0 ){
    rc = LSM_OK;
  }else{
    rc = fsPageGet(pFS, pRun, iPg, 0, ppEnd);
  }

  return rc;
}

/*
** Append a page to file iFile. Return a reference to it. lsmFsPageWrite()
** has already been called on the returned reference.
*/
int lsmFsSortedAppend(
  FileSystem *pFS, 
  Snapshot *pSnapshot,
  SortedRun *p, 
  Page **ppOut
){
  int rc = LSM_OK;
  Page *pPg = 0;
  PhantomRun *pPhantom = 0;
  *ppOut = 0;

  if( isPhantom(pFS, p) ){
    pPhantom = pFS->pPhantom;
  }

  if( pPhantom ){
    const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
    int nLimit = (nPagePerBlock - 2 - (fsFirstPageOnBlock(pFS, 1)-1) );

    if( pPhantom->nPhantom>=nLimit ){ 
      rc = lsmFsPhantomMaterialize(pFS, pSnapshot, p);
      if( rc!=LSM_OK ){
        return rc;
      }
      pPhantom = 0;
    }
  }

  if( pPhantom ){
    rc = fsPageBuffer(pFS, 0, &pPg);
    if( rc==LSM_OK ){
      pPg->iPg = 0;
      pPg->eType = LSM_DB_FILE;
      pPg->nRef = 1;
      pPg->flags = PAGE_DIRTY;
      pPg->pHashNext = 0;
      pPg->pLruNext = 0;
      pPg->pLruPrev = 0;
      pPg->pFS = pFS;
      if( pPhantom->pFirst ){
        assert( pPhantom->pLast );
        pPhantom->pLast->pHashNext = pPg;
      }else{
        pPhantom->pFirst = pPg;
      }
      pPhantom->pLast = pPg;
      pPhantom->nPhantom++;
    }
  }else{
    int iApp = 0;
    int iNext = 0;
    int iPrev = p->iLast;

    if( iPrev==0 ){
      iApp = findAppendPoint(pFS, 0);
    }else if( fsIsLast(pFS, iPrev) ){
      Page *pLast = 0;
      rc = fsPageGet(pFS, p, iPrev, 0, &pLast);
      if( rc!=LSM_OK ) return rc;
      iApp = lsmGetU32(&pLast->aData[pFS->nPagesize-4]);
      lsmFsPageRelease(pLast);
    }else{
      iApp = iPrev + 1;
    }

    /* If this is the first page allocated, or if the page allocated is the
     ** last in the block, allocate a new block here.  */
    if( iApp==0 || fsIsLast(pFS, iApp) ){
      int iNew;                     /* New block number */

      lsmBlockAllocate(pFS->pDb, &iNew);
      if( iApp==0 ){
        iApp = fsFirstPageOnBlock(pFS, iNew);
      }else{
        iNext = fsFirstPageOnBlock(pFS, iNew);
      }
    }

    /* Grab the new page. */
    pPg = 0;
    rc = fsPageGet(pFS, p, iApp, 1, &pPg);
    assert( rc==LSM_OK || pPg==0 );

    /* If this is the first or last page of a block, fill in the pointer 
     ** value at the end of the new page. */
    if( rc==LSM_OK ){
      p->nSize++;
      p->iLast = iApp;
      if( p->iFirst==0 ) p->iFirst = iApp;
      pPg->flags |= PAGE_DIRTY;

      if( fsIsLast(pFS, iApp) ){
        lsmPutU32(&pPg->aData[pFS->nPagesize-4], iNext);
      }else 
        if( fsIsFirst(pFS, iApp) ){
          lsmPutU32(&pPg->aData[pFS->nPagesize-4], iPrev);
        }
    }
  }

  *ppOut = pPg;
  return rc;
}

int lsmFsSortedFinish(FileSystem *pFS, Snapshot *pSnap, SortedRun *p){
  int rc = LSM_OK;
  if( p ){
    const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);

    if( pFS->pPhantom ) pFS->pPhantom->bRunFinished = 1;

    /* Check if the last page of this run happens to be the last of a block.
    ** If it is, then an extra block has already been allocated for this run.
    ** Shift this extra block back to the free-block list. 
    **
    ** Otherwise, add the first free page in the last block used by the run
    ** to the lAppend list.
    */
    if( (p->iLast % nPagePerBlock)==0 ){
      Page *pLast;
      rc = fsPageGet(pFS, p, p->iLast, 0, &pLast);
      if( rc==LSM_OK ){
        int iPg = (int)lsmGetU32(&pLast->aData[pFS->nPagesize-4]);
        int iBlk = fsPageToBlock(pFS, iPg);
        lsmBlockRefree(pFS->pDb, iBlk);
        lsmFsPageRelease(pLast);
      }
    }else{
      rc = lsmSharedAppendListAdd(pFS->pDb, p->iLast+1);
    }
  }
  return rc;
}

int lsmFsDbPageGet(FileSystem *pFS, SortedRun *p, int iLoad, Page **ppPg){
  assert( pFS );
  return fsPageGet(pFS, p, iLoad, 0, ppPg);
}

/*
** Return a reference to meta-page iPg. If successful, LSM_OK is returned
** and *ppPg populated with the new page reference. The reference should
** be released by the caller using lsmFsPageRelease().
**
** Otherwise, if an error occurs, *ppPg is set to NULL and an LSM error 
** code is returned.
*/
int lsmFsMetaPageGet(FileSystem *pFS, int iPg, Page **ppPg){
  int rc = LSM_OK;
  Page *pPg;
  assert( iPg==1 || iPg==2 );

  /* Search the linked list for the requested page. */
  for(pPg=pFS->pMeta; pPg && pPg->iPg!=iPg; pPg=pPg->pHashNext);

  /* If the page was not found, allocate it. */
  if( pPg==0 ){
    i64 iOff;

    pPg = lsmMallocZeroRc(pFS->pEnv, sizeof(Page), &rc);
    if( rc ) goto meta_page_out;
    pPg->aData = lsmMallocZeroRc(pFS->pEnv, pFS->nMetasize, &rc);
    if( rc ) goto meta_page_out;
    iOff = fsMetaOffset(pFS, iPg);
    rc = lsmEnvRead(pFS->pEnv, pFS->fdDb, iOff, pPg->aData, pFS->nMetasize);
    if( rc ) goto meta_page_out;

    pPg->iPg = iPg;
    pPg->eType = LSM_CKPT_FILE;
    pPg->pFS = pFS;
    pPg->pHashNext = pFS->pMeta;
    pFS->pMeta = pPg;
  }

meta_page_out:
  if( rc==LSM_OK ){
    pPg->nRef++;
  }else if( pPg ){
    lsmFree(pFS->pEnv, pPg->aData);
    lsmFree(pFS->pEnv, pPg);
    pPg = 0;
  }
  *ppPg = pPg;
  return rc;
}

/*
** Notify the file-system that the page needs to be written back to disk
** when the reference count next drops to zero.
*/
int lsmFsPageWrite(Page *pPg){
  pPg->flags |= PAGE_DIRTY;
  return LSM_OK;
}

int lsmFsPageWritable(Page *pPg){
  return (pPg->flags & PAGE_DIRTY) ? 1 : 0;
}


int lsmFsPagePersist(Page *pPg){
  int rc = LSM_OK;
  if( pPg && (pPg->flags & PAGE_DIRTY) ){
    FileSystem *pFS = pPg->pFS;
    int eType = pPg->eType;

    if( eType==LSM_CKPT_FILE ){
      i64 iOff;
      iOff = fsMetaOffset(pFS, pPg->iPg);
      rc = lsmEnvWrite(pFS->pEnv, pFS->fdDb, iOff, pPg->aData, pFS->nMetasize);
    }else{
      i64 iOff;                 /* Offset to write within database file */
      iOff = pFS->nPagesize * (pPg->iPg-1);
      rc = lsmEnvWrite(pFS->pEnv, pFS->fdDb, iOff, pPg->aData, pFS->nPagesize);
    }
    pPg->flags &= ~PAGE_DIRTY;
    pFS->nWrite++;
  }

  return rc;
}

/*
** Increment the reference count on the page object passed as the first
** argument.
*/
void lsmFsPageRef(Page *pPg){
  if( pPg ){
    pPg->nRef++;
  }
}

/*
** Release a page-reference obtained using fsPageGet().
*/
int lsmFsPageRelease(Page *pPg){
  int rc = LSM_OK;
  if( pPg ){
    assert( pPg->nRef>0 );
    pPg->nRef--;
    if( pPg->nRef==0 && pPg->iPg!=0 ){
      FileSystem *pFS = pPg->pFS;
      rc = lsmFsPagePersist(pPg);

      if( pPg->eType!=LSM_CKPT_FILE ){
        pFS->nOut--;
        assert( pPg->pLruNext==0 );
        assert( pPg->pLruPrev==0 );
#if 0
        pPg->pLruPrev = pFS->pLruLast;
        if( pPg->pLruPrev ){
          pPg->pLruPrev->pLruNext = pPg;
        }else{
          pFS->pLruFirst = pPg;
        }
        pFS->pLruLast = pPg;
#else
        fsPageRemoveFromHash(pFS, pPg);
        lsmFree(pFS->pEnv, pPg->aData);
        lsmFree(pFS->pEnv, pPg);
#endif
      }
    }
  }

  return rc;
}

/*
** Read only access to the number of pages read/written statistics.
*/
int lsmFsNRead(FileSystem *pFS){
  return pFS->nWrite;
}
int lsmFsNWrite(FileSystem *pFS){
  return pFS->nRead;
}

int lsmFsSortedPhantom(FileSystem *pFS, SortedRun *pRun){
  int rc = LSM_OK;
  PhantomRun *p;                  /* New phantom run object */

  p = (PhantomRun *)lsmMallocZeroRc(pFS->pEnv, sizeof(PhantomRun), &rc);
  if( p ){
    p->pRun = pRun;
  }
  assert( pFS->pPhantom==0 );
  pFS->pPhantom = p;
  return rc;
}

void lsmFsSortedPhantomFree(FileSystem *pFS){
  PhantomRun *p = pFS->pPhantom;
  if( p ){
    Page *pPg;
    Page *pNext;
    for(pPg=p->pFirst; pPg; pPg=pNext){
      pNext = pPg->pHashNext;
      fsPageBufferFree(pPg);
    }
    lsmFree(pFS->pEnv, p);
    pFS->pPhantom = 0;
  }
}

int lsmFsDbSync(FileSystem *pFS){
  return lsmEnvSync(pFS->pEnv, pFS->fdDb);
}

int lsmFsLogSync(FileSystem *pFS){
  return lsmEnvSync(pFS->pEnv, pFS->fdLog);
}

void lsmFsDumpBlockmap(lsm_db *pDb, SortedRun *p){
  if( p ){
    FileSystem *pFS = pDb->pFS;
    int iBlk;
    int iLastBlk;
    char *zMsg = 0;
    LsmString zBlk;

    lsmStringInit(&zBlk, pDb->pEnv);
    iBlk = fsPageToBlock(pFS, p->iFirst);
    iLastBlk = fsPageToBlock(pFS, p->iLast);

    while( iBlk ){
      lsmStringAppendf(&zBlk, " %d", iBlk);
      if( iBlk!=iLastBlk ){
        fsBlockNext(pFS, iBlk, &iBlk);
      }else{
        iBlk = 0;
      }
    }

    zMsg = lsmMallocPrintf(pDb->pEnv, "%d..%d: ", p->iFirst, p->iLast);
    lsmLogMessage(pDb, LSM_OK, "    % -15s %s", zMsg, zBlk.z);
    lsmFree(pDb->pEnv, zMsg);
    lsmStringClear(&zBlk);
  }
} 

lsm_env *lsmFsEnv(FileSystem *pFS) { return pFS->pEnv; }
lsm_env *lsmPageEnv(Page *pPg) { return pPg->pFS->pEnv; }

static SortedRun *startsWith(SortedRun *pRun, Pgno iFirst){
  return (iFirst==pRun->iFirst) ? pRun : 0;
}

/*
** Sector-size routine.
*/
int lsmFsSectorSize(FileSystem *pFS){
  return 512;
}

int lsmInfoArrayStructure(lsm_db *pDb, Pgno iFirst, char **pzOut){
  int rc = LSM_OK;
  Snapshot *pWorker;              /* Worker snapshot */
  Snapshot *pRelease = 0;         /* Snapshot to release */
  SortedRun *pArray = 0;          /* Array to report on */
  Level *pLvl;                    /* Used to iterate through db levels */

  if( iFirst==0 ) return LSM_ERROR;

  /* Obtain the worker snapshot */
  pWorker = pDb->pWorker;
  if( !pWorker ){
    pRelease = pWorker = lsmDbSnapshotWorker(pDb->pDatabase);
  }

  /* Search for the array that starts on page iFirst */
  for(pLvl=lsmDbSnapshotLevel(pWorker); pLvl && pArray==0; pLvl=pLvl->pNext){
    if( 0==(pArray = startsWith(&pLvl->lhs.sep, iFirst))
     && 0==(pArray = startsWith(&pLvl->lhs.run, iFirst))
    ){
      int i;
      for(i=0; i<pLvl->nRight; i++){
        if( (pArray = startsWith(&pLvl->aRhs[i].sep, iFirst)) ) break;
        if( (pArray = startsWith(&pLvl->aRhs[i].run, iFirst)) ) break;
      }
    }
  }

  if( pArray==0 ){
    /* Could not find the requested array. This is an error. */
    *pzOut = 0;
    rc = LSM_ERROR;
  }else{
    FileSystem *pFS = pDb->pFS;
    LsmString str;
    int iBlk;
    int iLastBlk;
   
    iBlk = fsPageToBlock(pFS, pArray->iFirst);
    iLastBlk = fsPageToBlock(pFS, pArray->iLast);

    lsmStringInit(&str, pDb->pEnv);
    lsmStringAppendf(&str, "%d", pArray->iFirst);
    while( iBlk!=iLastBlk ){
      lsmStringAppendf(&str, " %d", fsLastPageOnBlock(pFS, iBlk));
      fsBlockNext(pFS, iBlk, &iBlk);
      lsmStringAppendf(&str, " %d", fsFirstPageOnBlock(pFS, iBlk));
    }
    lsmStringAppendf(&str, " %d", pArray->iLast);

    *pzOut = str.z;
  }

  lsmDbSnapshotRelease(pRelease);
  return rc;
}

#ifdef LSM_EXPENSIVE_DEBUG
/*
** Helper function for lsmFsIntegrityCheck()
*/
static void checkBlocks(
  FileSystem *pFS, 
  Segment *pSeg, 
  int bExtra,
  u8 *aUsed
){
  if( pSeg ){
    int i;
    for(i=0; i<2; i++){
      SortedRun *p = (i ? pSeg->pRun : pSeg->pSep);

      if( p && p->nSize>0 ){
        const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);

        int iBlk;
        int iLastBlk;
        iBlk = fsPageToBlock(pFS, p->iFirst);
        iLastBlk = fsPageToBlock(pFS, p->iLast);

        while( iBlk ){
          assert( iBlk<=pFS->nBlock );
          /* assert( aUsed[iBlk-1]==0 ); */
          aUsed[iBlk-1] = 1;
          if( iBlk!=iLastBlk ){
            fsBlockNext(pFS, iBlk, &iBlk);
          }else{
            iBlk = 0;
          }
        }

        if( bExtra && (p->iLast % nPagePerBlock)==0 ){
          fsBlockNext(pFS, iLastBlk, &iBlk);
          aUsed[iBlk-1] = 1;
        }
      }
    }
  }
}

/*
** This function checks that all blocks in the database file are accounted
** for. For each block, exactly one of the following must be true:
**
**   + the block is part of a sorted run, or
**   + the block is on the lPending list, or
**   + the block is on the lFree list
**
** This function also checks that there are no references to blocks with
** out-of-range block numbers.
**
** If no errors are found, non-zero is returned. If an error is found, an
** assert() fails.
*/
int lsmFsIntegrityCheck(lsm_db *pDb){
  int nBlock;
  int i;
  FileSystem *pFS = pDb->pFS;
  u8 *aUsed;
  Level *pLevel;

  nBlock = pFS->nBlock;
  aUsed = lsmMallocZero(pDb->pEnv, nBlock);
  assert( aUsed );

  for(pLevel=pDb->pLevel; pLevel; pLevel=pLevel->pNext){
    int i;
    checkBlocks(pFS, &pLevel->lhs, (pLevel->pSMerger!=0), aUsed);

    for(i=0; i<pLevel->nRight; i++){
      checkBlocks(pFS, &pLevel->aRhs[i], 0, aUsed);
    }
  }

  for(i=0; i<pFS->lFree.n; i++){
    int iBlk = pFS->lFree.a[i];
    assert( aUsed[iBlk-1]==0 );
    aUsed[iBlk-1] = 1;
  }
  for(i=0; i<pFS->lPending.n; i++){
    int iBlk = pFS->lPending.a[i];
    assert( aUsed[iBlk-1]==0 );
    aUsed[iBlk-1] = 1;
  }

  for(i=0; i<nBlock; i++) assert( aUsed[i]==1 );

  lsmFree(pDb->pEnv, aUsed);
  return 1;
}
#endif
