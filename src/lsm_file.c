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
** The following database file format concepts are used by the code in
** this file to read and write the database file.
**
** Pages:
**
**   A database file is divided into pages. The first 8KB of the file consists
**   of two 4KB meta-pages. The meta-page size is not configurable. The 
**   remainder of the file is made up of database pages. The default database
**   page size is 4KB. Database pages are aligned to page-size boundaries,
**   so if the database page size is larger than 8KB there is a gap between
**   the end of the meta pages and the start of the database pages.
**
**   Database pages are numbered based on their position in the file. Page N
**   begins at byte offset ((N-1)*pgsz). This means that page 1 does not 
**   exist - since it would always overlap with the meta pages. If the 
**   page-size is (say) 512 bytes, then the first usable page in the database
**   is page 33.
**
**   It is assumed that the first two meta pages and the data that follows
**   them are located on different disk sectors. So that if a power failure 
**   while writing to a meta page there is no risk of damage to the other
**   meta page or any other part of the database file.
**
** Blocks:
**
**   The database file is also divided into blocks. The default block size is
**   2MB. When writing to the database file, an attempt is made to write data
**   in contiguous block-sized chunks.
**
**   The first and last page on each block are special in that they are 4 
**   bytes smaller than all other pages. This is because the last four bytes 
**   of space on the first and last pages of each block are reserved for a 
**   pointers to other blocks (i.e. a 32-bit block number).
**
** Runs:
**
**   A run is a sequence of pages that the upper layer uses to store a 
**   sorted array of database keys (and accompanying data - values, FC 
**   pointers and so on). Given a page within a run, it is possible to
**   navigate to the next page in the run as follows:
**
**     a) if the current page is not the last in a block, the next page 
**        in the run is located immediately after the current page, OR
**
**     b) if the current page is the last page in a block, the next page 
**        in the run is the first page on the block identified by the
**        block pointer stored in the last 4 bytes of the current block.
**
**   It is possible to navigate to the previous page in a similar fashion,
**   using the block pointer embedded in the last 4 bytes of the first page
**   of each block as required.
**
**   The upper layer is responsible for identifying by page number the 
**   first and last page of any run that it needs to navigate - there are
**   no "end-of-run" markers stored or identified by this layer. This is
**   necessary as clients reading different database snapshots may access 
**   different subsets of a run.
**
** THE LOG FILE 
**
** This file opens and closes the log file. But it does not contain any
** logic related to the log file format. Instead, it exports the following
** functions that are used by the code in lsm_log.c to read and write the
** log file:
**
**     lsmFsWriteLog
**     lsmFsSyncLog
**     lsmFsReadLog
**     lsmFsTruncateLog
**     lsmFsCloseAndDeleteLog
**
*/
#include "lsmInt.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


/*
** File-system object. Each database connection allocates a single instance
** of the following structure. It is used for all access to the database and
** log files.
**
** pLruFirst, pLruLast:
**   The first and last entries in a doubly-linked list of pages. The 
**   Page.pLruNext and Page.pLruPrev pointers are used to link the list
**   elements together.
**
**   In mmap() mode, this list contains all currently allocated pages that
**   are carrying pointers into the database file mapping (pMap/nMap). If the
**   file has to be unmapped and then remapped (required to grow the mapping
**   as the file grows), the Page.aData pointers are updated by iterating
**   through the contents of this list.
**
**   In non-mmap() mode, this list is an LRU list of cached pages with nRef==0.
*/
struct FileSystem {
  lsm_db *pDb;                    /* Database handle that owns this object */
  lsm_env *pEnv;                  /* Environment pointer */
  char *zDb;                      /* Database file name */
  int nMetasize;                  /* Size of meta pages in bytes */
  int nPagesize;                  /* Database page-size in bytes */
  int nBlocksize;                 /* Database block-size in bytes */

  /* r/w file descriptors for both files. */
  lsm_file *fdDb;                 /* Database file */
  lsm_file *fdLog;                /* Log file */

  /* mmap() mode things */
  int bUseMmap;                   /* True to use mmap() to access db file */
  void *pMap;                     /* Current mapping of database file */
  i64 nMap;                       /* Bytes mapped at pMap */

  /* Statistics */
  int nWrite;                     /* Total number of pages written */
  int nRead;                      /* Total number of pages read */

  /* Page cache parameters for non-mmap() mode */
  int nOut;                       /* Number of outstanding pages */
  int nCacheMax;                  /* Configured cache size (in pages) */
  int nCacheAlloc;                /* Current cache size (in pages) */
  Page *pLruFirst;                /* Head of the LRU list */
  Page *pLruLast;                 /* Tail of the LRU list */
  int nHash;                      /* Number of hash slots in hash table */
  Page **apHash;                  /* nHash Hash slots */
};

/*
** Database page handle.
*/
struct Page {
  u8 *aData;                      /* Buffer containing page data */
  int nData;                      /* Bytes of usable data at aData[] */
  int iPg;                        /* Page number */
  int nRef;                       /* Number of outstanding references */
  int flags;                      /* Combination of PAGE_XXX flags */
  Page *pHashNext;                /* Next page in hash table slot */
  Page *pLruNext;                 /* Next page in LRU list */
  Page *pLruPrev;                 /* Previous page in LRU list */
  FileSystem *pFS;                /* File system that owns this page */
};

/*
** Meta-data page handle. There are two meta-data pages at the start of
** the database file, each FileSystem.nMetasize bytes in size.
*/
struct MetaPage {
  int iPg;                        /* Either 1 or 2 */
  int bWrite;                     /* Write back to db file on release */
  u8 *aData;                      /* Pointer to buffer */
  FileSystem *pFS;                /* FileSystem that owns this page */
};

/* 
** Values for LsmPage.flags 
*/
#define PAGE_DIRTY 0x00000001     /* Set if page is dirty */
#define PAGE_FREE  0x00000002     /* Set if Page.aData requires lsmFree() */
#define PAGE_SHORT 0x00000004     /* Set if page is 4 bytes short */

/*
** Number of pgsz byte pages omitted from the start of block 1. The start
** of block 1 contains two 4096 byte meta pages (8192 bytes in total).
*/
#define BLOCK1_HDR_SIZE(pgsz)  LSM_MAX(1, 8192/(pgsz))


/*
** Wrappers around the VFS methods of the lsm_env object:
**
**     lsmEnvOpen()
**     lsmEnvRead()
**     lsmEnvWrite()
**     lsmEnvSync()
**     lsmEnvSectorSize()
**     lsmEnvClose()
**     lsmEnvTruncate()
**     lsmEnvUnlink()
**     lsmEnvRemap()
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
static int lsmEnvSectorSize(lsm_env *pEnv, lsm_file *pFile){
  return pEnv->xSectorSize(pFile);
}
static int lsmEnvClose(lsm_env *pEnv, lsm_file *pFile){
  return pEnv->xClose(pFile);
}
static int lsmEnvTruncate(lsm_env *pEnv, lsm_file *pFile, lsm_i64 nByte){
  return pEnv->xTruncate(pFile, nByte);
}
static int lsmEnvUnlink(lsm_env *pEnv, const char *zDel){
  return pEnv->xUnlink(pEnv, zDel);
}
static int lsmEnvRemap(
  lsm_env *pEnv, 
  lsm_file *pFile, 
  i64 szMin,
  void **ppMap,
  i64 *pszMap
){
  return pEnv->xRemap(pFile, szMin, ppMap, pszMap);
}

/*
** Write the contents of string buffer pStr into the log file, starting at
** offset iOff.
*/
int lsmFsWriteLog(FileSystem *pFS, i64 iOff, LsmString *pStr){
  return lsmEnvWrite(pFS->pEnv, pFS->fdLog, iOff, pStr->z, pStr->n);
}

/*
** fsync() the log file.
*/
int lsmFsSyncLog(FileSystem *pFS){
  return lsmEnvSync(pFS->pEnv, pFS->fdLog);
}

/*
** Read nRead bytes of data starting at offset iOff of the log file. Store
** the results in string buffer pStr.
*/
int lsmFsReadLog(FileSystem *pFS, i64 iOff, int nRead, LsmString *pStr){
  int rc;                         /* Return code */
  rc = lsmStringExtend(pStr, nRead);
  if( rc==LSM_OK ){
    rc = lsmEnvRead(pFS->pEnv, pFS->fdLog, iOff, &pStr->z[pStr->n], nRead);
    pStr->n += nRead;
  }
  return rc;
}

/*
** Truncate the log file to nByte bytes in size.
*/
int lsmFsTruncateLog(FileSystem *pFS, i64 nByte){
  if( pFS->fdLog==0 ) return LSM_OK;
  return lsmEnvTruncate(pFS->pEnv, pFS->fdLog, nByte);
}

/*
** Close the log file. Then delete it from the file-system. This function
** is called during database shutdown only.
*/
int lsmFsCloseAndDeleteLog(FileSystem *pFS){
  if( pFS->fdLog ){
    char *zDel = lsmMallocPrintf(pFS->pEnv, "%s-log", pFS->zDb);
    if( zDel ){
      lsmEnvClose(pFS->pEnv, pFS->fdLog );
      lsmEnvUnlink(pFS->pEnv, zDel);
      lsmFree(pFS->pEnv, zDel);
      pFS->fdLog = 0;
    }
  }
  return LSM_OK;
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
int lsmFsOpen(lsm_db *pDb, const char *zDb){
  FileSystem *pFS;
  int rc = LSM_OK;

  assert( pDb->pFS==0 );
  assert( pDb->pWorker==0 && pDb->pClient==0 );

  pFS = (FileSystem *)lsmMallocZeroRc(pDb->pEnv, sizeof(FileSystem), &rc);
  if( pFS ){
    pFS->nPagesize = LSM_PAGE_SIZE;
    pFS->nBlocksize = LSM_BLOCK_SIZE;
    pFS->nMetasize = 4 * 1024;
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
      if( pPg->flags & PAGE_FREE ) lsmFree(pEnv, pPg->aData);
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
** Allocate a buffer and populate it with the output of the xFileid() 
** method of the database file handle. If successful, set *ppId to point 
** to the buffer and *pnId to the number of bytes in the buffer and return
** LSM_OK. Otherwise, set *ppId and *pnId to zero and return an LSM
** error code.
*/
int lsmFsFileid(lsm_db *pDb, void **ppId, int *pnId){
  lsm_env *pEnv = pDb->pEnv;
  FileSystem *pFS = pDb->pFS;
  int rc;
  int nId = 0;
  void *pId;

  rc = pEnv->xFileid(pFS->fdDb, 0, &nId);
  pId = lsmMallocZeroRc(pEnv, nId, &rc);
  if( rc==LSM_OK ) rc = pEnv->xFileid(pFS->fdDb, pId, &nId);

  if( rc!=LSM_OK ){
    lsmFree(pEnv, pId);
    pId = 0;
    nId = 0;
  }

  *ppId = pId;
  *pnId = nId;
  return rc;
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

/*
** Return the page number of the first page on block iBlock. Blocks are
** numbered starting from 1.
*/
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

/*
** Return the page number of the last page on block iBlock. Blocks are
** numbered starting from 1.
*/
static Pgno fsLastPageOnBlock(FileSystem *pFS, int iBlock){
  const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
  return iBlock * nPagePerBlock;
}

/*
** Return true if page iPg is the last page on its block.
*/
static int fsIsLast(FileSystem *pFS, Pgno iPg){
  const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
  return ( iPg && (iPg % nPagePerBlock)==0 );
}

/*
** Return true if page iPg is the first page on its block.
*/
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
#ifndef NDEBUG
    int bShort = (
        fsIsFirst(pPage->pFS, pPage->iPg) || fsIsLast(pPage->pFS, pPage->iPg)
    );
    assert( bShort==!!(pPage->flags & PAGE_SHORT) );
    assert( PAGE_SHORT==4 );
#endif
    *pnData = pPage->pFS->nPagesize - (pPage->flags & PAGE_SHORT);
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

/*
** Page pPg is not currently part of the LRU list belonging to pFS. Add it.
*/
static void fsPageAddToLru(FileSystem *pFS, Page *pPg){
  assert( pPg->pLruNext==0 && pPg->pLruPrev==0 );
  pPg->pLruPrev = pFS->pLruLast;
  if( pPg->pLruPrev ){
    pPg->pLruPrev->pLruNext = pPg;
  }else{
    pFS->pLruFirst = pPg;
  }
  pFS->pLruLast = pPg;
}

/*
** Remove page pPg from the hash table.
*/
static void fsPageRemoveFromHash(FileSystem *pFS, Page *pPg){
  int iHash;
  Page **pp;

  iHash = fsHashKey(pFS->nHash, pPg->iPg);
  for(pp=&pFS->apHash[iHash]; *pp!=pPg; pp=&(*pp)->pHashNext);
  *pp = pPg->pHashNext;
}

static int fsPageBuffer(
  FileSystem *pFS, 
  int bRequireData,               /* True to allocate buffer as well */
  Page **ppOut
){
  int rc = LSM_OK;
  Page *pPage = 0;
  if( pFS->bUseMmap || pFS->pLruFirst==0 || pFS->nCacheAlloc<pFS->nCacheMax ){
    pPage = lsmMallocZero(pFS->pEnv, sizeof(Page));
    if( !pPage ){
      rc = LSM_NOMEM_BKPT;
    }else if( bRequireData ){
      pPage->aData = (u8 *)lsmMalloc(pFS->pEnv, pFS->nPagesize);
      pPage->flags = PAGE_FREE;
      if( !pPage->aData ){
        lsmFree(pFS->pEnv, pPage);
        rc = LSM_NOMEM_BKPT;
        pPage = 0;
      }
      pFS->nCacheAlloc++;
    }else{
      fsPageAddToLru(pFS, pPage);
    }
  }else{
    pPage = pFS->pLruFirst;
    fsPageRemoveFromLru(pFS, pPage);
    fsPageRemoveFromHash(pFS, pPage);
  }

  assert( pPage==0 || (pPage->flags & PAGE_DIRTY)==0 );
  *ppOut = pPage;
  return rc;
}

static void fsPageBufferFree(Page *pPg){
  if( pPg->flags & PAGE_FREE ){
    lsmFree(pPg->pFS->pEnv, pPg->aData);
  }
  else if( pPg->pFS->bUseMmap ){
    fsPageRemoveFromLru(pPg->pFS, pPg);
  }
  lsmFree(pPg->pFS->pEnv, pPg);
}

int fsPageToBlock(FileSystem *pFS, Pgno iPg){
  return 1 + ((iPg-1) / (pFS->nBlocksize / pFS->nPagesize));
}


static void fsGrowMapping(
  FileSystem *pFS,
  i64 iSz,
  int *pRc
){
  if( *pRc==LSM_OK && iSz>pFS->nMap ){
    Page *pFix;
    int rc;
    rc = lsmEnvRemap(pFS->pEnv, pFS->fdDb, iSz, &pFS->pMap, &pFS->nMap);
    if( rc==LSM_OK ){
      u8 *aData = (u8 *)pFS->pMap;
      for(pFix=pFS->pLruFirst; pFix; pFix=pFix->pLruNext){
        pFix->aData = &aData[pFS->nPagesize * (i64)(pFix->iPg-1)];
      }

      lsmSortedRemap(pFS->pDb);
    }
    *pRc = rc;
  }
}

/*
** Return a handle for a database page.
*/
int fsPageGet(
  FileSystem *pFS,                /* File-system handle */
  Pgno iPg,                       /* Page id */
  int noContent,                  /* True to not load content from disk */
  Page **ppPg                     /* OUT: New page handle */
){
  Page *p;
  int iHash;
  int rc = LSM_OK;

  assert( iPg>=fsFirstPageOnBlock(pFS, 1) );

  /* Search the hash-table for the page */
  iHash = fsHashKey(pFS->nHash, iPg);
  for(p=pFS->apHash[iHash]; p; p=p->pHashNext){
    if( p->iPg==iPg) break;
  }

  if( p==0 ){
    /* Set bRequireData to true if a buffer allocated by malloc() is required
    ** to store the page data (the alternative is to have the Page object
    ** carry a pointer into the mapped region at FileSystem.pMap). In 
    ** non-mmap mode, this should always be true. In mmap mode, it should
    ** always be false for readable pages (noContent==0), but may be set
    ** to either true or false for appended pages (noContent==1). Setting
    ** it to true in this case causes LSM to do "double-buffered" writes. */
    int bRequireData = (pFS->bUseMmap==0);

    rc = fsPageBuffer(pFS, bRequireData, &p);
    if( rc==LSM_OK ){
      p->iPg = iPg;
      p->nRef = 0;
      p->pFS = pFS;
      assert( p->flags==0 || p->flags==PAGE_FREE );
      if( fsIsLast(pFS, iPg) || fsIsFirst(pFS, iPg) ) p->flags |= PAGE_SHORT;

      if( pFS->bUseMmap && bRequireData==0 ){
        i64 iEnd = (i64)iPg * pFS->nPagesize;
        fsGrowMapping(pFS, iEnd, &rc);
        if( rc==LSM_OK ){
          p->aData = &((u8 *)pFS->pMap)[pFS->nPagesize * (i64)(iPg-1)];
        }
      }else{
#ifdef LSM_DEBUG
        memset(p->aData, 0x56, pFS->nPagesize);
#endif
        assert( p->pLruNext==0 && p->pLruPrev==0 );
        if( noContent==0 ){
          int nByte = pFS->nPagesize;
          i64 iOff;

          iOff = (i64)(iPg-1) * pFS->nPagesize;
          rc = lsmEnvRead(pFS->pEnv, pFS->fdDb, iOff, p->aData, nByte);
          pFS->nRead++;
        }
      }

      /* If the xRead() call was successful (or not attempted), link the
      ** page into the page-cache hash-table. Otherwise, if it failed,
      ** free the buffer. */
      if( rc==LSM_OK ){
        p->pHashNext = pFS->apHash[iHash];
        p->nData =  pFS->nPagesize - (p->flags & PAGE_SHORT);
        pFS->apHash[iHash] = p;
      }else{
        fsPageBufferFree(p);
        p = 0;
      }
    }
  }else if( p->nRef==0 && pFS->bUseMmap==0 ){
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
  
  rc = fsPageGet(pFS, iBlock*nPagePerBlock, 0, &pLast);
  if( rc==LSM_OK ){
    *piNext = fsPageToBlock(pFS, lsmGetU32(&pLast->aData[pFS->nPagesize-4]));
    lsmFsPageRelease(pLast);
  }
  return rc;
}

static int fsRunEndsBetween(
  Segment *pRun, 
  Segment *pIgnore, 
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
  Segment *pIgnore, 
  int iFirst, 
  int iLast
){
  int i;

  if( fsRunEndsBetween(&pLevel->lhs, pIgnore, iFirst, iLast) ){
    return 1;
  }
  for(i=0; i<pLevel->nRight; i++){
    if( fsRunEndsBetween(&pLevel->aRhs[i], pIgnore, iFirst, iLast) ){
      return 1;
    }
  }

  return 0;
}

static int fsFreeBlock(
  FileSystem *pFS, 
  Snapshot *pSnapshot, 
  Segment *pIgnore,             /* Ignore this run when searching */
  int iBlk
){
  int rc = LSM_OK;                /* Return code */
  int iFirst;                     /* First page on block iBlk */
  int iLast;                      /* Last page on block iBlk */
  Level *pLevel;                  /* Used to iterate through levels */

  int iIn;                        /* Used to iterate through append points */
  int iOut = 0;                   /* Used to output append points */
  u32 *aApp = pSnapshot->aiAppend;

  iFirst = fsFirstPageOnBlock(pFS, iBlk);
  iLast = fsLastPageOnBlock(pFS, iBlk);

  /* Check if any other run in the snapshot has a start or end page 
  ** within this block. If there is such a run, return early. */
  for(pLevel=lsmDbSnapshotLevel(pSnapshot); pLevel; pLevel=pLevel->pNext){
    if( fsLevelEndsBetween(pLevel, pIgnore, iFirst, iLast) ){
      return LSM_OK;
    }
  }

  for(iIn=0; iIn<LSM_APPLIST_SZ; iIn++){
    if( aApp[iIn]<iFirst || aApp[iIn]>iLast ){
      aApp[iOut++] = aApp[iIn];
    }
  }
  while( iOut<LSM_APPLIST_SZ ) aApp[iOut++] = 0;

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
  int bZero,                      /* True to zero the Segment structure */
  Segment *pDel
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

    if( bZero ) memset(pDel, 0, sizeof(Segment));
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
  Segment *pRun, 
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
** file page that is part of a sorted run. If parameter eDir is -1, this 
** function attempts to locate and load the previous page in the same run. 
** Or, if eDir is +1, it attempts to find the next page in the same run.
** The results of passing an eDir value other than positive or negative one
** are undefined.
**
** If parameter pRun is not NULL then it must point to the run that page
** pPg belongs to. In this case, if pPg is the first or last page of the
** run, and the request is for the previous or next page, respectively,
** *ppNext is set to NULL before returning LSM_OK. If pRun is NULL, then it
** is assumed that the next or previous page, as requested, exists.
**
** If the previous/next page does exist and is successfully loaded, *ppNext
** is set to point to it and LSM_OK is returned. Otherwise, if an error 
** occurs, *ppNext is set to NULL and and lsm error code returned.
**
** Page references returned by this function should be released by the 
** caller using lsmFsPageRelease().
*/
int lsmFsDbPageNext(Segment *pRun, Page *pPg, int eDir, Page **ppNext){
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

  return fsPageGet(pFS, iPg, 0, ppNext);
}

static Pgno findAppendPoint(FileSystem *pFS){
  int i;
  u32 *aiAppend = pFS->pDb->pWorker->aiAppend;
  u32 iRet = 0;

  for(i=LSM_APPLIST_SZ-1; iRet==0 && i>=0; i--){
    if( (iRet = aiAppend[i]) ) aiAppend[i] = 0;
  }
  return iRet;
}

/*
** Append a page to file iFile. Return a reference to it. lsmFsPageWrite()
** has already been called on the returned reference.
*/
int lsmFsSortedAppend(
  FileSystem *pFS, 
  Snapshot *pSnapshot,
  Segment *p, 
  Page **ppOut
){
  int rc = LSM_OK;
  Page *pPg = 0;
  *ppOut = 0;
  int iApp = 0;
  int iNext = 0;
  int iPrev = p->iLast;

  if( iPrev==0 ){
    iApp = findAppendPoint(pFS);
  }else if( fsIsLast(pFS, iPrev) ){
    Page *pLast = 0;
    rc = fsPageGet(pFS, iPrev, 0, &pLast);
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
  rc = fsPageGet(pFS, iApp, 1, &pPg);
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

  *ppOut = pPg;
  return rc;
}

/*
** Mark the sorted run passed as the second argument as finished. 
*/
int lsmFsSortedFinish(FileSystem *pFS, Segment *p){
  int rc = LSM_OK;
  if( p ){
    const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);

    /* Check if the last page of this run happens to be the last of a block.
    ** If it is, then an extra block has already been allocated for this run.
    ** Shift this extra block back to the free-block list. 
    **
    ** Otherwise, add the first free page in the last block used by the run
    ** to the lAppend list.
    */
    if( (p->iLast % nPagePerBlock)==0 ){
      Page *pLast;
      rc = fsPageGet(pFS, p->iLast, 0, &pLast);
      if( rc==LSM_OK ){
        int iPg = (int)lsmGetU32(&pLast->aData[pFS->nPagesize-4]);
        int iBlk = fsPageToBlock(pFS, iPg);
        lsmBlockRefree(pFS->pDb, iBlk);
        lsmFsPageRelease(pLast);
      }
    }else{
      int i;
      u32 *aiAppend = pFS->pDb->pWorker->aiAppend;
      for(i=0; i<LSM_APPLIST_SZ; i++){
        if( aiAppend[i]==0 ){
          aiAppend[i] = p->iLast+1;
          break;
        }
      }
    }
  }
  return rc;
}

/*
** Obtain a reference to page number iPg.
*/
int lsmFsDbPageGet(FileSystem *pFS, int iPg, Page **ppPg){
  assert( pFS );
  return fsPageGet(pFS, iPg, 0, ppPg);
}

/*
** Return a reference to meta-page iPg. If successful, LSM_OK is returned
** and *ppPg populated with the new page reference. The reference should
** be released by the caller using lsmFsPageRelease().
**
** Otherwise, if an error occurs, *ppPg is set to NULL and an LSM error 
** code is returned.
*/
int lsmFsMetaPageGet(
  FileSystem *pFS,                /* File-system connection */
  int bWrite,                     /* True for write access, false for read */
  int iPg,                        /* Either 1 or 2 */
  MetaPage **ppPg                 /* OUT: Pointer to MetaPage object */
){
  int rc = LSM_OK;
  MetaPage *pPg;
  assert( iPg==1 || iPg==2 );

  pPg = lsmMallocZeroRc(pFS->pEnv, sizeof(Page), &rc);

  if( pPg ){
    i64 iOff = (iPg-1) * pFS->nMetasize;
    if( pFS->bUseMmap ){
      fsGrowMapping(pFS, 2*pFS->nMetasize, &rc);
      pPg->aData = (u8 *)(pFS->pMap) + iOff;
    }else{
      pPg->aData = lsmMallocRc(pFS->pEnv, pFS->nMetasize, &rc);
      if( rc==LSM_OK && bWrite==0 ){
        rc = lsmEnvRead(pFS->pEnv, pFS->fdDb, iOff, pPg->aData, pFS->nMetasize);
      }
#ifndef NDEBUG
      /* pPg->aData causes an uninitialized access via a downstreadm write().
         After discussion on this list, this memory should not, for performance
         reasons, be memset. However, tracking down "real" misuse is more
         difficult with this "false" positive, so it is set when NDEBUG.
      */
      else if( rc==LSM_OK ){
        memset( pPg->aData, 0x77, pFS->nMetasize );
      }
#endif
    }

    if( rc!=LSM_OK ){
      if( pFS->bUseMmap==0 ) lsmFree(pFS->pEnv, pPg->aData);
      lsmFree(pFS->pEnv, pPg);
      pPg = 0;
    }else{
      pPg->iPg = iPg;
      pPg->bWrite = bWrite;
      pPg->pFS = pFS;
    }
  }

  *ppPg = pPg;
  return rc;
}

/*
** Release a meta-page reference obtained via a call to lsmFsMetaPageGet().
*/
int lsmFsMetaPageRelease(MetaPage *pPg){
  int rc = LSM_OK;
  if( pPg ){
    FileSystem *pFS = pPg->pFS;

    if( pFS->bUseMmap==0 ){
      if( pPg->bWrite ){
        i64 iOff = (pPg->iPg==2 ? pFS->nMetasize : 0);
        int nWrite = pFS->nMetasize;
        rc = lsmEnvWrite(pFS->pEnv, pFS->fdDb, iOff, pPg->aData, nWrite);
      }
      lsmFree(pFS->pEnv, pPg->aData);
    }

    lsmFree(pFS->pEnv, pPg);
  }
  return rc;
}

/*
** Return a pointer to a buffer containing the data associated with the
** meta-page passed as the first argument. If parameter pnData is not NULL,
** set *pnData to the size of the meta-page in bytes before returning.
*/
u8 *lsmFsMetaPageData(MetaPage *pPg, int *pnData){
  if( pnData ) *pnData = pPg->pFS->nMetasize;
  return pPg->aData;
}

/*
** Notify the file-system that the page needs to be written back to disk
** when the reference count next drops to zero.
*/
int lsmFsPageWrite(Page *pPg){
  pPg->flags |= PAGE_DIRTY;
  return LSM_OK;
}

/*
** Return true if page is currently writable.
*/
int lsmFsPageWritable(Page *pPg){
  return (pPg->flags & PAGE_DIRTY) ? 1 : 0;
}


/*
** If the page passed as an argument is dirty, update the database file
** (or mapping of the database file) with its current contents and mark
** the page as clean.
**
** Return LSM_OK if the operation is a success, or an LSM error code
** otherwise.
*/
int lsmFsPagePersist(Page *pPg){
  int rc = LSM_OK;
  if( pPg && (pPg->flags & PAGE_DIRTY) ){
    FileSystem *pFS = pPg->pFS;
    i64 iOff;                 /* Offset to write within database file */

    iOff = (i64)pFS->nPagesize * (i64)(pPg->iPg-1);
    if( pFS->bUseMmap==0 ){
      rc = lsmEnvWrite(pFS->pEnv, pFS->fdDb, iOff, pPg->aData,pFS->nPagesize);
    }else if( pPg->flags & PAGE_FREE ){
      fsGrowMapping(pFS, iOff + pFS->nPagesize, &rc);
      if( rc==LSM_OK ){
        u8 *aTo = &((u8 *)(pFS->pMap))[iOff];
        memcpy(aTo, pPg->aData, pFS->nPagesize);
        lsmFree(pFS->pEnv, pPg->aData);
        pPg->aData = aTo;
        pPg->flags &= ~PAGE_FREE;
        fsPageAddToLru(pFS, pPg);
      }
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

      pFS->nOut--;
      assert( pFS->bUseMmap || pPg->pLruNext==0 );
      assert( pFS->bUseMmap || pPg->pLruPrev==0 );
#if 0
      fsPageAddToLru(pFS, pPg);
#else
      fsPageRemoveFromHash(pFS, pPg);
      fsPageBufferFree(pPg);
#endif
    }
  }

  return rc;
}

/*
** Return the total number of pages read from the database file.
*/
int lsmFsNRead(FileSystem *pFS){ return pFS->nRead; }

/*
** Return the total number of pages written to the database file.
*/
int lsmFsNWrite(FileSystem *pFS){ return pFS->nWrite; }

/*
** fsync() the database file.
*/
int lsmFsSyncDb(FileSystem *pFS){
  return lsmEnvSync(pFS->pEnv, pFS->fdDb);
}

/*
** Return a copy of the environment pointer used by the file-system object.
*/
lsm_env *lsmFsEnv(FileSystem *pFS) { 
  return pFS->pEnv; 
}

/*
** Return a copy of the environment pointer used by the file-system object
** to which this page belongs.
*/
lsm_env *lsmPageEnv(Page *pPg) { 
  return pPg->pFS->pEnv; 
}

FileSystem *lsmPageFS(Page *pPg){
  return pPg->pFS;
}

/*
** Return the sector-size as reported by the log file handle.
*/
int lsmFsSectorSize(FileSystem *pFS){
  return lsmEnvSectorSize(pFS->pEnv, pFS->fdLog);
}

/*
** This function implements the lsm_config(LSM_CONFIG_MMAP) request. This
** setting may only be modified if there are currently no outstanding page
** references.
*/
int lsmConfigMmap(lsm_db *pDb, int *piParam){
  int iNew = *piParam;
  FileSystem *pFS = pDb->pFS;
  if( LSM_IS_64_BIT && (pFS->nOut==0 && (iNew==0 || iNew==1)) ){
    pFS->bUseMmap = iNew;
  }
  *piParam = pFS->bUseMmap;
  return LSM_OK;
}

/*
** Helper function for lsmInfoArrayStructure().
*/
static Segment *startsWith(Segment *pRun, Pgno iFirst){
  return (iFirst==pRun->iFirst) ? pRun : 0;
}

/*
** This function implements the lsm_info(LSM_INFO_ARRAY_STRUCTURE) request.
** If successful, *pzOut is set to point to a nul-terminated string 
** containing the array structure and LSM_OK is returned. The caller should
** eventually free the string using lsmFree().
**
** If an error occurs, *pzOut is set to NULL and an LSM error code returned.
*/
int lsmInfoArrayStructure(lsm_db *pDb, Pgno iFirst, char **pzOut){
  int rc = LSM_OK;
  Snapshot *pWorker;              /* Worker snapshot */
  Segment *pArray = 0;            /* Array to report on */
  Level *pLvl;                    /* Used to iterate through db levels */
  int bUnlock = 0;

  *pzOut = 0;
  if( iFirst==0 ) return LSM_ERROR;

  /* Obtain the worker snapshot */
  pWorker = pDb->pWorker;
  if( !pWorker ){
    rc = lsmBeginWork(pDb);
    if( rc!=LSM_OK ) return rc;
    pWorker = pDb->pWorker;
    bUnlock = 1;
  }

  /* Search for the array that starts on page iFirst */
  for(pLvl=lsmDbSnapshotLevel(pWorker); pLvl && pArray==0; pLvl=pLvl->pNext){
    if( 0==(pArray = startsWith(&pLvl->lhs, iFirst)) ){
      int i;
      for(i=0; i<pLvl->nRight; i++){
        if( (pArray = startsWith(&pLvl->aRhs[i], iFirst)) ) break;
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

  if( bUnlock ){
    int rcwork = LSM_BUSY;
    lsmFinishWork(pDb, &rcwork);
  }
  return rc;
}

/*
** Helper function for lsmFsIntegrityCheck()
*/
static void checkBlocks(
  FileSystem *pFS, 
  Segment *pSeg,
  int bExtra,                     /* If true, count the "next" block if any */
  int nUsed,
  u8 *aUsed
){
  if( pSeg ){
    int i;

    if( pSeg && pSeg->nSize>0 ){
      const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);

      int iBlk;
      int iLastBlk;
      iBlk = fsPageToBlock(pFS, pSeg->iFirst);
      iLastBlk = fsPageToBlock(pFS, pSeg->iLast);

      while( iBlk ){
        assert( iBlk<=nUsed );
        /* assert( aUsed[iBlk-1]==0 ); */
        aUsed[iBlk-1] = 1;
        if( iBlk!=iLastBlk ){
          fsBlockNext(pFS, iBlk, &iBlk);
        }else{
          iBlk = 0;
        }
      }

      if( bExtra && (pSeg->iLast % nPagePerBlock)==0 ){
        fsBlockNext(pFS, iLastBlk, &iBlk);
        aUsed[iBlk-1] = 1;
      }
    }
  }
}

/*
** This function checks that all blocks in the database file are accounted
** for. For each block, exactly one of the following must be true:
**
**   + the block is part of a sorted run, or
**   + the block is on the free-block list
**
** This function also checks that there are no references to blocks with
** out-of-range block numbers.
**
** If no errors are found, non-zero is returned. If an error is found, an
** assert() fails.
*/
int lsmFsIntegrityCheck(lsm_db *pDb){
  int i;
  FileSystem *pFS = pDb->pFS;
  u8 *aUsed;
  Level *pLevel;
  Snapshot *pWorker = pDb->pWorker;
  int nBlock = pWorker->nBlock;


  aUsed = lsmMallocZero(pDb->pEnv, nBlock);
  if( aUsed==0 ){
    /* Malloc has failed. Since this function is only called within debug
    ** builds, this probably means the user is running an OOM injection test.
    ** Regardless, it will not be possible to run the integrity-check at this
    ** time, so assume the database is Ok and return non-zero. */
    return 1;
  }

  for(pLevel=pWorker->pLevel; pLevel; pLevel=pLevel->pNext){
    int i;
    checkBlocks(pFS, &pLevel->lhs, (pLevel->nRight!=0), nBlock, aUsed);
    for(i=0; i<pLevel->nRight; i++){
      checkBlocks(pFS, &pLevel->aRhs[i], 0, nBlock, aUsed);
    }
  }

  for(i=0; i<pWorker->freelist.nEntry; i++){
    u32 iBlk = pWorker->freelist.aEntry[i].iBlk;
    assert( iBlk<=nBlock );
    assert( aUsed[iBlk-1]==0 );
    aUsed[iBlk-1] = 1;
  }

  for(i=0; i<nBlock; i++) assert( aUsed[i]==1 );

  lsmFree(pDb->pEnv, aUsed);
  return 1;
}
