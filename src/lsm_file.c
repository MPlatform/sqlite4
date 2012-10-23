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
** NORMAL DATABASE FILE FORMAT
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
**   meta page or any other part of the database file. TODO: This may need
**   to be revisited.
**
** Blocks:
**
**   The database file is also divided into blocks. The default block size is
**   2MB. When writing to the database file, an attempt is made to write data
**   in contiguous block-sized chunks.
**
**   The first and last page on each block are special in that they are 4 
**   bytes smaller than all other pages. This is because the last four bytes 
**   of space on the first and last pages of each block are reserved for
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
**     lsmFsOpenLog
**     lsmFsWriteLog
**     lsmFsSyncLog
**     lsmFsReadLog
**     lsmFsTruncateLog
**     lsmFsCloseAndDeleteLog
**
** COMPRESSED DATABASE FILE FORMAT
**
** The compressed database file format is very similar to the normal format.
** The file still begins with two 4KB meta-pages (which are never compressed).
** It is still divided into blocks.
**
** The first and last four bytes of each block are reserved for 32-bit 
** pointer values. Similar to the way four bytes are carved from the end of 
** the first and last page of each block in uncompressed databases. From
** the point of view of the upper layer, all pages are the same size - this
** is different from the uncompressed format where the first and last pages
** on each block are 4 bytes smaller than the others.
**
** Pages are stored in variable length compressed form, as follows:
**
**     * Number of bytes in compressed page image, as a varint.
**
**     * Compressed page image.
**
**     * Number of bytes in compressed page image, as a varint. Except,
**       the first byte of the varint is moved so that it is the last
**       byte (i.e. for a 4 byte varint: ABCD -> BCDA). This is done
**       to make it possible to iterate through a packed array of compressed 
**       pages in reverse order.
**
** A page number is a byte offset into the database file. So the smallest
** possible page number is 8192 (immediately after the two meta-pages).
** The first and root page of a segment are identified by a page number
** corresponding to the byte offset of the first byte in the corresponding
** page record. The last page of a segment is identified by the byte offset
** of the last byte in its record.
**
** Unlike uncompressed pages, compressed page records may span blocks.
**
** Sometimes, in order to avoid touching sectors that contain synced data
** when writing, it is necessary to insert unused space between compressed
** page records. This can be done as follows:
**
**     * For less than 4 bytes of empty space, a series of 0x00 bytes.
**
**     * For 4 or more bytes, the block of free space begins with an 
**       0x01 byte, followed by a varint containing the total size of the 
**       free space. Similarly, it ends with an (ABCD -> BCDA) transformed
**       varint an a final 0x01 byte.
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
  char *zLog;                     /* Database file name */
  int nMetasize;                  /* Size of meta pages in bytes */
  int nPagesize;                  /* Database page-size in bytes */
  int nBlocksize;                 /* Database block-size in bytes */

  /* r/w file descriptors for both files. */
  LsmFile *pLsmFile;
  lsm_file *fdDb;                 /* Database file */
  lsm_file *fdLog;                /* Log file */

  /* If this is a compressed database, a pointer to the compression methods.
  ** For an uncompressed database, a NULL pointer.  */
  lsm_compress *pCompress;
  u8 *aBuffer;                    /* Buffer to compress into */

  /* mmap() mode things */
  int bUseMmap;                   /* True to use mmap() to access db file */
  void *pMap;                     /* Current mapping of database file */
  i64 nMap;                       /* Bytes mapped at pMap */
  Page *pFree;

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
**
** pSeg:
**   When lsmFsSortedAppend() is called on a compressed database, the new
**   page is not assigned a page number or location in the database file
**   immediately. Instead, these are assigned by the lsmFsPagePersist() call
**   right before it writes the compressed page image to disk.
**
**   The lsmFsSortedAppend() function sets the pSeg pointer to point to the
**   segment that the new page will be a part of. It is unset by
**   lsmFsPagePersist() after the page is written to disk.
*/
struct Page {
  u8 *aData;                      /* Buffer containing page data */
  int nData;                      /* Bytes of usable data at aData[] */
  Pgno iPg;                       /* Page number */
  int nRef;                       /* Number of outstanding references */
  int flags;                      /* Combination of PAGE_XXX flags */
  int nCompress;                  /* Compressed size (or 0 for uncomp. db) */
  Segment *pSeg;                  /* Segment this page will be written to */
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
** If NDEBUG is not defined, set a breakpoint in function lsmIoerrBkpt()
** to catch IO errors. 
*/
#ifndef NDEBUG
static int lsmIoerrBkpt(){
  static int nErr = 0;
  nErr++;
}
static int IOERR_WRAPPER(int rc){
  if( rc!=LSM_OK ) lsmIoerrBkpt();
  return rc;
}
#else
# define IOERR_WRAPPER(rc) (rc)
#endif

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
int lsmEnvOpen(lsm_env *pEnv, const char *zFile, lsm_file **ppNew){
  return pEnv->xOpen(pEnv, zFile, ppNew);
}
static int lsmEnvRead(
  lsm_env *pEnv, 
  lsm_file *pFile, 
  lsm_i64 iOff, 
  void *pRead, 
  int nRead
){
  return IOERR_WRAPPER( pEnv->xRead(pFile, iOff, pRead, nRead) );
}
static int lsmEnvWrite(
  lsm_env *pEnv, 
  lsm_file *pFile, 
  lsm_i64 iOff, 
  const void *pWrite, 
  int nWrite
){
  return IOERR_WRAPPER( pEnv->xWrite(pFile, iOff, (void *)pWrite, nWrite) );
}
static int lsmEnvSync(lsm_env *pEnv, lsm_file *pFile){
  return IOERR_WRAPPER( pEnv->xSync(pFile) );
}
static int lsmEnvSectorSize(lsm_env *pEnv, lsm_file *pFile){
  return pEnv->xSectorSize(pFile);
}
int lsmEnvClose(lsm_env *pEnv, lsm_file *pFile){
  return IOERR_WRAPPER( pEnv->xClose(pFile) );
}
static int lsmEnvTruncate(lsm_env *pEnv, lsm_file *pFile, lsm_i64 nByte){
  return IOERR_WRAPPER( pEnv->xTruncate(pFile, nByte) );
}
static int lsmEnvUnlink(lsm_env *pEnv, const char *zDel){
  return IOERR_WRAPPER( pEnv->xUnlink(pEnv, zDel) );
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

int lsmEnvLock(lsm_env *pEnv, lsm_file *pFile, int iLock, int eLock){
  if( pFile==0 ) return LSM_OK;
  return pEnv->xLock(pFile, iLock, eLock);
}

int lsmEnvShmMap(
  lsm_env *pEnv, 
  lsm_file *pFile, 
  int iChunk, 
  int sz, 
  void **ppOut
){
  return pEnv->xShmMap(pFile, iChunk, sz, ppOut);
}

void lsmEnvShmBarrier(lsm_env *pEnv){
  return pEnv->xShmBarrier();
}

void lsmEnvShmUnmap(lsm_env *pEnv, lsm_file *pFile, int bDel){
  pEnv->xShmUnmap(pFile, bDel);
}

void lsmEnvSleep(lsm_env *pEnv, int nUs){
  pEnv->xSleep(pEnv, nUs);
}


/*
** Write the contents of string buffer pStr into the log file, starting at
** offset iOff.
*/
int lsmFsWriteLog(FileSystem *pFS, i64 iOff, LsmString *pStr){
  assert( pFS->fdLog );
  return lsmEnvWrite(pFS->pEnv, pFS->fdLog, iOff, pStr->z, pStr->n);
}

/*
** fsync() the log file.
*/
int lsmFsSyncLog(FileSystem *pFS){
  assert( pFS->fdLog );
  return lsmEnvSync(pFS->pEnv, pFS->fdLog);
}

/*
** Read nRead bytes of data starting at offset iOff of the log file. Append
** the results to string buffer pStr.
*/
int lsmFsReadLog(FileSystem *pFS, i64 iOff, int nRead, LsmString *pStr){
  int rc;                         /* Return code */
  assert( pFS->fdLog );
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
  char *zDel;

  if( pFS->fdLog ){
    lsmEnvClose(pFS->pEnv, pFS->fdLog );
    pFS->fdLog = 0;
  }

  zDel = lsmMallocPrintf(pFS->pEnv, "%s-log", pFS->zDb);
  if( zDel ){
    lsmEnvUnlink(pFS->pEnv, zDel);
    lsmFree(pFS->pEnv, zDel);
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
    *pRc = lsmEnvOpen(pFS->pEnv, (bLog ? pFS->zLog : pFS->zDb), &pFile);
  }
  return pFile;
}

/*
** If it is not already open, this function opens the log file. It returns
** LSM_OK if successful (or if the log file was already open) or an LSM
** error code otherwise.
**
** The log file must be opened before any of the following may be called:
**
**     lsmFsWriteLog
**     lsmFsSyncLog
**     lsmFsReadLog
*/
int lsmFsOpenLog(FileSystem *pFS){
  int rc = LSM_OK;
  if( 0==pFS->fdLog ){ pFS->fdLog = fsOpenFile(pFS, 1, &rc); }
  return rc;
}

/*
** Open a connection to a database stored within the file-system (the
** "system of files").
*/
int lsmFsOpen(lsm_db *pDb, const char *zDb){
  FileSystem *pFS;
  int rc = LSM_OK;
  int nDb = strlen(zDb);
  int nByte;

  assert( pDb->pFS==0 );
  assert( pDb->pWorker==0 && pDb->pClient==0 );

  nByte = sizeof(FileSystem) + nDb+1 + nDb+4+1;
  pFS = (FileSystem *)lsmMallocZeroRc(pDb->pEnv, nByte, &rc);
  if( pFS ){
    LsmFile *pLsmFile;
    pFS->zDb = (char *)&pFS[1];
    pFS->zLog = &pFS->zDb[nDb+1];
    pFS->nPagesize = LSM_DFLT_PAGE_SIZE;
    pFS->nBlocksize = LSM_DFLT_BLOCK_SIZE;
    pFS->nMetasize = 4 * 1024;
    pFS->pDb = pDb;
    pFS->pEnv = pDb->pEnv;
    pFS->bUseMmap = pDb->bMmap;
    if( pDb->compress.xCompress ){
      pFS->pCompress = &pDb->compress;
    }

    /* Make a copy of the database and log file names. */
    memcpy(pFS->zDb, zDb, nDb+1);
    memcpy(pFS->zLog, zDb, nDb);
    memcpy(&pFS->zLog[nDb], "-log", 5);

    /* Allocate the hash-table here. At some point, it should be changed
    ** so that it can grow dynamicly. */
    pFS->nCacheMax = 2048;
    pFS->nHash = 4096;
    pFS->apHash = lsmMallocZeroRc(pDb->pEnv, sizeof(Page *) * pFS->nHash, &rc);

    /* Open the database file */
    pLsmFile = lsmDbRecycleFd(pDb);
    if( pLsmFile ){
      pFS->pLsmFile = pLsmFile;
      pFS->fdDb = pLsmFile->pFile;
      memset(pLsmFile, 0, sizeof(LsmFile));
    }else{
      pFS->pLsmFile = lsmMallocZeroRc(pDb->pEnv, sizeof(LsmFile), &rc);
      if( rc==LSM_OK ){
        pFS->fdDb = fsOpenFile(pFS, 0, &rc);
      }
    }

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
    lsmFree(pEnv, pFS->pLsmFile);
    lsmFree(pEnv, pFS->apHash);
    lsmFree(pEnv, pFS);
  }
}

void lsmFsDeferClose(FileSystem *pFS, LsmFile **pp){
  LsmFile *p = pFS->pLsmFile;
  assert( p->pNext==0 );
  p->pFile = pFS->fdDb;
  pFS->fdDb = 0;
  pFS->pLsmFile = 0;
  *pp = p;
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
**
** For a compressed database, page numbers are byte offsets. The first
** page on each block is the byte offset immediately following the 4-byte
** "previous block" pointer at the start of each block.
*/
static Pgno fsFirstPageOnBlock(FileSystem *pFS, int iBlock){
  Pgno iPg;
  if( pFS->pCompress ){
    if( iBlock==1 ){
      iPg = pFS->nMetasize * 2 + 4;
    }else{
      iPg = pFS->nBlocksize * (Pgno)(iBlock-1) + 4;
    }
  }else{
    const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
    if( iBlock==1 ){
      iPg = 1 + ((pFS->nMetasize*2 + pFS->nPagesize - 1) / pFS->nPagesize);
    }else{
      iPg = 1 + (iBlock-1) * nPagePerBlock;
    }
  }
  return iPg;
}

/*
** Return the page number of the last page on block iBlock. Blocks are
** numbered starting from 1.
**
** For a compressed database, page numbers are byte offsets. The first
** page on each block is the byte offset of the byte immediately before 
** the 4-byte "next block" pointer at the end of each block.
*/
static Pgno fsLastPageOnBlock(FileSystem *pFS, int iBlock){
  if( pFS->pCompress ){
    return pFS->nBlocksize * (Pgno)iBlock - 1 - 4;
  }else{
    const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
    return iBlock * nPagePerBlock;
  }
}

/*
** Return the block number of the block that page iPg is located on. 
** Blocks are numbered starting from 1.
*/
static int fsPageToBlock(FileSystem *pFS, Pgno iPg){
  if( pFS->pCompress ){
    return (iPg / pFS->nBlocksize) + 1;
  }else{
    return 1 + ((iPg-1) / (pFS->nBlocksize / pFS->nPagesize));
  }
}

/*
** Return true if page iPg is the last page on its block.
**
** This function is only called in non-compressed database mode.
*/
static int fsIsLast(FileSystem *pFS, Pgno iPg){
  const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
  assert( !pFS->pCompress );
  return ( iPg && (iPg % nPagePerBlock)==0 );
}

/*
** Return true if page iPg is the first page on its block.
**
** This function is only called in non-compressed database mode.
*/
static int fsIsFirst(FileSystem *pFS, Pgno iPg){
  const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
  assert( !pFS->pCompress );

  return ( (iPg % nPagePerBlock)==1
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
    int bShort = (pPage->pFS->pCompress==0 &&
        (fsIsFirst(pPage->pFS, pPage->iPg) || fsIsLast(pPage->pFS, pPage->iPg))
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
  assert( (pPage->flags & PAGE_DIRTY)==0 );
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

static void fsGrowMapping(
  FileSystem *pFS,
  i64 iSz,
  int *pRc
){
  /* This function won't work with compressed databases yet. */
  assert( pFS->pCompress==0 );

  if( *pRc==LSM_OK && iSz>pFS->nMap ){
    Page *pFix;
    int rc;
    u8 *aOld = pFS->pMap;
    rc = lsmEnvRemap(pFS->pEnv, pFS->fdDb, iSz, &pFS->pMap, &pFS->nMap);
    if( rc==LSM_OK && pFS->pMap!=aOld ){
      u8 *aData = (u8 *)pFS->pMap;
      for(pFix=pFS->pLruFirst; pFix; pFix=pFix->pLruNext){
        assert( &aOld[pFS->nPagesize * (i64)(pFix->iPg-1)]==pFix->aData );
        pFix->aData = &aData[pFS->nPagesize * (i64)(pFix->iPg-1)];
      }
      lsmSortedRemap(pFS->pDb);
    }
    *pRc = rc;
  }
}

static int fsPageGet(FileSystem *, Pgno, int, Page **);

/*
** Parameter iBlock is a database file block. This function reads the value 
** stored in the blocks "next block" pointer and stores it in *piNext.
** LSM_OK is returned if everything is successful, or an LSM error code
** otherwise.
*/
static int fsBlockNext(
  FileSystem *pFS,                /* File-system object handle */
  int iBlock,                     /* Read field from this block */
  int *piNext                     /* OUT: Next block in linked list */
){
  int rc;

  assert( pFS->bUseMmap==0 || pFS->pCompress==0 );
  if( pFS->pCompress ){
    i64 iOff;                     /* File offset to read data from */
    u8 aNext[4];                  /* 4-byte pointer read from db file */

    iOff = (i64)iBlock * pFS->nBlocksize - sizeof(aNext);
    rc = lsmEnvRead(pFS->pEnv, pFS->fdDb, iOff, aNext, sizeof(aNext));
    if( rc==LSM_OK ){
      *piNext = (int)lsmGetU32(aNext);
    }
  }else{
    const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);
    Page *pLast;
    rc = fsPageGet(pFS, iBlock*nPagePerBlock, 0, &pLast);
    if( rc==LSM_OK ){
      *piNext = fsPageToBlock(pFS, lsmGetU32(&pLast->aData[pFS->nPagesize-4]));
      lsmFsPageRelease(pLast);
    }
  }
  return rc;
}

/*
** This function is only called in compressed database mode.
*/
static int fsReadData(
  FileSystem *pFS,                /* File-system handle */
  i64 iOff,                       /* Read data from this offset */
  u8 *aData,                      /* Buffer to read data into */
  int nData                       /* Number of bytes to read */
){
  i64 iEob;                       /* End of block */
  int nRead;
  int rc;

  assert( pFS->pCompress );

  iEob = fsLastPageOnBlock(pFS, fsPageToBlock(pFS, iOff)) + 1;
  nRead = LSM_MIN(iEob - iOff, nData);

  rc = lsmEnvRead(pFS->pEnv, pFS->fdDb, iOff, aData, nRead);
  if( rc==LSM_OK && nRead!=nData ){
    int iBlk;

    rc = fsBlockNext(pFS, fsPageToBlock(pFS, iOff), &iBlk);
    if( rc==LSM_OK ){
      i64 iOff2 = fsFirstPageOnBlock(pFS, iBlk);
      rc = lsmEnvRead(pFS->pEnv, pFS->fdDb, iOff2, &aData[nRead], nData-nRead);
    }
  }

  return rc;
}

/*
** This function is only called in compressed database mode. It reads and
** uncompresses the compressed data for page pPg from the database and
** populates the pPg->aData[] buffer and pPg->nCompress field.
**
** LSM_OK is returned if successful, or an LSM error code otherwise.
*/
static int fsReadPagedata(
  FileSystem *pFS,                /* File-system handle */
  Page *pPg                       /* Page to read and uncompress data for */
){
  i64 iOff;
  u8 aVarint[9];
  int rc;

  assert( pFS->pCompress && pPg->nCompress==0 );

  iOff = pPg->iPg;
  rc = fsReadData(pFS, iOff, aVarint, sizeof(aVarint));
  if( rc==LSM_OK ){
    iOff += lsmVarintGet32(aVarint, &pPg->nCompress);
    rc = fsReadData(pFS, iOff, pPg->aData, pPg->nCompress);
  }
  return rc;
}

/*
** Return a handle for a database page.
*/
static int fsPageGet(
  FileSystem *pFS,                /* File-system handle */
  Pgno iPg,                       /* Page id */
  int noContent,                  /* True to not load content from disk */
  Page **ppPg                     /* OUT: New page handle */
){
  Page *p;
  int iHash;
  int rc = LSM_OK;

  assert( iPg>=fsFirstPageOnBlock(pFS, 1) );
  *ppPg = 0;

  if( pFS->bUseMmap ){
    i64 iEnd = (i64)iPg * pFS->nPagesize;
    fsGrowMapping(pFS, iEnd, &rc);
    if( rc!=LSM_OK ) return rc;

    if( pFS->pFree ){
      p = pFS->pFree;
      pFS->pFree = p->pHashNext;
      assert( p->nRef==0 );
    }else{
      p = lsmMallocZeroRc(pFS->pEnv, sizeof(Page), &rc);
      if( rc ) return rc;
      fsPageAddToLru(pFS, p);
      p->pFS = pFS;
    }
    p->aData = &((u8 *)pFS->pMap)[pFS->nPagesize * (i64)(iPg-1)];
    p->iPg = iPg;
    if( fsIsLast(pFS, iPg) || fsIsFirst(pFS, iPg) ){
      p->flags = PAGE_SHORT;
    }else{
      p->flags = 0;
    }
    p->nData = pFS->nPagesize - (p->flags & PAGE_SHORT);
  }else{

    /* Search the hash-table for the page */
    iHash = fsHashKey(pFS->nHash, iPg);
    for(p=pFS->apHash[iHash]; p; p=p->pHashNext){
      if( p->iPg==iPg) break;
    }

    if( p==0 ){
      rc = fsPageBuffer(pFS, 1, &p);
      if( rc==LSM_OK ){
        p->iPg = iPg;
        p->nRef = 0;
        p->pFS = pFS;
        assert( p->flags==0 || p->flags==PAGE_FREE );
        if( pFS->pCompress==0 && (fsIsLast(pFS, iPg) || fsIsFirst(pFS, iPg)) ){
          p->flags |= PAGE_SHORT;
        }

#ifdef LSM_DEBUG
        memset(p->aData, 0x56, pFS->nPagesize);
#endif
        assert( p->pLruNext==0 && p->pLruPrev==0 );
        if( noContent==0 ){
          if( pFS->pCompress ){
            rc = fsReadPagedata(pFS, p);
          }else{
            int nByte = pFS->nPagesize;
            i64 iOff = (i64)(iPg-1) * pFS->nPagesize;
            rc = lsmEnvRead(pFS->pEnv, pFS->fdDb, iOff, p->aData, nByte);
          }
          pFS->nRead++;
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
    }else if( p->nRef==0 ){
      fsPageRemoveFromLru(pFS, p);
    }

    assert( (rc==LSM_OK && p) || (rc!=LSM_OK && p==0) );
  }
  if( rc==LSM_OK ){
    pFS->nOut += (p->nRef==0);
    p->nRef++;
  }
  *ppPg = p;
  return rc;
}

/*
** Parameter iBlock is a database file block. This function reads the value 
** stored in the blocks "previous block" pointer and stores it in *piPrev.
** LSM_OK is returned if everything is successful, or an LSM error code
** otherwise.
*/
static int fsBlockPrev(
  FileSystem *pFS,                /* File-system object handle */
  int iBlock,                     /* Read field from this block */
  int *piPrev                     /* OUT: Previous block in linked list */
){
  int rc = LSM_OK;                /* Return code */

  assert( pFS->bUseMmap==0 || pFS->pCompress==0 );
  assert( iBlock>0 );

  if( pFS->pCompress ){
    i64 iOff = (i64)(iBlock-1) * pFS->nBlocksize;
    u8 aPrev[4];                  /* 4-byte pointer read from db file */
    rc = lsmEnvRead(pFS->pEnv, pFS->fdDb, iOff, aPrev, sizeof(aPrev));
    if( rc==LSM_OK ){
      *piPrev = (int)lsmGetU32(aPrev);
    }
  }else{
    assert( 0 );
  }
  return rc;
}


static int fsRunEndsBetween(
  Segment *pRun, 
  Segment *pIgnore, 
  Pgno iFirst, 
  Pgno iLast
){
  return (pRun!=pIgnore && (
        (pRun->iFirst>=iFirst && pRun->iFirst<=iLast)
     || (pRun->iLastPg>=iFirst && pRun->iLastPg<=iLast)
  ));
}

static int fsLevelEndsBetween(
  Level *pLevel, 
  Segment *pIgnore, 
  Pgno iFirst, 
  Pgno iLast
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
    iLastBlk = fsPageToBlock(pFS, pDel->iLastPg);

    /* Mark all blocks currently used by this sorted run as free */
    while( iBlk && rc==LSM_OK ){
      int iNext = 0;
      if( iBlk!=iLastBlk ){
        rc = fsBlockNext(pFS, iBlk, &iNext);
      }else if( bZero==0 && pDel->iLastPg!=fsLastPageOnBlock(pFS, iLastBlk) ){
        break;
      }
      rc = fsFreeBlock(pFS, pSnapshot, pDel, iBlk);
      iBlk = iNext;
    }

    if( bZero ) memset(pDel, 0, sizeof(Segment));
  }
  return LSM_OK;
}

static Pgno firstOnBlock(FileSystem *pFS, int iBlk, Pgno *aPgno, int nPgno){
  Pgno iRet = 0;
  int i;
  for(i=0; i<nPgno; i++){
    Pgno iPg = aPgno[i];
    if( fsPageToBlock(pFS, iPg)==iBlk && (iRet==0 || iPg<iRet) ){
      iRet = iPg;
    }
  }
  return iRet;
}

/*
** Argument aPgno is an array of nPgno page numbers. All pages belong to
** the segment pRun. This function gobbles from the start of the run to the
** first page that appears in aPgno[] (i.e. so that the aPgno[] entry is
** the new first page of the run).
*/
void lsmFsGobble(
  lsm_db *pDb,
  Segment *pRun, 
  Pgno *aPgno,
  int nPgno
){
  int rc = LSM_OK;
  FileSystem *pFS = pDb->pFS;
  Snapshot *pSnapshot = pDb->pWorker;
  int iBlk;

  assert( pRun->nSize>0 );
  iBlk = fsPageToBlock(pFS, pRun->iFirst);
  pRun->nSize += (pRun->iFirst - fsFirstPageOnBlock(pFS, iBlk));

  while( rc==LSM_OK ){
    int iNext = 0;
    Pgno iFirst = firstOnBlock(pFS, iBlk, aPgno, nPgno);
    if( iFirst ){
      pRun->iFirst = iFirst;
      break;
    }
    rc = fsBlockNext(pFS, iBlk, &iNext);
    if( rc==LSM_OK ) rc = fsFreeBlock(pFS, pSnapshot, pRun, iBlk);
    pRun->nSize -= (
        1 + fsLastPageOnBlock(pFS, iBlk) - fsFirstPageOnBlock(pFS, iBlk)
    );
    iBlk = iNext;
  }

  pRun->nSize -= (pRun->iFirst - fsFirstPageOnBlock(pFS, iBlk));
  assert( pRun->nSize>0 );
}

static int fsNextPageOffset(Segment *pSeg, Page *pPg, Pgno *piNext){
  int rc = LSM_OK;                /* Return code */
  FileSystem *pFS = pPg->pFS;
  Pgno iPg = pPg->iPg;
  i64 iEob;
  int nByte;

  assert( pFS->pCompress );

  iEob = 1 + fsLastPageOnBlock(pFS, fsPageToBlock(pFS, iPg));
  nByte = 2 * lsmVarintLen32(pPg->nCompress) + pPg->nCompress;

  if( pSeg && (iPg + nByte)<=iEob && (iPg + nByte - 1)==pSeg->iLastPg ){
    *piNext = 0;
  }else if( (iPg + nByte)>=iEob ){
    int iNext;
    Pgno iNextPg;

    rc = fsBlockNext(pFS, fsPageToBlock(pFS, iPg), &iNext);
    iNextPg = fsFirstPageOnBlock(pFS, iNext) + (nByte - (iEob-iPg));
    if( pSeg && pSeg->iLastPg==(iNextPg-1) ){
      iNextPg = 0;
    }
    *piNext = iNextPg;
  }else{
    *piNext = iPg + nByte;
  }

  return rc;
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
  Pgno iPg = pPg->iPg;

  if( pFS->pCompress ){
    if( eDir<0 ){
      assert( 0 );
    }else{
      int rc = fsNextPageOffset(pRun, pPg, &iPg);
      if( rc!=LSM_OK || iPg==0 ){
        *ppNext = 0;
        return rc;
      }
    }
  }else{
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
      if( pRun && iPg==pRun->iLastPg ){
        *ppNext = 0;
        return LSM_OK;
      }else if( fsIsLast(pFS, iPg) ){
        iPg = lsmGetU32(&pPg->aData[pFS->nPagesize-4]);
      }else{
        iPg++;
      }
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
** Append a page to file iFile. Set the ref-count to 1 and return a pointer
** to it. The page is writable until either lsmFsPagePersist() is called on 
** it or the ref-count drops to zero.
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
  int iPrev = p->iLastPg;

  if( pFS->pCompress ){
    /* In compressed database mode the page is not assigned a page number
    ** or location in the database file at this point. This will be done
    ** by the lsmFsPagePersist() call.  */
    rc = fsPageBuffer(pFS, 1, &pPg);
    if( rc==LSM_OK ){
      pPg->pFS = pFS;
      pPg->pSeg = p;
      pPg->iPg = 0;
      pPg->flags = PAGE_DIRTY;
      pPg->nData = pFS->nPagesize;
      assert( pPg->aData );

      pPg->nRef = 1;
      pFS->nOut++;
    }
  }else{
    if( iPrev==0 ){
      iApp = findAppendPoint(pFS);
    }else if( fsIsLast(pFS, iPrev) ){
      int iNext;
      rc = fsBlockNext(pFS, fsPageToBlock(pFS, iPrev), &iNext);
      if( rc!=LSM_OK ) return rc;
      iApp = fsFirstPageOnBlock(pFS, iNext);
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
      p->iLastPg = iApp;
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
    if( (p->iLastPg % nPagePerBlock)==0 ){
      Page *pLast;
      rc = fsPageGet(pFS, p->iLastPg, 0, &pLast);
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
          aiAppend[i] = p->iLastPg+1;
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
int lsmFsDbPageGet(FileSystem *pFS, Pgno iPg, Page **ppPg){
  assert( pFS );
  return fsPageGet(pFS, iPg, 0, ppPg);
}

static int fsReadReverseVarint32(FileSystem *pFS, Pgno iPg, int *pnVal){
  int rc;
  Pgno iFirst;

  iFirst = fsFirstPageOnBlock(pFS, fsPageToBlock(pFS, iPg));
  if( (iPg - iFirst)<4 ){
    int nRead = 4 + (1 + iPg - iFirst);
    u8 aRead[5 + 4];              /* Space for varint + ptr */
    rc = lsmEnvRead(pFS->pEnv, pFS->fdDb, iFirst-4, aRead, nRead);

  }else{
    u8 aRead[5];                  /* Space for varint + ptr */

    rc = lsmEnvRead(
        pFS->pEnv, pFS->fdDb, iPg+1-sizeof(aRead), aRead, sizeof(aRead)
    );

    if( aRead[4]<=240 ){
      *pnVal = aRead[4];
    }else if( aRead[4]<=248 ){
      *pnVal = 240 + 256 * (aRead[4]-241) + aRead[3];
    }else{
      *pnVal = ((int)(aRead[1])<<16) + ((int)(aRead[2])<<8) + (int)(aRead[3]);
      if( aRead[4]==250 ) *pnVal += (((int)aRead[0]) << 24);
    }

  }
  return rc;
}

/*
** Obtain a reference to the last page in the segment passed as the 
** second argument.
*/
int lsmFsDbPageLast(FileSystem *pFS, Segment *pSeg, Page **ppPg){
  Pgno iLast = pSeg->iLastPg;
  if( pFS->pCompress ){
    int nCompress;
    rc = fsReadReverseVarint32(pFS, iLast, &nCompress);
  }
  return fsPageGet(pFS, iLast, 0, ppPg);
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
** Return true if page is currently writable.
*/
int lsmFsPageWritable(Page *pPg){
  return (pPg->flags & PAGE_DIRTY) ? 1 : 0;
}

/*
** Append raw data to a segment. This function is only used in compressed
** database mode.
*/
static Pgno fsAppendData(
  FileSystem *pFS,                /* File-system handle */
  Segment *pSeg,                  /* Segment to append to */
  const u8 *aData,                /* Buffer containing data to write */
  int nData,                      /* Size of buffer aData[] in bytes */
  int *pRc                        /* IN/OUT: Error code */
){
  Pgno iRet = 0;
  int rc = *pRc;
  assert( pFS->pCompress );
  if( rc==LSM_OK ){
    int nRem;
    int nWrite;
    Pgno iApp = pSeg->iLastPg+1;

    /* If this is the first data written into the segment, find an append-point
    ** or allocate a new block.  */
    if( iApp==1 ){
      pSeg->iFirst = iApp = findAppendPoint(pFS);
      if( iApp==0 ){
        int iBlk;
        rc = lsmBlockAllocate(pFS->pDb, &iBlk);
        pSeg->iFirst = iApp = fsFirstPageOnBlock(pFS, iBlk);
      }
    }

    iRet = iApp;

    /* Write as much data as is possible at iApp (usually all of it). */
    if( rc==LSM_OK ){
      int nSpace = fsLastPageOnBlock(pFS, fsPageToBlock(pFS, iApp)) - iApp + 1;
      nWrite = LSM_MIN(nData, nSpace);
      nRem = nData - nWrite;
      rc = lsmEnvWrite(pFS->pEnv, pFS->fdDb, iApp, aData, nWrite);

      iApp += nWrite;
    }

    /* If required, allocate a new block and write the rest of the data
    ** into it. Set the next and previous block pointers to link the new
    ** block to the old.  */
    if( rc==LSM_OK && nRem ){
      u8 aPtr[4];                 /* Space to serialize a u32 */
      int iBlk;                   /* New block number */

      /* Allocate a new block. */
      rc = lsmBlockAllocate(pFS->pDb, &iBlk);

      /* Set the "next" pointer on the old block */
      if( rc==LSM_OK ){
        assert( iApp==(fsPageToBlock(pFS, iApp)*pFS->nBlocksize)-4 );
        lsmPutU32(aPtr, iBlk);
        rc = lsmEnvWrite(pFS->pEnv, pFS->fdDb, iApp, aPtr, sizeof(aPtr));
      }

      /* Set the "prev" pointer on the new block */
      if( rc==LSM_OK ){
        lsmPutU32(aPtr, fsPageToBlock(pFS, iApp));
        iApp = fsFirstPageOnBlock(pFS, iBlk);
        rc = lsmEnvWrite(pFS->pEnv, pFS->fdDb, iApp-4, aPtr, sizeof(aPtr));
      }

      /* Write the remaining data into the new block */
      if( rc==LSM_OK ){
        rc = lsmEnvWrite(pFS->pEnv, pFS->fdDb, iApp, &aData[nWrite], nRem);
        iApp += nRem;
      }
    }

    pSeg->iLastPg = iApp-1;
    *pRc = rc;
  }

  return iRet;
}

/*
** This function is only called in compressed database mode. It 
** compresses the contents of page pPg and writes the result to the 
** buffer at pFS->aBuffer. The size of the compressed data is stored in
** pPg->nCompress.
**
** If buffer pFS->aBuffer[] has not been allocated then this function
** allocates it. If this fails, LSM_NOMEM is returned. Otherwise, LSM_OK.
*/
static int fsCompressIntoBuffer(FileSystem *pFS, Page *pPg){
  /* TODO: Fill in a real version of this function */

  if( pFS->aBuffer==0 ){
    pFS->aBuffer = lsmMalloc(pFS->pEnv, pFS->nPagesize);
    if( pFS->aBuffer==0 ) return LSM_NOMEM_BKPT;
  }

  assert( pPg->nData==pFS->nPagesize );
  memcpy(pFS->aBuffer, pPg->aData, pFS->nPagesize);
  pPg->nCompress = pFS->nPagesize;
  return LSM_OK;
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

    if( pFS->pCompress ){
      int iHash;                  /* Hash key of assigned page number */
      u8 aVarint[10];             /* pPg->nCompress as a varint */
      int nVarint;                /* Length of varint stored in aVarint[] */
      assert( pPg->pSeg && pPg->iPg==0 && pPg->nCompress==0 );

      /* Compress the page image. */
      rc = fsCompressIntoBuffer(pFS, pPg);

      /* Serialize the compressed size into buffer aVarint[] */
      nVarint = lsmVarintPut64(aVarint, pPg->nCompress);
      aVarint[nVarint] = aVarint[0];

      /* Write the serialized page record into the database file. */
      pPg->iPg = fsAppendData(pFS, pPg->pSeg, aVarint, nVarint, &rc);
      fsAppendData(pFS, pPg->pSeg, pFS->aBuffer, pPg->nCompress, &rc);
      fsAppendData(pFS, pPg->pSeg, &aVarint[1], nVarint, &rc);

      /* Now that it has a page number, insert the page into the hash table */
      iHash = fsHashKey(pFS->nHash, pPg->iPg);
      pPg->pHashNext = pFS->apHash[iHash];
      pFS->apHash[iHash] = pPg;

      pPg->pSeg->nSize += (nVarint * 2) + pPg->nCompress;

    }else{
      i64 iOff;                   /* Offset to write within database file */
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

      if( pFS->bUseMmap ){
        pPg->pHashNext = pFS->pFree;
        pFS->pFree = pPg;
      }else{
        assert( pPg->pLruNext==0 );
        assert( pPg->pLruPrev==0 );
        fsPageRemoveFromHash(pFS, pPg);
        fsPageBufferFree(pPg);
      }
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
    iLastBlk = fsPageToBlock(pFS, pArray->iLastPg);

    lsmStringInit(&str, pDb->pEnv);
    lsmStringAppendf(&str, "%d", pArray->iFirst);
    while( iBlk!=iLastBlk ){
      lsmStringAppendf(&str, " %d", fsLastPageOnBlock(pFS, iBlk));
      fsBlockNext(pFS, iBlk, &iBlk);
      lsmStringAppendf(&str, " %d", fsFirstPageOnBlock(pFS, iBlk));
    }
    lsmStringAppendf(&str, " %d", pArray->iLastPg);

    *pzOut = str.z;
  }

  if( bUnlock ){
    int rcwork = LSM_BUSY;
    lsmFinishWork(pDb, 0, 0, &rcwork);
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
    if( pSeg && pSeg->nSize>0 ){
      const int nPagePerBlock = (pFS->nBlocksize / pFS->nPagesize);

      int iBlk;
      int iLastBlk;
      iBlk = fsPageToBlock(pFS, pSeg->iFirst);
      iLastBlk = fsPageToBlock(pFS, pSeg->iLastPg);

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

      if( bExtra && (pSeg->iLastPg % nPagePerBlock)==0 ){
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
  FileSystem *pFS = pDb->pFS;
  if( pFS->pCompress==0 ){
    int i;
    int j;
    Freelist freelist = {0, 0, 0};
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

    if( pWorker->nFreelistOvfl ){
      int rc = lsmCheckpointOverflowLoad(pDb, &freelist);
      assert( rc==LSM_OK || rc==LSM_NOMEM );
      if( rc!=LSM_OK ) return 1;
    }

    for(j=0; j<2; j++){
      Freelist *pFreelist;
      if( j==0 ) pFreelist = &pWorker->freelist;
      if( j==1 ) pFreelist = &freelist;

      for(i=0; i<pFreelist->nEntry; i++){
        u32 iBlk = pFreelist->aEntry[i].iBlk;
        assert( iBlk<=nBlock );
        assert( aUsed[iBlk-1]==0 );
        aUsed[iBlk-1] = 1;
      }
    }

    for(i=0; i<nBlock; i++) assert( aUsed[i]==1 );

    lsmFree(pDb->pEnv, aUsed);
    lsmFree(pDb->pEnv, freelist.aEntry);
  }

  return 1;
}

#ifndef NDEBUG
/*
** Return true if pPg happens to be the last page in segment pSeg. Or false
** otherwise. This function is only invoked as part of assert() conditions.
*/
int lsmFsDbPageIsLast(Segment *pSeg, Page *pPg){
  if( pPg->pFS->pCompress ){
    Pgno iNext = 0;
    int rc = fsNextPageOffset(pSeg, pPg, &iNext);
    return (rc!=LSM_OK || iNext==0);
  }
  return (pPg->iPg==pSeg->iLastPg);
}
#endif
