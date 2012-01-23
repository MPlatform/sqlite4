/*
** 2007 August 22
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
** This file implements a special kind of sqlite4_file object used
** by SQLite to create journal files if the atomic-write optimization
** is enabled.
**
** The distinctive characteristic of this sqlite4_file is that the
** actual on disk file is created lazily. When the file is created,
** the caller specifies a buffer size for an in-memory buffer to
** be used to service read() and write() requests. The actual file
** on disk is not created or populated until either:
**
**   1) The in-memory representation grows too large for the allocated 
**      buffer, or
**   2) The sqlite4JournalCreate() function is called.
*/
#ifdef SQLITE_ENABLE_ATOMIC_WRITE
#include "sqliteInt.h"


/*
** A JournalFile object is a subclass of sqlite4_file used by
** as an open file handle for journal files.
*/
struct JournalFile {
  sqlite4_io_methods *pMethod;    /* I/O methods on journal files */
  int nBuf;                       /* Size of zBuf[] in bytes */
  char *zBuf;                     /* Space to buffer journal writes */
  int iSize;                      /* Amount of zBuf[] currently used */
  int flags;                      /* xOpen flags */
  sqlite4_vfs *pVfs;              /* The "real" underlying VFS */
  sqlite4_file *pReal;            /* The "real" underlying file descriptor */
  const char *zJournal;           /* Name of the journal file */
};
typedef struct JournalFile JournalFile;

/*
** If it does not already exists, create and populate the on-disk file 
** for JournalFile p.
*/
static int createFile(JournalFile *p){
  int rc = SQLITE_OK;
  if( !p->pReal ){
    sqlite4_file *pReal = (sqlite4_file *)&p[1];
    rc = sqlite4OsOpen(p->pVfs, p->zJournal, pReal, p->flags, 0);
    if( rc==SQLITE_OK ){
      p->pReal = pReal;
      if( p->iSize>0 ){
        assert(p->iSize<=p->nBuf);
        rc = sqlite4OsWrite(p->pReal, p->zBuf, p->iSize, 0);
      }
    }
  }
  return rc;
}

/*
** Close the file.
*/
static int jrnlClose(sqlite4_file *pJfd){
  JournalFile *p = (JournalFile *)pJfd;
  if( p->pReal ){
    sqlite4OsClose(p->pReal);
  }
  sqlite4_free(p->zBuf);
  return SQLITE_OK;
}

/*
** Read data from the file.
*/
static int jrnlRead(
  sqlite4_file *pJfd,    /* The journal file from which to read */
  void *zBuf,            /* Put the results here */
  int iAmt,              /* Number of bytes to read */
  sqlite_int64 iOfst     /* Begin reading at this offset */
){
  int rc = SQLITE_OK;
  JournalFile *p = (JournalFile *)pJfd;
  if( p->pReal ){
    rc = sqlite4OsRead(p->pReal, zBuf, iAmt, iOfst);
  }else if( (iAmt+iOfst)>p->iSize ){
    rc = SQLITE_IOERR_SHORT_READ;
  }else{
    memcpy(zBuf, &p->zBuf[iOfst], iAmt);
  }
  return rc;
}

/*
** Write data to the file.
*/
static int jrnlWrite(
  sqlite4_file *pJfd,    /* The journal file into which to write */
  const void *zBuf,      /* Take data to be written from here */
  int iAmt,              /* Number of bytes to write */
  sqlite_int64 iOfst     /* Begin writing at this offset into the file */
){
  int rc = SQLITE_OK;
  JournalFile *p = (JournalFile *)pJfd;
  if( !p->pReal && (iOfst+iAmt)>p->nBuf ){
    rc = createFile(p);
  }
  if( rc==SQLITE_OK ){
    if( p->pReal ){
      rc = sqlite4OsWrite(p->pReal, zBuf, iAmt, iOfst);
    }else{
      memcpy(&p->zBuf[iOfst], zBuf, iAmt);
      if( p->iSize<(iOfst+iAmt) ){
        p->iSize = (iOfst+iAmt);
      }
    }
  }
  return rc;
}

/*
** Truncate the file.
*/
static int jrnlTruncate(sqlite4_file *pJfd, sqlite_int64 size){
  int rc = SQLITE_OK;
  JournalFile *p = (JournalFile *)pJfd;
  if( p->pReal ){
    rc = sqlite4OsTruncate(p->pReal, size);
  }else if( size<p->iSize ){
    p->iSize = size;
  }
  return rc;
}

/*
** Sync the file.
*/
static int jrnlSync(sqlite4_file *pJfd, int flags){
  int rc;
  JournalFile *p = (JournalFile *)pJfd;
  if( p->pReal ){
    rc = sqlite4OsSync(p->pReal, flags);
  }else{
    rc = SQLITE_OK;
  }
  return rc;
}

/*
** Query the size of the file in bytes.
*/
static int jrnlFileSize(sqlite4_file *pJfd, sqlite_int64 *pSize){
  int rc = SQLITE_OK;
  JournalFile *p = (JournalFile *)pJfd;
  if( p->pReal ){
    rc = sqlite4OsFileSize(p->pReal, pSize);
  }else{
    *pSize = (sqlite_int64) p->iSize;
  }
  return rc;
}

/*
** Table of methods for JournalFile sqlite4_file object.
*/
static struct sqlite4_io_methods JournalFileMethods = {
  1,             /* iVersion */
  jrnlClose,     /* xClose */
  jrnlRead,      /* xRead */
  jrnlWrite,     /* xWrite */
  jrnlTruncate,  /* xTruncate */
  jrnlSync,      /* xSync */
  jrnlFileSize,  /* xFileSize */
  0,             /* xLock */
  0,             /* xUnlock */
  0,             /* xCheckReservedLock */
  0,             /* xFileControl */
  0,             /* xSectorSize */
  0,             /* xDeviceCharacteristics */
  0,             /* xShmMap */
  0,             /* xShmLock */
  0,             /* xShmBarrier */
  0              /* xShmUnmap */
};

/* 
** Open a journal file.
*/
int sqlite4JournalOpen(
  sqlite4_vfs *pVfs,         /* The VFS to use for actual file I/O */
  const char *zName,         /* Name of the journal file */
  sqlite4_file *pJfd,        /* Preallocated, blank file handle */
  int flags,                 /* Opening flags */
  int nBuf                   /* Bytes buffered before opening the file */
){
  JournalFile *p = (JournalFile *)pJfd;
  memset(p, 0, sqlite4JournalSize(pVfs));
  if( nBuf>0 ){
    p->zBuf = sqlite4MallocZero(nBuf);
    if( !p->zBuf ){
      return SQLITE_NOMEM;
    }
  }else{
    return sqlite4OsOpen(pVfs, zName, pJfd, flags, 0);
  }
  p->pMethod = &JournalFileMethods;
  p->nBuf = nBuf;
  p->flags = flags;
  p->zJournal = zName;
  p->pVfs = pVfs;
  return SQLITE_OK;
}

/*
** If the argument p points to a JournalFile structure, and the underlying
** file has not yet been created, create it now.
*/
int sqlite4JournalCreate(sqlite4_file *p){
  if( p->pMethods!=&JournalFileMethods ){
    return SQLITE_OK;
  }
  return createFile((JournalFile *)p);
}

/* 
** Return the number of bytes required to store a JournalFile that uses vfs
** pVfs to create the underlying on-disk files.
*/
int sqlite4JournalSize(sqlite4_vfs *pVfs){
  return (pVfs->szOsFile+sizeof(JournalFile));
}
#endif
