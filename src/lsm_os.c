/*
** 2011-12-03
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
** Operating-system interface for LSM.
*/
#include "lsm.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <string.h>

#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include <ctype.h>

#include <unistd.h>

typedef struct PosixFile PosixFile;

struct PosixFile {
  lsm_env *pEnv;
  int fd;
};

static int lsm_ioerr(void){ return LSM_IOERR; }

static int posixOsOpen(
  lsm_env *pEnv,
  const char *zFile, 
  lsm_file **ppFile
){
  int rc = LSM_OK;
  PosixFile *p;

  p = lsm_malloc(pEnv, sizeof(PosixFile));
  if( p==0 ){
    rc = LSM_NOMEM;
  }else{
    p->pEnv = pEnv;
    p->fd = open(zFile, O_RDWR|O_CREAT, 0644);
    if( p->fd<0 ){
      lsm_free(pEnv, p);
      p = 0;
      rc = lsm_ioerr();
    }
  }

  *ppFile = (lsm_file *)p;
  return rc;
}

static int posixOsWrite(
  lsm_file *pFile,                /* File to write to */
  lsm_i64 iOff,                   /* Offset to write to */
  void *pData,                    /* Write data from this buffer */
  int nData                       /* Bytes of data to write */
){
  int rc = LSM_OK;
  PosixFile *p = (PosixFile *)pFile;
  off_t offset;

  offset = lseek(p->fd, (off_t)iOff, SEEK_SET);
  if( offset!=iOff ){
    rc = lsm_ioerr();
  }else{
    ssize_t prc = write(p->fd, pData, (size_t)nData);
    if( prc<0 ) rc = lsm_ioerr();
  }

  return rc;
}

static int posixOsRead(
  lsm_file *pFile,                /* File to read from */
  lsm_i64 iOff,                   /* Offset to read from */
  void *pData,                    /* Read data into this buffer */
  int nData                       /* Bytes of data to read */
){
  int rc = LSM_OK;
  PosixFile *p = (PosixFile *)pFile;
  off_t offset;

  offset = lseek(p->fd, (off_t)iOff, SEEK_SET);
  if( offset!=iOff ){
    rc = lsm_ioerr();
  }else{
    ssize_t prc = read(p->fd, pData, (size_t)nData);
    if( prc<0 ) rc = lsm_ioerr();
  }

  return rc;
}

static int posixOsSync(lsm_file *pFile){
  int rc = LSM_OK;

#ifndef LSM_NO_SYNC
  PosixFile *p = (PosixFile *)pFile;
  int prc;

  prc = fdatasync(p->fd);
  if( prc<0 ) rc = lsm_ioerr();
#else
  (void)pFile;
#endif

  return rc;
}

static int posixOsClose(lsm_file *pFile){
  PosixFile *p = (PosixFile *)pFile;
  close(p->fd);
  lsm_free(p->pEnv, p);
  return LSM_OK;
}

lsm_vfs *lsm_default_vfs(void){
  static lsm_vfs posix_vfs = {
    posixOsOpen,
    posixOsRead,
    posixOsWrite,
    posixOsSync,
    posixOsClose
  };
  return &posix_vfs;
}
