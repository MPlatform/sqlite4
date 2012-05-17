/*
** 2011-08-13
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
** This file contains the implementation of LSM database logging. Logging
** has one purpose in LSM - to make transactions durable.
**
** When data is written to an LSM database, it is initially stored in an
** in-memory tree structure. Since this structure is in volatile memory,
** if a power failure or application crash occurs, it may be lost. To
** prevent loss of data in this case, each time a record is written to the
** in-memory tree, an equivalent record is appended to the log on disk.
** If a power failure or application crash does occur, data can be recovered
** by reading the log.
**
** A log file consists of the following types of records representing data
** written into the database:
**
**   LOG_WRITE:  A key-value pair written to the database.
**   LOG_DELETE: A delete key issued to the database.
**   LOG_COMMIT: A transaction commit.
**
** And the following types of records for ancillary purposes..
**
**   LOG_CKSUM:  A record containing a checksum.
**   LOG_EOF:    A record indicating the end of a log file.
**   LOG_PAD1:   A single byte padding record.
**   LOG_PAD2:   An N byte padding record (N>1).
**
** Each transaction written to the log contains one or more LOG_WRITE and/or
** LOG_DELETE records, followed by a LOG_COMMIT record. The LOG_COMMIT record
** contains an 8-byte checksum based on all previous data written to the
** log file.
**
** LOG CHECKSUMS & RECOVERY
**
**   Checksums are found in two types of log records: LOG_COMMIT and
**   LOG_CKSUM records. In order to recover content from a log, a client
**   reads each record from the start of the log, calculating a checksum as
**   it does. Each time a LOG_COMMIT or LOG_CKSUM is encountered, the 
**   recovery process verifies that the checksum stored in the log 
**   matches the calculated checksum. If it does not, the recovery process
**   can stop reading the log.
**
**   If a recovery process reads records (other than COMMIT or CKSUM) 
**   consisting of at least LSM_CKSUM_MAXDATA bytes, then the next record in
**   the log must be either a LOG_CKSUM or LOG_COMMIT record. If it is
**   not, the recovery process also stops reading the log.
**
**   To recover the log file, it must be read twice. The first time to 
**   determine the location of the last valid commit record. And the second
**   time to load data into the in-memory tree.
**
**   Todo: Surely there is a better way...
**
** LOG WRAPPING
**
**   If the log file were never deleted or wrapped, it would be possible to
**   read it from start to end each time is required recovery (i.e each time
**   the number of database clients changes from 0 to 1). Effectively reading
**   the entire history of the database each time. This would quickly become 
**   inefficient.
**
**   Instead, part of each checkpoint written into the database file contains 
**   a log file offset (and the current value of the log file checksum at that 
**   offset) at which to begin recovery. Offset $O.
**
**   Once a checkpoint has been written and synced into the database file, it
**   is guaranteed that no recovery process will need to read any data before
**   offset $O of the log file. It is therefore safe to begin overwriting
**   any data that occurs before offset $O.
**
**   This implementation uses two files on disk to store the log. File B and
**   file C. First, file B is written to until it reaches some configured
**   limit (say 1MB). It appends a LOG_EOF to file B and the log continues 
**   in file C. Once $O reaches an offset high enough to correspond to file 
**   C, writing switches back to file B. And so on.
**
**   Offsets are 64-bit numbers. The least-significant bit is set to 0 for
**   file B and 1 for file C. The offset within the file is ($offset/2).
**   It doesn't matter that this may result in non-increasing offsets for
**   a log file. 
**
** LOG_PAD1 & LOG_PAD2 RECORDS
**
**   PAD1 and PAD2 records may appear in a log file at any point. They allow
**   a process writing the log file align the beginning of transactions with 
**   the beginning of disk sectors, which increases robustness.
**
** RECORD FORMATS:
**
**   LOG_PAD1:   * A single 0x01 byte.
**
**   LOG_PAD2:   * A single 0x02 byte, followed by
**               * The number of unused bytes (N) as a varint,
**               * An N byte block of unused space.
**
**   LOG_WRITE:  * A single 0x03 byte, 
**               * The number of bytes in the key, encoded as a varint, 
**               * The key data,
**               * The number of bytes in the value, encoded as a varint, 
**               * The value data.
**
**   LOG_DELETE: * A single 0x04 byte, 
**               * The number of bytes in the key, encoded as a varint, 
**               * The key data.
**
**   LOG_COMMIT: * A single 0x05 byte.
**               * An 8-byte checksum.
**
**   LOG_CKSUM:  * A single 0x06 byte.
**               * An 8-byte checksum.
**
**   LOG_EOF:    * A single 0x07 byte.
**
**   Varints are as described in lsm_varint.c (SQLite 4 format).
**
** CHECKSUMS:
**
**   The checksum is calculated using two 32-bit unsigned integers, s0 and
**   s1. The initial value for both is 42. It is updated each time a record
**   is written into the log file by treating the encoded (binary) record as 
**   an array of 32-bit little-endian integers. Then, if x[] is the integer
**   array, updating the checksum accumulators as follows:
**
**     for i from 0 to n-1 step 2:
**       s0 += x[i] + s1;
**       s1 += x[i+1] + s0;
**     endfor
**
**   If the record is not an even multiple of 8-bytes in size it is padded
**   with zeroes to make it so before the checksum is updated.
**
**   The checksum stored in a CKSUM or COMMIT record is based on all records
**   that appear in the file before it, but not the contents of the CKSUM or
**   COMMIT itself.
**
** VARINT FORMAT
**
** See s4_varint.c.
*/


#ifndef _LSM_INT_H
# include "lsmInt.h"
#endif

/* Log record types */
#define LSM_LOG_EOF      0x00
#define LSM_LOG_PAD1     0x01
#define LSM_LOG_PAD2     0x02
#define LSM_LOG_WRITE    0x03
#define LSM_LOG_DELETE   0x04
#define LSM_LOG_COMMIT   0x05
#define LSM_LOG_CKSUM    0x06

/* Require a checksum every 32KB. */
/* #define LSM_CKSUM_MAXDATA (32*1024) */
#define LSM_CKSUM_MAXDATA 256

/* Initial values for checksum (required only if there is no checkpoint
** at all in the database file).  */
#define LSM_CKSUM0_INIT 42
#define LSM_CKSUM1_INIT 42

struct DbLog {
  i64 iOff;                       /* Offset into log (see lsm_log.c) */
  u32 cksum0;                     /* Checksum 0 at offset iOff */
  u32 cksum1;                     /* Checksum 1 at offset iOff */
  LsmString buf;                  /* Buffer containing data not yet written */
};

/*
** The logging sub-system may be enabled or disabled on a per-database
** basis (per-database, not per-connection). This function returns true if
** the logging sub-system is enabled, or false otherwise.
**
** If the logging sub-system is disabled, the following functions are no-ops:
**
**     lsmLogWrite()
**     lsmLogCommit()
*/
static int logFileEnabled(lsm_db *pDb){
  return 1;
}

/*
** Return a pointer to the DbLog object associated with connection pDb.
** Allocate and initialize it if necessary.
*/
static DbLog *logGetHandle(lsm_db *pDb, int *pRc){
  if( pDb->pDbLog==0 ){
    DbLog *pLog;
    pLog = (DbLog *)lsmMallocZeroRc(pDb->pEnv, sizeof(DbLog), pRc);
    if( pLog ){
      pLog->cksum0 = LSM_CKSUM0_INIT;
      pLog->cksum1 = LSM_CKSUM1_INIT;
      lsmStringInit(&pLog->buf, pDb->pEnv);
    }
    pDb->pDbLog = pLog;
  }

  return pDb->pDbLog;
}

void lsmLogClose(lsm_db *pDb){
  DbLog *pLog = pDb->pDbLog;
  if( pLog ){
    lsmStringClear(&pLog->buf);
    lsmFree(pDb->pEnv, pLog);
    pDb->pDbLog = 0;
  }
}

/*
** Return the result of interpreting the first 4 bytes in buffer aIn as 
** a 32-bit unsigned little-endian integer.
*/
static u32 getU32le(u8 *aIn){
  return ((u32)aIn[3] << 24) 
       + ((u32)aIn[2] << 16) 
       + ((u32)aIn[1] << 8) 
       + ((u32)aIn[0]);
}

/*
** This function is used to calculate log checksums.
*/
void logCksum(
  const char *a,                  /* Input buffer */
  int n,                          /* Size of input buffer in bytes */
  u32 *pCksum0,                   /* IN/OUT: Checksum value 1 */
  u32 *pCksum1                    /* IN/OUT: Checksum value 2 */
){
  u32 cksum0 = *pCksum0;
  u32 cksum1 = *pCksum1;
  u32 *aIn = (u32 *)a;
  int nIn = ((n+7)/8) * 2;
  int i;

  /* Check that the input buffer is aligned to an 8-byte boundary. Also
  ** that if the input is not an integer multiple of 8 bytes in size, it
  ** has been padded with 0x00 bytes.  */
  /* assert( EIGHT_BYTE_ALIGNMENT(a) ); */
  assert( nIn*4==n || memcmp("\00\00\00\00\00\00\00", &a[n], nIn*4-n) );

  for(i=0; i<nIn; i+=2){
    cksum0 += aIn[0] + cksum1;
    cksum1 += aIn[1] + cksum0;
  }

  *pCksum0 = cksum0;
  *pCksum1 = cksum1;
}

/*
** This function is the same as logCksum(), except that pointer "a" need
** not be aligned to an 8-byte boundary or padded with zero bytes. This
** version is slower, but sometimes more convenient to use.
*/
void logCksumUnaligned(
  char *z,                        /* Input buffer */
  int n,                          /* Size of input buffer in bytes */
  u32 *pCksum0,                   /* IN/OUT: Checksum value 1 */
  u32 *pCksum1                    /* IN/OUT: Checksum value 2 */
){
  u8 *a = (u8 *)z;
  u32 cksum0 = *pCksum0;
  u32 cksum1 = *pCksum1;
  int nIn = (n/8) * 8;
  int i;

  assert( n>0 );
  for(i=0; i<nIn; i+=8){
    cksum0 += getU32le(&a[i]) + cksum1;
    cksum1 += getU32le(&a[i+4]) + cksum0;
  }

  if( nIn!=n ){
    u8 aBuf[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    assert( (n-nIn)<8 && n>nIn );
    memcpy(aBuf, &a[nIn], n-nIn);
    cksum0 += getU32le(aBuf) + cksum1;
    cksum1 += getU32le(&aBuf[4]) + cksum0;
  }

  *pCksum0 = cksum0;
  *pCksum1 = cksum1;
}


/*
** Write the contents of the log-buffer to disk. Then write either a CKSUM
** or COMMIT record, depending on the value of parameter eType.
*/
static int logFlush(lsm_db *pDb, int eType){
  int rc;
  DbLog *pLog = pDb->pDbLog;
  
#if 0
printf("write type %d\n", eType);
fflush(stdout);
#endif

  assert( eType==LSM_LOG_COMMIT || eType==LSM_LOG_CKSUM );
  assert( pLog );

  /* Make sure there is room in the log-buffer to add the CKSUM or COMMIT
  ** record. Then add the first byte of it.  */
  rc = lsmStringExtend(&pLog->buf, 9);
  if( rc!=LSM_OK ) return rc;
  pLog->buf.z[pLog->buf.n++] = eType;
  memset(&pLog->buf.z[pLog->buf.n], 0, 8);

  /* Calculate the checksum value. Append it to the buffer*/
#if 0
  logCksum(pLog->buf.z, ROUND8(pLog->buf.n), &pLog->cksum0, &pLog->cksum1);
#else
  logCksumUnaligned(pLog->buf.z, pLog->buf.n, &pLog->cksum0, &pLog->cksum1);
#endif

  lsmPutU32((u8 *)&pLog->buf.z[pLog->buf.n], pLog->cksum0);
  pLog->buf.n += 4;
  lsmPutU32((u8 *)&pLog->buf.z[pLog->buf.n], pLog->cksum1);
  pLog->buf.n += 4;

  /* Write the contents of the buffer to disk. */
  rc = lsmFsWriteLog(pDb->pFS, pLog->iOff, &pLog->buf);
  pLog->iOff += pLog->buf.n;
  pLog->buf.n = 0;

  /* If this is a commit and synchronous=full, sync the log to disk. */
  if( rc==LSM_OK && eType==LSM_LOG_COMMIT && pDb->eSafety==LSM_SAFETY_FULL ){
    rc = lsmFsSyncLog(pDb->pFS);
  }
  return rc;
}

/*
** Append an LSM_LOG_WRITE (if nVal>=0) or LSM_LOG_DELETE (if nVal<0) 
** record to the database log.
*/
int lsmLogWrite(
  lsm_db *pDb,                    /* Database handle */
  void *pKey, int nKey,           /* Database key to write to log */
  void *pVal, int nVal            /* Database value (or nVal<0) to write */
){
  int rc = LSM_OK;
  DbLog *pLog;                    /* Log object to write to */

#if 0
printf("write type %d\n", LSM_LOG_WRITE);
fflush(stdout);
#endif

  pLog = logGetHandle(pDb, &rc);
  if( pLog ){
    int nReq;
    nReq = 1 + lsmVarintLen32(nKey) + nKey;
    if( nVal>=0 ) nReq += lsmVarintLen32(nVal) + nVal;
    rc = lsmStringExtend(&pLog->buf, nReq);
    if( rc==LSM_OK ){
      u8 *a = (u8 *)&pLog->buf.z[pLog->buf.n];
      *(a++) = (nVal>=0 ? LSM_LOG_WRITE : LSM_LOG_DELETE);
      a += lsmVarintPut32(a, nKey);
      memcpy(a, pKey, nKey);
      a += nKey;
      if( nVal>=0 ){
        a += lsmVarintPut32(a, nVal);
        memcpy(a, pVal, nVal);
      }
      pLog->buf.n += nReq;
      assert( pLog->buf.n<=pLog->buf.nAlloc );
      if( pLog->buf.n>=LSM_CKSUM_MAXDATA ){
        rc = logFlush(pDb, LSM_LOG_CKSUM);
        assert( rc!=LSM_OK || pLog->buf.n==0 );
      }
    }
  }

  return rc;
}

/*
** Append an LSM_LOG_COMMIT record to the database log.
*/
int lsmLogCommit(lsm_db *pDb){
  int rc = LSM_OK;
  if( pDb->pDbLog ){
    rc = logFlush(pDb, LSM_LOG_COMMIT);
  }
  return rc;
}

void lsmLogTell(
  lsm_db *pDb,                    /* Database handle */
  LogMark *pMark                  /* Populate this object with current offset */
){
  DbLog *pLog = pDb->pDbLog;
  if( pLog ){
    pMark->iOff = pLog->iOff;
    pMark->nBuf = pLog->buf.n;
    pMark->cksum0 = pLog->cksum0;
    pMark->cksum1 = pLog->cksum1;
  }else{
    pMark->iOff = 0;
    pMark->nBuf = 0;
    pMark->cksum0 = LSM_CKSUM0_INIT;
    pMark->cksum1 = LSM_CKSUM1_INIT;
  }
}

int lsmLogSeek(
  lsm_db *pDb,                    /* Database handle */
  LogMark *pMark                  /* Object containing log offset to seek to */
){
  DbLog *pLog = pDb->pDbLog;      /* Database log handle */
  int rc;                         /* This functions return code */

  assert( pLog );
  assert( pLog->iOff!=pMark->iOff || pLog->cksum0==pMark->cksum0 );
  assert( pLog->iOff!=pMark->iOff || pLog->cksum1==pMark->cksum1 );

  if( pLog->iOff==pMark->iOff ){
    pLog->buf.n = pMark->nBuf;
    rc = LSM_OK;
  }else{
    pLog->iOff = pMark->iOff;
    pLog->cksum0 = pMark->cksum0;
    pLog->cksum1 = pMark->cksum1;
    pLog->buf.n = 0;
    rc = lsmFsReadLog(pDb->pFS, pLog->iOff, pMark->nBuf, &pLog->buf);
  }

  return rc;
}


/*************************************************************************
** Begin code for log recovery.
*/

typedef struct LogReader LogReader;
struct LogReader {
  FileSystem *pFS;                /* File system to read from */
  i64 iOff;                       /* File offset at end of buf content */
  int iBuf;                       /* Current read offset in buf */
  LsmString buf;                  /* Buffer containing file content */

  int iCksumBuf;                  /* Offset in buf corresponding to cksum[01] */
  u32 cksum0;                     /* Checksum 0 at offset iCksumBuf */
  u32 cksum1;                     /* Checksum 1 at offset iCksumBuf */
};

static int logReaderBlob(
  LogReader *p,                   /* Log reader object */
  LsmString *pBuf,                /* Dynamic storage, if required */
  int nBlob,                      /* Number of bytes to read */
  u8 **ppBlob                     /* OUT: Pointer to blob read */
){
  static const int LOG_READ_SIZE = 512;
  int rc = LSM_OK;                /* Return code */
  int nReq = nBlob;               /* Bytes required */

  while( nReq>0 ){
    int nAvail;                   /* Bytes of data available in p->buf */
    if( p->buf.n==p->iBuf ){
      int nCksum;                 /* Total bytes requiring checksum */
      int nCarry = 0;             /* Total bytes requiring checksum */

      nCksum = p->iBuf - p->iCksumBuf;
      nCarry = nCksum % 8;
      nCksum = ((nCksum / 8) * 8);

      if( nCksum>0 ){
        logCksumUnaligned(
            &p->buf.z[p->iCksumBuf], (nCksum/8)*8, &p->cksum0, &p->cksum1
        );
        if( nCarry>0 ) memcpy(p->buf.z, &p->buf.z[p->iBuf-nCarry], nCarry);
      }
      p->buf.n = nCarry;
      p->iBuf = nCarry;

      rc = lsmFsReadLog(p->pFS, p->iOff, LOG_READ_SIZE, &p->buf);
      if( rc!=LSM_OK ) break;

      p->iCksumBuf = 0;
      p->iOff += LOG_READ_SIZE;
    }
    nAvail = p->buf.n - p->iBuf;

    if( ppBlob && nReq==nBlob && nBlob<=nAvail ){
      *ppBlob = (u8 *)&p->buf.z[p->iBuf];
      p->iBuf += nBlob;
      nReq = 0;
    }else{
      int nCopy = MIN(nAvail, nReq);
      if( nBlob==nReq ){
        if( ppBlob ) *ppBlob = (u8 *)pBuf->z;
        pBuf->n = 0;
      }
      rc = lsmStringBinAppend(pBuf, &p->buf.z[p->iBuf], nCopy);
      if( rc!=LSM_OK ) break;
      nReq -= nCopy;
      p->iBuf += nCopy;
    }
  }

  return rc;
}

static int logReaderVarint(LogReader *p, LsmString *pBuf, int *piVal){
  u8 *aVarint;
  if( p->buf.n==p->iBuf ){
    int rc = logReaderBlob(p, 0, 1, &aVarint);
    if( rc!=LSM_OK ) return rc;
    p->iBuf = 0;
  }
  logReaderBlob(p, 0, lsmVarintSize(p->buf.z[p->iBuf]), &aVarint);
  lsmVarintGet32(aVarint, piVal);
  return LSM_OK;
}

static int logReaderByte(LogReader *p, u8 *pByte){
  int rc;
  u8 *pPtr;
  rc = logReaderBlob(p, 0, 1, &pPtr);
  *pByte = *pPtr;
  return rc;
}

static void logReaderCksum(LogReader *p, LsmString *pBuf, int *pbOk){
  u8 *pPtr;
  u32 cksum0, cksum1;
  int nCksum = p->iBuf - p->iCksumBuf;

  /* Update in-memory (expected) checksums */
  assert( nCksum>=0 );
  logCksumUnaligned(&p->buf.z[p->iCksumBuf], nCksum, &p->cksum0, &p->cksum1);
  p->iCksumBuf = p->iBuf + 8;
  logReaderBlob(p, pBuf, 8, &pPtr);

  /* Read the checksums from the log file. Set *pbOk if they match. */
  cksum0 = lsmGetU32(pPtr);
  cksum1 = lsmGetU32(&pPtr[4]);
#if 0
  printf("Expecting (%x %x) have (%x %x)\n", p->cksum0,p->cksum1,cksum0,cksum1);
#endif

  *pbOk = (cksum0==p->cksum0 && cksum1==p->cksum1);
  p->iCksumBuf = p->iBuf;
}

static void logReaderInit(lsm_db *pDb, LogReader *p){
  memset(p, 0, sizeof(LogReader));
  p->pFS = pDb->pFS;
  lsmStringInit(&p->buf, pDb->pEnv);
  p->cksum0 = LSM_CKSUM0_INIT;
  p->cksum1 = LSM_CKSUM1_INIT;
}


/*
** Recover the contents of the log file.
*/
int lsmLogRecover(lsm_db *pDb){
  LsmString buf1;                 /* Key buffer */
  LsmString buf2;                 /* Value buffer */
  LogReader reader;               /* Log reader object */
  int rc;                         /* Return code */
  int nCommit = 0;                /* Number of transactions to recover */
  int iPass;
  DbLog *pLog;

  rc = lsmBeginRecovery(pDb);
  if( rc!=LSM_OK ) return rc;

  logReaderInit(pDb, &reader);
  lsmStringInit(&buf1, pDb->pEnv);
  lsmStringInit(&buf2, pDb->pEnv);

  /* The outer for() loop runs at most twice. The first iteration is to 
  ** count the number of committed transactions in the log. The second 
  ** iterates through those transactions and updates the in-memory tree 
  ** structure with their contents.  */
  for(iPass=0; iPass<2; iPass++){
    int bEof = 0;

    while( rc==LSM_OK && !bEof ){
      u8 eType;
      rc = logReaderByte(&reader, &eType);
#if 0
static int iRead = 0;
      printf("iPass=%d %d read type %d\n", iPass, ++iRead, (int)eType);
fflush(stdout);
#endif

      switch( eType ){
        case LSM_LOG_PAD1:
        case LSM_LOG_PAD2:
          assert( 0 );
          break;

        case LSM_LOG_WRITE: {
          int nKey; u8 *aKey;
          int nVal; u8 *aVal;
          logReaderVarint(&reader, &buf1, &nKey);
          logReaderBlob(&reader, &buf1, nKey, 0);
          logReaderVarint(&reader, &buf2, &nVal);
          logReaderBlob(&reader, &buf2, nVal, &aVal);
          if( iPass==1 ){ 
            rc = lsmTreeInsert(pDb->pTV, (u8 *)buf1.z, nKey, aVal, nVal);
          }
          break;
        }

        case LSM_LOG_DELETE: {
          int nKey; u8 *aKey;
          logReaderVarint(&reader, &buf1, &nKey);
          logReaderBlob(&reader, &buf1, nKey, &aKey);
          if( iPass==1 ){ 
            rc = lsmTreeInsert(pDb->pTV, aKey, nKey, NULL, -1);
          }
          break;
        }

        case LSM_LOG_COMMIT:
        case LSM_LOG_CKSUM: {
          int bOk;
          logReaderCksum(&reader, &buf1, &bOk);
          if( !bOk ){
            bEof = 1;
            assert( iPass==0 );
          }else if( eType==LSM_LOG_COMMIT ){
            nCommit++;
            assert( nCommit>0 || iPass==1 );
            if( nCommit==0 ) bEof = 1;
          }
          break;
        }

        default:
          /* Including LSM_LOG_EOF */
          bEof = 1;
          break;
      }
    }

    if( iPass==0 ){
      reader.iOff = 0;
      reader.iBuf = 0;
      reader.buf.n = 0;
      reader.cksum0 = LSM_CKSUM0_INIT;
      reader.cksum1 = LSM_CKSUM1_INIT;
      reader.iCksumBuf = 0;
      if( nCommit==0 ) break;
      nCommit = nCommit * -1;
    }
  }

  /* Initialize DbLog object */
  pLog = logGetHandle(pDb, &rc);
  assert( pLog || rc!=LSM_OK );
  if( pLog ){
    pLog->iOff = reader.iOff - reader.buf.n + reader.iBuf;
    pLog->cksum0 = reader.cksum0;
    pLog->cksum1 = reader.cksum1;
  }

  lsmFinishRecovery(pDb);
  lsmStringClear(&buf1);
  lsmStringClear(&buf2);
  return rc;
}



