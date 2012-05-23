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
**   inefficient. Additionally, since the log file would grow without bound,
**   it wastes storage space.
**
**   Instead, part of each checkpoint written into the database file contains 
**   a log offset (and other information required to read the log starting at
**   at this offset) at which to begin recovery. Offset $O.
**
**   Once a checkpoint has been written and synced into the database file, it
**   is guaranteed that no recovery process will need to read any data before
**   offset $O of the log file. It is therefore safe to begin overwriting
**   any data that occurs before offset $O.
**
**   This implementation separates the log into three regions mapped into
**   the log file - regions 0, 1 and 2. During recovery, regions are read
**   in ascending order (i.e. 0, then 1, then 2). Each region is zero or
**   more bytes in size.
**
**     |---1---|..|--0--|.|--2--|....
**
**   New records are always appended to the end of region 2.
**
**   Initially (when it is empty), all three regions are zero bytes in size.
**   Each of them are located at the beginning of the file. As records are
**   added to the log, region 2 grows, so that the log consists of a zero
**   byte region 1, followed by a zero byte region 0, followed by an N byte
**   region 2. After one or more checkpoints have been written to disk, 
**   the start point of region 2 is moved to $O. For example:
**
**     A) ||.........|--2--|....
**   
**   (both regions 0 and 1 are 0 bytes in size at offset 0).
**
**   Eventually, the log wraps around to write new records into the start.
**   At this point, region 2 is renamed to region 0. Region 0 is renamed
**   to region 2. After appending a few records to the new region 2, the
**   log file looks like this:
**
**     B) ||--2--|...|--0--|....
**
**   (region 1 is still 0 bytes in size, located at offset 0).
**
**   Any checkpoints made at this point may reduce the size of region 0.
**   However, if they do not, and region 2 expands so that it is about to
**   overwrite the start of region 0, then region 2 is renamed to region 1,
**   and a new region 2 created at the end of the file following the existing
**   region 0.
**
**     C) |---1---|..|--0--|.|-2-|
**
**   In this state records are appended to region 2 until checkpoints have
**   contracted regions 0 and 1 until they are both zero bytes in size. They 
**   are then shifted to the start of the log file, leaving the system in 
**   the equivalent of state A above.
**
**   Alternatively, state B may transition directly to state A if the size
**   of region 0 is reduced to zero bytes before region 2 threatens to 
**   encroach upon it.
**
** LOG_PAD1 & LOG_PAD2 RECORDS
**
**   PAD1 and PAD2 records may appear in a log file at any point. They allow
**   a process writing the log file align the beginning of transactions with 
**   the beginning of disk sectors, which increases robustness.
**
** RECORD FORMATS:
**
**   LOG_EOF:    * A single 0x00 byte.
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
**   LOG_JUMP:   * A single 0x07 byte.
**               * Absolute file offset to jump to, encoded as a varint.
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

/*
** THREAD SAFETY NOTES
**
** The DbLog object that the functions in this file operate on is shared by
** multiple database connections. Access to the DbLog object associated with
** a database is required in the following scenarios:
**
**   * During recovery. The DbLog object is initialized during database 
**     recovery. Since while recovery is running there may be no other 
**     database users of any type, this is not much of a concern.
**
**   * By writers. Records are appended to the log file and the DbLog 
**     object updated as a result of calls to lsm_write()/delete()/commit()
**     etc.
**
**     Also when creating a new snapshot (after flushing the contents of
**     the in-memory tree to disk).
**
**   * By checkpointers. After a checkpoint is written to the database file,
**     the DbLog object must be updated.
**
**   * For LSM_INFO_LOG_STRUCTURE requests.
**
** TODO: Come back to this.
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
#define LSM_LOG_JUMP     0x07

/* Require a checksum every 32KB. */
/* #define LSM_CKSUM_MAXDATA (32*1024) */
#define LSM_CKSUM_MAXDATA (32*1024)

struct LogWriter {
  u32 cksum0;                     /* Checksum 0 at offset iOff */
  u32 cksum1;                     /* Checksum 1 at offset iOff */
  i64 iOff;                       /* Offset at start of buffer buf */
  LogRegion jump;                 /* Avoid writing to this region */
  LsmString buf;                  /* Buffer containing data not yet written */
  int nWritten;                   /* First nWritten bytes of buf are written */
  i64 iRegion1End;
  i64 iRegion2Start;
};

int lsmLogBegin(lsm_db *pDb, DbLog *pLog){
  int rc = LSM_OK;
  LogWriter *pNew;
  LogRegion *aReg;

  assert( lsmHoldingClientMutex(pDb) );

  pNew = lsmMallocZeroRc(pDb->pEnv, sizeof(LogWriter), &rc);
  if( pNew ){
    lsmStringInit(&pNew->buf, pDb->pEnv);
    rc = lsmStringExtend(&pNew->buf, 2);
  }
  if( rc!=LSM_OK ) return rc;

  /* There are now three scenarios:
  **
  **   1) Regions 0 and 1 are both zero bytes in size and region 2 begins
  **      at a file offset greater than N, where N is the value configured
  **      by LSM_CONFIG_LOG_SIZE. In this case, wrap around to the start
  **      and write data into the start of the log file. 
  **
  **   2) Region 1 is zero bytes in size and region 2 occurs earlier in the 
  **      file than region 0. In this case, append data to region 2, but
  **      remember to jump over region 1 if required.
  **
  **   3) Region 2 is the last in the file. Append to it.
  */
  aReg = &pLog->aRegion[0];

  assert( aReg[0].iEnd==0 || aReg[0].iEnd>aReg[0].iStart );
  assert( aReg[1].iEnd==0 || aReg[1].iEnd>aReg[1].iStart );

  pNew->cksum0 = pLog->cksum0;
  pNew->cksum1 = pLog->cksum1;

  if( aReg[0].iEnd==0 && aReg[1].iEnd==0 && aReg[2].iStart>=pDb->nLogSz ){
    /* Case 1. Wrap around to the start of the file. Write an LSM_LOG_JUMP 
    ** into the log file in this case. */
    u8 aJump[] = { LSM_LOG_JUMP, 0x00 };
    lsmStringBinAppend(&pNew->buf, aJump, sizeof(aJump));
    rc = lsmFsWriteLog(pDb->pFS, aReg[2].iEnd, &pNew->buf);
    aReg[2].iEnd += 2;
    pNew->nWritten = 2;
    pNew->jump = aReg[0] = aReg[2];
    aReg[2].iStart = aReg[2].iEnd = 0;
  }else if( aReg[1].iEnd==0 && aReg[2].iEnd<aReg[0].iEnd ){
    /* Case 2. */
    pNew->iOff = aReg[2].iEnd;
    pNew->jump = aReg[0];
  }else{
    /* Case 3. */
    assert( aReg[2].iStart>=aReg[0].iEnd && aReg[2].iStart>=aReg[1].iEnd );
    pNew->iOff = aReg[2].iEnd;
  }

  pDb->pLogWriter = pNew;
  return rc;
}

void lsmLogEnd(lsm_db *pDb, DbLog *pLog, int bCommit){
  LogWriter *p;
  assert( lsmHoldingClientMutex(pDb) );
  p = pDb->pLogWriter;
  if( bCommit ){
    pLog->aRegion[2].iEnd = p->iOff;
    pLog->cksum0 = p->cksum0;
    pLog->cksum1 = p->cksum1;
    if( p->iRegion1End ){
      assert( pLog->aRegion[1].iEnd==0 );
      assert( pLog->aRegion[2].iStart==0 );
      pLog->aRegion[1].iEnd = p->iRegion1End;
      pLog->aRegion[2].iStart = p->iRegion2Start;
    }
  }
  lsmFree(pDb->pEnv, p);
  pDb->pLogWriter = 0;
}

/*
** This function is called after a checkpoint is synced into the database
** file. The checkpoint specifies that the log starts at offset iOff.
*/ 
int lsmLogCheckpoint(lsm_db *pDb, DbLog *pLog, lsm_i64 iOff){
  int iRegion;
  assert( lsmHoldingClientMutex(pDb) );

  for(iRegion=0; iRegion<3; iRegion++){
    LogRegion *p = &pLog->aRegion[iRegion];
    if( iOff>=p->iStart && iOff<=p->iEnd ) break;
    p->iStart = 0;
    p->iEnd = 0;
  }
  assert( iRegion<3 );

  pLog->aRegion[iRegion].iStart = iOff;
  return LSM_OK;
}

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
  LogWriter *pLog = pDb->pLogWriter;
  
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
  if( pLog->nWritten ){
    LsmString str = {0, 0, 0, 0};
    str.z = &pLog->buf.z[pLog->nWritten];
    str.n = pLog->buf.n - pLog->nWritten;
    rc = lsmFsWriteLog(pDb->pFS, pLog->iOff, &str);
    pLog->iOff -= pLog->nWritten;
    pLog->nWritten = 0;
  }else{
    rc = lsmFsWriteLog(pDb->pFS, pLog->iOff, &pLog->buf);
  }
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
  LogWriter *pLog;                /* Log object to write to */
  int nReq;                       /* Bytes of space required in log */
  pLog = pDb->pLogWriter;

  /* Determine how many bytes of space are required for this record. Store
  ** this value in stack variable nReq. */
  nReq = 1 + lsmVarintLen32(nKey) + nKey;
  if( nVal>=0 ) nReq += lsmVarintLen32(nVal) + nVal;

  if( (pLog->jump.iStart > (pLog->iOff + pLog->buf.n)) 
   && (pLog->jump.iStart < (pLog->iOff + pLog->buf.n + (nReq + 9) + 10)) 
  ){
    i64 iJump;                    /* Offset to jump to */
    u8 aJump[10];
    int nJump;

    iJump = pLog->jump.iEnd+1;
    aJump[0] = LSM_LOG_JUMP;
    nJump = 1 + lsmVarintPut64(&aJump[1], iJump);
    rc = lsmStringBinAppend(&pLog->buf, aJump, nJump);
    if( rc!=LSM_OK ) return rc;
    rc = lsmFsWriteLog(pDb->pFS, pLog->iOff, &pLog->buf);
    if( rc!=LSM_OK ) return rc;

    pLog->iRegion1End = (pLog->iOff + pLog->buf.n);
    pLog->iRegion2Start = iJump;
    pLog->nWritten = pLog->buf.n;
    pLog->iOff = iJump;
  }

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

  return rc;
}

/*
** Append an LSM_LOG_COMMIT record to the database log.
*/
int lsmLogCommit(lsm_db *pDb){
  return logFlush(pDb, LSM_LOG_COMMIT);
}

void lsmLogTell(
  lsm_db *pDb,                    /* Database handle */
  LogMark *pMark                  /* Populate this object with current offset */
){
  LogWriter *pLog = pDb->pLogWriter;
  pMark->iOff = pLog->iOff;
  pMark->nBuf = pLog->buf.n;
  pMark->cksum0 = pLog->cksum0;
  pMark->cksum1 = pLog->cksum1;
}

int lsmLogSeek(
  lsm_db *pDb,                    /* Database handle */
  LogMark *pMark                  /* Object containing log offset to seek to */
){
  int rc = LSM_OK;                /* This functions return code */
#if 0
  DbLog *pLog = lsmDatabaseLog(pDb);

  assert( pLog );
  assert( pLog->iOff!=pMark->iOff || pLog->cksum0==pMark->cksum0 );
  assert( pLog->iOff!=pMark->iOff || pLog->cksum1==pMark->cksum1 );

  if( pLog->iOff==pMark->iOff ){
    pLog->buf.n = pMark->nBuf;
  }else{
    pLog->iOff = pMark->iOff;
    pLog->cksum0 = pMark->cksum0;
    pLog->cksum1 = pMark->cksum1;
    pLog->buf.n = 0;
    rc = lsmFsReadLog(pDb->pFS, pLog->iOff, pMark->nBuf, &pLog->buf);
  }
#endif

  return rc;
}

/*
** TODO: Thread safety of this function?
*/
int lsmLogStructure(lsm_db *pDb, char **pzVal){
  DbLog *pLog = lsmDatabaseLog(pDb);
  *pzVal = lsmMallocPrintf(pDb->pEnv, 
      "%d %d %d %d %d %d", 
      (int)pLog->aRegion[0].iStart, (int)pLog->aRegion[0].iEnd,
      (int)pLog->aRegion[1].iStart, (int)pLog->aRegion[1].iEnd,
      (int)pLog->aRegion[2].iStart, (int)pLog->aRegion[2].iEnd
  );
  return (*pzVal ? LSM_OK : LSM_NOMEM_BKPT);
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
      if( nCksum>0 ){
        nCarry = nCksum % 8;
        nCksum = ((nCksum / 8) * 8);
        if( nCksum>0 ){
          logCksumUnaligned(
              &p->buf.z[p->iCksumBuf], nCksum, &p->cksum0, &p->cksum1
          );
        }
      }
      if( nCarry>0 ) memcpy(p->buf.z, &p->buf.z[p->iBuf-nCarry], nCarry);
      p->buf.n = nCarry;
      p->iBuf = nCarry;

      rc = lsmFsReadLog(p->pFS, p->iOff, LOG_READ_SIZE, &p->buf);
assert( rc==LSM_OK );
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
      rc = lsmStringBinAppend(pBuf, (u8 *)&p->buf.z[p->iBuf], nCopy);
assert( rc==LSM_OK );
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

  *pbOk = (cksum0==p->cksum0 && cksum1==p->cksum1);
  p->iCksumBuf = p->iBuf;
}

static void logReaderInit(
  lsm_db *pDb,                    /* Database handle */
  DbLog *pLog,                    /* Log object associated with pDb */
  int bInitBuf,                   /* True if p->buf is uninitialized */
  LogReader *p                    /* Initialize this LogReader object */
){
  p->pFS = pDb->pFS;
  p->iOff = pLog->aRegion[2].iStart;
  p->cksum0 = pLog->cksum0;
  p->cksum1 = pLog->cksum1;
  if( bInitBuf ){ lsmStringInit(&p->buf, pDb->pEnv); }
  p->buf.n = 0;
  p->iCksumBuf = 0;
  p->iBuf = 0;
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
  int nJump = 0;                  /* Number of LSM_LOG_JUMP records in pass 0 */
  DbLog *pLog;

  rc = lsmBeginRecovery(pDb);
  if( rc!=LSM_OK ) return rc;

  pLog = lsmDatabaseLog(pDb);
  logReaderInit(pDb, pLog, 1, &reader);
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

      switch( eType ){
        case LSM_LOG_PAD1:
        case LSM_LOG_PAD2:
          assert( 0 );
          break;

        case LSM_LOG_WRITE: {
          int nKey;
          int nVal;
          u8 *aVal;
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

        case LSM_LOG_JUMP: {
          int iOff = 0;
          logReaderVarint(&reader, &buf1, &iOff);

          if( iPass==1 ){
            if( pLog->aRegion[2].iStart==0 ){
              assert( pLog->aRegion[1].iStart==0 );
              pLog->aRegion[1].iEnd = reader.iOff;
            }else{
              assert( pLog->aRegion[0].iStart==0 );
              pLog->aRegion[0].iStart = pLog->aRegion[2].iStart;
              pLog->aRegion[0].iEnd = reader.iOff - reader.buf.n + reader.iBuf;
            }
            pLog->aRegion[2].iStart = iOff;
          }else{
            if( (nJump++)==2 ){
              bEof = 1;
            }
          }

          reader.iOff = iOff;
          reader.buf.n = reader.iBuf;
          break;
        }

        default:
          /* Including LSM_LOG_EOF */
          bEof = 1;
          break;
      }
    }

    if( iPass==0 ){
      if( nCommit==0 ){
        if( pLog->aRegion[2].iStart==0 ){
          iPass = 1;
        }else{
          pLog->aRegion[2].iStart = 0;
          iPass = -1;
        }
      }
      logReaderInit(pDb, pLog, 0, &reader);
      nCommit = nCommit * -1;
    }
  }

  /* Initialize DbLog object */
  pLog->aRegion[2].iEnd = reader.iOff - reader.buf.n + reader.iBuf;
  pLog->cksum0 = reader.cksum0;
  pLog->cksum1 = reader.cksum1;

  lsmFinishRecovery(pDb);
  lsmStringClear(&buf1);
  lsmStringClear(&buf2);
  return rc;
}


