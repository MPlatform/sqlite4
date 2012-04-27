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
** by reading the log file.
**
** A log file consists of the following types of records representing data
** written into the database:
**
**   LOG_WRITE:  A key-value pair written to the database.
**   LOG_DELETE: A delete key issued to the database.
**   LOG_COMMIT: A transaction commit.
**
** And the following types of records for ancilliary purposes..
**
**   LOG_CKSUM:  A record containing a checksum.
**   LOG_EOF:    A record indicating the end of the log file.
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
**   Checksums are found in two types of log file records: LOG_COMMIT and
**   LOG_CKSUM records. In order to recover content from a log file, a client
**   reads each record from the start of the file, calculating a checksum as
**   it does. Each time a LOG_COMMIT or LOG_CKSUM is encountered, the 
**   recovery process verifies that the checksum stored in the log file 
**   matches the calculated checksum. If it does not, the recovery process
**   can stop reading the log file.
**
**   If a recovery process reads records (other than COMMIT or CKSUM) 
**   consisting of at least LSM_CKSUM_MAXDATA bytes, then the next record in
**   the log file must be either a LOG_CKSUM or LOG_COMMIT record. If it is
**   not, the recovery process also stops reading the log file.
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
**   limit (say 1MB). Then, the log continues in file C. Once $O reaches
**   an offset high enough to correspond to file C, writing switches back
**   to file B. And so on.
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
*/

/*
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
** LOG FILE FORMAT:
**
** A log file is a series of records. Each record is one of the following 
** types:
**
** Each transaction written to the log file consists of one or more LOG_WRITE
** and/or LOG_DELETE records followed by a single LOG_COMMIT. The LOG_COMMIT
** record contains a 64-bit checksum calculated based on the checksum of
** the previous COMMIT record and the contents of the log file from the
** end of the previous COMMIT up to the start of the checksum itself (i.e.
** including the first byte of the LOG_COMMIT record).
**
** LOG_PAD1 and LOG_PAD2 records may appear at any point in the file. They
** have three purposes:
**
**     * To ensure that the 8 checksum bytes in a LOG_COMMIT record are
**       always 8-byte aligned,
**
**     * To add padding immediately after a LOG_COMMIT record so that the
**       next write to the log file is aligned to the next sector boundary,
**
**     * To add some random salt to the start of the log file. Or as 
**       required.
**
** The log file is divided into 4KB pages. Each page contains up to 4088
** bytes of log data and an 8 byte checksum. The checksum is computed based
** on the contents of the current page and the checksum of the previous page
** in the log. When the log file is read to recover the database state, a 
** record is only considered valid if all pages up to and including the page 
** containing the commit record for the records transaction have valid 
** checksums.
**   LOG_CKPTID: * A single 0x06 byte,
**               * 8-bytes of pseudo-random data.
**
** The page checksum is calculated using the same method as the checksum in 
** an SQLite WAL file. By treating the input as an array of big-endian 
** unsigned 32-bit integers x[0] through x[N] where N is a multiple of 2. If 
** the size of the input is not a multiple of 8-byte it is treated as if it 
** has been padded with zero bytes. The checksum is then calculated as in 
** the following pseudo-code, where s0 and s1 are both 32-bit numbers. The 
** checksum is formed by concatenating the two of them together (s0 becomes
** the most significant 4 bytes, s1 the least).
**
**   for i from 0 to n-1 step 2:
**     s0 += x[i] + s1;
**     s1 += x[i+1] + s0;
**   endfor
*/

/*
** RECOVERY:
**
** To recover the log file, it must be read twice. The first time to 
** determine the location of the last valid commit record. And the second
** time to load data into the in-memory tree.
**
** Todo: Surely there is a better way...
*/

/*
** VARIABLE LENGTH INTEGER FORMAT:
**
** See lsm_varint.c.
*/

#ifndef _LSM_INT_H
# include "lsmInt.h"
#endif

#define LSM_LOG_EOF      0x00

#define LSM_LOG_PAD1     0x01
#define LSM_LOG_PAD2     0x02
#define LSM_LOG_WRITE    0x03
#define LSM_LOG_DELETE   0x04
#define LSM_LOG_COMMIT   0x05
#define LSM_LOG_CKPTID   0x06

#define LOG_CKSUM0_INIT 1
#define LOG_CKSUM1_INIT 1

typedef struct DbLogBuffer DbLogBuffer;

struct DbLogBuffer {
  int nAlloc;
  u8 *aAlloc;
};

struct DbLog {
  Page *pPg;                      /* Log file page currently being filled */
  int iPgOff;                     /* Current write offset within page pPg */
  u32 aCksum[2];                  /* Current checksum values */
  DbLogBuffer aBuf[2];            /* Buffers for keys+values read from log */

  int bRequireCkpt;               /* True if an LSM_LOG_CKPTID is required */
  Pgno iCkptPg;                   /* Last page an LSM_LOG_CKPTID was written */
  u32 aCkptSalt[2];               /* Initial checksum values for iCkptPg */
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
**     lsmLogFlush()
*/
static int logFileEnabled(lsm_db *pDb){
  /* TODO: Re-enable logging */
  return 0;
}

/*
** Make sure the current page has been stored on disk.
*/
static int logFilePersistPage(DbLog *pDbLog, int bRelease){
  Page *pPg = pDbLog->pPg;        /* Current page object */
  u32 aCksum[2] = {0, 0};         /* Page checksum */
  int rc;                         /* Return code */
  u8 *aData;                      /* Data for page pPg */
  int nData;                      /* Size of aData[] in bytes */

  aData = lsmFsPageData(pPg, &nData);
  nData -= 8;
  lsmChecksumBytes(aData, nData, pDbLog->aCksum, aCksum);
  lsmPutU32(&aData[nData], aCksum[0]);
  lsmPutU32(&aData[nData+4], aCksum[1]);

  if( bRelease ){
    pDbLog->aCksum[0] = aCksum[0];
    pDbLog->aCksum[1] = aCksum[1];
    rc = lsmFsPageRelease(pPg);
    pDbLog->pPg = 0;
  }else{
    rc = lsmFsPagePersist(pPg);
  }

  return rc;
}

/*
** Write data to the current log file offset. This is just an IO function.
** It does not interpret the blob of data read.
*/
static int logFileWrite(lsm_db *pDb, DbLog *pDbLog, u8 *a, int n){
  int rc = LSM_OK;
  int nRem = n;

  while( nRem && rc==LSM_OK ){
    rc = lsmFsPageWrite(pDbLog->pPg);
    if( rc==LSM_OK ){
      u8 *aData;
      int nData;
      int nCopy;

      /* Set aData[] to point to the page data. And nData to the size of
      ** aData[], not including the 8-byte checksum that will be added
      ** at the end of the page. */
      aData = lsmFsPageData(pDbLog->pPg, &nData);
      nData -= 8;

      /* Copy as much data as will fit into the body of the current page */
      nCopy = MIN(nRem, nData - pDbLog->iPgOff);
      assert( nCopy>=0 );
      if( a ){
        memcpy(&aData[pDbLog->iPgOff], &a[n-nRem], nCopy);
      }else{
        memset(&aData[pDbLog->iPgOff], 0, nCopy);
      }
      pDbLog->iPgOff += nCopy;
      nRem -= nCopy;

      /* If the current page is now full, write it out to disk and load
      ** the next page in the log file. */
      if( pDbLog->iPgOff==nData ){
        int iNext = 1 + lsmFsPageNumber(pDbLog->pPg);
        rc = logFilePersistPage(pDbLog, 1);
        if( rc==LSM_OK ){
          pDbLog->iPgOff = 0;
          rc = lsmFsLogPageGet(pDb->pFS, iNext, &pDbLog->pPg);
        }
      }else{
        assert( nRem==0 );
        aData[pDbLog->iPgOff] = '\0';
      }
    }
  }
  return LSM_OK;
}

static void logFileLoadPage(
  lsm_db *pDb,
  DbLog *pDbLog,
  Pgno iPg,
  int *pbEof,
  int *pRc
){
  if( *pRc==LSM_OK ){
    int rc = LSM_OK;

    assert( *pbEof==0 );
    if( pDbLog->pPg && lsmFsPageWritable(pDbLog->pPg) ){
      rc = logFilePersistPage(pDbLog, 0);
    }
    assert( pDbLog->pPg==0 || lsmFsPageWritable(pDbLog->pPg)==0 );
    lsmFsPageRelease(pDbLog->pPg);
    pDbLog->pPg = 0;

    /* Load the requested page */
    if( rc==LSM_OK ){
      rc = lsmFsLogPageGet(pDb->pFS, iPg, &pDbLog->pPg);
      pDbLog->iPgOff = 0;
    }

    /* If the page was loaded successfully, verify that the checksum is 
    ** correct. If not, set *pbEof to true to indicate that there is no
    ** valid content on this page.  */
    if( rc==LSM_OK ){
      u8 *aData;
      int nData;
      u32 aCksum[2];

      aData = lsmFsPageData(pDbLog->pPg, &nData);
      lsmChecksumBytes(aData, nData-8, pDbLog->aCksum, aCksum);
      if( aCksum[0]!=lsmGetU32(&aData[nData-8])
       || aCksum[1]!=lsmGetU32(&aData[nData-4])
      ){
        *pbEof = 1;
      }
    }

    *pRc = rc;
  }
}

static void logFileNextPage(
  lsm_db *pDb,
  DbLog *pDbLog,
  int *pbEof,
  int *pRc
){
  if( *pbEof==0 && *pRc==LSM_OK ){
    int iNext = 1;

    assert( pDbLog->pPg==0 || lsmFsPageWritable(pDbLog->pPg)==0 );
    if( pDbLog->pPg ){
      u8 *aData;
      int nData;
      aData = lsmFsPageData(pDbLog->pPg, &nData);
      pDbLog->aCksum[0] = lsmGetU32(&aData[nData-8]);
      pDbLog->aCksum[1] = lsmGetU32(&aData[nData-4]);

      iNext = lsmFsPageNumber(pDbLog->pPg) + 1;
      lsmFsPageRelease(pDbLog->pPg);
      pDbLog->pPg = 0;
    }

    logFileLoadPage(pDb, pDbLog, iNext, pbEof, pRc); 
  }
}

static i64 logFileReadVarint2(
  lsm_db *pDb,
  DbLog *pDbLog,
  int *pbEof,
  int *pRc
){
  i64 iRet = 0;
  if( *pbEof==0 && *pRc==LSM_OK ){
    u8 *aData;
    int nData;

    aData = lsmFsPageData(pDbLog->pPg, &nData);
    nData -= 8;

    /* Figure out if this varint overflow onto the next log file page */
    if( pDbLog->iPgOff + lsmVarintSize(aData[pDbLog->iPgOff]) > nData ){
      u8 aTmp[9];                 /* Tmp buffer for next 9 bytes of input */
      int nCopy;                  /* Number of bytes copied from current page */
      nCopy = (nData - pDbLog->iPgOff);
      memcpy(aTmp, &aData[pDbLog->iPgOff], nCopy);
      logFileNextPage(pDb, pDbLog, pbEof, pRc);
      if( *pbEof==0 && *pRc==0 ){
        aData = lsmFsPageData(pDbLog->pPg, 0);
        memcpy(&aTmp[nCopy], aData, 9-nCopy);
        pDbLog->iPgOff = (lsmVarintGet64(aTmp, &iRet) - nCopy);
      }
    }else{
      pDbLog->iPgOff += lsmVarintGet64(&aData[pDbLog->iPgOff], &iRet);
    }
  }

  return iRet;
}

static u8 *logFileReadData(
  lsm_db *pDb,
  DbLog *pDbLog,
  int nByte,
  int iBuf,                       /* Buffer to store result in, if required */
  int *pbEof,
  int *pRc
){
  u8 *aRet = 0;

  assert( iBuf==0 || iBuf==1 );

  if( *pbEof==0 && *pRc==LSM_OK ){
    DbLogBuffer *pBuf;
    int nRem;
    int rc = LSM_OK;

    pBuf = &pDbLog->aBuf[iBuf];
    if( pBuf->nAlloc<nByte ){
      lsmFree(pDb->pEnv, pBuf->aAlloc);
      pBuf->aAlloc = lsmMallocRc(pDb->pEnv, nByte*2, &rc);
      pBuf->nAlloc = nByte*2;
    }

    nRem = nByte;
    while( rc==LSM_OK && nRem>0 ){
      u8 *aData;
      int nData;
      int nCopy;

      aData = lsmFsPageData(pDbLog->pPg, &nData);
      nData -=8;
      nCopy = MIN(nRem, nData-pDbLog->iPgOff);
      assert( nCopy>=0 );

      memcpy(&pBuf->aAlloc[nByte-nRem], &aData[pDbLog->iPgOff], nCopy);
      nRem -= nCopy;
      pDbLog->iPgOff += nCopy;

      if( nRem>0 ){
        logFileNextPage(pDb, pDbLog, pbEof, &rc);
        if( *pbEof ) return 0;
      }
    }

    if( rc==LSM_OK ){
      aRet = pBuf->aAlloc;
    }else{
      *pRc = rc;
    }
  }
  return aRet;
}

static int logFileReadByte(
  lsm_db *pDb,
  DbLog *pDbLog,
  int *pbEof,
  int *pRc
){
  int iRet = 0;
  if( pDbLog->pPg==0 ){
    logFileNextPage(pDb, pDbLog, pbEof, pRc);
  }
  if( *pRc==LSM_OK && *pbEof==0 ){
    u8 *aVal;
    aVal = logFileReadData(pDb, pDbLog, 1, 0, pbEof, pRc);
    if( aVal ) iRet = (int)aVal[0];
  }

  return iRet;
}

static u32 logFileReadU32(DbLog *pDbLog){
  u8 *aData;                      /* Data for current log page */
  u32 ret;                        /* Return value (read from page) */

  assert( pDbLog->pPg );
  assert( (LOG_FILE_PGSZ - pDbLog->iPgOff) > 4 );

  aData = lsmFsPageData(pDbLog->pPg, 0);
  ret = lsmGetU32(&aData[pDbLog->iPgOff]);
  pDbLog->iPgOff += 4;
  return ret;
}

static int logFileReadRecord(
  lsm_db *pDb,                    /* Database connection handle */
  DbLog *pDbLog,                  /* Database log handle */
  int *peType,                    /* OUT: Record type (LSM_LOG_XXX) */
  u8 **paKey, int *pnKey,         /* OUT: Key */
  u8 **paVal, int *pnVal          /* OUT: Value */
){
  int bEof = 0;
  int rc = LSM_OK;
  int eType;                      /* Type of record read. Or LOG_EOF. */

  /* Read the type byte */
  eType = logFileReadByte(pDb, pDbLog, &bEof, &rc);
  assert( (bEof==0 && rc==LSM_OK) || eType==LSM_LOG_EOF );
  assert( eType==LSM_LOG_EOF    || eType==LSM_LOG_PAD1
       || eType==LSM_LOG_PAD2   || eType==LSM_LOG_WRITE
       || eType==LSM_LOG_DELETE || eType==LSM_LOG_COMMIT
       || eType==LSM_LOG_CKPTID
  );

  if( eType==LSM_LOG_CKPTID ){
    u32 iSalt1;                   /* First salt value read from log file */
    u32 iSalt2;                   /* Second salt value read from log file */

    /* LSM_LOG_CKPTID entries appear at the start of pages only */
    assert( pDbLog->iPgOff==1 );
    iSalt1 = logFileReadU32(pDbLog);
    iSalt2 = logFileReadU32(pDbLog);

#if 0
    if( rc==LSM_OK && pDb->pWorker ){
      u32 iExpect1;
      u32 iExpect2;
      lsmSnapshotGetSalt(pDb->pWorker, &iExpect1, &iExpect2);
      if( iExpect1!=iSalt1 || iExpect2!=iSalt2 ){
      }
      bEof = 1;
    }
#endif
  }else

  if( eType==LSM_LOG_PAD2 || eType==LSM_LOG_DELETE || eType==LSM_LOG_WRITE ){
    int nKey;
    u8 *aKey;
    int nVal;
    u8 *aVal;

    nKey = (int)logFileReadVarint2(pDb, pDbLog, &bEof, &rc);
    aKey = logFileReadData(pDb, pDbLog, nKey, 0, &bEof, &rc);
    if( eType==LSM_LOG_WRITE ){
      nVal = (int)logFileReadVarint2(pDb, pDbLog, &bEof, &rc);
      aVal = logFileReadData(pDb, pDbLog, nVal, 1, &bEof, &rc);
    }else{
      nVal = 0;
      aVal = 0;
    }

    if( paKey ){
      *paKey = aKey;
      *pnKey = nKey;
      *paVal = aVal;
      *pnVal = nVal;
    }
  }

  if( bEof ) eType = LSM_LOG_EOF;
  *peType = eType;
  return rc;
}

/*
** Return a pointer to the DbLog object associated with connection pDb.
** Allocate and initialize it if necessary.
*/
static DbLog *getLog(lsm_db *pDb, int *pRc){
  DbLog *pDbLog = pDb->pDbLog;
  if( pDbLog==0 ){
    pDb->pDbLog = pDbLog = (DbLog *)lsmMallocZeroRc(pDb->pEnv, 
                                                    sizeof(DbLog), pRc);
    if( pDbLog ) pDbLog->bRequireCkpt = 1;
  }
  return pDbLog;
}

/*
** If required, write an LSM_LOG_CKPTID record to the log file.
*/
static int logFileCkptid(lsm_db *pDb){
  int rc = LSM_OK;                /* Return code */
  DbLog *pDbLog = pDb->pDbLog;    /* Log system handle */

  if( pDbLog->bRequireCkpt ){
    u32 iSalt1;
    u32 iSalt2;

    lsmSnapshotGetSalt(pDb->pClient, &iSalt1, &iSalt2);

    assert( pDbLog->iPgOff==0 );
    assert( pDbLog->pPg );
#if 0 
    if( pDbLog->pPg==0 ){
      rc = lsmFsLogPageGet(pDb->pFS, 1, &pDbLog->pPg);
    }
#endif

    /* Assemble the LSM_LOG_CKPTID log entry in memory. Then write it to
    ** the log file.  */
    if( rc==LSM_OK ){
      u8 aCkpt[9];
      aCkpt[0] = LSM_LOG_CKPTID;
      lsmPutU32(&aCkpt[1], iSalt1);
      lsmPutU32(&aCkpt[5], iSalt2);
      rc = logFileWrite(pDb, pDbLog, aCkpt, 9);
    }

    if( rc==LSM_OK ){
      pDbLog->bRequireCkpt = 0;
      pDbLog->iCkptPg = lsmFsPageNumber(pDbLog->pPg);
      pDbLog->aCksum[0] = iSalt1;
      pDbLog->aCksum[1] = iSalt2;
    }
  }

  return rc;
}

static int logFileAppendRecord(
  lsm_db *pDb,                    /* Database connection */
  int eType,                      /* Record type - LSM_LOG_XXX */
  void *pKey, int nKey,           /* Key, if applicable */
  void *pVal, int nVal            /* Value, if applicable */
){
  int rc = LSM_OK;
  DbLog *pDbLog = getLog(pDb, &rc);

  assert( eType==LSM_LOG_WRITE 
       || eType==LSM_LOG_DELETE 
       || eType==LSM_LOG_PAD2 
  );

  /* If required, write an LSM_LOG_CKPTID record to the log file. */
  rc = logFileCkptid(pDb);

  if( rc==LSM_OK ){
    int nVarint;
    u8 aVarint[10];
    aVarint[0] = eType;

    nVarint = 1 + lsmVarintPut32(&aVarint[1], nKey);
    rc = logFileWrite(pDb, pDbLog, aVarint, nVarint);
    if( rc==LSM_OK ){
      rc = logFileWrite(pDb, pDbLog, pKey, nKey);
    }

    if( rc==LSM_OK && eType==LSM_LOG_WRITE ){
      nVarint = lsmVarintPut32(aVarint, nVal);
      rc = logFileWrite(pDb, pDbLog, aVarint, nVarint);
      if( rc==LSM_OK ){
        rc = logFileWrite(pDb, pDbLog, pVal, nVal);
      }
    }
  }

  return rc;
}

/*
** If the current log file offset is not at the start of a page, fill
** the remainder of the current page with LSM_LOG_PAD1 and LSM_LOG_PAD2 
** records. So that (DbLog.iPgOff==0) when this function returns.
*/
static int logFilePad(lsm_db *pDb){
  int rc = LSM_OK;                /* Return Code */
  DbLog *pDbLog = pDb->pDbLog;    /* Log handle */

  if( pDbLog->iPgOff!=0 ){
    /* Pad out the rest of the page. */
    int nData;
    int nPad;

    lsmFsPageData(pDbLog->pPg, &nData);
    nPad = nData - pDbLog->iPgOff - 8;

    while( nPad>0 && rc==LSM_OK ){
      if( nPad==1 ){
        u8 aPad1[1] = { LSM_LOG_PAD1 };
        rc = logFileWrite(pDb, pDbLog, aPad1, 1);
        nPad--;
      }else{
        int nWrite = MIN(129, nPad);
        rc = logFileAppendRecord(pDb, LSM_LOG_PAD2, 0, nWrite-2, 0, 0);
        nPad -= nWrite;
      }
    }

    assert( rc!=LSM_OK || pDbLog->iPgOff==0 );
  }

  return rc;
}

#if 0
/*
** Restart the log from the beginning. This is called only after all data
** in the current log file has been safely stored elsewhere in the 
** database.
*/
int lsmLogRestart(lsm_db *pDb){
  DbLog *pDbLog = pDb->pDbLog;
  int rc = LSM_OK;
  if( pDbLog ){
    lsmFsPageRelease(pDbLog->pPg);
    pDbLog->pPg = 0;
    pDbLog->iPgOff = 0;
    pDbLog->bRequireCkpt = 1;
    pDbLog->aCksum[0] = LOG_CKSUM0_INIT;
    pDbLog->aCksum[1] = LOG_CKSUM1_INIT;
  }
  return rc;
}
#endif

static void logFileReset(lsm_db *pDb){
  DbLog *pDbLog = pDb->pDbLog;

  lsmFsPageRelease(pDbLog->pPg);
  pDbLog->pPg = 0;
  pDbLog->iPgOff = 0;
  pDbLog->aCksum[0] = LOG_CKSUM0_INIT;
  pDbLog->aCksum[1] = LOG_CKSUM1_INIT;
}

static int logFilePlayback(lsm_db *pDb, int iPg, int iOff){
  int rc = LSM_OK;
  int bEof = 0;
  DbLog *pDbLog = pDb->pDbLog;

  /* Jump back to the start of the log */
  pDbLog->aCksum[0] = pDbLog->aCkptSalt[0];
  pDbLog->aCksum[1] = pDbLog->aCkptSalt[1];
  logFileLoadPage(pDb, pDbLog, pDbLog->iCkptPg, &bEof, &rc);

  if( bEof==0 ){
    while( !rc && (iPg!=lsmFsPageNumber(pDbLog->pPg) || iOff!=pDbLog->iPgOff) ){
      u8 *aKey; int nKey;
      u8 *aVal; int nVal;
      int eType;

      rc = logFileReadRecord(pDb, pDbLog, &eType, &aKey,&nKey, &aVal,&nVal);
      if( rc==LSM_OK ){
        if( eType==LSM_LOG_WRITE ){
          rc = lsmTreeInsert(pDb->pTV, aKey, nKey, aVal, nVal);
        }else if( eType==LSM_LOG_DELETE ){
          rc = lsmTreeInsert(pDb->pTV, aKey, nKey, NULL, -1);
        }
      }
    }

    assert( rc!=LSM_OK || pDbLog->pPg );
  }

  return rc;
}

/*
** Append an LSM_LOG_WRITE (if nVal>=0) or LSM_LOG_DELETE (if nVal<0) record 
** to the current log file.
*/
int lsmLogWrite(lsm_db *pDb, void *pKey, int nKey, void *pVal, int nVal){
  int rc = LSM_OK;
  if( logFileEnabled(pDb) ){
    if( nVal>=0 ){
      rc = logFileAppendRecord(pDb, LSM_LOG_WRITE, pKey, nKey, pVal, nVal);
    }else{
      rc = logFileAppendRecord(pDb, LSM_LOG_DELETE, pKey, nKey, 0, 0);
    }
  }
  return rc;
}

/*
** Append a commit record to the current log file.
*/
int lsmLogCommit(lsm_db *pDb){
  int rc = LSM_OK;
  DbLog *pDbLog = pDb->pDbLog;
  if( pDbLog && logFileEnabled(pDb) ){
    u8 aCommit[1] = { LSM_LOG_COMMIT };
    rc = logFileWrite(pDb, pDbLog, aCommit, 1);

    if( rc==LSM_OK ){
      if( pDb->eSafety==LSM_SAFETY_FULL ){
        rc = logFilePad(pDb);
      }else if( pDbLog->iPgOff>0 ){
        rc = logFilePersistPage(pDbLog, 0);
      }
    }
  }
  return rc;
}

/*
** Recover the contents of the log file.
*/
int lsmLogRecover(lsm_db *pDb){
  int rc = LSM_OK;
  DbLog *pDbLog;

  assert( pDb->pDbLog==0 );
  pDbLog = getLog(pDb, &rc);
  if( pDbLog ){
    int bEof = 0;
    Pgno iPg;

    assert( pDbLog->pPg==0 );
    assert( pDb->pWorker );
    assert( pDbLog->bRequireCkpt==1 );
    assert( rc==LSM_OK );

    lsmSnapshotGetSalt(pDb->pWorker, &pDbLog->aCksum[0], &pDbLog->aCksum[1]);
    iPg = lsmSnapshotGetLogpgno(pDb->pWorker);
    assert( iPg!=0 );

    pDbLog->iCkptPg = iPg;
    pDbLog->aCkptSalt[0] = pDbLog->aCksum[0];
    pDbLog->aCkptSalt[1] = pDbLog->aCksum[1];

    /* Load the first page of this log file. */
    rc = lsmBeginRecovery(pDb);
    logFileLoadPage(pDb, pDbLog, iPg, &bEof, &rc);
    if( rc==LSM_OK && bEof==0 ){
      int eType;                  /* Record type */
      int iCommitOff = 0;
      int iCommitPg = 0;

      /* First pass. Read from the start of the log file until the checksums
      ** fail. Store the location of the last commit record in the file in
      ** stack variables iCommitPg and iCommitOff.  */
      do {
        rc = logFileReadRecord(pDb, pDbLog, &eType, 0, 0, 0, 0);
        if( eType==LSM_LOG_COMMIT ){
          iCommitPg = lsmFsPageNumber(pDbLog->pPg);
          iCommitOff = pDbLog->iPgOff;
        }
      }while( rc==LSM_OK && eType!=LSM_LOG_EOF );

      /* Second pass. Populate the in-memory b-tree with the contents of the
      ** log file.  */
      if( rc==LSM_OK ) rc = logFilePlayback(pDb, iCommitPg, iCommitOff);
      if( rc==LSM_OK ) pDbLog->bRequireCkpt = 0;
    }

    lsmFinishRecovery(pDb);
  }
  return rc;
}

/*
** This function is called when a checkpoint is run.
*/
int lsmLogFlush(lsm_db *pDb){
  int rc = LSM_OK;
  DbLog *pDbLog = pDb->pDbLog;

  assert( pDb->pWorker );
  if( pDbLog && logFileEnabled(pDb) ){
    rc = logFilePad(pDb);
    if( rc==LSM_OK ){
      pDbLog->bRequireCkpt = 1;
      pDbLog->iCkptPg = lsmFsPageNumber(pDbLog->pPg);
      pDbLog->aCkptSalt[0] = pDbLog->aCksum[0];
      pDbLog->aCkptSalt[1] = pDbLog->aCksum[1];
      lsmSnapshotSetSalt(pDb->pWorker, pDbLog->aCksum[0], pDbLog->aCksum[1]);
      lsmSnapshotSetLogpgno(pDb->pWorker, pDbLog->iCkptPg);
    }
  }
  return rc;
}

/*
** This function is called to shut down the logging sub-system.
*/
int lsmLogClose(lsm_db *pDb){
  DbLog *pDbLog = pDb->pDbLog;
  if( pDbLog ){
    lsmFsPageRelease(pDbLog->pPg);

    lsmFree(pDb->pEnv, pDbLog->aBuf[0].aAlloc);
    lsmFree(pDb->pEnv, pDbLog->aBuf[1].aAlloc);

    lsmFree(pDb->pEnv, pDbLog);
    pDb->pDbLog = 0;
  }
  return LSM_OK;
}
