/*
** 2011-08-10
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
** This file defines the LSM API.
*/
#ifndef _LSM_H
#define _LSM_H

#ifdef __cplusplus
extern "C" {
#endif

/*
** Opaque handle types.
*/
typedef struct lsm_db lsm_db;               /* Database connection handle */
typedef struct lsm_cursor lsm_cursor;       /* Database cursor handle */
typedef struct lsm_mutex lsm_mutex;         /* Mutex handle */

/*
** Types required to implement new OS interfaces for lsm.
*/
typedef struct lsm_vfs lsm_vfs;             /* OS interface structure */
typedef struct lsm_file lsm_file;           /* OS file handle */
typedef long long int lsm_i64;              /* 64-bit signed integer type */

/* Forward reference */
typedef struct lsm_env lsm_env;             /* Runtime environment */

typedef struct lsm_heap_methods lsm_heap_methods;
typedef struct lsm_mutex_methods lsm_mutex_methods;

/*
** Run-time environoment used by LSM
*/
struct lsm_env {
  int nByte;                 /* Size of this structure in bytes */
  int iVersion;              /* Version number of this structure */
  /****** file i/o ***********************************************/
  void *pVfsCtx;
  int (*xOpen)(lsm_env*, const char *, lsm_file **);
  int (*xRead)(lsm_file *, lsm_i64, void *, int);
  int (*xWrite)(lsm_file *, lsm_i64, void *, int);
  int (*xSync)(lsm_file *);
  int (*xClose)(lsm_file *);
  /****** memory allocation ****************************************/
  void *pMemCtx;
  void *(*xMalloc)(lsm_env*, int);            /* malloc(3) function */
  void *(*xRealloc)(lsm_env*, void *, int);   /* realloc(3) function */
  void (*xFree)(lsm_env*, void *);            /* free(3) function */
  /****** mutexes ****************************************************/
  void *pMutexCtx;
  int (*xMutexStatic)(lsm_env*,int,lsm_mutex**); /* Obtain a static mutex */
  int (*xMutexNew)(lsm_env*, lsm_mutex**);       /* Get a new dynamic mutex */
  void (*xMutexDel)(lsm_mutex *);           /* Delete an allocated mutex */
  void (*xMutexEnter)(lsm_mutex *);         /* Grab a mutex */
  int (*xMutexTry)(lsm_mutex *);            /* Attempt to obtain a mutex */
  void (*xMutexLeave)(lsm_mutex *);         /* Leave a mutex */
  int (*xMutexHeld)(lsm_mutex *);           /* Return true if mutex is held */
  int (*xMutexNotHeld)(lsm_mutex *);        /* Return true if mutex not held */
  /* New fields may be added in future releases, in which case the
  ** iVersion value will increase. */
};

/*
** Return a pointer to the default LSM run-time environment
*/
lsm_env *lsm_default_env(void);

struct lsm_vfs {
  int (*xOpen)(lsm_env*, const char *, lsm_file **);
  int (*xRead)(lsm_file *, lsm_i64, void *, int);
  int (*xWrite)(lsm_file *, lsm_i64, void *, int);
  int (*xSync)(lsm_file *);
  int (*xClose)(lsm_file *);
};

/*
** Error codes.
*/
#define LSM_OK         0
#define LSM_BUSY       5
#define LSM_NOMEM      7
#define LSM_IOERR     10
#define LSM_CORRUPT   11
#define LSM_FULL      13
#define LSM_CANTOPEN  14
#define LSM_MISUSE    21

/* 
** Open and close a connection to a named database.
*/
int lsm_new(lsm_env*, lsm_db **ppDb);
int lsm_open(lsm_db *pDb, const char *zFilename);
int lsm_close(lsm_db *pDb);

/*
** Configure a database connection.
*/
int lsm_config(lsm_db *, int, ...);

/*
** The following values may be passed as the second argument to lsm_config().
**
**   LSM_CONFIG_WRITE_BUFFER
**     A read/write integer parameter. This value determines the maximum amount
**     of space (in bytes) used to accumulate writes in main-memory before 
**     they are flushed to a level 0 segment.
**
**   LSM_CONFIG_SEGMENT_RATIO
**     A read/write integer parameter. Used as the approximate maximum ratio
**     between adjacent segments in the data structure. This value must be
**     greater than or equal to 2.
**
**   LSM_CONFIG_PAGE_SIZE
**     A read/write integer parameter.
**
**   LSM_CONFIG_BLOCK_SIZE
**     A read/write integer parameter.
**
**   LSM_CONFIG_SAFETY
**     A read/write integer parameter. Valid values are 1, 2 (the default) 
**     and 3. This parameter determines how robust the database is in the
**     face of a system crash (e.g. a power failure or operating system 
**     crash). As follows:
**
**       1 (off):    No robustness. A system crash may corrupt the database.
**
**       2 (normal): Some robustness. A system crash may not corrupt the
**                   database file, but recently committed transactions may
**                   be lost following recovery.
**
**       3 (full):   Full robustness. A system crash may not corrupt the
**                   database file. Following recovery the database file
**                   contains all successfully committed transactions.
**
**   LSM_CONFIG_AUTOWORK
**     A read/write integer parameter.
**
**     TODO: This should configure some kind of threshold for turning on 
**     auto-work. Right now it is Boolean - 1 for on and 0 for off. Default 1.
*/
#define LSM_CONFIG_WRITE_BUFFER  1
#define LSM_CONFIG_PAGE_SIZE     3
#define LSM_CONFIG_SAFETY        4
#define LSM_CONFIG_BLOCK_SIZE    5

#define LSM_CONFIG_SEGMENT_RATIO 2
#define LSM_CONFIG_AUTOWORK      6

#define LSM_SAFETY_OFF    1
#define LSM_SAFETY_NORMAL 2
#define LSM_SAFETY_FULL   3


/*
** Invoke the memory allocation functions registered using 
** lsm_global_config(). Or the system defaults if no memory allocation
** functions have been registered.
*/
void *lsm_malloc(lsm_env*, int);
void *lsm_realloc(lsm_env*, void *, int);
void lsm_free(lsm_env*, void *);

lsm_vfs *lsm_default_vfs(void);

/*
** Configure a callback to which debugging and other messages should 
** be directed. Only useful for debugging lsm.
*/
void lsm_config_log(lsm_db *, void (*)(void *, int, const char *), void *);

/*
** Configure a callback that is invoked if the database connection ever
** writes to the database file.
*/
void lsm_config_work_hook(lsm_db *, void (*)(lsm_db *, void *), void *);

/*
** Query a database connection for operational statistics or data.
*/
int lsm_info(lsm_db *, int, ...);

/*
** The following values may be passed as the second argument to lsm_info().
**
**   LSM_INFO_NWRITE
**     The third parameter should be of type (int *). The location pointed
**     to by the third parameter is set to the number of 4KB pages written to
**     the file-system during the lifetime of this connection. Including data
**     written to the log file.
**
**   LSM_INFO_NREAD
**     The third parameter should be of type (int *). The location pointed
**     to by the third parameter is set to the number of 4KB pages read from
**     the file-system during the lifetime of this connection. Including data
**     read from the log file.
**
**   LSM_INFO_CKPT
**     The third argument should be of type (int **). The location pointed
**     to is populated with a pointer to an array of integers - the same array
**     saved to the database for a checkpoint. See comments in checkpoint.c
**     for instructions on interpreting the array. Unlike the data written
**     to the database file, the integers in the returned array are in native
**     format (i.e. little-endian on x86) and there are no checksum values.
**     The returned array should eventually be freed by the caller using
**     lsm_free().  
**
**   LSM_INFO_STRUCTLIST
**     The third argument should be of type (char **). The location pointed
**     to is populated with a pointer to a nul-terminated string containing
**     the string representation of a Tcl data-structure reflecting the 
**     current structure of the database. The returned string should be
**     eventually freed by the caller using lsm_free().
**
**     The returned list contains one element for each level in the database,
**     in order from most to least recent. Each element contains a 
**     single element for each segment comprising the corresponding level,
**     starting with the lhs segment, then each of the rhs segments (if any)
**     in order from most to least recent.
**
**   LSM_INFO_BLOCKLIST
**
*/
#define LSM_INFO_NWRITE       1
#define LSM_INFO_NREAD        2
#define LSM_INFO_CKPT         3
#define LSM_INFO_BLOCKLIST    4
#define LSM_INFO_STRUCTLIST   5


/* 
** Open and close transactions and nested transactions.
**
** lsm_begin():
**   Used to open transactions and sub-transactions. A successful call to 
**   lsm_begin() ensures that there are at least iLevel nested transactions 
**   open. To open a top-level transaction, pass iLevel==1. To open a 
**   sub-transaction within the top-level transaction, iLevel==2. Passing 
**   iLevel==0 is a no-op.
**
** lsm_commit():
**   Used to commit transactions and sub-transactions. A successful call 
**   to lsm_commit() ensures that there are at most iLevel nested 
**   transactions open. To commit a top-level transaction, pass iLevel==0. 
**   To commit all sub-transactions inside the main transaction, pass
**   iLevel==1.
**
** lsm_rollback():
**   Used to roll back transactions and sub-transactions. A successful call 
**   to lsm_rollback() restores the database to the state it was in when
**   the iLevel'th nested sub-transaction (if any) was first opened. And then
**   closes transactions to ensure that there are at most iLevel nested
**   transactions open.
**
**   Passing iLevel==0 rolls back and closes the top-level transaction.
**   iLevel==1 also rolls back the top-level transaction, but leaves it
**   open. iLevel==2 rolls back the sub-transaction nested directly inside
**   the top-level transaction (and leaves it open).
*/
int lsm_begin(lsm_db *pDb, int iLevel);
int lsm_commit(lsm_db *pDb, int iLevel);
int lsm_rollback(lsm_db *pDb, int iLevel);

/* 
** Write a new value into the database. If a value with a duplicate key 
** already exists it is replaced.
*/
int lsm_write(lsm_db *pDb, void *pKey, int nKey, void *pVal, int nVal);

/*
** Delete a value from the database. No error is returned if the specified
** key value does not exist in the database.
*/
int lsm_delete(lsm_db *pDb, void *pKey, int nKey);

/*
** This function is called by a thread to work on the database structure.
** Work performed within this function takes two forms:
**
**   * Checkpointing, and
**   * Merging.
**
** TODO: Describe the flags and arguments.
**
** TODO: Define the auto-work algorithm somewhere.
*/
int lsm_work(lsm_db *pDb, int flags, int nPage, int *pnWrite);

#define LSM_WORK_CHECKPOINT      0x00000001
#define LSM_WORK_OPTIMIZE        0x00000002

/* 
** Open and close a database cursor.
*/
int lsm_csr_open(lsm_db *pDb, lsm_cursor **ppCsr);
int lsm_csr_close(lsm_cursor *pCsr);

/* 
** Search the database for an entry with key (pKey/nKey). If an error 
** occurs, return an LSM error code, Otherwise, if the entry exists, 
** return LSM_OK and leave the cursor is left pointing to the entry with
** the matching key.
**
** If no error occurs, but the specified key is not present in the database,
** LSM_OK is returned. The state of the cursor depends on the value passed
** as the final parameter to lsm_csr_seek(), as follows:
**
**   LSM_SEEK_EQ:
**     The cursor is left at EOF (invalidated). A call to lsm_csr_valid()
**     returns non-zero.
**
**   LSM_SEEK_LE:
**     The cursor is left pointing to the largest key in the database that
**     is smaller than (pKey/nKey). If the database contains no keys smaller
**     than (pKey/nKey), the cursor is left at EOF.
**
**   LSM_SEEK_GE:
**     The cursor is left pointing to the smallest key in the database that
**     is larger than (pKey/nKey). If the database contains no keys larger
**     than (pKey/nKey), the cursor is left at EOF.
*/
int lsm_csr_seek(lsm_cursor *pCsr, void *pKey, int nKey, int eSeek);

/*
** Values that may be passed as the fourth argument to lsm_csr_seek().
*/
#define LSM_SEEK_EQ  0
#define LSM_SEEK_GE  1
#define LSM_SEEK_LE -1

int lsm_csr_first(lsm_cursor *pCsr);
int lsm_csr_last(lsm_cursor *pCsr);

int lsm_csr_next(lsm_cursor *pCsr);
int lsm_csr_prev(lsm_cursor *pCsr);

/* 
** Retrieve data from a database cursor.
*/
int lsm_csr_valid(lsm_cursor *pCsr);
int lsm_csr_key(lsm_cursor *pCsr, void **ppKey, int *pnKey);
int lsm_csr_value(lsm_cursor *pCsr, void **ppVal, int *pnVal);


/*
** Set or query global library configuration parameters.
**
**   LSM_GLOBAL_CONFIG_SET_HEAP:
**     By default, LSM uses the system malloc(), realloc() and free() 
**     functions to allocate and relinquish dynamic memory. Calling the
**     lsm_global_config() function with LSM_GLOBAL_CONFIG_SET_HEAP as
**     the first argument allows the user to override this and force the 
**     library to use custom routines instead.
**
**     The second parameter passed to lsm_global_config() should be a pointer
**     to an instance of struct lsm_heap_methods containing pointers to the
**     custom routines.
**
**   LSM_GLOBAL_CONFIG_SET_MUTEX:
**     Configure the functions LSM uses to allocate, enter, leave and free
**     mutex objects.
**
**     The second parameter passed to lsm_global_config() should be a pointer
**     to an instance of struct lsm_mutex_methods containing pointers to the
**     custom routines.
**
**   LSM_GLOBAL_CONFIG_GET_HEAP:
**     The second parameter to this call should be a pointer to a struct
**     of type lsm_heap_methods. Before returning, the structure is populated
**     with pointers to the routines used for dynamic memory management.
**
**   LSM_GLOBAL_CONFIG_GET_MUTEX:
**     The second parameter to this call should be a pointer to a struct
**     of type lsm_mutex_methods. Before returning, the structure is populated
**     with pointers to the routines used to create and manipulate mutex
**     objects.
*/
int lsm_global_config(int, ...);

#define LSM_GLOBAL_CONFIG_SET_HEAP     1
#define LSM_GLOBAL_CONFIG_GET_HEAP     2
#define LSM_GLOBAL_CONFIG_SET_MUTEX    3
#define LSM_GLOBAL_CONFIG_GET_MUTEX    4

struct lsm_heap_methods {
  void *(*xMalloc)(lsm_env*,int);                    /* malloc(3) function */
  void *(*xRealloc)(lsm_env*,void *, int);           /* realloc(3) function */
  void (*xFree)(lsm_env*,void *);                    /* free(3) function */
};

/*
** xMutexHeld:
**   Return true if the calling thread hold the specified mutex.
**
**   The xMutexHeld() method is not strictly required. If present, it is used
**   by assert() statements only to verify that the LSM code is operating as
**   expected. If the xMutexHeld pointer of an lsm_mutex_methods struct is 
**   NULL, these assert() statements are omitted. Library functionality is 
**   not affected.
*/
struct lsm_mutex_methods {
  int (*xMutexStatic)(lsm_env*, int, lsm_mutex **);   /* new static mutex  */
  int (*xMutexNew)(lsm_env*, lsm_mutex **);           /* new dynamic mutex */
  void (*xMutexDel)(lsm_mutex *);           /* Delete an allocated mutex */
  void (*xMutexEnter)(lsm_mutex *);         /* Grab a mutex */
  int (*xMutexTry)(lsm_mutex *);            /* Attempt to obtain a mutex */
  void (*xMutexLeave)(lsm_mutex *);         /* Leave a mutex */
  int (*xMutexHeld)(lsm_mutex *);           /* Return true if mutex is held */
};

/* 
** Values that may be passed as the first argument to xMutexStatic. 
*/
#define LSM_MUTEX_GLOBAL 1
#define LSM_MUTEX_HEAP   2

#ifdef __cplusplus
}  /* End of the 'extern "C"' block */
#endif
#endif /* ifndef _LSM_H */
