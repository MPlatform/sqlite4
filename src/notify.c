/*
** 2009 March 3
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
** This file contains the implementation of the sqlite4_unlock_notify()
** API method and its associated functionality.
*/
#include "sqliteInt.h"
#include "btreeInt.h"

/* Omit this entire file if SQLITE_ENABLE_UNLOCK_NOTIFY is not defined. */
#ifdef SQLITE_ENABLE_UNLOCK_NOTIFY

/*
** Public interfaces:
**
**   sqlite4ConnectionBlocked()
**   sqlite4ConnectionUnlocked()
**   sqlite4ConnectionClosed()
**   sqlite4_unlock_notify()
*/

#define assertMutexHeld() \
  assert( sqlite4_mutex_held(sqlite4MutexAlloc(SQLITE_MUTEX_STATIC_MASTER)) )

/*
** Head of a linked list of all sqlite4 objects created by this process
** for which either sqlite4.pBlockingConnection or sqlite4.pUnlockConnection
** is not NULL. This variable may only accessed while the STATIC_MASTER
** mutex is held.
*/
static sqlite4 *SQLITE_WSD sqlite4BlockedList = 0;

#ifndef NDEBUG
/*
** This function is a complex assert() that verifies the following 
** properties of the blocked connections list:
**
**   1) Each entry in the list has a non-NULL value for either 
**      pUnlockConnection or pBlockingConnection, or both.
**
**   2) All entries in the list that share a common value for 
**      xUnlockNotify are grouped together.
**
**   3) If the argument db is not NULL, then none of the entries in the
**      blocked connections list have pUnlockConnection or pBlockingConnection
**      set to db. This is used when closing connection db.
*/
static void checkListProperties(sqlite4 *db){
  sqlite4 *p;
  for(p=sqlite4BlockedList; p; p=p->pNextBlocked){
    int seen = 0;
    sqlite4 *p2;

    /* Verify property (1) */
    assert( p->pUnlockConnection || p->pBlockingConnection );

    /* Verify property (2) */
    for(p2=sqlite4BlockedList; p2!=p; p2=p2->pNextBlocked){
      if( p2->xUnlockNotify==p->xUnlockNotify ) seen = 1;
      assert( p2->xUnlockNotify==p->xUnlockNotify || !seen );
      assert( db==0 || p->pUnlockConnection!=db );
      assert( db==0 || p->pBlockingConnection!=db );
    }
  }
}
#else
# define checkListProperties(x)
#endif

/*
** Remove connection db from the blocked connections list. If connection
** db is not currently a part of the list, this function is a no-op.
*/
static void removeFromBlockedList(sqlite4 *db){
  sqlite4 **pp;
  assertMutexHeld();
  for(pp=&sqlite4BlockedList; *pp; pp = &(*pp)->pNextBlocked){
    if( *pp==db ){
      *pp = (*pp)->pNextBlocked;
      break;
    }
  }
}

/*
** Add connection db to the blocked connections list. It is assumed
** that it is not already a part of the list.
*/
static void addToBlockedList(sqlite4 *db){
  sqlite4 **pp;
  assertMutexHeld();
  for(
    pp=&sqlite4BlockedList; 
    *pp && (*pp)->xUnlockNotify!=db->xUnlockNotify; 
    pp=&(*pp)->pNextBlocked
  );
  db->pNextBlocked = *pp;
  *pp = db;
}

/*
** Obtain the STATIC_MASTER mutex.
*/
static void enterMutex(void){
  sqlite4_mutex_enter(sqlite4MutexAlloc(SQLITE_MUTEX_STATIC_MASTER));
  checkListProperties(0);
}

/*
** Release the STATIC_MASTER mutex.
*/
static void leaveMutex(void){
  assertMutexHeld();
  checkListProperties(0);
  sqlite4_mutex_leave(sqlite4MutexAlloc(SQLITE_MUTEX_STATIC_MASTER));
}

/*
** Register an unlock-notify callback.
**
** This is called after connection "db" has attempted some operation
** but has received an SQLITE_LOCKED error because another connection
** (call it pOther) in the same process was busy using the same shared
** cache.  pOther is found by looking at db->pBlockingConnection.
**
** If there is no blocking connection, the callback is invoked immediately,
** before this routine returns.
**
** If pOther is already blocked on db, then report SQLITE_LOCKED, to indicate
** a deadlock.
**
** Otherwise, make arrangements to invoke xNotify when pOther drops
** its locks.
**
** Each call to this routine overrides any prior callbacks registered
** on the same "db".  If xNotify==0 then any prior callbacks are immediately
** cancelled.
*/
int sqlite4_unlock_notify(
  sqlite4 *db,
  void (*xNotify)(void **, int),
  void *pArg
){
  int rc = SQLITE_OK;

  sqlite4_mutex_enter(db->mutex);
  enterMutex();

  if( xNotify==0 ){
    removeFromBlockedList(db);
    db->pBlockingConnection = 0;
    db->pUnlockConnection = 0;
    db->xUnlockNotify = 0;
    db->pUnlockArg = 0;
  }else if( 0==db->pBlockingConnection ){
    /* The blocking transaction has been concluded. Or there never was a 
    ** blocking transaction. In either case, invoke the notify callback
    ** immediately. 
    */
    xNotify(&pArg, 1);
  }else{
    sqlite4 *p;

    for(p=db->pBlockingConnection; p && p!=db; p=p->pUnlockConnection){}
    if( p ){
      rc = SQLITE_LOCKED;              /* Deadlock detected. */
    }else{
      db->pUnlockConnection = db->pBlockingConnection;
      db->xUnlockNotify = xNotify;
      db->pUnlockArg = pArg;
      removeFromBlockedList(db);
      addToBlockedList(db);
    }
  }

  leaveMutex();
  assert( !db->mallocFailed );
  sqlite4Error(db, rc, (rc?"database is deadlocked":0));
  sqlite4_mutex_leave(db->mutex);
  return rc;
}

/*
** This function is called while stepping or preparing a statement 
** associated with connection db. The operation will return SQLITE_LOCKED
** to the user because it requires a lock that will not be available
** until connection pBlocker concludes its current transaction.
*/
void sqlite4ConnectionBlocked(sqlite4 *db, sqlite4 *pBlocker){
  enterMutex();
  if( db->pBlockingConnection==0 && db->pUnlockConnection==0 ){
    addToBlockedList(db);
  }
  db->pBlockingConnection = pBlocker;
  leaveMutex();
}

/*
** This function is called when
** the transaction opened by database db has just finished. Locks held 
** by database connection db have been released.
**
** This function loops through each entry in the blocked connections
** list and does the following:
**
**   1) If the sqlite4.pBlockingConnection member of a list entry is
**      set to db, then set pBlockingConnection=0.
**
**   2) If the sqlite4.pUnlockConnection member of a list entry is
**      set to db, then invoke the configured unlock-notify callback and
**      set pUnlockConnection=0.
**
**   3) If the two steps above mean that pBlockingConnection==0 and
**      pUnlockConnection==0, remove the entry from the blocked connections
**      list.
*/
void sqlite4ConnectionUnlocked(sqlite4 *db){
  void (*xUnlockNotify)(void **, int) = 0; /* Unlock-notify cb to invoke */
  int nArg = 0;                            /* Number of entries in aArg[] */
  sqlite4 **pp;                            /* Iterator variable */
  void **aArg;               /* Arguments to the unlock callback */
  void **aDyn = 0;           /* Dynamically allocated space for aArg[] */
  void *aStatic[16];         /* Starter space for aArg[].  No malloc required */

  aArg = aStatic;
  enterMutex();         /* Enter STATIC_MASTER mutex */

  /* This loop runs once for each entry in the blocked-connections list. */
  for(pp=&sqlite4BlockedList; *pp; /* no-op */ ){
    sqlite4 *p = *pp;

    /* Step 1. */
    if( p->pBlockingConnection==db ){
      p->pBlockingConnection = 0;
    }

    /* Step 2. */
    if( p->pUnlockConnection==db ){
      assert( p->xUnlockNotify );
      if( p->xUnlockNotify!=xUnlockNotify && nArg!=0 ){
        xUnlockNotify(aArg, nArg);
        nArg = 0;
      }

      sqlite4BeginBenignMalloc();
      assert( aArg==aDyn || (aDyn==0 && aArg==aStatic) );
      assert( nArg<=(int)ArraySize(aStatic) || aArg==aDyn );
      if( (!aDyn && nArg==(int)ArraySize(aStatic))
       || (aDyn && nArg==(int)(sqlite4MallocSize(aDyn)/sizeof(void*)))
      ){
        /* The aArg[] array needs to grow. */
        void **pNew = (void **)sqlite4Malloc(nArg*sizeof(void *)*2);
        if( pNew ){
          memcpy(pNew, aArg, nArg*sizeof(void *));
          sqlite4_free(aDyn);
          aDyn = aArg = pNew;
        }else{
          /* This occurs when the array of context pointers that need to
          ** be passed to the unlock-notify callback is larger than the
          ** aStatic[] array allocated on the stack and the attempt to 
          ** allocate a larger array from the heap has failed.
          **
          ** This is a difficult situation to handle. Returning an error
          ** code to the caller is insufficient, as even if an error code
          ** is returned the transaction on connection db will still be
          ** closed and the unlock-notify callbacks on blocked connections
          ** will go unissued. This might cause the application to wait
          ** indefinitely for an unlock-notify callback that will never 
          ** arrive.
          **
          ** Instead, invoke the unlock-notify callback with the context
          ** array already accumulated. We can then clear the array and
          ** begin accumulating any further context pointers without 
          ** requiring any dynamic allocation. This is sub-optimal because
          ** it means that instead of one callback with a large array of
          ** context pointers the application will receive two or more
          ** callbacks with smaller arrays of context pointers, which will
          ** reduce the applications ability to prioritize multiple 
          ** connections. But it is the best that can be done under the
          ** circumstances.
          */
          xUnlockNotify(aArg, nArg);
          nArg = 0;
        }
      }
      sqlite4EndBenignMalloc();

      aArg[nArg++] = p->pUnlockArg;
      xUnlockNotify = p->xUnlockNotify;
      p->pUnlockConnection = 0;
      p->xUnlockNotify = 0;
      p->pUnlockArg = 0;
    }

    /* Step 3. */
    if( p->pBlockingConnection==0 && p->pUnlockConnection==0 ){
      /* Remove connection p from the blocked connections list. */
      *pp = p->pNextBlocked;
      p->pNextBlocked = 0;
    }else{
      pp = &p->pNextBlocked;
    }
  }

  if( nArg!=0 ){
    xUnlockNotify(aArg, nArg);
  }
  sqlite4_free(aDyn);
  leaveMutex();         /* Leave STATIC_MASTER mutex */
}

/*
** This is called when the database connection passed as an argument is 
** being closed. The connection is removed from the blocked list.
*/
void sqlite4ConnectionClosed(sqlite4 *db){
  sqlite4ConnectionUnlocked(db);
  enterMutex();
  removeFromBlockedList(db);
  checkListProperties(db);
  leaveMutex();
}
#endif
