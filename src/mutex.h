/*
** 2007 August 28
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
** This file contains the common header for all mutex implementations.
** The sqliteInt.h header #includes this file so that it is available
** to all source files.  We break it out in an effort to keep the code
** better organized.
**
** NOTE:  source files should *not* #include this header file directly.
** Source files should #include the sqliteInt.h file and let that file
** include this one indirectly.
*/


/*
** Figure out what version of the code to use.  The choices are
**
**   SQLITE4_MUTEX_OMIT         No mutex logic.  Not even stubs.  The
**                             mutexes implemention cannot be overridden
**                             at start-time.
**
**   SQLITE4_MUTEX_NOOP         For single-threaded applications.  No
**                             mutual exclusion is provided.  But this
**                             implementation can be overridden at
**                             start-time.
**
**   SQLITE4_MUTEX_PTHREADS     For multi-threaded applications on Unix.
**
**   SQLITE4_MUTEX_W32          For multi-threaded applications on Win32.
*/
#if !SQLITE4_THREADSAFE
# define SQLITE4_MUTEX_OMIT
#endif
#if SQLITE4_THREADSAFE && !defined(SQLITE4_MUTEX_NOOP)
#  if SQLITE4_OS_UNIX
#    define SQLITE4_MUTEX_PTHREADS
#  elif SQLITE4_OS_WIN
#    define SQLITE4_MUTEX_W32
#  else
#    define SQLITE4_MUTEX_NOOP
#  endif
#endif

#ifdef SQLITE4_MUTEX_OMIT
/*
** If this is a no-op implementation, implement everything as macros.
*/
#define sqlite4_mutex_alloc(X,Y)  ((sqlite4_mutex*)8)
#define sqlite4_mutex_free(X)
#define sqlite4_mutex_enter(X)    
#define sqlite4_mutex_try(X)      SQLITE4_OK
#define sqlite4_mutex_leave(X)    
#define sqlite4_mutex_held(X)     ((void)(X),1)
#define sqlite4_mutex_notheld(X)  ((void)(X),1)
#define sqlite4MutexAlloc(X,Y)    ((sqlite4_mutex*)8)
#define sqlite4MutexInit(E)       SQLITE4_OK
#define sqlite4MutexEnd(E)
#define MUTEX_LOGIC(X)
#else
#define MUTEX_LOGIC(X)            X
#endif /* defined(SQLITE4_MUTEX_OMIT) */
