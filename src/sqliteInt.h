/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Internal interface definitions for SQLite.
**
*/
#ifndef _SQLITEINT_H_
#define _SQLITEINT_H_

#define SQLITE4_OMIT_ANALYZE 1
#define SQLITE4_OMIT_PROGRESS_CALLBACK 1
#define SQLITE4_OMIT_VIRTUALTABLE 1
#define SQLITE4_OMIT_XFER_OPT 1
#define SQLITE4_OMIT_LOCALTIME 1

/*
** These #defines should enable >2GB file support on POSIX if the
** underlying operating system supports it.  If the OS lacks
** large file support, or if the OS is windows, these should be no-ops.
**
** Ticket #2739:  The _LARGEFILE_SOURCE macro must appear before any
** system #includes.  Hence, this block of code must be the very first
** code in all source files.
**
** Large file support can be disabled using the -DSQLITE4_DISABLE_LFS switch
** on the compiler command line.  This is necessary if you are compiling
** on a recent machine (ex: Red Hat 7.2) but you want your code to work
** on an older machine (ex: Red Hat 6.0).  If you compile on Red Hat 7.2
** without this option, LFS is enable.  But LFS does not exist in the kernel
** in Red Hat 6.0, so the code won't work.  Hence, for maximum binary
** portability you should omit LFS.
**
** Similar is true for Mac OS X.  LFS is only supported on Mac OS X 9 and later.
*/
#ifndef SQLITE4_DISABLE_LFS
# define _LARGE_FILE       1
# ifndef _FILE_OFFSET_BITS
#   define _FILE_OFFSET_BITS 64
# endif
# define _LARGEFILE_SOURCE 1
#endif

/*
** Include the configuration header output by 'configure' if we're using the
** autoconf-based build
*/
#ifdef _HAVE_SQLITE4_CONFIG_H
#include "config.h"
#endif

#include "sqliteLimit.h"

/* Disable nuisance warnings on Borland compilers */
#if defined(__BORLANDC__)
#pragma warn -rch /* unreachable code */
#pragma warn -ccc /* Condition is always true or false */
#pragma warn -aus /* Assigned value is never used */
#pragma warn -csu /* Comparing signed and unsigned */
#pragma warn -spa /* Suspicious pointer arithmetic */
#endif

/* Needed for various definitions... */
#ifndef _GNU_SOURCE
# define _GNU_SOURCE
#endif

/*
** Include standard header files as necessary
*/
#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif
#ifdef HAVE_INTTYPES_H
#include <inttypes.h>
#endif

/*
** The following macros are used to cast pointers to integers and
** integers to pointers.  The way you do this varies from one compiler
** to the next, so we have developed the following set of #if statements
** to generate appropriate macros for a wide range of compilers.
**
** The correct "ANSI" way to do this is to use the intptr_t type. 
** Unfortunately, that typedef is not available on all compilers, or
** if it is available, it requires an #include of specific headers
** that vary from one machine to the next.
**
** Ticket #3860:  The llvm-gcc-4.2 compiler from Apple chokes on
** the ((void*)&((char*)0)[X]) construct.  But MSVC chokes on ((void*)(X)).
** So we have to define the macros in different ways depending on the
** compiler.
*/
#if defined(__PTRDIFF_TYPE__)  /* This case should work for GCC */
# define SQLITE4_INT_TO_PTR(X)  ((void*)(__PTRDIFF_TYPE__)(X))
# define SQLITE4_PTR_TO_INT(X)  ((int)(__PTRDIFF_TYPE__)(X))
#elif !defined(__GNUC__)       /* Works for compilers other than LLVM */
# define SQLITE4_INT_TO_PTR(X)  ((void*)&((char*)0)[X])
# define SQLITE4_PTR_TO_INT(X)  ((int)(((char*)X)-(char*)0))
#elif defined(HAVE_STDINT_H)   /* Use this case if we have ANSI headers */
# define SQLITE4_INT_TO_PTR(X)  ((void*)(intptr_t)(X))
# define SQLITE4_PTR_TO_INT(X)  ((int)(intptr_t)(X))
#else                          /* Generates a warning - but it always works */
# define SQLITE4_INT_TO_PTR(X)  ((void*)(X))
# define SQLITE4_PTR_TO_INT(X)  ((int)(X))
#endif

/*
** The SQLITE4_THREADSAFE macro must be defined as 0, 1, or 2.
** 0 means mutexes are permanently disable and the library is never
** threadsafe.  1 means the library is serialized which is the highest
** level of threadsafety.  2 means the libary is multithreaded - multiple
** threads can use SQLite as long as no two threads try to use the same
** database connection at the same time.
**
** Older versions of SQLite used an optional THREADSAFE macro.
** We support that for legacy.
*/
#if !defined(SQLITE4_THREADSAFE)
#if defined(THREADSAFE)
# define SQLITE4_THREADSAFE THREADSAFE
#else
# define SQLITE4_THREADSAFE 1 /* IMP: R-07272-22309 */
#endif
#endif

/*
** Powersafe overwrite is on by default.  But can be turned off using
** the -DSQLITE4_POWERSAFE_OVERWRITE=0 command-line option.
*/
#ifndef SQLITE4_POWERSAFE_OVERWRITE
# define SQLITE4_POWERSAFE_OVERWRITE 1
#endif

/*
** The SQLITE4_DEFAULT_MEMSTATUS macro must be defined as either 0 or 1.
** It determines whether or not the features related to 
** SQLITE4_CONFIG_MEMSTATUS are available by default or not. This value can
** be overridden at runtime using the sqlite4_config() API.
*/
#if !defined(SQLITE4_DEFAULT_MEMSTATUS)
# define SQLITE4_DEFAULT_MEMSTATUS 1
#endif

/*
** Exactly one of the following macros must be defined in order to
** specify which memory allocation subsystem to use.
**
**     SQLITE4_SYSTEM_MALLOC          // Use normal system malloc()
**     SQLITE4_WIN32_MALLOC           // Use Win32 native heap API
**     SQLITE4_MEMDEBUG               // Debugging version of system malloc()
**
** On Windows, if the SQLITE4_WIN32_MALLOC_VALIDATE macro is defined and the
** assert() macro is enabled, each call into the Win32 native heap subsystem
** will cause HeapValidate to be called.  If heap validation should fail, an
** assertion will be triggered.
**
** (Historical note:  There used to be several other options, but we've
** pared it down to just these three.)
**
** If none of the above are defined, then set SQLITE4_SYSTEM_MALLOC as
** the default.
*/
#if defined(SQLITE4_SYSTEM_MALLOC)+defined(SQLITE4_WIN32_MALLOC)+defined(SQLITE4_MEMDEBUG)>1
# error "At most one of the following compile-time configuration options\
 is allows: SQLITE4_SYSTEM_MALLOC, SQLITE4_WIN32_MALLOC, SQLITE4_MEMDEBUG"
#endif
#if defined(SQLITE4_SYSTEM_MALLOC)+defined(SQLITE4_WIN32_MALLOC)+defined(SQLITE4_MEMDEBUG)==0
# define SQLITE4_SYSTEM_MALLOC 1
#endif

/*
** If SQLITE4_MALLOC_SOFT_LIMIT is not zero, then try to keep the
** sizes of memory allocations below this value where possible.
*/
#if !defined(SQLITE4_MALLOC_SOFT_LIMIT)
# define SQLITE4_MALLOC_SOFT_LIMIT 1024
#endif

/*
** We need to define _XOPEN_SOURCE as follows in order to enable
** recursive mutexes on most Unix systems.  But Mac OS X is different.
** The _XOPEN_SOURCE define causes problems for Mac OS X we are told,
** so it is omitted there.  See ticket #2673.
**
** Later we learn that _XOPEN_SOURCE is poorly or incorrectly
** implemented on some systems.  So we avoid defining it at all
** if it is already defined or if it is unneeded because we are
** not doing a threadsafe build.  Ticket #2681.
**
** See also ticket #2741.
*/
#if !defined(_XOPEN_SOURCE) && !defined(__DARWIN__) && !defined(__APPLE__) && SQLITE4_THREADSAFE
#  define _XOPEN_SOURCE 500  /* Needed to enable pthread recursive mutexes */
#endif

/*
** The TCL headers are only needed when compiling the TCL bindings.
*/
#if defined(SQLITE4_TCL) || defined(TCLSH)
# include <tcl.h>
#endif

/*
** Many people are failing to set -DNDEBUG=1 when compiling SQLite.
** Setting NDEBUG makes the code smaller and run faster.  So the following
** lines are added to automatically set NDEBUG unless the -DSQLITE4_DEBUG=1
** option is set.  Thus NDEBUG becomes an opt-in rather than an opt-out
** feature.
*/
#if !defined(NDEBUG) && !defined(SQLITE4_DEBUG) 
# define NDEBUG 1
#endif

/*
** The testcase() macro is used to aid in coverage testing.  When 
** doing coverage testing, the condition inside the argument to
** testcase() must be evaluated both true and false in order to
** get full branch coverage.  The testcase() macro is inserted
** to help ensure adequate test coverage in places where simple
** condition/decision coverage is inadequate.  For example, testcase()
** can be used to make sure boundary values are tested.  For
** bitmask tests, testcase() can be used to make sure each bit
** is significant and used at least once.  On switch statements
** where multiple cases go to the same block of code, testcase()
** can insure that all cases are evaluated.
**
*/
#ifdef SQLITE4_COVERAGE_TEST
  void sqlite4Coverage(int);
# define testcase(X)  if( X ){ sqlite4Coverage(__LINE__); }
#else
# define testcase(X)
#endif

/*
** The TESTONLY macro is used to enclose variable declarations or
** other bits of code that are needed to support the arguments
** within testcase() and assert() macros.
*/
#if !defined(NDEBUG) || defined(SQLITE4_COVERAGE_TEST)
# define TESTONLY(X)  X
#else
# define TESTONLY(X)
#endif

/*
** Sometimes we need a small amount of code such as a variable initialization
** to setup for a later assert() statement.  We do not want this code to
** appear when assert() is disabled.  The following macro is therefore
** used to contain that setup code.  The "VVA" acronym stands for
** "Verification, Validation, and Accreditation".  In other words, the
** code within VVA_ONLY() will only run during verification processes.
*/
#ifndef NDEBUG
# define VVA_ONLY(X)  X
#else
# define VVA_ONLY(X)
#endif

/*
** The ALWAYS and NEVER macros surround boolean expressions which 
** are intended to always be true or false, respectively.  Such
** expressions could be omitted from the code completely.  But they
** are included in a few cases in order to enhance the resilience
** of SQLite to unexpected behavior - to make the code "self-healing"
** or "ductile" rather than being "brittle" and crashing at the first
** hint of unplanned behavior.
**
** In other words, ALWAYS and NEVER are added for defensive code.
**
** When doing coverage testing ALWAYS and NEVER are hard-coded to
** be true and false so that the unreachable code then specify will
** not be counted as untested code.
*/
#if defined(SQLITE4_COVERAGE_TEST)
# define ALWAYS(X)      (1)
# define NEVER(X)       (0)
#elif !defined(NDEBUG)
# define ALWAYS(X)      ((X)?1:(assert(0),0))
# define NEVER(X)       ((X)?(assert(0),1):0)
#else
# define ALWAYS(X)      (X)
# define NEVER(X)       (X)
#endif

#include "sqlite4.h"
#include "hash.h"
#include "parse.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stddef.h>

/*
** If compiling for a processor that lacks floating point support,
** substitute integer for floating-point
*/
#ifdef SQLITE4_OMIT_FLOATING_POINT
# define double sqlite4_int64
# define float sqlite4_int64
# define LONGDOUBLE_TYPE sqlite4_int64
# ifndef SQLITE4_BIG_DBL
#   define SQLITE4_BIG_DBL (((sqlite4_int64)1)<<50)
# endif
# define SQLITE4_OMIT_DATETIME_FUNCS 1
# define SQLITE4_OMIT_TRACE 1
# undef SQLITE4_MIXED_ENDIAN_64BIT_FLOAT
# undef SQLITE4_HAVE_ISNAN
#endif
#ifndef SQLITE4_BIG_DBL
# define SQLITE4_BIG_DBL (1e99)
#endif

/*
** OMIT_TEMPDB is set to 1 if SQLITE4_OMIT_TEMPDB is defined, or 0
** afterward. Having this macro allows us to cause the C compiler 
** to omit code used by TEMP tables without messy #ifndef statements.
*/
#ifdef SQLITE4_OMIT_TEMPDB
#define OMIT_TEMPDB 1
#else
#define OMIT_TEMPDB 0
#endif

/*
** Provide a default value for SQLITE4_TEMP_STORE in case it is not specified
** on the command-line
*/
#ifndef SQLITE4_TEMP_STORE
# define SQLITE4_TEMP_STORE 1
#endif

/*
** GCC does not define the offsetof() macro so we'll have to do it
** ourselves.
*/
#ifndef offsetof
#define offsetof(STRUCTURE,FIELD) ((int)((char*)&((STRUCTURE*)0)->FIELD))
#endif

/*
** Check to see if this machine uses EBCDIC.  (Yes, believe it or
** not, there are still machines out there that use EBCDIC.)
*/
#if 'A' == '\301'
# define SQLITE4_EBCDIC 1
#else
# define SQLITE4_ASCII 1
#endif

/*
** Integers of known sizes.  These typedefs might change for architectures
** where the sizes very.  Preprocessor macros are available so that the
** types can be conveniently redefined at compile-type.  Like this:
**
**         cc '-DUINTPTR_TYPE=long long int' ...
*/
#ifndef UINT32_TYPE
# ifdef HAVE_UINT32_T
#  define UINT32_TYPE uint32_t
# else
#  define UINT32_TYPE unsigned int
# endif
#endif
#ifndef UINT16_TYPE
# ifdef HAVE_UINT16_T
#  define UINT16_TYPE uint16_t
# else
#  define UINT16_TYPE unsigned short int
# endif
#endif
#ifndef INT16_TYPE
# ifdef HAVE_INT16_T
#  define INT16_TYPE int16_t
# else
#  define INT16_TYPE short int
# endif
#endif
#ifndef UINT8_TYPE
# ifdef HAVE_UINT8_T
#  define UINT8_TYPE uint8_t
# else
#  define UINT8_TYPE unsigned char
# endif
#endif
#ifndef INT8_TYPE
# ifdef HAVE_INT8_T
#  define INT8_TYPE int8_t
# else
#  define INT8_TYPE signed char
# endif
#endif
#ifndef LONGDOUBLE_TYPE
# define LONGDOUBLE_TYPE long double
#endif
typedef sqlite4_int64 i64;          /* 8-byte signed integer */
typedef sqlite4_uint64 u64;         /* 8-byte unsigned integer */
typedef UINT32_TYPE u32;           /* 4-byte unsigned integer */
typedef UINT16_TYPE u16;           /* 2-byte unsigned integer */
typedef INT16_TYPE i16;            /* 2-byte signed integer */
typedef UINT8_TYPE u8;             /* 1-byte unsigned integer */
typedef INT8_TYPE i8;              /* 1-byte signed integer */

/*
** SQLITE4_MAX_U32 is a u64 constant that is the maximum u64 value
** that can be stored in a u32 without loss of data.  The value
** is 0x00000000ffffffff.  But because of quirks of some compilers, we
** have to specify the value in the less intuitive manner shown:
*/
#define SQLITE4_MAX_U32  ((((u64)1)<<32)-1)

/*
** In the sqlite4_num object, the maximum exponent value.  Values
** larger than this are +Inf, or -Inf, or NaN.
*/
#define SQLITE4_MX_EXP   999    /* Maximum exponent */
#define SQLITE4_NAN_EXP 2000    /* Exponent to use for NaN */

/*
** The datatype used to store estimates of the number of rows in a
** table or index.  This is an unsigned integer type.  For 99.9% of
** the world, a 32-bit integer is sufficient.  But a 64-bit integer
** can be used at compile-time if desired.
*/
#ifdef SQLITE4_64BIT_STATS
 typedef u64 tRowcnt;    /* 64-bit only if requested at compile-time */
#else
 typedef u32 tRowcnt;    /* 32-bit is the default */
#endif

/*
** Macros to determine whether the machine is big or little endian,
** evaluated at runtime.
*/
#ifdef SQLITE4_AMALGAMATION
const int sqlite4one = 1;
#else
extern const int sqlite4one;
#endif
#if defined(i386) || defined(__i386__) || defined(_M_IX86)\
                             || defined(__x86_64) || defined(__x86_64__)
# define SQLITE4_BIGENDIAN    0
# define SQLITE4_LITTLEENDIAN 1
# define SQLITE4_UTF16NATIVE  SQLITE4_UTF16LE
#else
# define SQLITE4_BIGENDIAN    (*(char *)(&sqlite4one)==0)
# define SQLITE4_LITTLEENDIAN (*(char *)(&sqlite4one)==1)
# define SQLITE4_UTF16NATIVE (SQLITE4_BIGENDIAN?SQLITE4_UTF16BE:SQLITE4_UTF16LE)
#endif

/*
** Constants for the largest and smallest possible 64-bit signed integers.
** These macros are designed to work correctly on both 32-bit and 64-bit
** compilers.
*/
#define LARGEST_INT64  (0xffffffff|(((i64)0x7fffffff)<<32))
#define SMALLEST_INT64 (((i64)-1) - LARGEST_INT64)
#define LARGEST_UINT64  (0xffffffff|(((i64)0xffffffff)<<32))

/* 
** Round up a number to the next larger multiple of 8.  This is used
** to force 8-byte alignment on 64-bit architectures.
*/
#define ROUND8(x)     (((x)+7)&~7)

/*
** Round down to the nearest multiple of 8
*/
#define ROUNDDOWN8(x) ((x)&~7)

/*
** Min and max macros.
*/
#define SQLITE4_MIN(a,b) (((a)<(b)) ? (a) : (b))
#define SQLITE4_MAX(a,b) (((a)>(b)) ? (a) : (b))

/*
** Assert that the pointer X is aligned to an 8-byte boundary.  This
** macro is used only within assert() to verify that the code gets
** all alignment restrictions correct.
**
** Except, if SQLITE4_4_BYTE_ALIGNED_MALLOC is defined, then the
** underlying malloc() implemention might return us 4-byte aligned
** pointers.  In that case, only verify 4-byte alignment.
*/
#ifdef SQLITE4_4_BYTE_ALIGNED_MALLOC
# define EIGHT_BYTE_ALIGNMENT(X)   ((((char*)(X) - (char*)0)&3)==0)
#else
# define EIGHT_BYTE_ALIGNMENT(X)   ((((char*)(X) - (char*)0)&7)==0)
#endif


/*
** Name of the master database table.  The master database table
** is a special table that holds the names and attributes of all
** user tables and indices.
*/
#define MASTER_NAME       "sqlite_master"
#define TEMP_MASTER_NAME  "sqlite_temp_master"

/*
** The root-page of the master database table.
*/
#define MASTER_ROOT       1

/*
** The name of the schema table.
*/
#define SCHEMA_TABLE(x)  ((!OMIT_TEMPDB)&&(x==1)?TEMP_MASTER_NAME:MASTER_NAME)

/*
** A convenience macro that returns the number of elements in
** an array.
*/
#define ArraySize(X)    ((int)(sizeof(X)/sizeof(X[0])))

/*
** The following macros are used to suppress compiler warnings and to
** make it clear to human readers when a function parameter is deliberately 
** left unused within the body of a function. This usually happens when
** a function is called via a function pointer. For example the 
** implementation of an SQL aggregate step callback may not use the
** parameter indicating the number of arguments passed to the aggregate,
** if it knows that this is enforced elsewhere.
**
** When a function parameter is not used at all within the body of a function,
** it is generally named "NotUsed" or "NotUsed2" to make things even clearer.
** However, these macros may also be used to suppress warnings related to
** parameters that may or may not be used depending on compilation options.
** For example those parameters only used in assert() statements. In these
** cases the parameters are named as per the usual conventions.
*/
#define UNUSED_PARAMETER(x) (void)(x)
#define UNUSED_PARAMETER2(x,y) UNUSED_PARAMETER(x),UNUSED_PARAMETER(y)

/*
** Forward references to structures
*/
typedef struct AggInfo AggInfo;
typedef struct AggInfoCol AggInfoCol;
typedef struct AggInfoFunc AggInfoFunc;
typedef struct AuthContext AuthContext;
typedef struct AutoincInfo AutoincInfo;
typedef struct CollSeq CollSeq;
typedef struct Column Column;
typedef struct CreateIndex CreateIndex;
typedef struct Db Db;
typedef struct Schema Schema;
typedef struct Expr Expr;
typedef struct ExprList ExprList;
typedef struct ExprListItem ExprListItem;
typedef struct ExprSpan ExprSpan;
typedef struct FKey FKey;
typedef struct FuncDestructor FuncDestructor;
typedef struct FuncDef FuncDef;
typedef struct FuncDefTable FuncDefTable;
typedef struct Fts5Tokenizer Fts5Tokenizer;
typedef struct Fts5Index Fts5Index;
typedef struct Fts5Info Fts5Info;
typedef struct Fts5Cursor Fts5Cursor;
typedef struct IdList IdList;
typedef struct IdListItem IdListItem;
typedef struct Index Index;
typedef struct IndexSample IndexSample;
typedef struct KeyClass KeyClass;
typedef struct KeyInfo KeyInfo;
typedef struct Lookaside Lookaside;
typedef struct LookasideSlot LookasideSlot;
typedef struct Module Module;
typedef struct NameContext NameContext;
typedef struct Parse Parse;
typedef struct ParseYColCache ParseYColCache;
typedef struct RowSet RowSet;
typedef struct Savepoint Savepoint;
typedef struct Select Select;
typedef struct Sqlite4InitInfo Sqlite4InitInfo;
typedef struct SrcList SrcList;
typedef struct SrcListItem SrcListItem;
typedef struct StrAccum StrAccum;
typedef struct Table Table;
typedef struct Token Token;
typedef struct Trigger Trigger;
typedef struct TriggerPrg TriggerPrg;
typedef struct TriggerStep TriggerStep;
typedef struct UnpackedRecord UnpackedRecord;
typedef struct VTable VTable;
typedef struct VtabCtx VtabCtx;
typedef struct Walker Walker;
typedef struct WherePlan WherePlan;
typedef struct WhereInfo WhereInfo;
typedef struct WhereLevel WhereLevel;


#include "vdbe.h"
#include "kv.h"

#include "os.h"
#include "mutex.h"


/*
** Each database file to be accessed by the system is an instance
** of the following structure.  There are normally two of these structures
** in the sqlite.aDb[] array.  aDb[0] is the main database file and
** aDb[1] is the database file used to hold temporary tables.  Additional
** databases may be attached.
*/
struct Db {
  char *zName;         /* Name of this database */
  KVStore *pKV;        /* KV store for the database file */
  u8 inTrans;          /* 0: not writable.  1: Transaction.  2: Checkpoint */
  u8 chngFlag;         /* True if modified */
  Schema *pSchema;     /* Pointer to database schema (possibly shared) */
};

/*
** Each SQL function is defined by an instance of the following
** structure.  A pointer to this structure is stored in the sqlite.aFunc
** hash table.  When multiple functions have the same name, the hash table
** points to a linked list of these structures.
*/
struct FuncDef {
  i16 nArg;            /* Number of arguments.  -1 means unlimited */
  u8 iPrefEnc;         /* Preferred text encoding (SQLITE4_UTF8, 16LE, 16BE) */
  u8 flags;            /* Some combination of SQLITE4_FUNC_* */
  void *pUserData;     /* User data parameter */
  FuncDef *pSameName;  /* Next with a different name but the same hash */
  void (*xFunc)(sqlite4_context*,int,sqlite4_value**); /* Regular function */
  void (*xStep)(sqlite4_context*,int,sqlite4_value**); /* Aggregate step */
  void (*xFinalize)(sqlite4_context*);                /* Aggregate finalizer */
  char *zName;         /* SQL name of the function. */
  FuncDef *pNextName;  /* Next function with a different name */
  FuncDestructor *pDestructor;   /* Reference counted destructor function */
  u8 bMatchinfo;       /* True for matchinfo function */
};

/*
** A table of SQL functions.  
**
** The content is a linked list of FuncDef structures with branches.  When
** there are two or more FuncDef objects with the same name, they are 
** connected using FuncDef.pSameName.  FuncDef objects with different names
** are connected using FuncDef.pNextName.
*/
struct FuncDefTable {
  FuncDef *pFirst;     /* First function definition */
  FuncDef *pLast;      /* Last function definition */
  FuncDef *pSame;      /* Tail of pSameName list for pLast */
};

/*
** An instance of the following structure stores a database schema.
**
** Most Schema objects are associated with a database file.  The exception is
** the Schema for the TEMP databaes (sqlite4.aDb[1]) which is free-standing.
** 
** Schema objects are automatically deallocated when the last database that
** references them is destroyed.   The TEMP Schema is manually freed by
** sqlite4_close().
*
** A thread must be holding a mutex on the corresponding database in order
** to access Schema content.  This implies that the thread must also be
** holding a mutex on the sqlite4 connection pointer that owns the database
** For a TEMP Schema, only the connection mutex is required.
*/
struct Schema {
  int schema_cookie;   /* Database schema version number for this file */
  int iGeneration;     /* Generation counter.  Incremented with each change */
  Hash tblHash;        /* All tables indexed by name */
  Hash idxHash;        /* All (named) indices indexed by name */
  Hash trigHash;       /* All triggers indexed by name */
  Hash fkeyHash;       /* All foreign keys by referenced table name */
  Table *pSeqTab;      /* The sqlite_sequence table used by AUTOINCREMENT */
  u8 file_format;      /* Schema format version for this file */
  u8 enc;              /* Text encoding used by this database */
  u16 flags;           /* Flags associated with this schema */
  int cache_size;      /* Number of pages to use in the cache */
};

/*
** These macros can be used to test, set, or clear bits in the 
** Db.pSchema->flags field.
*/
#define DbHasProperty(D,I,P)     (((D)->aDb[I].pSchema->flags&(P))==(P))
#define DbHasAnyProperty(D,I,P)  (((D)->aDb[I].pSchema->flags&(P))!=0)
#define DbSetProperty(D,I,P)     (D)->aDb[I].pSchema->flags|=(P)
#define DbClearProperty(D,I,P)   (D)->aDb[I].pSchema->flags&=~(P)

/*
** Allowed values for the DB.pSchema->flags field.
**
** The DB_SchemaLoaded flag is set after the database schema has been
** read into internal hash tables.
**
** DB_UnresetViews means that one or more views have column names that
** have been filled out.  If the schema changes, these column names might
** changes and so the view will need to be reset.
*/
#define DB_SchemaLoaded    0x0001  /* The schema has been loaded */
#define DB_UnresetViews    0x0002  /* Some views have defined column names */
#define DB_Empty           0x0004  /* The file is empty (length 0 bytes) */

/*
** The number of different kinds of things that can be limited
** using the sqlite4_limit() interface.
*/
#define SQLITE4_N_LIMIT (SQLITE4_LIMIT_TRIGGER_DEPTH+1)

/*
** Lookaside malloc is a set of fixed-size buffers that can be used
** to satisfy small transient memory allocation requests for objects
** associated with a particular database connection.  The use of
** lookaside malloc provides a significant performance enhancement
** (approx 10%) by avoiding numerous malloc/free requests while parsing
** SQL statements.
**
** The Lookaside structure holds configuration information about the
** lookaside malloc subsystem.  Each available memory allocation in
** the lookaside subsystem is stored on a linked list of LookasideSlot
** objects.
**
** Lookaside allocations are only allowed for objects that are associated
** with a particular database connection.  Hence, schema information cannot
** be stored in lookaside because in shared cache mode the schema information
** is shared by multiple database connections.  Therefore, while parsing
** schema information, the Lookaside.bEnabled flag is cleared so that
** lookaside allocations are not used to construct the schema objects.
*/
struct Lookaside {
  u16 sz;                 /* Size of each buffer in bytes */
  u8 bEnabled;            /* False to disable new lookaside allocations */
  u8 bMalloced;           /* True if pStart obtained from sqlite4_malloc() */
  int nOut;               /* Number of buffers currently checked out */
  int mxOut;              /* Highwater mark for nOut */
  int anStat[3];          /* 0: hits.  1: size misses.  2: full misses */
  LookasideSlot *pFree;   /* List of available buffers */
  void *pStart;           /* First byte of available memory space */
  void *pEnd;             /* First byte past end of available space */
};
struct LookasideSlot {
  LookasideSlot *pNext;    /* Next buffer in the list of free buffers */
};

/*
** Information used during initialization.
*/
struct Sqlite4InitInfo {
  int iDb;                    /* When back is being initialized */
  int newTnum;                /* Rootpage of table being initialized */
  u8 busy;                    /* TRUE if currently initializing */
  u8 orphanTrigger;           /* Last statement is orphaned TEMP trigger */
};


/*
** Each database connection is an instance of the following structure.
**
** The sqlite.lastRowid records the last insert rowid generated by an
** insert statement.  Inserts on views do not affect its value.  Each
** trigger has its own context, so that lastRowid can be updated inside
** triggers as usual.  The previous value will be restored once the trigger
** exits.  Upon entering a before or instead of trigger, lastRowid is no
** longer (since after version 2.8.12) reset to -1.
**
** The sqlite.nChange does not count changes within triggers and keeps no
** context.  It is reset at start of sqlite4_exec.
** The sqlite.lsChange represents the number of changes made by the last
** insert, update, or delete statement.  It remains constant throughout the
** length of a statement and is then updated by OP_SetCounts.  It keeps a
** context stack just like lastRowid so that the count of changes
** within a trigger is not seen outside the trigger.  Changes to views do not
** affect the value of lsChange.
** The sqlite.csChange keeps track of the number of current changes (since
** the last statement) and is used to update sqlite_lsChange.
**
** The member variables sqlite.errCode, sqlite.zErrMsg and sqlite.zErrMsg16
** store the most recent error code and, if applicable, string. The
** internal function sqlite4Error() is used to set these variables
** consistently.
*/
struct sqlite4 {
  sqlite4_env *pEnv;            /* The run-time environment */
  int nDb;                      /* Number of backends currently in use */
  Db *aDb;                      /* All backends */
  int flags;                    /* Miscellaneous flags. See below */
  unsigned int openFlags;       /* Flags passed to sqlite4_vfs.xOpen() */
  int errCode;                  /* Most recent error code (SQLITE4_*) */
  u8 temp_store;                /* 1: file 2: memory 0: default */
  u8 mallocFailed;              /* True if we have seen a malloc failure */
  u8 dfltLockMode;              /* Default locking-mode for attached dbs */
  signed char nextAutovac;      /* Autovac setting after VACUUM if >=0 */
  u8 suppressErr;               /* Do not issue error messages if true */
  u8 vtabOnConflict;            /* Value to return for s3_vtab_on_conflict() */
  int nextPagesize;             /* Pagesize after VACUUM if >0 */
  int nTable;                   /* Number of tables in the database */
  CollSeq *pDfltColl;           /* The default collating sequence (BINARY) */
  i64 lastRowid;                /* ROWID of most recent insert (see above) */
  u32 magic;                    /* Magic number for detect library misuse */
  int nChange;                  /* Value returned by sqlite4_changes() */
  int nTotalChange;             /* Value returned by sqlite4_total_changes() */
  sqlite4_mutex *mutex;         /* Connection mutex */
  int aLimit[SQLITE4_N_LIMIT];   /* Limits */
  Sqlite4InitInfo init;         /* Information used during initialization */
  int nExtension;               /* Number of loaded extensions */
  void **aExtension;            /* Array of shared library handles */
  struct Vdbe *pVdbe;           /* List of active virtual machines */
  int activeVdbeCnt;            /* Number of VDBEs currently executing */
  int writeVdbeCnt;             /* Number of active VDBEs that are writing */
  int vdbeExecCnt;              /* Number of nested calls to VdbeExec() */
  void (*xTrace)(void*,const char*);        /* Trace function */
  void *pTraceArg;                          /* Argument to the trace function */
  void (*xProfile)(void*,const char*,u64);  /* Profiling function */
  void *pProfileArg;                        /* Argument to profile function */
#ifndef SQLITE4_OMIT_WAL
  int (*xWalCallback)(void *, sqlite4 *, const char *, int);
  void *pWalArg;
#endif
  void(*xCollNeeded)(void*,sqlite4*,int eTextRep,const char*);
  void(*xCollNeeded16)(void*,sqlite4*,int eTextRep,const void*);
  void *pCollNeededArg;
  sqlite4_value *pErr;          /* Most recent error message */
  char *zErrMsg;                /* Most recent error message (UTF-8 encoded) */
  char *zErrMsg16;              /* Most recent error message (UTF-16 encoded) */
  union {
    volatile int isInterrupted; /* True if sqlite4_interrupt has been called */
    double notUsed1;            /* Spacer */
  } u1;
  Lookaside lookaside;          /* Lookaside malloc configuration */
#ifndef SQLITE4_OMIT_AUTHORIZATION
  int (*xAuth)(void*,int,const char*,const char*,const char*,const char*);
                                /* Access authorization function */
  void *pAuthArg;               /* 1st argument to the access auth function */
#endif
#ifndef SQLITE4_OMIT_PROGRESS_CALLBACK
  int (*xProgress)(void *);     /* The progress callback */
  void *pProgressArg;           /* Argument to the progress callback */
  int nProgressOps;             /* Number of opcodes for progress callback */
#endif
#ifndef SQLITE4_OMIT_VIRTUALTABLE
  Hash aModule;                 /* populated by sqlite4_create_module() */
  VtabCtx *pVtabCtx;            /* Context for active vtab connect/create */
  VTable **aVTrans;             /* Virtual tables with open transactions */
  int nVTrans;                  /* Allocated size of aVTrans */
  VTable *pDisconnect;    /* Disconnect these in next sqlite4_prepare() */
#endif
  FuncDefTable aFunc;            /* Hash table of connection functions */
  Hash aCollSeq;                /* All collating sequences */
  Db aDbStatic[2];              /* Static space for the 2 default backends */
  Savepoint *pSavepoint;        /* List of active savepoints */
  int nSavepoint;               /* Number of open savepoints */
  int nStatement;               /* Number of nested statement-transactions  */
  i64 nDeferredCons;            /* Net deferred constraints this transaction. */
  int *pnBytesFreed;            /* If not NULL, increment this in DbFree() */

#ifdef SQLITE4_ENABLE_UNLOCK_NOTIFY
  /* The following variables are all protected by the STATIC_MASTER 
  ** mutex, not by sqlite4.mutex. They are used by code in notify.c. 
  **
  ** When X.pUnlockConnection==Y, that means that X is waiting for Y to
  ** unlock so that it can proceed.
  **
  ** When X.pBlockingConnection==Y, that means that something that X tried
  ** tried to do recently failed with an SQLITE4_LOCKED error due to locks
  ** held by Y.
  */
  sqlite4 *pBlockingConnection; /* Connection that caused SQLITE4_LOCKED */
  sqlite4 *pUnlockConnection;           /* Connection to watch for unlock */
  void *pUnlockArg;                     /* Argument to xUnlockNotify */
  void (*xUnlockNotify)(void **, int);  /* Unlock notify callback */
  sqlite4 *pNextBlocked;        /* Next in list of all blocked connections */
#endif

#ifndef SQLITE_OMIT_FTS5
  Fts5Tokenizer *pTokenizer;      /* First in list of tokenizers */
#endif
};

/*
** A macro to discover the encoding of a database.
*/
#define ENC(db) ((db)->aDb[0].pSchema->enc)

/*
** Possible values for the sqlite4.flags.
*/
#define SQLITE4_VdbeTrace      0x00000100  /* True to trace VDBE execution */
#define SQLITE4_InternChanges  0x00000200  /* Uncommitted Hash table changes */
#define SQLITE4_CountRows      0x00001000  /* Count rows changed by INSERT, */
                                          /*   DELETE, or UPDATE and return */
                                          /*   the count using a callback. */
#define SQLITE4_SqlTrace       0x00004000  /* Debug print SQL as it executes */
#define SQLITE4_VdbeListing    0x00008000  /* Debug listings of VDBE programs */
#define SQLITE4_WriteSchema    0x00010000  /* OK to update SQLITE4_MASTER */
#define SQLITE4_KvTrace        0x00020000  /* Trace Key/value storage calls */
#define SQLITE4_IgnoreChecks   0x00040000  /* Do not enforce check constraints */
#define SQLITE4_ReadUncommitted 0x0080000  /* For shared-cache mode */
#define SQLITE4_LegacyFileFmt  0x00100000  /* Create new databases in format 1 */
#define SQLITE4_RecoveryMode   0x00800000  /* Ignore schema errors */
#define SQLITE4_ReverseOrder   0x01000000  /* Reverse unordered SELECTs */
#define SQLITE4_RecTriggers    0x02000000  /* Enable recursive triggers */
#define SQLITE4_ForeignKeys    0x04000000  /* Enforce foreign key constraints  */
#define SQLITE4_AutoIndex      0x08000000  /* Enable automatic indexes */
#define SQLITE4_PreferBuiltin  0x10000000  /* Preference to built-in funcs */
#define SQLITE4_EnableTrigger  0x40000000  /* True to enable triggers */

/*
** Bits of the sqlite4.flags field that are used by the
** sqlite4_test_control(SQLITE4_TESTCTRL_OPTIMIZATIONS,...) interface.
** These must be the low-order bits of the flags field.
*/
#define SQLITE4_QueryFlattener 0x01        /* Disable query flattening */
#define SQLITE4_ColumnCache    0x02        /* Disable the column cache */
#define SQLITE4_IndexSort      0x04        /* Disable indexes for sorting */
#define SQLITE4_IndexSearch    0x08        /* Disable indexes for searching */
#define SQLITE4_IndexCover     0x10        /* Disable index covering table */
#define SQLITE4_GroupByOrder   0x20        /* Disable GROUPBY cover of ORDERBY */
#define SQLITE4_FactorOutConst 0x40        /* Disable factoring out constants */
#define SQLITE4_IdxRealAsInt   0x80        /* Store REAL as INT in indices */
#define SQLITE4_DistinctOpt    0x80        /* DISTINCT using indexes */
#define SQLITE4_OptMask        0xff        /* Mask of all disablable opts */

/*
** Possible values for the sqlite.magic field.
** The numbers are obtained at random and have no special meaning, other
** than being distinct from one another.
*/
#define SQLITE4_MAGIC_OPEN     0xa029a697  /* Database is open */
#define SQLITE4_MAGIC_CLOSED   0x9f3c2d33  /* Database is closed */
#define SQLITE4_MAGIC_SICK     0x4b771290  /* Error and awaiting close */
#define SQLITE4_MAGIC_BUSY     0xf03b7906  /* Database currently in use */
#define SQLITE4_MAGIC_ERROR    0xb5357930  /* An SQLITE4_MISUSE error occurred */

/*
** This structure encapsulates a user-function destructor callback (as
** configured using create_function_v2()) and a reference counter. When
** create_function_v2() is called to create a function with a destructor,
** a single object of this type is allocated. FuncDestructor.nRef is set to 
** the number of FuncDef objects created (either 1 or 3, depending on whether
** or not the specified encoding is SQLITE4_ANY). The FuncDef.pDestructor
** member of each of the new FuncDef objects is set to point to the allocated
** FuncDestructor.
**
** Thereafter, when one of the FuncDef objects is deleted, the reference
** count on this object is decremented. When it reaches 0, the destructor
** is invoked and the FuncDestructor structure freed.
*/
struct FuncDestructor {
  int nRef;
  void (*xDestroy)(void *);
  void *pUserData;
};

/*
** Possible values for FuncDef.flags
*/
#define SQLITE4_FUNC_LIKE     0x01 /* Candidate for the LIKE optimization */
#define SQLITE4_FUNC_CASE     0x02 /* Case-sensitive LIKE-type function */
#define SQLITE4_FUNC_EPHEM    0x04 /* Ephemeral.  Delete with VDBE */
#define SQLITE4_FUNC_NEEDCOLL 0x08 /* sqlite4GetFuncCollSeq() might be called */
#define SQLITE4_FUNC_PRIVATE  0x10 /* Allowed for internal use only */
#define SQLITE4_FUNC_COUNT    0x20 /* Built-in count(*) aggregate */
#define SQLITE4_FUNC_COALESCE 0x40 /* Built-in coalesce() or ifnull() function */

/*
** The following three macros, FUNCTION(), LIKEFUNC() and AGGREGATE() are
** used to create the initializers for the FuncDef structures.
**
**   FUNCTION(zName, nArg, iArg, bNC, xFunc)
**     Used to create a scalar function definition of a function zName 
**     implemented by C function xFunc that accepts nArg arguments. The
**     value passed as iArg is cast to a (void*) and made available
**     as the user-data (sqlite4_user_data()) for the function. If 
**     argument bNC is true, then the SQLITE4_FUNC_NEEDCOLL flag is set.
**
**   AGGREGATE(zName, nArg, iArg, bNC, xStep, xFinal)
**     Used to create an aggregate function definition implemented by
**     the C functions xStep and xFinal. The first four parameters
**     are interpreted in the same way as the first 4 parameters to
**     FUNCTION().
**
**   LIKEFUNC(zName, nArg, pArg, flags)
**     Used to create a scalar function definition of a function zName 
**     that accepts nArg arguments and is implemented by a call to C 
**     function likeFunc. Argument pArg is cast to a (void *) and made
**     available as the function user-data (sqlite4_user_data()). The
**     FuncDef.flags variable is set to the value passed as the flags
**     parameter.
*/
#define FUNCTION(zName, nArg, iArg, bNC, xFunc) \
  {nArg, SQLITE4_UTF8, bNC*SQLITE4_FUNC_NEEDCOLL, \
   SQLITE4_INT_TO_PTR(iArg), 0, xFunc, 0, 0, #zName, 0, 0}
#define STR_FUNCTION(zName, nArg, pArg, bNC, xFunc) \
  {nArg, SQLITE4_UTF8, bNC*SQLITE4_FUNC_NEEDCOLL, \
   pArg, 0, xFunc, 0, 0, #zName, 0, 0}
#define LIKEFUNC(zName, nArg, arg, flags) \
  {nArg, SQLITE4_UTF8, flags, (void *)arg, 0, likeFunc, 0, 0, #zName, 0, 0}
#define AGGREGATE(zName, nArg, arg, nc, xStep, xFinal) \
  {nArg, SQLITE4_UTF8, nc*SQLITE4_FUNC_NEEDCOLL, \
   SQLITE4_INT_TO_PTR(arg), 0, 0, xStep,xFinal,#zName,0,0}

/*
** All current savepoints are stored in a linked list starting at
** sqlite4.pSavepoint. The first element in the list is the most recently
** opened savepoint. Savepoints are added to the list by the vdbe
** OP_Savepoint instruction.
*/
struct Savepoint {
  char *zName;                        /* Savepoint name (nul-terminated) */
  i64 nDeferredCons;                  /* Number of deferred fk violations */
  Savepoint *pNext;                   /* Parent savepoint (if any) */
};

/*
** The following are used as the second parameter to sqlite4Savepoint(),
** and as the P1 argument to the OP_Savepoint instruction.
*/
#define SAVEPOINT_BEGIN      0
#define SAVEPOINT_RELEASE    1
#define SAVEPOINT_ROLLBACK   2


/*
** Each SQLite module (virtual table definition) is defined by an
** instance of the following structure, stored in the sqlite4.aModule
** hash table.
*/
struct Module {
  const sqlite4_module *pModule;       /* Callback pointers */
  const char *zName;                   /* Name passed to create_module() */
  void *pAux;                          /* pAux passed to create_module() */
  void (*xDestroy)(void *);            /* Module destructor function */
};

/*
** information about each column of an SQL table is held in an instance
** of this structure.
*/
struct Column {
  char *zName;     /* Name of this column */
  Expr *pDflt;     /* Default value of this column */
  char *zDflt;     /* Original text of the default value */
  char *zType;     /* Data type for this column */
  char *zColl;     /* Collating sequence.  If NULL, use the default */
  u8 notNull;      /* True if there is a NOT NULL constraint */
  u8 isPrimKey;    /* True if this column is part of the PRIMARY KEY */
  char affinity;   /* One of the SQLITE4_AFF_... values */
#ifndef SQLITE4_OMIT_VIRTUALTABLE
  u8 isHidden;     /* True if this column is 'hidden' */
#endif
};

/*
** A "Collating Sequence" is defined by an instance of the following
** structure. Conceptually, a collating sequence consists of a name and
** a comparison routine that defines the order of that sequence.
**
** There may two separate implementations of the collation function, one
** that processes text in UTF-8 encoding (CollSeq.xCmp) and another that
** processes text encoded in UTF-16 (CollSeq.xCmp16), using the machine
** native byte order. When a collation sequence is invoked, SQLite selects
** the version that will require the least expensive encoding
** translations, if any.
**
** The CollSeq.pUser member variable is an extra parameter that passed in
** as the first argument to the UTF-8 comparison function, xCmp.
** CollSeq.pUser16 is the equivalent for the UTF-16 comparison function,
** xCmp16.
**
** If both CollSeq.xCmp and CollSeq.xCmp16 are NULL, it means that the
** collating sequence is undefined.  Indices built on an undefined
** collating sequence may not be read or written.
*/
struct CollSeq {
  char *zName;          /* Name of the collating sequence, UTF-8 encoded */
  u8 enc;               /* Text encoding handled by xCmp() */
  void *pUser;          /* First argument to xCmp() */
  int (*xCmp)(void*,int, const void*, int, const void*);
  int (*xMkKey)(void*,int, const void*, int, void*);
  void (*xDel)(void*);  /* Destructor for pUser */
};

/*
** A sort order can be either ASC or DESC.
*/
#define SQLITE4_SO_ASC       0  /* Sort in ascending order */
#define SQLITE4_SO_DESC      1  /* Sort in ascending order */

/*
** Column affinity types.
**
** These used to have mnemonic name like 'i' for SQLITE4_AFF_INTEGER and
** 't' for SQLITE4_AFF_TEXT.  But we can save a little space and improve
** the speed a little by numbering the values consecutively.  
**
** But rather than start with 0 or 1, we begin with 'a'.  That way,
** when multiple affinity types are concatenated into a string and
** used as the P4 operand, they will be more readable.
**
** Note also that the numeric types are grouped together so that testing
** for a numeric type is a single comparison.
*/
#define SQLITE4_AFF_TEXT     'a'
#define SQLITE4_AFF_NONE     'b'
#define SQLITE4_AFF_NUMERIC  'c'
#define SQLITE4_AFF_INTEGER  'd'
#define SQLITE4_AFF_REAL     'e'

#define sqlite4IsNumericAffinity(X)  ((X)>=SQLITE4_AFF_NUMERIC)

/*
** The SQLITE4_AFF_MASK values masks off the significant bits of an
** affinity value. 
*/
#define SQLITE4_AFF_MASK     0x67

/*
** Additional bit values that can be ORed with an affinity without
** changing the affinity.
*/
#define SQLITE4_JUMPIFNULL   0x08  /* jumps if either operand is NULL */
#define SQLITE4_STOREP2      0x10  /* Store result in reg[P2] rather than jump */
#define SQLITE4_NULLEQ       0x80  /* NULL=NULL */

/*
** An object of this type is created for each virtual table present in
** the database schema. 
**
** If the database schema is shared, then there is one instance of this
** structure for each database connection (sqlite4*) that uses the shared
** schema. This is because each database connection requires its own unique
** instance of the sqlite4_vtab* handle used to access the virtual table 
** implementation. sqlite4_vtab* handles can not be shared between 
** database connections, even when the rest of the in-memory database 
** schema is shared, as the implementation often stores the database
** connection handle passed to it via the xConnect() or xCreate() method
** during initialization internally. This database connection handle may
** then be used by the virtual table implementation to access real tables 
** within the database. So that they appear as part of the callers 
** transaction, these accesses need to be made via the same database 
** connection as that used to execute SQL operations on the virtual table.
**
** All VTable objects that correspond to a single table in a shared
** database schema are initially stored in a linked-list pointed to by
** the Table.pVTable member variable of the corresponding Table object.
** When an sqlite4_prepare() operation is required to access the virtual
** table, it searches the list for the VTable that corresponds to the
** database connection doing the preparing so as to use the correct
** sqlite4_vtab* handle in the compiled query.
**
** When an in-memory Table object is deleted (for example when the
** schema is being reloaded for some reason), the VTable objects are not 
** deleted and the sqlite4_vtab* handles are not xDisconnect()ed 
** immediately. Instead, they are moved from the Table.pVTable list to
** another linked list headed by the sqlite4.pDisconnect member of the
** corresponding sqlite4 structure. They are then deleted/xDisconnected 
** next time a statement is prepared using said sqlite4*. This is done
** to avoid deadlock issues involving multiple sqlite4.mutex mutexes.
** Refer to comments above function sqlite4VtabUnlockList() for an
** explanation as to why it is safe to add an entry to an sqlite4.pDisconnect
** list without holding the corresponding sqlite4.mutex mutex.
**
** The memory for objects of this type is always allocated by 
** sqlite4DbMalloc(), using the connection handle stored in VTable.db as 
** the first argument.
*/
struct VTable {
  sqlite4 *db;              /* Database connection associated with this table */
  Module *pMod;             /* Pointer to module implementation */
  sqlite4_vtab *pVtab;      /* Pointer to vtab instance */
  int nRef;                 /* Number of pointers to this structure */
  u8 bConstraint;           /* True if constraints are supported */
  int iSavepoint;           /* Depth of the SAVEPOINT stack */
  VTable *pNext;            /* Next in linked list (see above) */
};

/*
** Each SQL table is represented in memory by an instance of the
** following structure.
*/
struct Table {
  char *zName;         /* Name of the table or view */
  int nCol;            /* Number of columns in this table */
  Column *aCol;        /* Information about each column */
  Index *pIndex;       /* List of SQL indexes on this table. */
  tRowcnt nRowEst;     /* Estimated rows in table - from sqlite_stat1 table */
  Select *pSelect;     /* NULL for tables.  Points to definition if a view. */
  u16 nRef;            /* Number of pointers to this Table */
  u8 tabFlags;         /* Mask of TF_* values */
  FKey *pFKey;         /* Linked list of all foreign keys in this table */
  char *zColAff;       /* String defining the affinity of each column */
#ifndef SQLITE4_OMIT_CHECK
  Expr *pCheck;        /* The AND of all CHECK constraints */
#endif
#ifndef SQLITE4_OMIT_ALTERTABLE
  int addColOffset;    /* Offset in CREATE TABLE stmt to add a new column */
#endif
#ifndef SQLITE4_OMIT_VIRTUALTABLE
  VTable *pVTable;     /* List of VTable objects. */
  int nModuleArg;      /* Number of arguments to the module */
  char **azModuleArg;  /* Text of all module args. [0] is module name */
#endif
  Trigger *pTrigger;   /* List of triggers stored in pSchema */
  Schema *pSchema;     /* Schema that contains this table */
  Table *pNextZombie;  /* Next on the Parse.pZombieTab list */
};

/*
** Allowed values for Tabe.tabFlags.
*/
#define TF_Readonly        0x01    /* Read-only system table */
#define TF_Ephemeral       0x02    /* An ephemeral table */
#define TF_HasPrimaryKey   0x04    /* Table has a primary key */
#define TF_Autoincrement   0x08    /* Integer primary key is autoincrement */
#define TF_Virtual         0x10    /* Is a virtual table */
#define TF_NeedMetadata    0x20    /* aCol[].zType and aCol[].pColl missing */



/*
** Test to see whether or not a table is a virtual table.  This is
** done as a macro so that it will be optimized out when virtual
** table support is omitted from the build.
*/
#ifndef SQLITE4_OMIT_VIRTUALTABLE
#  define IsVirtual(X)      (((X)->tabFlags & TF_Virtual)!=0)
#  define IsHiddenColumn(X) ((X)->isHidden)
#else
#  define IsVirtual(X)      0
#  define IsHiddenColumn(X) 0
#endif

/* Test to see if a table is actually a view. */
#ifndef SQLITE4_OMIT_VIEW
#  define IsView(X)         ((X)->pSelect!=0)
#else
#  define IsView(X)         0
#endif

/*
** Each foreign key constraint is an instance of the following structure.
**
** A foreign key is associated with two tables.  The "from" table is
** the table that contains the REFERENCES clause that creates the foreign
** key.  The "to" table is the table that is named in the REFERENCES clause.
** Consider this example:
**
**     CREATE TABLE ex1(
**       a INTEGER PRIMARY KEY,
**       b INTEGER CONSTRAINT fk1 REFERENCES ex2(x)
**     );
**
** For foreign key "fk1", the from-table is "ex1" and the to-table is "ex2".
**
** Each REFERENCES clause generates an instance of the following structure
** which is attached to the from-table.  The to-table need not exist when
** the from-table is created.  The existence of the to-table is not checked.
*/
struct FKey {
  Table *pFrom;     /* Table containing the REFERENCES clause (aka: Child) */
  FKey *pNextFrom;  /* Next foreign key in pFrom */
  char *zTo;        /* Name of table that the key points to (aka: Parent) */
  FKey *pNextTo;    /* Next foreign key on table named zTo */
  FKey *pPrevTo;    /* Previous foreign key on table named zTo */
  int nCol;         /* Number of columns in this key */
  /* EV: R-30323-21917 */
  u8 isDeferred;    /* True if constraint checking is deferred till COMMIT */
  u8 aAction[2];          /* ON DELETE and ON UPDATE actions, respectively */
  Trigger *apTrigger[2];  /* Triggers for aAction[] actions */
  struct sColMap {  /* Mapping of columns in pFrom to columns in zTo */
    int iFrom;         /* Index of column in pFrom */
    char *zCol;        /* Name of column in zTo.  If 0 use PRIMARY KEY */
  } aCol[1];        /* One entry for each of nCol column s */
};

/*
** SQLite supports many different ways to resolve a constraint
** error.  ROLLBACK processing means that a constraint violation
** causes the operation in process to fail and for the current transaction
** to be rolled back.  ABORT processing means the operation in process
** fails and any prior changes from that one operation are backed out,
** but the transaction is not rolled back.  FAIL processing means that
** the operation in progress stops and returns an error code.  But prior
** changes due to the same operation are not backed out and no rollback
** occurs.  IGNORE means that the particular row that caused the constraint
** error is not inserted or updated.  Processing continues and no error
** is returned.  REPLACE means that preexisting database rows that caused
** a UNIQUE constraint violation are removed so that the new insert or
** update can proceed.  Processing continues and no error is reported.
**
** RESTRICT, SETNULL, and CASCADE actions apply only to foreign keys.
** RESTRICT is the same as ABORT for IMMEDIATE foreign keys and the
** same as ROLLBACK for DEFERRED keys.  SETNULL means that the foreign
** key is set to NULL.  CASCADE means that a DELETE or UPDATE of the
** referenced table row is propagated into the row that holds the
** foreign key.
** 
** The following symbolic values are used to record which type
** of action to take.
*/
#define OE_None     0   /* There is no constraint to check */
#define OE_Rollback 1   /* Fail the operation and rollback the transaction */
#define OE_Abort    2   /* Back out changes but do no rollback transaction */
#define OE_Fail     3   /* Stop the operation but leave all prior changes */
#define OE_Ignore   4   /* Ignore the error. Do not do the INSERT or UPDATE */
#define OE_Replace  5   /* Delete existing record, then do INSERT or UPDATE */

#define OE_Restrict 6   /* OE_Abort for IMMEDIATE, OE_Rollback for DEFERRED */
#define OE_SetNull  7   /* Set the foreign key value to NULL */
#define OE_SetDflt  8   /* Set the foreign key value to its default */
#define OE_Cascade  9   /* Cascade the changes */

#define OE_Default  99  /* Do whatever the default action is */


/*
** An instance of the following structure describes an index key.  It 
** includes information such as sort order and collating sequence for
** each key, and the number of primary key fields appended to the end.
*/
struct KeyInfo {
  sqlite4 *db;        /* The database connection */
  u8 enc;             /* Text encoding - one of the SQLITE4_UTF* values */
  u16 nField;         /* Total number of entries in aColl[] */
  u16 nPK;            /* Number of primary key entries at the end of aColl[] */
  u16 nData;          /* Number of columns of data in KV entry value */
  u8 *aSortOrder;     /* Sort order for each column.  May be NULL */
  CollSeq *aColl[1];  /* Collating sequence for each term of the key */
};

/*
** An instance of the following structure holds information about a
** single index record that has already been parsed out into individual
** values.
**
** A record is an object that contains one or more fields of data.
** Records are used to store the content of a table row and to store
** the key of an index.  A blob encoding of a record is created by
** the OP_MakeRecord opcode of the VDBE and is disassembled by the
** OP_Column opcode.
**
** This structure holds a record that has already been disassembled
** into its constituent fields.
*/
struct UnpackedRecord {
  KeyInfo *pKeyInfo;  /* Collation and sort-order information */
  u16 nField;         /* Number of entries in apMem[] */
  u8 flags;           /* Boolean settings.  UNPACKED_... below */
  i64 rowid;          /* Used by UNPACKED_PREFIX_SEARCH */
  Mem *aMem;          /* Values */
};

/*
** Allowed values of UnpackedRecord.flags
*/
#define UNPACKED_INCRKEY       0x01  /* Make this key an epsilon larger */
#define UNPACKED_PREFIX_MATCH  0x02  /* A prefix match is considered OK */
#define UNPACKED_PREFIX_SEARCH 0x04  /* Ignore final (rowid) field */

/*
** Each SQL index is represented in memory by an
** instance of the following structure.
**
** The columns of the table that are to be indexed are described
** by the aiColumn[] field of this structure.  For example, suppose
** we have the following table and index:
**
**     CREATE TABLE Ex1(c1 int, c2 int, c3 text);
**     CREATE INDEX Ex2 ON Ex1(c3,c1);
**
** In the Table structure describing Ex1, nCol==3 because there are
** three columns in the table.  In the Index structure describing
** Ex2, nColumn==2 since 2 of the 3 columns of Ex1 are indexed.
** The value of aiColumn is {2, 0}.  aiColumn[0]==2 because the 
** first column to be indexed (c3) has an index of 2 in Ex1.aCol[].
** The second column to be indexed (c1) has an index of 0 in
** Ex1.aCol[], hence Ex2.aiColumn[1]==0.
**
** The Index.onError field determines whether or not the indexed columns
** must be unique and what to do if they are not.  When Index.onError=OE_None,
** it means this is not a unique index.  Otherwise it is a unique index
** and the value of Index.onError indicate the which conflict resolution 
** algorithm to employ whenever an attempt is made to insert a non-unique
** element.
*/
struct Index {
  char *zName;     /* Name of this index */
  int nColumn;     /* Number of columns in the table used by this index */
  int *aiColumn;   /* Which columns are used by this index.  1st is 0 */
  tRowcnt *aiRowEst; /* Result of ANALYZE: Est. rows selected by each column */
  Table *pTable;   /* The SQL table being indexed */
  int tnum;        /* Page containing root of this index in database file */
  u8 onError;      /* OE_Abort, OE_Ignore, OE_Replace, or OE_None */
  u8 eIndexType;   /* SQLITE4_INDEX_USER, UNIQUE or PRIMARYKEY */
  u8 fIndex;       /* One or more of the IDX_* flags below */
  char *zColAff;   /* String defining the affinity of each column */
  Index *pNext;    /* The next index associated with the same table */
  Schema *pSchema; /* Schema containing this index */
  u8 *aSortOrder;  /* Array of size Index.nColumn. True==DESC, False==ASC */
  char **azColl;   /* Array of collation sequence names for index */
#ifdef SQLITE4_ENABLE_STAT3
  int nSample;             /* Number of elements in aSample[] */
  tRowcnt avgEq;           /* Average nEq value for key values not in aSample */
  IndexSample *aSample;    /* Samples of the left-most key */
#endif
  Fts5Index *pFts; /* Fts5 data (or NULL if this is not an fts index) */
};

/* Index.eIndexType must be set to one of the following. */
#define SQLITE4_INDEX_USER       0 /* Index created by CREATE INDEX statement */
#define SQLITE4_INDEX_UNIQUE     1 /* Index created by UNIQUE constraint */
#define SQLITE4_INDEX_PRIMARYKEY 2 /* Index is the tables PRIMARY KEY */
#define SQLITE4_INDEX_FTS5       3 /* Index is an FTS5 index */
#define SQLITE4_INDEX_TEMP       4 /* Index is an automatic index */

/* Allowed values for Index.fIndex */
#define IDX_IntPK             0x01 /* An INTEGER PRIMARY KEY index */
#define IDX_Unordered         0x02 /* Implemented as a hashing index */

/*
** Each sample stored in the sqlite_stat3 table is represented in memory 
** using a structure of this type.  See documentation at the top of the
** analyze.c source file for additional information.
*/
struct IndexSample {
  union {
    char *z;        /* Value if eType is SQLITE4_TEXT or SQLITE4_BLOB */
    double r;       /* Value if eType is SQLITE4_FLOAT */
    i64 i;          /* Value if eType is SQLITE4_INTEGER */
  } u;
  u8 eType;         /* SQLITE4_NULL, SQLITE4_INTEGER ... etc. */
  int nByte;        /* Size in byte of text or blob. */
  tRowcnt nEq;      /* Est. number of rows where the key equals this sample */
  tRowcnt nLt;      /* Est. number of rows where key is less than this sample */
  tRowcnt nDLt;     /* Est. number of distinct keys less than this sample */
};

/*
** Each token coming out of the lexer is an instance of
** this structure.  Tokens are also used as part of an expression.
**
** Note if Token.z==0 then Token.dyn and Token.n are undefined and
** may contain random values.  Do not make any assumptions about Token.dyn
** and Token.n when Token.z==0.
*/
struct Token {
  const char *z;     /* Text of the token.  Not NULL-terminated! */
  unsigned int n;    /* Number of characters in this token */
};

/*
** An instance of this structure holds the results of parsing the first
** part of a CREATE INDEX statement. Instances exist only transiently 
** during parsing.
*/
struct CreateIndex {
  int bUnique;                    /* True if the UNIQUE keyword was present */
  int bIfnotexist;                /* True if IF NOT EXISTS was present */
  Token tCreate;                  /* CREATE token */
  Token tName1;                   /* First part of two part name */
  Token tName2;                   /* Second part of two part name */
  SrcList *pTblName;              /* Table index is created on */ 
};

/*
** One for each column used in source tables.
*/
struct AggInfoCol {
  Table *pTab;             /* Source table */
  int iTable;              /* Cursor number of the source table */
  int iColumn;             /* Column number within the source table */
  int iSorterColumn;       /* Column number in the sorting index */
  int iMem;                /* Memory location that acts as accumulator */
  Expr *pExpr;             /* The original expression */
};

/*
** One for each aggregate function.
*/
struct AggInfoFunc {
  Expr *pExpr;             /* Expression encoding the function */
  FuncDef *pFunc;          /* The aggregate function implementation */
  int iMem;                /* Memory location that acts as accumulator */
  int iDistinct;           /* Ephemeral table used to enforce DISTINCT */
};

/*
** An instance of this structure contains information needed to generate
** code for a SELECT that contains aggregate functions.
**
** If Expr.op==TK_AGG_COLUMN or TK_AGG_FUNCTION then Expr.pAggInfo is a
** pointer to this structure.  The Expr.iColumn field is the index in
** AggInfo.aCol[] or AggInfo.aFunc[] of information needed to generate
** code for that node.
**
** AggInfo.pGroupBy and AggInfo.aFunc.pExpr point to fields within the
** original Select structure that describes the SELECT statement.  These
** fields do not need to be freed when deallocating the AggInfo structure.
*/
struct AggInfo {
  u8 directMode;          /* Direct rendering mode means take data directly
                          ** from source tables rather than from accumulators */
  u8 useSortingIdx;       /* In direct mode, reference the sorting index rather
                          ** than the source table */
  int sortingIdx;         /* Cursor number of the sorting index */
  ExprList *pGroupBy;     /* The group by clause */
  int nSortingColumn;     /* Number of columns in the sorting index */
  AggInfoCol *aCol;       /* For each column used in source tables. */
  int nColumn;            /* Number of used entries in aCol[] */
  int nColumnAlloc;       /* Number of slots allocated for aCol[] */
  int nAccumulator;       /* Number of columns that show through to the output.
                          ** Additional columns are used only as parameters to
                          ** aggregate functions */
  AggInfoFunc *aFunc;     /* For each aggregate function */
  int nFunc;              /* Number of entries in aFunc[] */
  int nFuncAlloc;         /* Number of slots allocated for aFunc[] */
};

/*
** The datatype ynVar is a signed integer, either 16-bit or 32-bit.
** Usually it is 16-bits.  But if SQLITE4_MAX_VARIABLE_NUMBER is greater
** than 32767 we have to make it 32-bit.  16-bit is preferred because
** it uses less memory in the Expr object, which is a big memory user
** in systems with lots of prepared statements.  And few applications
** need more than about 10 or 20 variables.  But some extreme users want
** to have prepared statements with over 32767 variables, and for them
** the option is available (at compile-time).
*/
#if SQLITE4_MAX_VARIABLE_NUMBER<=32767
typedef i16 ynVar;
#else
typedef int ynVar;
#endif

/*
** Each node of an expression in the parse tree is an instance
** of this structure.
**
** Expr.op is the opcode. The integer parser token codes are reused
** as opcodes here. For example, the parser defines TK_GE to be an integer
** code representing the ">=" operator. This same integer code is reused
** to represent the greater-than-or-equal-to operator in the expression
** tree.
**
** If the expression is an SQL literal (TK_INTEGER, TK_FLOAT, TK_BLOB, 
** or TK_STRING), then Expr.token contains the text of the SQL literal. If
** the expression is a variable (TK_VARIABLE), then Expr.token contains the 
** variable name. Finally, if the expression is an SQL function (TK_FUNCTION),
** then Expr.token contains the name of the function.
**
** Expr.pRight and Expr.pLeft are the left and right subexpressions of a
** binary operator. Either or both may be NULL.
**
** Expr.x.pList is a list of arguments if the expression is an SQL function,
** a CASE expression or an IN expression of the form "<lhs> IN (<y>, <z>...)".
** Expr.x.pSelect is used if the expression is a sub-select or an expression of
** the form "<lhs> IN (SELECT ...)". If the EP_xIsSelect bit is set in the
** Expr.flags mask, then Expr.x.pSelect is valid. Otherwise, Expr.x.pList is 
** valid.
**
** An expression of the form ID or ID.ID refers to a column in a table.
** For such expressions, Expr.op is set to TK_COLUMN and Expr.iTable is
** the integer cursor number of a VDBE cursor pointing to that table and
** Expr.iColumn is the column number for the specific column.  If the
** expression is used as a result in an aggregate SELECT, then the
** value is also stored in the Expr.iAgg column in the aggregate so that
** it can be accessed after all aggregates are computed.
**
** If the expression is an unbound variable marker (a question mark 
** character '?' in the original SQL) then the Expr.iTable holds the index 
** number for that variable.
**
** If the expression is a subquery then Expr.iColumn holds an integer
** register number containing the result of the subquery.  If the
** subquery gives a constant result, then iTable is -1.  If the subquery
** gives a different answer at different times during statement processing
** then iTable is the address of a subroutine that computes the subquery.
**
** If the Expr is of type OP_Column, and the table it is selecting from
** is a disk table or the "old.*" pseudo-table, then pTab points to the
** corresponding table definition.
**
** ALLOCATION NOTES:
**
** Expr objects can use a lot of memory space in database schema.  To
** help reduce memory requirements, sometimes an Expr object will be
** truncated.  And to reduce the number of memory allocations, sometimes
** two or more Expr objects will be stored in a single memory allocation,
** together with Expr.zToken strings.
**
** If the EP_Reduced and EP_TokenOnly flags are set when
** an Expr object is truncated.  When EP_Reduced is set, then all
** the child Expr objects in the Expr.pLeft and Expr.pRight subtrees
** are contained within the same memory allocation.  Note, however, that
** the subtrees in Expr.x.pList or Expr.x.pSelect are always separately
** allocated, regardless of whether or not EP_Reduced is set.
*/
struct Expr {
  u8 op;                 /* Operation performed by this node */
  char affinity;         /* The affinity of the column or 0 if not a column */
  u16 flags;             /* Various flags.  EP_* See below */
  union {
    char *zToken;          /* Token value. Zero terminated and dequoted */
    int iValue;            /* Non-negative integer value if EP_IntValue */
  } u;

  /* If the EP_TokenOnly flag is set in the Expr.flags mask, then no
  ** space is allocated for the fields below this point. An attempt to
  ** access them will result in a segfault or malfunction. 
  *********************************************************************/

  Expr *pLeft;           /* Left subnode */
  Expr *pRight;          /* Right subnode */
  union {
    ExprList *pList;     /* Function arguments or in "<expr> IN (<expr-list)" */
    Select *pSelect;     /* Used for sub-selects and "<expr> IN (<select>)" */
  } x;
  CollSeq *pColl;        /* The collation type of the column or 0 */

  /* If the EP_Reduced flag is set in the Expr.flags mask, then no
  ** space is allocated for the fields below this point. An attempt to
  ** access them will result in a segfault or malfunction.
  *********************************************************************/

  int iTable;            /* TK_COLUMN: cursor number of table holding column
                         ** TK_REGISTER: register number
                         ** TK_TRIGGER: 1 -> new, 0 -> old */
  ynVar iColumn;         /* TK_COLUMN: column index.  -1 for rowid.
                         ** TK_VARIABLE: variable number (always >= 1). */
  i16 iAgg;              /* Which entry in pAggInfo->aCol[] or ->aFunc[] */
  i16 iRightJoinTable;   /* If EP_FromJoin, the right table of the join */
  u8 flags2;             /* Second set of flags.  EP2_... */
  u8 op2;                /* If a TK_REGISTER, the original value of Expr.op */
  AggInfo *pAggInfo;     /* Used by TK_AGG_COLUMN and TK_AGG_FUNCTION */
  Table *pTab;           /* Table for TK_COLUMN expressions. */
  Index *pIdx;           /* Fts index used by MATCH expressions */
#if SQLITE4_MAX_EXPR_DEPTH>0
  int nHeight;           /* Height of the tree headed by this node */
#endif
};

/*
** The following are the meanings of bits in the Expr.flags field.
*/
#define EP_FromJoin   0x0001  /* Originated in ON or USING clause of a join */
#define EP_Agg        0x0002  /* Contains one or more aggregate functions */
#define EP_Resolved   0x0004  /* IDs have been resolved to COLUMNs */
#define EP_Error      0x0008  /* Expression contains one or more errors */
#define EP_Distinct   0x0010  /* Aggregate function with DISTINCT keyword */
#define EP_VarSelect  0x0020  /* pSelect is correlated, not constant */
#define EP_DblQuoted  0x0040  /* token.z was originally in "..." */
#define EP_InfixFunc  0x0080  /* True for an infix function: LIKE, GLOB, etc */
#define EP_ExpCollate 0x0100  /* Collating sequence specified explicitly */
#define EP_FixedDest  0x0200  /* Result needed in a specific register */
#define EP_IntValue   0x0400  /* Integer value contained in u.iValue */
#define EP_xIsSelect  0x0800  /* x.pSelect is valid (otherwise x.pList is) */
#define EP_Hint       0x1000  /* Optimizer hint. Not required for correctness */
#define EP_Reduced    0x2000  /* Expr struct is EXPR_REDUCEDSIZE bytes only */
#define EP_TokenOnly  0x4000  /* Expr struct is EXPR_TOKENONLYSIZE bytes only */
#define EP_Static     0x8000  /* Held in memory not obtained from malloc() */

/*
** The following are the meanings of bits in the Expr.flags2 field.
*/
#define EP2_MallocedToken  0x0001  /* Need to sqlite4DbFree() Expr.zToken */
#define EP2_Irreducible    0x0002  /* Cannot EXPRDUP_REDUCE this Expr */

/*
** The pseudo-routine sqlite4ExprSetIrreducible sets the EP2_Irreducible
** flag on an expression structure.  This flag is used for VV&A only.  The
** routine is implemented as a macro that only works when in debugging mode,
** so as not to burden production code.
*/
#ifdef SQLITE4_DEBUG
# define ExprSetIrreducible(X)  (X)->flags2 |= EP2_Irreducible
#else
# define ExprSetIrreducible(X)
#endif

/*
** These macros can be used to test, set, or clear bits in the 
** Expr.flags field.
*/
#define ExprHasProperty(E,P)     (((E)->flags&(P))==(P))
#define ExprHasAnyProperty(E,P)  (((E)->flags&(P))!=0)
#define ExprSetProperty(E,P)     (E)->flags|=(P)
#define ExprClearProperty(E,P)   (E)->flags&=~(P)

/*
** Macros to determine the number of bytes required by a normal Expr 
** struct, an Expr struct with the EP_Reduced flag set in Expr.flags 
** and an Expr struct with the EP_TokenOnly flag set.
*/
#define EXPR_FULLSIZE           sizeof(Expr)           /* Full size */
#define EXPR_REDUCEDSIZE        offsetof(Expr,iTable)  /* Common features */
#define EXPR_TOKENONLYSIZE      offsetof(Expr,pLeft)   /* Fewer features */

/*
** Flags passed to the sqlite4ExprDup() function. See the header comment 
** above sqlite4ExprDup() for details.
*/
#define EXPRDUP_REDUCE         0x0001  /* Used reduced-size Expr nodes */

/*
** One entry for each expression.
*/
struct ExprListItem {
  Expr *pExpr;           /* The list of expressions */
  char *zName;           /* Token associated with this expression */
  char *zSpan;           /* Original text of the expression */
  u8 sortOrder;          /* 1 for DESC or 0 for ASC */
  u8 done;               /* A flag to indicate when processing is finished */
  u16 iOrderByCol;       /* For ORDER BY, column number in result set */
  u16 iAlias;            /* Index into Parse.aAlias[] for zName */
};

/*
** A list of expressions.  Each expression may optionally have a
** name.  An expr/name combination can be used in several ways, such
** as the list of "expr AS ID" fields following a "SELECT" or in the
** list of "ID = expr" items in an UPDATE.  A list of expressions can
** also be used as the argument to a function, in which case the a.zName
** field is not used.
*/
struct ExprList {
  int nExpr;             /* Number of expressions on the list */
  int nAlloc;            /* Number of entries allocated below */
  int iECursor;          /* VDBE Cursor associated with this ExprList */
  ExprListItem *a;       /* One entry for each expression */
};

/*
** An instance of this structure is used by the parser to record both
** the parse tree for an expression and the span of input text for an
** expression.
*/
struct ExprSpan {
  Expr *pExpr;          /* The expression parse tree */
  const char *zStart;   /* First character of input text */
  const char *zEnd;     /* One character past the end of input text */
};

struct IdListItem {
  char *zName;      /* Name of the identifier */
  int idx;          /* Index in some Table.aCol[] of a column named zName */
};


/*
** An instance of this structure can hold a simple list of identifiers,
** such as the list "a,b,c" in the following statements:
**
**      INSERT INTO t(a,b,c) VALUES ...;
**      CREATE INDEX idx ON t(a,b,c);
**      CREATE TRIGGER trig BEFORE UPDATE ON t(a,b,c) ...;
**
** The IdList.a.idx field is used when the IdList represents the list of
** column names after a table name in an INSERT statement.  In the statement
**
**     INSERT INTO t(a,b,c) ...
**
** If "a" is the k-th column of table "t", then IdList.a[0].idx==k.
*/
struct IdList {
  IdListItem *a;
  int nId;         /* Number of identifiers on the list */
  int nAlloc;      /* Number of entries allocated for a[] below */
};

/*
** The bitmask datatype defined below is used for various optimizations.
**
** Changing this from a 64-bit to a 32-bit type limits the number of
** tables in a join to 32 instead of 64.  But it also reduces the size
** of the library by 738 bytes on ix86.
*/
typedef u64 Bitmask;

/*
** The number of bits in a Bitmask.  "BMS" means "BitMask Size".
*/
#define BMS  ((int)(sizeof(Bitmask)*8))

/*
** One entry for each identifier on the list.
*/
struct SrcListItem {
  char *zDatabase;  /* Name of database holding this table */
  char *zName;      /* Name of the table */
  char *zAlias;     /* The "B" part of a "A AS B" phrase.  zName is the "A" */
  Table *pTab;      /* An SQL table corresponding to zName */
  Select *pSelect;  /* A SELECT statement used in place of a table name */
  int addrFillSub;  /* Address of subroutine to manifest a subquery */
  int regReturn;    /* Register holding return address of addrFillSub */
  u8 jointype;      /* Type of join between this able and the previous */
  u8 notIndexed;    /* True if there is a NOT INDEXED clause */
  u8 isCorrelated;  /* True if sub-query is correlated */
#ifndef SQLITE4_OMIT_EXPLAIN
  u8 iSelectId;     /* If pSelect!=0, the id of the sub-select in EQP */
#endif
  int iCursor;      /* The VDBE cursor number used to access this table */
  Expr *pOn;        /* The ON clause of a join */
  IdList *pUsing;   /* The USING clause of a join */
  Bitmask colUsed;  /* Bit N (1<<N) set if column N of pTab is used */
  char *zIndex;     /* Identifier from "INDEXED BY <zIndex>" clause */
  Index *pIndex;    /* Index structure corresponding to zIndex, if any */
};

/*
** The following structure describes the FROM clause of a SELECT statement.
** Each table or subquery in the FROM clause is a separate element of
** the SrcList.a[] array.
**
** With the addition of multiple database support, the following structure
** can also be used to describe a particular table such as the table that
** is modified by an INSERT, DELETE, or UPDATE statement.  In standard SQL,
** such a table must be a simple name: ID.  But in SQLite, the table can
** now be identified by a database name, a dot, then the table name: ID.ID.
**
** The jointype starts out showing the join type between the current table
** and the next table on the list.  The parser builds the list this way.
** But sqlite4SrcListShiftJoinType() later shifts the jointypes so that each
** jointype expresses the join between the table and the previous table.
**
** In the colUsed field, the high-order bit (bit 63) is set if the table
** contains more than 63 columns and the 64-th or later column is used.
*/
struct SrcList {
  i16 nSrc;        /* Number of tables or subqueries in the FROM clause */
  i16 nAlloc;      /* Number of entries allocated in a[] below */
  SrcListItem a[1]; /* One entry for each identifier on the list */
};

/*
** Permitted values of the SrcList.a.jointype field
*/
#define JT_INNER     0x0001    /* Any kind of inner or cross join */
#define JT_CROSS     0x0002    /* Explicit use of the CROSS keyword */
#define JT_NATURAL   0x0004    /* True for a "natural" join */
#define JT_LEFT      0x0008    /* Left outer join */
#define JT_RIGHT     0x0010    /* Right outer join */
#define JT_OUTER     0x0020    /* The "OUTER" keyword is present */
#define JT_ERROR     0x0040    /* unknown or unsupported join type */


/*
** A WherePlan object holds information that describes a lookup
** strategy.
**
** This object is intended to be opaque outside of the where.c module.
** It is included here only so that that compiler will know how big it
** is.  None of the fields in this object should be used outside of
** the where.c module.
**
** Within the union, pIdx is only used when wsFlags&WHERE_INDEXED is true.
** pTerm is only used when wsFlags&WHERE_MULTI_OR is true.  And pVtabIdx
** is only used when wsFlags&WHERE_VIRTUALTABLE is true.  It is never the
** case that more than one of these conditions is true.
*/
struct WherePlan {
  u32 wsFlags;                   /* WHERE_* flags that describe the strategy */
  u32 nEq;                       /* Number of == constraints */
  double nRow;                   /* Estimated number of rows (for EQP) */
  union {
    Index *pIdx;                   /* Index when WHERE_INDEXED is true */
    struct WhereTerm *pTerm;       /* WHERE clause term for OR-search */
    sqlite4_index_info *pVtabIdx;  /* Virtual table index to use */
  } u;
};

/*
** For each nested loop in a WHERE clause implementation, the WhereInfo
** structure contains a single instance of this structure.  This structure
** is intended to be private the the where.c module and should not be
** access or modified by other modules.
**
** The pIdxInfo field is used to help pick the best index on a
** virtual table.  The pIdxInfo pointer contains indexing
** information for the i-th table in the FROM clause before reordering.
** All the pIdxInfo pointers are freed by whereInfoFree() in where.c.
** All other information in the i-th WhereLevel object for the i-th table
** after FROM clause ordering.
*/
struct WhereLevel {
  WherePlan plan;       /* query plan for this element of the FROM clause */
  int iLeftJoin;        /* Memory cell used to implement LEFT OUTER JOIN */
  int iTabCur;          /* The VDBE cursor used to access the table */
  int iIdxCur;          /* The VDBE cursor used to access pIdx */
  int addrBrk;          /* Jump here to break out of the loop */
  int addrNxt;          /* Jump here to start the next IN combination */
  int addrCont;         /* Jump here to continue with the next loop cycle */
  int addrFirst;        /* First instruction of interior of the loop */
  u8 iFrom;             /* Which entry in the FROM clause */
  u8 op, p5;            /* Opcode and P5 of the opcode that ends the loop */
  int p1, p2;           /* Operands of the opcode used to ends the loop */
  union {               /* Information that depends on plan.wsFlags */
    struct {
      int nIn;              /* Number of entries in aInLoop[] */
      struct InLoop {
        int iCur;              /* The VDBE cursor used by this IN operator */
        int addrInTop;         /* Top of the IN loop */
      } *aInLoop;           /* Information about each nested IN operator */
    } in;                 /* Used when plan.wsFlags&WHERE_IN_ABLE */
  } u;

  /* The following field is really not part of the current level.  But
  ** we need a place to cache virtual table index information for each
  ** virtual table in the FROM clause and the WhereLevel structure is
  ** a convenient place since there is one WhereLevel for each FROM clause
  ** element.
  */
  sqlite4_index_info *pIdxInfo;  /* Index info for n-th source table */
};

/*
** Flags appropriate for the wctrlFlags parameter of sqlite4WhereBegin()
** and the WhereInfo.wctrlFlags member.
*/
#define WHERE_ORDERBY_NORMAL   0x0000 /* No-op */
#define WHERE_ORDERBY_MIN      0x0001 /* ORDER BY processing for min() func */
#define WHERE_ORDERBY_MAX      0x0002 /* ORDER BY processing for max() func */
#define WHERE_ONEPASS_DESIRED  0x0004 /* Want to do one-pass UPDATE/DELETE */
#define WHERE_DUPLICATES_OK    0x0008 /* Ok to return a row more than once */
#define WHERE_OMIT_OPEN_CLOSE  0x0010 /* Table cursors are already open */
#define WHERE_NO_AUTOINDEX     0x0020 /* Do not use an auto-index search */
#define WHERE_ONETABLE_ONLY    0x0040 /* Only code the 1st table in pTabList */
#define WHERE_AND_ONLY         0x0080 /* Don't use indices for OR terms */

/*
** The WHERE clause processing routine has two halves.  The
** first part does the start of the WHERE loop and the second
** half does the tail of the WHERE loop.  An instance of
** this structure is returned by the first half and passed
** into the second half to give some continuity.
*/
struct WhereInfo {
  Parse *pParse;       /* Parsing and code generating context */
  u16 wctrlFlags;      /* Flags originally passed to sqlite4WhereBegin() */
  u8 okOnePass;        /* Ok to use one-pass algorithm for UPDATE or DELETE */
  u8 untestedTerms;    /* Not all WHERE terms resolved by outer loop */
  u8 eDistinct;
  SrcList *pTabList;             /* List of tables in the join */
  int iTop;                      /* The very beginning of the WHERE loop */
  int iContinue;                 /* Jump here to continue with next record */
  int iBreak;                    /* Jump here to break out of the loop */
  int nLevel;                    /* Number of nested loop */
  struct WhereClause *pWC;       /* Decomposition of the WHERE clause */
  double savedNQueryLoop;        /* pParse->nQueryLoop outside the WHERE loop */
  double nRowOut;                /* Estimated number of output rows */
  WhereLevel a[1];               /* Information about each nest loop in WHERE */
};

#define WHERE_DISTINCT_UNIQUE 1
#define WHERE_DISTINCT_ORDERED 2

/*
** A NameContext defines a context in which to resolve table and column
** names.  The context consists of a list of tables (the pSrcList) field and
** a list of named expression (pEList).  The named expression list may
** be NULL.  The pSrc corresponds to the FROM clause of a SELECT or
** to the table being operated on by INSERT, UPDATE, or DELETE.  The
** pEList corresponds to the result set of a SELECT and is NULL for
** other statements.
**
** NameContexts can be nested.  When resolving names, the inner-most 
** context is searched first.  If no match is found, the next outer
** context is checked.  If there is still no match, the next context
** is checked.  This process continues until either a match is found
** or all contexts are check.  When a match is found, the nRef member of
** the context containing the match is incremented. 
**
** Each subquery gets a new NameContext.  The pNext field points to the
** NameContext in the parent query.  Thus the process of scanning the
** NameContext list corresponds to searching through successively outer
** subqueries looking for a match.
*/
struct NameContext {
  Parse *pParse;       /* The parser */
  SrcList *pSrcList;   /* One or more tables used to resolve names */
  ExprList *pEList;    /* Optional list of named expressions */
  int nRef;            /* Number of names resolved by this context */
  int nErr;            /* Number of errors encountered while resolving names */
  u8 allowAgg;         /* Aggregate functions allowed here */
  u8 hasAgg;           /* True if aggregates are seen */
  u8 isCheck;          /* True if resolving names in a CHECK constraint */
  int nDepth;          /* Depth of subquery recursion. 1 for no recursion */
  AggInfo *pAggInfo;   /* Information about aggregates at this level */
  NameContext *pNext;  /* Next outer name context.  NULL for outermost */
};

/*
** An instance of the following structure contains all information
** needed to generate code for a single SELECT statement.
**
** nLimit is set to -1 if there is no LIMIT clause.  nOffset is set to 0.
** If there is a LIMIT clause, the parser sets nLimit to the value of the
** limit and nOffset to the value of the offset (or 0 if there is not
** offset).  But later on, nLimit and nOffset become the memory locations
** in the VDBE that record the limit and offset counters.
**
** addrOpenEphm[] entries contain the address of OP_OpenEphemeral opcodes.
** These addresses must be stored so that we can go back and fill in
** the P4_KEYINFO and P2 parameters later.  Neither the KeyInfo nor
** the number of columns in P2 can be computed at the same time
** as the OP_OpenEphm instruction is coded because not
** enough information about the compound query is known at that point.
** The KeyInfo for addrOpenTran[0] and [1] contains collating sequences
** for the result set.  The KeyInfo for addrOpenTran[2] contains collating
** sequences for the ORDER BY clause.
*/
struct Select {
  ExprList *pEList;      /* The fields of the result */
  u8 op;                 /* One of: TK_UNION TK_ALL TK_INTERSECT TK_EXCEPT */
  char affinity;         /* MakeRecord with this affinity for SRT_Set */
  u16 selFlags;          /* Various SF_* values */
  SrcList *pSrc;         /* The FROM clause */
  Expr *pWhere;          /* The WHERE clause */
  ExprList *pGroupBy;    /* The GROUP BY clause */
  Expr *pHaving;         /* The HAVING clause */
  ExprList *pOrderBy;    /* The ORDER BY clause */
  Select *pPrior;        /* Prior select in a compound select statement */
  Select *pNext;         /* Next select to the left in a compound */
  Select *pRightmost;    /* Right-most select in a compound select statement */
  Expr *pLimit;          /* LIMIT expression. NULL means not used. */
  Expr *pOffset;         /* OFFSET expression. NULL means not used. */
  int iLimit, iOffset;   /* Memory registers holding LIMIT & OFFSET counters */
  int addrOpenEphm[3];   /* OP_OpenEphem opcodes related to this select */
  double nSelectRow;     /* Estimated number of result rows */
};

/*
** Allowed values for Select.selFlags.  The "SF" prefix stands for
** "Select Flag".
*/
#define SF_Distinct        0x01  /* Output should be DISTINCT */
#define SF_Resolved        0x02  /* Identifiers have been resolved */
#define SF_Aggregate       0x04  /* Contains aggregate functions */
#define SF_UsesEphemeral   0x08  /* Uses the OpenEphemeral opcode */
#define SF_Expanded        0x10  /* sqlite4SelectExpand() called on this */
#define SF_HasTypeInfo     0x20  /* FROM subqueries have Table metadata */
#define SF_UseSorter       0x40  /* Sort using a sorter */
#define SF_Values          0x80  /* Synthesized from VALUES clause */


/*
** The results of a select can be distributed in several ways.  The
** "SRT" prefix means "SELECT Result Type".
*/
#define SRT_Union        1  /* Store result as keys in an index */
#define SRT_Except       2  /* Remove result from a UNION index */
#define SRT_Exists       3  /* Store 1 if the result is not empty */
#define SRT_Discard      4  /* Do not save the results anywhere */

/* The ORDER BY clause is ignored for all of the above */
#define IgnorableOrderby(X) ((X->eDest)<=SRT_Discard)

#define SRT_Output       5  /* Output each row of result */
#define SRT_Mem          6  /* Store result in a memory cell */
#define SRT_Set          7  /* Store results as keys in an index */
#define SRT_Table        8  /* Store result as data with an automatic rowid */
#define SRT_EphemTab     9  /* Create transient tab and store like SRT_Table */
#define SRT_Coroutine   10  /* Generate a single row of result */

/*
** A structure used to customize the behavior of sqlite4Select(). See
** comments above sqlite4Select() for details.
*/
typedef struct SelectDest SelectDest;
struct SelectDest {
  u8 eDest;         /* How to dispose of the results */
  u8 affinity;      /* Affinity used when eDest==SRT_Set */
  int iParm;        /* A parameter used by the eDest disposal method */
  int iMem;         /* Base register where results are written */
  int nMem;         /* Number of registers allocated */
};

/*
** During code generation of statements that do inserts into AUTOINCREMENT 
** tables, the following information is attached to the Table.u.autoInc.p
** pointer of each autoincrement table to record some side information that
** the code generator needs.  We have to keep per-table autoincrement
** information in case inserts are down within triggers.  Triggers do not
** normally coordinate their activities, but we do need to coordinate the
** loading and saving of autoincrement information.
*/
struct AutoincInfo {
  AutoincInfo *pNext;   /* Next info block in a list of them all */
  Table *pTab;          /* Table this info block refers to */
  int iDb;              /* Index in sqlite4.aDb[] of database holding pTab */
  int regCtr;           /* Memory register holding the rowid counter */
};

/*
** Size of the column cache
*/
#ifndef SQLITE4_N_COLCACHE
# define SQLITE4_N_COLCACHE 10
#endif

/*
** At least one instance of the following structure is created for each 
** trigger that may be fired while parsing an INSERT, UPDATE or DELETE
** statement. All such objects are stored in the linked list headed at
** Parse.pTriggerPrg and deleted once statement compilation has been
** completed.
**
** A Vdbe sub-program that implements the body and WHEN clause of trigger
** TriggerPrg.pTrigger, assuming a default ON CONFLICT clause of
** TriggerPrg.orconf, is stored in the TriggerPrg.pProgram variable.
** The Parse.pTriggerPrg list never contains two entries with the same
** values for both pTrigger and orconf.
**
** The TriggerPrg.aColmask[0] variable is set to a mask of old.* columns
** accessed (or set to 0 for triggers fired as a result of INSERT 
** statements). Similarly, the TriggerPrg.aColmask[1] variable is set to
** a mask of new.* columns used by the program.
*/
struct TriggerPrg {
  Trigger *pTrigger;      /* Trigger this program was coded from */
  int orconf;             /* Default ON CONFLICT policy */
  SubProgram *pProgram;   /* Program implementing pTrigger/orconf */
  u32 aColmask[2];        /* Masks of old.*, new.* columns accessed */
  TriggerPrg *pNext;      /* Next entry in Parse.pTriggerPrg list */
};

/*
** The yDbMask datatype for the bitmask of all attached databases.
*/
#if SQLITE4_MAX_ATTACHED>30
  typedef sqlite4_uint64 yDbMask;
#else
  typedef unsigned int yDbMask;
#endif

/*
** Internal column cache for Parse objects. One of these is used
** for each column cache entry.
*/
struct ParseYColCache {
  int iTable;           /* Table cursor number */
  int iColumn;          /* Table column number */
  u8 tempReg;           /* iReg is a temp register that needs to be freed */
  int iLevel;           /* Nesting level */
  int iReg;             /* Reg with value of this column. 0 means none. */
  int lru;              /* Least recently used entry has the smallest value */
};


/*
** An SQL parser context.  A copy of this structure is passed through
** the parser and down into all the parser action routine in order to
** carry around information that is global to the entire parse.
**
** The structure is divided into two parts.  When the parser and code
** generate call themselves recursively, the first part of the structure
** is constant but the second part is reset at the beginning and end of
** each recursion.
*/
struct Parse {
  sqlite4 *db;         /* The main database structure */
  int rc;              /* Return code from execution */
  char *zErrMsg;       /* An error message */
  Vdbe *pVdbe;         /* An engine for executing database bytecode */
  u8 colNamesSet;      /* TRUE after OP_ColumnName has been issued to pVdbe */
  u8 checkSchema;      /* Causes schema cookie check after an error */
  u8 nested;           /* Number of nested calls to the parser/code generator */
  u8 nTempReg;         /* Number of temporary registers in aTempReg[] */
  u8 nTempInUse;       /* Number of aTempReg[] currently checked out */
  int aTempReg[8];     /* Holding area for temporary registers */
  int nRangeReg;       /* Size of the temporary register block */
  int iRangeReg;       /* First register in temporary register block */
  int nErr;            /* Number of errors seen */
  int nTab;            /* Number of previously allocated VDBE cursors */
  int nMem;            /* Number of memory cells used so far */
  int nSet;            /* Number of sets used so far */
  int nOnce;           /* Number of OP_Once instructions so far */
  int ckBase;          /* Base register of data during check constraints */
  int iCacheLevel;     /* ColCache valid when aColCache[].iLevel<=iCacheLevel */
  int iCacheCnt;       /* Counter used to generate aColCache[].lru values */
  int iNewidxReg;      /* First argument to OP_NewIdxid */
  u8 nColCache;        /* Number of entries in aColCache[] */
  u8 iColCache;        /* Next entry in aColCache[] to replace */
  ParseYColCache aColCache[SQLITE4_N_COLCACHE]; /* One for each column cache entry */
  yDbMask writeMask;   /* Start a write transaction on these databases */
  yDbMask cookieMask;  /* Bitmask of schema verified databases */
  u8 isMultiWrite;     /* True if statement may affect/insert multiple rows */
  u8 mayAbort;         /* True if statement may throw an ABORT exception */
  int cookieGoto;      /* Address of OP_Goto to cookie verifier subroutine */
  int cookieValue[SQLITE4_MAX_ATTACHED+2];  /* Values of cookies to verify */
  int regRowid;        /* Register holding rowid of CREATE TABLE entry */
  AutoincInfo *pAinc;  /* Information about AUTOINCREMENT counters */
  int nMaxArg;         /* Max args passed to user function by sub-program */

  /* Information used while coding trigger programs. */
  Parse *pToplevel;    /* Parse structure for main program (or NULL) */
  Table *pTriggerTab;  /* Table triggers are being coded for */
  u32 oldmask;         /* Mask of old.* columns referenced */
  u32 newmask;         /* Mask of new.* columns referenced */
  u8 eTriggerOp;       /* TK_UPDATE, TK_INSERT or TK_DELETE */
  u8 eOrconf;          /* Default ON CONFLICT policy for trigger steps */
  u8 disableTriggers;  /* True to disable triggers */
  double nQueryLoop;   /* Estimated number of iterations of a query */

  /* Above is constant between recursions.  Below is reset before and after
  ** each recursion */

  int nVar;            /* Number of '?' variables seen in the SQL so far */
  int nzVar;           /* Number of available slots in azVar[] */
  char **azVar;        /* Pointers to names of parameters */
  Vdbe *pReprepare;    /* VM being reprepared (sqlite4Reprepare()) */
  int nAlias;          /* Number of aliased result set columns */
  int *aAlias;         /* Register used to hold aliased result */
  u8 explain;          /* True if the EXPLAIN flag is found on the query */
  Token sNameToken;    /* Token with unqualified schema object name */
  Token sLastToken;    /* The last token parsed */
  const char *zTail;   /* All SQL text past the last semicolon parsed */
  Table *pNewTable;    /* A table being constructed by CREATE TABLE */
  Trigger *pNewTrigger;     /* Trigger under construct by a CREATE TRIGGER */
  const char *zAuthContext; /* The 6th parameter to db->xAuth callbacks */
#ifndef SQLITE4_OMIT_VIRTUALTABLE
  Token sArg;                /* Complete text of a module argument */
  u8 declareVtab;            /* True if inside sqlite4_declare_vtab() */
  int nVtabLock;             /* Number of virtual tables to lock */
  Table **apVtabLock;        /* Pointer to virtual tables needing locking */
#endif
  int nHeight;            /* Expression tree height of current sub-select */
  Table *pZombieTab;      /* List of Table objects to delete after code gen */
  TriggerPrg *pTriggerPrg;    /* Linked list of coded triggers */

#ifndef SQLITE4_OMIT_EXPLAIN
  int iSelectId;
  int iNextSelectId;
#endif
};

#ifdef SQLITE4_OMIT_VIRTUALTABLE
  #define IN_DECLARE_VTAB 0
#else
  #define IN_DECLARE_VTAB (pParse->declareVtab)
#endif

/*
** An instance of the following structure can be declared on a stack and used
** to save the Parse.zAuthContext value so that it can be restored later.
*/
struct AuthContext {
  const char *zAuthContext;   /* Put saved Parse.zAuthContext here */
  Parse *pParse;              /* The Parse structure */
};

/*
** Bitfield flags for P5 value in OP_Insert and OP_Delete
*/
#define OPFLAG_NCHANGE       0x01    /* Set to update db->nChange */
#define OPFLAG_LASTROWID     0x02    /* Set to update db->lastRowid */
#define OPFLAG_ISUPDATE      0x04    /* This OP_Insert is an sql UPDATE */
#define OPFLAG_APPEND        0x08    /* This is likely to be an append */
#define OPFLAG_SEQCOUNT      0x10    /* Append sequence number to key */
#define OPFLAG_CLEARCACHE    0x20    /* Clear pseudo-table cache in OP_Column */
#define OPFLAG_APPENDBIAS    0x40    /* Bias inserts for appending */

/*
 * Each trigger present in the database schema is stored as an instance of
 * struct Trigger. 
 *
 * Pointers to instances of struct Trigger are stored in two ways.
 * 1. In the "trigHash" hash table (part of the sqlite4* that represents the 
 *    database). This allows Trigger structures to be retrieved by name.
 * 2. All triggers associated with a single table form a linked list, using the
 *    pNext member of struct Trigger. A pointer to the first element of the
 *    linked list is stored as the "pTrigger" member of the associated
 *    struct Table.
 *
 * The "step_list" member points to the first element of a linked list
 * containing the SQL statements specified as the trigger program.
 */
struct Trigger {
  char *zName;            /* The name of the trigger                        */
  char *table;            /* The table or view to which the trigger applies */
  u8 op;                  /* One of TK_DELETE, TK_UPDATE, TK_INSERT         */
  u8 tr_tm;               /* One of TRIGGER_BEFORE, TRIGGER_AFTER */
  Expr *pWhen;            /* The WHEN clause of the expression (may be NULL) */
  IdList *pColumns;       /* If this is an UPDATE OF <column-list> trigger,
                             the <column-list> is stored here */
  Schema *pSchema;        /* Schema containing the trigger */
  Schema *pTabSchema;     /* Schema containing the table */
  TriggerStep *step_list; /* Link list of trigger program steps             */
  Trigger *pNext;         /* Next trigger associated with the table */
};

/*
** A trigger is either a BEFORE or an AFTER trigger.  The following constants
** determine which. 
**
** If there are multiple triggers, you might of some BEFORE and some AFTER.
** In that cases, the constants below can be ORed together.
*/
#define TRIGGER_BEFORE  1
#define TRIGGER_AFTER   2

/*
 * An instance of struct TriggerStep is used to store a single SQL statement
 * that is a part of a trigger-program. 
 *
 * Instances of struct TriggerStep are stored in a singly linked list (linked
 * using the "pNext" member) referenced by the "step_list" member of the 
 * associated struct Trigger instance. The first element of the linked list is
 * the first step of the trigger-program.
 * 
 * The "op" member indicates whether this is a "DELETE", "INSERT", "UPDATE" or
 * "SELECT" statement. The meanings of the other members is determined by the 
 * value of "op" as follows:
 *
 * (op == TK_INSERT)
 * orconf    -> stores the ON CONFLICT algorithm
 * pSelect   -> If this is an INSERT INTO ... SELECT ... statement, then
 *              this stores a pointer to the SELECT statement. Otherwise NULL.
 * target    -> A token holding the quoted name of the table to insert into.
 * pExprList -> If this is an INSERT INTO ... VALUES ... statement, then
 *              this stores values to be inserted. Otherwise NULL.
 * pIdList   -> If this is an INSERT INTO ... (<column-names>) VALUES ... 
 *              statement, then this stores the column-names to be
 *              inserted into.
 *
 * (op == TK_DELETE)
 * target    -> A token holding the quoted name of the table to delete from.
 * pWhere    -> The WHERE clause of the DELETE statement if one is specified.
 *              Otherwise NULL.
 * 
 * (op == TK_UPDATE)
 * target    -> A token holding the quoted name of the table to update rows of.
 * pWhere    -> The WHERE clause of the UPDATE statement if one is specified.
 *              Otherwise NULL.
 * pExprList -> A list of the columns to update and the expressions to update
 *              them to. See sqlite4Update() documentation of "pChanges"
 *              argument.
 * 
 */
struct TriggerStep {
  u8 op;               /* One of TK_DELETE, TK_UPDATE, TK_INSERT, TK_SELECT */
  u8 orconf;           /* OE_Rollback etc. */
  Trigger *pTrig;      /* The trigger that this step is a part of */
  Select *pSelect;     /* SELECT statment or RHS of INSERT INTO .. SELECT ... */
  Token target;        /* Target table for DELETE, UPDATE, INSERT */
  Expr *pWhere;        /* The WHERE clause for DELETE or UPDATE steps */
  ExprList *pExprList; /* SET clause for UPDATE.  VALUES clause for INSERT */
  IdList *pIdList;     /* Column names for INSERT */
  TriggerStep *pNext;  /* Next in the link-list */
  TriggerStep *pLast;  /* Last element in link-list. Valid for 1st elem only */
};

/*
** The following structure contains information used by the sqliteFix...
** routines as they walk the parse tree to make database references
** explicit.  
*/
typedef struct DbFixer DbFixer;
struct DbFixer {
  Parse *pParse;      /* The parsing context.  Error messages written here */
  const char *zDb;    /* Make sure all objects are contained in this database */
  const char *zType;  /* Type of the container - used for error messages */
  const Token *pName; /* Name of the container - used for error messages */
};

/*
** An objected used to accumulate the text of a string where we
** do not necessarily know how big the string will be in the end.
*/
struct StrAccum {
  sqlite4 *db;         /* Optional database for lookaside.  Can be NULL */
  sqlite4_env *pEnv;   /* Malloc context */
  char *zBase;         /* A base allocation.  Not from malloc. */
  char *zText;         /* The string collected so far */
  int  nChar;          /* Length of the string so far */
  int  nAlloc;         /* Amount of space allocated in zText */
  int  mxAlloc;        /* Maximum allowed string length */
  u8   mallocFailed;   /* Becomes true if any memory allocation fails */
  u8   useMalloc;      /* 0: none,  1: sqlite4DbMalloc,  2: sqlite4_malloc */
  u8   tooBig;         /* Becomes true if string size exceeds limits */
};

/*
** A pointer to this structure is used to communicate information
** from sqlite4Init and OP_ParseSchema into the sqlite4InitCallback.
*/
typedef struct {
  sqlite4 *db;        /* The database being initialized */
  int iDb;            /* 0 for main database.  1 for TEMP, 2.. for ATTACHed */
  char **pzErrMsg;    /* Error message stored here */
  int rc;             /* Result code stored here */
} InitData;

/*
** A pluggable storage engine
*/
typedef struct KVFactory {
  struct KVFactory *pNext;       /* Next in list of all storage engines */
  const char *zName;             /* Name of this factory */
  sqlite4_kvfactory xFactory;    /* Function to make an sqlite4_kvstore obj */
  int isPerm;                    /* True if a built-in.  Cannot be popped */
} KVFactory;

/*
** An instance of this structure defines the run-time environment.
*/
struct sqlite4_env {
  int nByte;                        /* Size of this object in bytes */
  int iVersion;                     /* Version number of this structure */
  int bMemstat;                     /* True to enable memory status */
  int bCoreMutex;                   /* True to enable core mutexing */
  int bFullMutex;                   /* True to enable full mutexing */
  int mxStrlen;                     /* Maximum string length */
  int szLookaside;                  /* Default lookaside buffer size */
  int nLookaside;                   /* Default lookaside buffer count */
  sqlite4_mem_methods m;            /* Low-level memory allocation interface */
  sqlite4_mutex_methods mutex;      /* Low-level mutex interface */
  void *pHeap;                      /* Heap storage space */
  int nHeap;                        /* Size of pHeap[] */
  int mnReq, mxReq;                 /* Min and max heap requests sizes */
  int mxParserStack;                /* maximum depth of the parser stack */
  KVFactory *pFactory;              /* List of factories */
  int (*xRandomness)(sqlite4_env*, int, unsigned char*);
  int (*xCurrentTime)(sqlite4_env*, sqlite4_uint64*);
  /* The above might be initialized to non-zero.  The following need to always
  ** initially be zero, however. */
  int isInit;                       /* True after initialization has finished */
  sqlite4_mutex *pFactoryMutex;     /* Mutex for pFactory */
  sqlite4_mutex *pPrngMutex;        /* Mutex for the PRNG */
  u32 prngX, prngY;                 /* State of the PRNG */
  void (*xLog)(void*,int,const char*); /* Function for logging */
  void *pLogArg;                       /* First argument to xLog() */
  int bLocaltimeFault;              /* True to fail localtime() calls */
  sqlite4_mutex *pMemMutex;         /* Mutex for nowValue[] and mxValue[] */
  sqlite4_uint64 nowValue[4];       /* sqlite4_env_status() current values */
  sqlite4_uint64 mxValue[4];        /* sqlite4_env_status() max values */
  FuncDefTable aGlobalFuncs;        /* Lookup table of global functions */
};

/*
** Context pointer passed down through the tree-walk.
*/
struct Walker {
  int (*xExprCallback)(Walker*, Expr*);     /* Callback for expressions */
  int (*xSelectCallback)(Walker*,Select*);  /* Callback for SELECTs */
  Parse *pParse;                            /* Parser context.  */
  union {                                   /* Extra data for callback */
    NameContext *pNC;                          /* Naming context */
    int i;                                     /* Integer value */
  } u;
};

/*
** An instance of this structure is used as the p4 argument to some fts5
** related vdbe opcodes.
*/
struct Fts5Info {
  int iDb;                        /* Database containing this index */
  int iRoot;                      /* Root page number of index */
  int iTbl;                       /* Root page number of indexed table */
  int nCol;                       /* Number of columns in indexed table */
  char **azCol;                   /* Column names for table */
  Fts5Tokenizer *pTokenizer;      /* Tokenizer module */
  sqlite4_tokenizer *p;           /* Tokenizer instance */
};

int sqlite4WalkExpr(Walker*, Expr*);
int sqlite4WalkExprList(Walker*, ExprList*);
int sqlite4WalkSelect(Walker*, Select*);
int sqlite4WalkSelectExpr(Walker*, Select*);
int sqlite4WalkSelectFrom(Walker*, Select*);

/*
** Return code from the parse-tree walking primitives and their
** callbacks.
*/
#define WRC_Continue    0   /* Continue down into children */
#define WRC_Prune       1   /* Omit children but continue walking siblings */
#define WRC_Abort       2   /* Abandon the tree walk */

/*
** Assuming zIn points to the first byte of a UTF-8 character,
** advance zIn to point to the first byte of the next UTF-8 character.
*/
#define SQLITE4_SKIP_UTF8(zIn) {                        \
  if( (*(zIn++))>=0xc0 ){                              \
    while( (*zIn & 0xc0)==0x80 ){ zIn++; }             \
  }                                                    \
}

/*
** The SQLITE4_*_BKPT macros are substitutes for the error codes with
** the same name but without the _BKPT suffix.  These macros invoke
** routines that report the line-number on which the error originated
** using sqlite4_log().  The routines also provide a convenient place
** to set a debugger breakpoint.
*/
int sqlite4CorruptError(int);
int sqlite4MisuseError(int);
int sqlite4CantopenError(int);
#define SQLITE4_CORRUPT_BKPT sqlite4CorruptError(__LINE__)
#define SQLITE4_MISUSE_BKPT sqlite4MisuseError(__LINE__)
#define SQLITE4_CANTOPEN_BKPT sqlite4CantopenError(__LINE__)


/*
** FTS4 is really an extension for FTS3.  It is enabled using the
** SQLITE4_ENABLE_FTS3 macro.  But to avoid confusion we also all
** the SQLITE4_ENABLE_FTS4 macro to serve as an alisse for SQLITE4_ENABLE_FTS3.
*/
#if defined(SQLITE4_ENABLE_FTS4) && !defined(SQLITE4_ENABLE_FTS3)
# define SQLITE4_ENABLE_FTS3
#endif

/*
** The ctype.h header is needed for non-ASCII systems.  It is also
** needed by FTS3 when FTS3 is included in the amalgamation.
*/
#if !defined(SQLITE4_ASCII) || \
    (defined(SQLITE4_ENABLE_FTS3) && defined(SQLITE4_AMALGAMATION))
# include <ctype.h>
#endif

/*
** The following macros mimic the standard library functions toupper(),
** isspace(), isalnum(), isdigit() and isxdigit(), respectively. The
** sqlite versions only work for ASCII characters, regardless of locale.
*/
#ifdef SQLITE4_ASCII
# define sqlite4Toupper(x)  ((x)&~(sqlite4CtypeMap[(unsigned char)(x)]&0x20))
# define sqlite4Isspace(x)   (sqlite4CtypeMap[(unsigned char)(x)]&0x01)
# define sqlite4Isalnum(x)   (sqlite4CtypeMap[(unsigned char)(x)]&0x06)
# define sqlite4Isalpha(x)   (sqlite4CtypeMap[(unsigned char)(x)]&0x02)
# define sqlite4Isdigit(x)   (sqlite4CtypeMap[(unsigned char)(x)]&0x04)
# define sqlite4Isxdigit(x)  (sqlite4CtypeMap[(unsigned char)(x)]&0x08)
# define sqlite4Tolower(x)   (sqlite4UpperToLower[(unsigned char)(x)])
#else
# define sqlite4Toupper(x)   toupper((unsigned char)(x))
# define sqlite4Isspace(x)   isspace((unsigned char)(x))
# define sqlite4Isalnum(x)   isalnum((unsigned char)(x))
# define sqlite4Isalpha(x)   isalpha((unsigned char)(x))
# define sqlite4Isdigit(x)   isdigit((unsigned char)(x))
# define sqlite4Isxdigit(x)  isxdigit((unsigned char)(x))
# define sqlite4Tolower(x)   tolower((unsigned char)(x))
#endif

/*
** Internal function prototypes
*/
int sqlite4StrICmp(const char *, const char *);
int sqlite4Strlen30(const char*);
#define sqlite4StrNICmp sqlite4_strnicmp

int sqlite4MallocInit(sqlite4_env*);
void sqlite4MallocEnd(sqlite4_env*);
void *sqlite4Malloc(sqlite4_env*, int);
void *sqlite4MallocZero(sqlite4_env*, int);
void *sqlite4DbMallocZero(sqlite4*, int);
void *sqlite4DbMallocRaw(sqlite4*, int);
char *sqlite4DbStrDup(sqlite4*,const char*);
char *sqlite4DbStrNDup(sqlite4*,const char*, int);
void *sqlite4Realloc(sqlite4_env*, void*, int);
void *sqlite4DbReallocOrFree(sqlite4 *, void *, int);
void *sqlite4DbRealloc(sqlite4 *, void *, int);
void sqlite4DbFree(sqlite4*, void*);
int sqlite4MallocSize(sqlite4_env*, void*);
int sqlite4DbMallocSize(sqlite4*, void*);
void sqlite4MemSetDefault(sqlite4_env*);
void sqlite4BenignMallocHooks(sqlite4_env*,void (*)(void), void (*)(void));

/*
** On systems with ample stack space and that support alloca(), make
** use of alloca() to obtain space for large automatic objects.  By default,
** obtain space from malloc().
**
** The alloca() routine never returns NULL.  This will cause code paths
** that deal with sqlite4StackAlloc() failures to be unreachable.
*/
#ifdef SQLITE4_USE_ALLOCA
# define sqlite4StackAllocRaw(D,N)   alloca(N)
# define sqlite4StackAllocZero(D,N)  memset(alloca(N), 0, N)
# define sqlite4StackFree(D,P)       
#else
# define sqlite4StackAllocRaw(D,N)   sqlite4DbMallocRaw(D,N)
# define sqlite4StackAllocZero(D,N)  sqlite4DbMallocZero(D,N)
# define sqlite4StackFree(D,P)       sqlite4DbFree(D,P)
#endif

#ifdef SQLITE4_ENABLE_MEMSYS3
const sqlite4_mem_methods *sqlite4MemGetMemsys3(void);
#endif
#ifdef SQLITE4_ENABLE_MEMSYS5
const sqlite4_mem_methods *sqlite4MemGetMemsys5(void);
#endif


#ifndef SQLITE4_MUTEX_OMIT
  sqlite4_mutex_methods const *sqlite4DefaultMutex(void);
  sqlite4_mutex_methods const *sqlite4NoopMutex(void);
  sqlite4_mutex *sqlite4MutexAlloc(sqlite4_env*,int);
  int sqlite4MutexInit(sqlite4_env*);
  int sqlite4MutexEnd(sqlite4_env*);
#endif

void sqlite4StatusAdd(sqlite4_env*, int, sqlite4_int64);
void sqlite4StatusSet(sqlite4_env*, int, sqlite4_uint64);

#ifndef SQLITE4_OMIT_FLOATING_POINT
  int sqlite4IsNaN(double);
  int sqlite4IsInf(double);
#else
# define sqlite4IsNaN(X)  0
# define sqlite4IsInf(X)  0
#endif

void sqlite4VXPrintf(StrAccum*, int, const char*, va_list);
#ifndef SQLITE4_OMIT_TRACE
void sqlite4XPrintf(StrAccum*, const char*, ...);
#endif
char *sqlite4MPrintf(sqlite4*,const char*, ...);
char *sqlite4VMPrintf(sqlite4*,const char*, va_list);
char *sqlite4MAppendf(sqlite4*,char*,const char*,...);
#if defined(SQLITE4_TEST) || defined(SQLITE4_DEBUG)
  void sqlite4DebugPrintf(const char*, ...);
#endif
#if defined(SQLITE4_TEST)
  void *sqlite4TestTextToPtr(const char*);
#endif

/* Output formatting for SQLITE4_TESTCTRL_EXPLAIN */
#if defined(SQLITE4_ENABLE_TREE_EXPLAIN)
  void sqlite4ExplainBegin(Vdbe*);
  void sqlite4ExplainPrintf(Vdbe*, const char*, ...);
  void sqlite4ExplainNL(Vdbe*);
  void sqlite4ExplainPush(Vdbe*);
  void sqlite4ExplainPop(Vdbe*);
  void sqlite4ExplainFinish(Vdbe*);
  void sqlite4ExplainSelect(Vdbe*, Select*);
  void sqlite4ExplainExpr(Vdbe*, Expr*);
  void sqlite4ExplainExprList(Vdbe*, ExprList*);
  const char *sqlite4VdbeExplanation(Vdbe*);
#else
# define sqlite4ExplainBegin(X)
# define sqlite4ExplainSelect(A,B)
# define sqlite4ExplainExpr(A,B)
# define sqlite4ExplainExprList(A,B)
# define sqlite4ExplainFinish(X)
# define sqlite4VdbeExplanation(X) 0
#endif


void sqlite4SetString(char **, sqlite4*, const char*, ...);
void sqlite4ErrorMsg(Parse*, const char*, ...);
int sqlite4Dequote(char*);
int sqlite4KeywordCode(const unsigned char*, int);
int sqlite4RunParser(Parse*, const char*, char **);
void sqlite4FinishCoding(Parse*);
int sqlite4GetTempReg(Parse*);
void sqlite4ReleaseTempReg(Parse*,int);
int sqlite4GetTempRange(Parse*,int);
void sqlite4ReleaseTempRange(Parse*,int,int);
void sqlite4ClearTempRegCache(Parse*);
Expr *sqlite4ExprAlloc(sqlite4*,int,const Token*,int);
Expr *sqlite4Expr(sqlite4*,int,const char*);
void sqlite4ExprAttachSubtrees(sqlite4*,Expr*,Expr*,Expr*);
Expr *sqlite4PExpr(Parse*, int, Expr*, Expr*, const Token*);
Expr *sqlite4ExprAnd(sqlite4*,Expr*, Expr*);
Expr *sqlite4ExprFunction(Parse*,ExprList*, Token*);
void sqlite4ExprAssignVarNumber(Parse*, Expr*);
void sqlite4ExprDelete(sqlite4*, Expr*);
ExprList *sqlite4ExprListAppend(Parse*,ExprList*,Expr*);
void sqlite4ExprListSetName(Parse*,ExprList*,Token*,int);
void sqlite4ExprListSetSpan(Parse*,ExprList*,ExprSpan*);
void sqlite4ExprListDelete(sqlite4*, ExprList*);
int sqlite4Init(sqlite4*, char**);
int sqlite4InitCallback(void*, int, char**, char**);
void sqlite4Pragma(Parse*,Token*,Token*,Token*,int);
void sqlite4ResetInternalSchema(sqlite4*, int);
void sqlite4BeginParse(Parse*,int);
void sqlite4CommitInternalChanges(sqlite4*);
Table *sqlite4ResultSetOfSelect(Parse*,Select*);
void sqlite4OpenMasterTable(Parse *, int);
void sqlite4StartTable(Parse*,Token*,Token*,int,int,int,int);
void sqlite4AddColumn(Parse*,Token*);
void sqlite4AddNotNull(Parse*, int);
void sqlite4AddPrimaryKey(Parse*, ExprList*, int, int, int);
void sqlite4AddCheckConstraint(Parse*, Expr*);
void sqlite4AddColumnType(Parse*,Token*);
void sqlite4AddDefaultValue(Parse*,ExprSpan*);
void sqlite4AddCollateType(Parse*, Token*);
void sqlite4EndTable(Parse*,Token*,Token*,Select*);
int sqlite4ParseUri(sqlite4_env*,const char*,unsigned int*,char**,char **);
int sqlite4CodeOnce(Parse *);

RowSet *sqlite4RowSetInit(sqlite4 *, void *, unsigned int);
void sqlite4RowSetClear(RowSet *);
void sqlite4RowSetInsert(RowSet *, u8 *, int);
int sqlite4RowSetNext(RowSet *);
const u8 *sqlite4RowSetRead(RowSet *, int *);
int sqlite4RowSetTest(RowSet *, u8, u8 *, int);

void sqlite4CreateView(Parse*,Token*,Token*,Token*,Select*,int,int);

#if !defined(SQLITE4_OMIT_VIEW) || !defined(SQLITE4_OMIT_VIRTUALTABLE)
  int sqlite4ViewGetColumnNames(Parse*,Table*);
#else
# define sqlite4ViewGetColumnNames(A,B) 0
#endif

void sqlite4DropTable(Parse*, SrcList*, int, int);
void sqlite4CodeDropTable(Parse*, Table*, int, int);
void sqlite4DeleteTable(sqlite4*, Table*);
#ifndef SQLITE4_OMIT_AUTOINCREMENT
  void sqlite4AutoincrementBegin(Parse *pParse);
  void sqlite4AutoincrementEnd(Parse *pParse);
#else
# define sqlite4AutoincrementBegin(X)
# define sqlite4AutoincrementEnd(X)
#endif
void sqlite4Insert(Parse*, SrcList*, ExprList*, Select*, IdList*, int);
void *sqlite4ArrayAllocate(sqlite4*,void*,int,int,int*,int*,int*);
IdList *sqlite4IdListAppend(sqlite4*, IdList*, Token*);
int sqlite4IdListIndex(IdList*,const char*);
SrcList *sqlite4SrcListEnlarge(sqlite4*, SrcList*, int, int);
SrcList *sqlite4SrcListAppend(sqlite4*, SrcList*, Token*, Token*);
SrcList *sqlite4SrcListAppendFromTerm(Parse*, SrcList*, Token*, Token*,
                                      Token*, Select*, Expr*, IdList*);
void sqlite4SrcListIndexedBy(Parse *, SrcList *, Token *);
int sqlite4IndexedByLookup(Parse *, SrcListItem *);
void sqlite4SrcListShiftJoinType(SrcList*);
void sqlite4SrcListAssignCursors(Parse*, SrcList*);
void sqlite4IdListDelete(sqlite4*, IdList*);
void sqlite4SrcListDelete(sqlite4*, SrcList*);
Index *sqlite4CreateIndex(Parse*,Token*,Token*,SrcList*,ExprList*,int,Token*,
                        Token*, int, int, int);
void sqlite4DropIndex(Parse*, SrcList*, int);
int sqlite4Select(Parse*, Select*, SelectDest*);
Select *sqlite4SelectNew(Parse*,ExprList*,SrcList*,Expr*,ExprList*,
                         Expr*,ExprList*,int,Expr*,Expr*);
void sqlite4SelectDelete(sqlite4*, Select*);
Table *sqlite4SrcListLookup(Parse*, SrcList*);
int sqlite4IsReadOnly(Parse*, Table*, int);
void sqlite4OpenTable(Parse*, int iCur, int iDb, Table*, int);
#if defined(SQLITE4_ENABLE_UPDATE_DELETE_LIMIT) && !defined(SQLITE4_OMIT_SUBQUERY)
Expr *sqlite4LimitWhere(Parse *, SrcList *, Expr *, ExprList *, Expr *, Expr *, char *);
#endif
void sqlite4DeleteFrom(Parse*, SrcList*, Expr*);
void sqlite4Update(Parse*, SrcList*, ExprList*, Expr*, int);
WhereInfo *sqlite4WhereBegin(Parse*, SrcList*, Expr*, ExprList**,ExprList*,u16);
void sqlite4WhereEnd(WhereInfo*);
int sqlite4ExprCodeGetColumn(Parse*, Table*, int, int, int);
void sqlite4ExprCodeGetColumnOfTable(Vdbe*, Table*, int, int, int);
void sqlite4ExprCodeMove(Parse*, int, int, int);
void sqlite4ExprCodeCopy(Parse*, int, int, int);
void sqlite4ExprCacheStore(Parse*, int, int, int);
void sqlite4ExprCachePush(Parse*);
void sqlite4ExprCachePop(Parse*, int);
void sqlite4ExprCacheRemove(Parse*, int, int);
void sqlite4ExprCacheClear(Parse*);
void sqlite4ExprCacheAffinityChange(Parse*, int, int);
int sqlite4ExprCode(Parse*, Expr*, int);
int sqlite4ExprCodeTemp(Parse*, Expr*, int*);
int sqlite4ExprCodeTarget(Parse*, Expr*, int);
int sqlite4ExprCodeAndCache(Parse*, Expr*, int);
void sqlite4ExprCodeConstants(Parse*, Expr*);
int sqlite4ExprCodeExprList(Parse*, ExprList*, int, int);
void sqlite4ExprIfTrue(Parse*, Expr*, int, int);
void sqlite4ExprIfFalse(Parse*, Expr*, int, int);
Table *sqlite4FindTable(sqlite4*,const char*, const char*);
Table *sqlite4LocateTable(Parse*,int isView,const char*, const char*);
Index *sqlite4FindIndex(sqlite4*,const char*, const char*);
void sqlite4UnlinkAndDeleteTable(sqlite4*,int,const char*);
void sqlite4UnlinkAndDeleteIndex(sqlite4*,int,const char*);
void sqlite4Vacuum(Parse*);
int sqlite4RunVacuum(char**, sqlite4*);
char *sqlite4NameFromToken(sqlite4*, Token*);
int sqlite4ExprCompare(Expr*, Expr*);
int sqlite4ExprListCompare(ExprList*, ExprList*);
void sqlite4ExprAnalyzeAggregates(NameContext*, Expr*);
void sqlite4ExprAnalyzeAggList(NameContext*,ExprList*);
Vdbe *sqlite4GetVdbe(Parse*);
void sqlite4PrngSaveState(void);
void sqlite4PrngRestoreState(void);
void sqlite4PrngResetState(void);
void sqlite4CodeVerifySchema(Parse*, int);
void sqlite4CodeVerifyNamedSchema(Parse*, const char *zDb);
void sqlite4BeginTransaction(Parse*, int);
void sqlite4EndTransaction(Parse *, int);
void sqlite4Savepoint(Parse*, int, Token*);
void sqlite4CloseSavepoints(sqlite4 *);
int sqlite4ExprIsConstant(Expr*);
int sqlite4ExprIsConstantNotJoin(Expr*);
int sqlite4ExprIsConstantOrFunction(Expr*);
int sqlite4ExprIsInteger(Expr*, int*);
int sqlite4ExprCanBeNull(const Expr*);
void sqlite4ExprCodeIsNullJump(Vdbe*, const Expr*, int, int);
int sqlite4ExprNeedsNoAffinityChange(const Expr*, char);
void sqlite4GenerateRowDelete(Parse*, Table*, int, int, int, Trigger *, int);
void sqlite4GenerateRowIndexDelete(Parse*, Table*, int, int, int*);
void sqlite4EncodeIndexKey(Parse *, Index *, int, Index *, int, int, int);
void sqlite4GenerateConstraintChecks(Parse*,Table*,int,int,
                                     int*,int,int,int,int,int*);
void sqlite4CompleteInsertion(Parse*, Table*, int, int, int*, int, int, int);
int sqlite4OpenTableAndIndices(Parse*, Table*, int, int);
void sqlite4BeginWriteOperation(Parse*, int, int);
void sqlite4MultiWrite(Parse*);
void sqlite4MayAbort(Parse*);
void sqlite4HaltConstraint(Parse*, int, char*, int);
Expr *sqlite4ExprDup(sqlite4*,Expr*,int);
ExprList *sqlite4ExprListDup(sqlite4*,ExprList*,int);
SrcList *sqlite4SrcListDup(sqlite4*,SrcList*,int);
IdList *sqlite4IdListDup(sqlite4*,IdList*);
Select *sqlite4SelectDup(sqlite4*,Select*,int);
void sqlite4FuncDefInsert(FuncDefTable*, FuncDef*, int);
FuncDef *sqlite4FindFunction(sqlite4*,const char*,int,int,u8,int);
void sqlite4RegisterBuiltinFunctions(sqlite4*);
void sqlite4RegisterDateTimeFunctions(sqlite4_env*);
void sqlite4RegisterGlobalFunctions(sqlite4_env*);
int sqlite4SafetyCheckOk(sqlite4*);
int sqlite4SafetyCheckSickOrOk(sqlite4*);
void sqlite4ChangeCookie(Parse*, int);

#if !defined(SQLITE4_OMIT_VIEW) && !defined(SQLITE4_OMIT_TRIGGER)
void sqlite4MaterializeView(Parse*, Table*, Expr*, int);
#else
# define sqlite4MaterializeView(w,x,y,z)
#endif

#ifndef SQLITE4_OMIT_TRIGGER
  void sqlite4BeginTrigger(Parse*, Token*,Token*,int,int,IdList*,SrcList*,
                           Expr*,int, int);
  void sqlite4FinishTrigger(Parse*, TriggerStep*, Token*);
  void sqlite4DropTrigger(Parse*, SrcList*, int);
  void sqlite4DropTriggerPtr(Parse*, Trigger*);
  Trigger *sqlite4TriggersExist(Parse *, Table*, int, ExprList*, int *pMask);
  Trigger *sqlite4TriggerList(Parse *, Table *);
  void sqlite4CodeRowTrigger(Parse*, Trigger *, int, ExprList*, int, Table *,
                            int, int, int);
  void sqlite4CodeRowTriggerDirect(Parse *, Trigger *, Table *, int, int, int);
  void sqliteViewTriggers(Parse*, Table*, Expr*, int, ExprList*);
  void sqlite4DeleteTriggerStep(sqlite4*, TriggerStep*);
  TriggerStep *sqlite4TriggerSelectStep(sqlite4*,Select*);
  TriggerStep *sqlite4TriggerInsertStep(sqlite4*,Token*, IdList*,
                                        ExprList*,Select*,u8);
  TriggerStep *sqlite4TriggerUpdateStep(sqlite4*,Token*,ExprList*, Expr*, u8);
  TriggerStep *sqlite4TriggerDeleteStep(sqlite4*,Token*, Expr*);
  void sqlite4DeleteTrigger(sqlite4*, Trigger*);
  void sqlite4UnlinkAndDeleteTrigger(sqlite4*,int,const char*);
  u32 sqlite4TriggerColmask(Parse*,Trigger*,ExprList*,int,int,Table*,int);
# define sqlite4ParseToplevel(p) ((p)->pToplevel ? (p)->pToplevel : (p))
#else
# define sqlite4TriggersExist(B,C,D,E,F) 0
# define sqlite4DeleteTrigger(A,B)
# define sqlite4DropTriggerPtr(A,B)
# define sqlite4UnlinkAndDeleteTrigger(A,B,C)
# define sqlite4CodeRowTrigger(A,B,C,D,E,F,G,H,I)
# define sqlite4CodeRowTriggerDirect(A,B,C,D,E,F)
# define sqlite4TriggerList(X, Y) 0
# define sqlite4ParseToplevel(p) p
# define sqlite4TriggerColmask(A,B,C,D,E,F,G) 0
#endif

int sqlite4JoinType(Parse*, Token*, Token*, Token*);
void sqlite4CreateForeignKey(Parse*, ExprList*, Token*, ExprList*, int);
void sqlite4DeferForeignKey(Parse*, int);
#ifndef SQLITE4_OMIT_AUTHORIZATION
  void sqlite4AuthRead(Parse*,Expr*,Schema*,SrcList*);
  int sqlite4AuthCheck(Parse*,int, const char*, const char*, const char*);
  void sqlite4AuthContextPush(Parse*, AuthContext*, const char*);
  void sqlite4AuthContextPop(AuthContext*);
  int sqlite4AuthReadCol(Parse*, const char *, const char *, int);
#else
# define sqlite4AuthRead(a,b,c,d)
# define sqlite4AuthCheck(a,b,c,d,e)    SQLITE4_OK
# define sqlite4AuthContextPush(a,b,c)
# define sqlite4AuthContextPop(a)  ((void)(a))
#endif
void sqlite4Attach(Parse*, Expr*, Expr*, Expr*);
void sqlite4Detach(Parse*, Expr*);
int sqlite4FixInit(DbFixer*, Parse*, int, const char*, const Token*);
int sqlite4FixSrcList(DbFixer*, SrcList*);
int sqlite4FixSelect(DbFixer*, Select*);
int sqlite4FixExpr(DbFixer*, Expr*);
int sqlite4FixExprList(DbFixer*, ExprList*);
int sqlite4FixTriggerStep(DbFixer*, TriggerStep*);
int sqlite4AtoF(const char *z, double*, int, u8);
int sqlite4GetInt32(const char *, int*);
int sqlite4Atoi(const char*);
int sqlite4Utf16ByteLen(const void *pData, int nChar);
int sqlite4Utf8CharLen(const char *pData, int nByte);
u32 sqlite4Utf8Read(const u8*, const u8**);

/*
** Routines to read and write variable-length integers.  These used to
** be defined locally, but now we use the varint routines in the util.c
** file.  Code should use the MACRO forms below, as the Varint32 versions
** are coded to assume the single byte case is already handled (which 
** the MACRO form does).
*/
int sqlite4PutVarint(unsigned char*, u64);
int sqlite4PutVarint32(unsigned char*, u32);
u8 sqlite4GetVarint(const unsigned char *, u64 *);
u8 sqlite4GetVarint32(const unsigned char *, u32 *);
int sqlite4VarintLen(u64 v);
int sqlite4GetVarint64(const unsigned char*, int, sqlite4_uint64 *pResult);
int sqlite4PutVarint64(unsigned char*, sqlite4_uint64);

/*
** The header of a record consists of a sequence variable-length integers.
** These integers are almost always small and are encoded as a single byte.
** The following macros take advantage this fact to provide a fast encode
** and decode of the integers in a record header.  It is faster for the common
** case where the integer is a single byte.  It is a little slower when the
** integer is two or more bytes.  But overall it is faster.
**
** The following expressions are equivalent:
**
**     x = sqlite4GetVarint32( A, &B );
**     x = sqlite4PutVarint32( A, B );
**
**     x = getVarint32( A, B );
**     x = putVarint32( A, B );
**
*/
#define getVarint32(A,B)  (u8)((*(A)<(u8)0x80) ? ((B) = (u32)*(A)),1 : sqlite4GetVarint32((A), (u32 *)&(B)))
#define putVarint32(A,B)  (u8)(((u32)(B)<(u32)0x80) ? (*(A) = (unsigned char)(B)),1 : sqlite4PutVarint32((A), (B)))
#define getVarint    sqlite4GetVarint
#define putVarint    sqlite4PutVarint


const char *sqlite4IndexAffinityStr(Vdbe *, Index *);
void sqlite4TableAffinityStr(Vdbe *, Table *);
char sqlite4CompareAffinity(Expr *pExpr, char aff2);
int sqlite4IndexAffinityOk(Expr *pExpr, char idx_affinity);
char sqlite4ExprAffinity(Expr *pExpr);
int sqlite4Atoi64(const char*, i64*, int, u8);
void sqlite4Error(sqlite4*, int, const char*,...);
void *sqlite4HexToBlob(sqlite4*, const char *z, int n);
u8 sqlite4HexToInt(int h);
int sqlite4TwoPartName(Parse *, Token *, Token *, Token **);
const char *sqlite4ErrStr(int);
int sqlite4ReadSchema(Parse *pParse);
CollSeq *sqlite4FindCollSeq(sqlite4*,u8 enc, const char*,int);
CollSeq *sqlite4LocateCollSeq(Parse *pParse, const char*zName);
CollSeq *sqlite4ExprCollSeq(Parse *pParse, Expr *pExpr);
Expr *sqlite4ExprSetColl(Expr*, CollSeq*);
Expr *sqlite4ExprSetCollByToken(Parse *pParse, Expr*, Token*);
int sqlite4CheckCollSeq(Parse *, CollSeq *);
int sqlite4CheckObjectName(Parse *, const char *);
void sqlite4VdbeSetChanges(sqlite4 *, int);
int sqlite4AddInt64(i64*,i64);
int sqlite4SubInt64(i64*,i64);
int sqlite4MulInt64(i64*,i64);
int sqlite4AbsInt32(int);
#ifdef SQLITE4_ENABLE_8_3_NAMES
void sqlite4FileSuffix3(const char*, char*);
#else
# define sqlite4FileSuffix3(X,Y)
#endif
u8 sqlite4GetBoolean(const char *z);

const void *sqlite4ValueText(sqlite4_value*, u8);
int sqlite4ValueBytes(sqlite4_value*, u8);
void sqlite4ValueSetStr(sqlite4_value*, int, const void *,u8, 
                        void(*)(void*));
void sqlite4ValueFree(sqlite4_value*);
sqlite4_value *sqlite4ValueNew(sqlite4 *);
char *sqlite4Utf16to8(sqlite4 *, const void*, int, u8);
#ifdef SQLITE4_ENABLE_STAT3
char *sqlite4Utf8to16(sqlite4 *, u8, char *, int, int *);
#endif
int sqlite4ValueFromExpr(sqlite4 *, Expr *, u8, u8, sqlite4_value **);
void sqlite4ValueApplyAffinity(sqlite4_value *, u8, u8);
#ifndef SQLITE4_AMALGAMATION
extern const unsigned char sqlite4OpcodeProperty[];
extern const unsigned char sqlite4UpperToLower[];
extern const unsigned char sqlite4CtypeMap[];
extern const Token sqlite4IntTokens[];
extern struct sqlite4_env sqlite4DefaultEnv;
extern struct KVFactory sqlite4BuiltinFactory;
#endif
void sqlite4RootPageMoved(sqlite4*, int, int, int);
void sqlite4Reindex(Parse*, Token*, Token*);
void sqlite4AlterFunctions(sqlite4_env*);
void sqlite4AlterRenameTable(Parse*, SrcList*, Token*);
int sqlite4GetToken(const unsigned char *, int *);
void sqlite4NestedParse(Parse*, const char*, ...);
void sqlite4ExpirePreparedStatements(sqlite4*);
int sqlite4CodeSubselect(Parse *, Expr *, int, int);
void sqlite4SelectPrep(Parse*, Select*, NameContext*);
int sqlite4ResolveExprNames(NameContext*, Expr*);
void sqlite4ResolveSelectNames(Parse*, Select*, NameContext*);
int sqlite4ResolveOrderGroupBy(Parse*, Select*, ExprList*, const char*);
void sqlite4ColumnDefault(Vdbe *, Table *, int, int);
void sqlite4AlterFinishAddColumn(Parse *, Token *);
void sqlite4AlterBeginAddColumn(Parse *, SrcList *);
CollSeq *sqlite4GetCollSeq(sqlite4*, u8, CollSeq *, const char*);
char sqlite4AffinityType(const char*);
void sqlite4Analyze(Parse*, Token*, Token*);
int sqlite4FindDb(sqlite4*, Token*);
int sqlite4FindDbName(sqlite4 *, const char *);
int sqlite4AnalysisLoad(sqlite4*,int iDB);
void sqlite4DeleteIndexSamples(sqlite4*,Index*);
void sqlite4DefaultRowEst(Index*);
void sqlite4RegisterLikeFunctions(sqlite4*, int);
int sqlite4IsLikeFunction(sqlite4*,Expr*,int*,char*);
void sqlite4SchemaClear(sqlite4_env*,Schema*);
Schema *sqlite4SchemaGet(sqlite4*);
int sqlite4SchemaToIndex(sqlite4 *db, Schema *);
KeyInfo *sqlite4IndexKeyinfo(Parse *, Index *);
int sqlite4CreateFunc(sqlite4 *, const char *, int, int, void *, 
  void (*)(sqlite4_context*,int,sqlite4_value **),
  void (*)(sqlite4_context*,int,sqlite4_value **), void (*)(sqlite4_context*),
  FuncDestructor *pDestructor
);
int sqlite4ApiExit(sqlite4 *db, int);
int sqlite4OpenTempDatabase(Parse *);

void sqlite4StrAccumInit(StrAccum*, char*, int, int);
void sqlite4StrAccumAppend(StrAccum*,const char*,int);
void sqlite4AppendSpace(StrAccum*,int);
char *sqlite4StrAccumFinish(StrAccum*);
void sqlite4StrAccumReset(StrAccum*);
void sqlite4SelectDestInit(SelectDest*,int,int);
Expr *sqlite4CreateColumnExpr(sqlite4 *, SrcList *, int, int);

void sqlite4OpenPrimaryKey(Parse*, int iCur, int iDb, Table*, int);
void sqlite4OpenIndex(Parse*, int iCur, int iDb, Index*, int);
int sqlite4OpenAllIndexes(Parse *, Table *, int, int);
void sqlite4CloseAllIndexes(Parse *, Table *, int);
Index *sqlite4FindPrimaryKey(Table *, int *);

/*
** The interface to the LEMON-generated parser
*/
void *sqlite4ParserAlloc(void*(*)(void*,size_t), void*);
void sqlite4ParserFree(void*, void(*)(void*,void*));
void sqlite4Parser(void*, int, Token, Parse*);
#ifdef YYTRACKMAXSTACKDEPTH
  int sqlite4ParserStackPeak(void*);
#endif

void sqlite4AutoLoadExtensions(sqlite4*);
#ifndef SQLITE4_OMIT_LOAD_EXTENSION
  void sqlite4CloseExtensions(sqlite4*);
#else
# define sqlite4CloseExtensions(X)
#endif

#ifdef SQLITE4_TEST
  int sqlite4Utf8To8(unsigned char*);
#endif

#ifdef SQLITE4_OMIT_VIRTUALTABLE
#  define sqlite4VtabClear(Y)
#  define sqlite4VtabSync(X,Y) SQLITE4_OK
#  define sqlite4VtabRollback(X)
#  define sqlite4VtabCommit(X)
#  define sqlite4VtabInSync(db) 0
#  define sqlite4VtabLock(X) 
#  define sqlite4VtabUnlock(X)
#  define sqlite4VtabUnlockList(X)
#  define sqlite4VtabSavepoint(X, Y, Z) SQLITE4_OK
#  define sqlite4GetVTable(X,Y)  ((VTable*)0)
#else
   void sqlite4VtabClear(sqlite4 *db, Table*);
   int sqlite4VtabSync(sqlite4 *db, char **);
   int sqlite4VtabRollback(sqlite4 *db);
   int sqlite4VtabCommit(sqlite4 *db);
   void sqlite4VtabLock(VTable *);
   void sqlite4VtabUnlock(VTable *);
   void sqlite4VtabUnlockList(sqlite4*);
   int sqlite4VtabSavepoint(sqlite4 *, int, int);
   VTable *sqlite4GetVTable(sqlite4*, Table*);
#  define sqlite4VtabInSync(db) ((db)->nVTrans>0 && (db)->aVTrans==0)
#endif
void sqlite4VtabMakeWritable(Parse*,Table*);
void sqlite4VtabBeginParse(Parse*, Token*, Token*, Token*);
void sqlite4VtabFinishParse(Parse*, Token*);
void sqlite4VtabArgInit(Parse*);
void sqlite4VtabArgExtend(Parse*, Token*);
int sqlite4VtabCallCreate(sqlite4*, int, const char *, char **);
int sqlite4VtabCallConnect(Parse*, Table*);
int sqlite4VtabCallDestroy(sqlite4*, int, const char *);
int sqlite4VtabBegin(sqlite4 *, VTable *);
FuncDef *sqlite4VtabOverloadFunction(sqlite4 *,FuncDef*, int nArg, Expr*);
void sqlite4InvalidFunction(sqlite4_context*,int,sqlite4_value**);
int sqlite4VdbeParameterIndex(Vdbe*, const char*, int);
int sqlite4TransferBindings(sqlite4_stmt *, sqlite4_stmt *);
int sqlite4Reprepare(Vdbe*);
void sqlite4ExprListCheckLength(Parse*, ExprList*, const char*);
CollSeq *sqlite4BinaryCompareCollSeq(Parse *, Expr *, Expr *);
int sqlite4TempInMemory(const sqlite4*);
const char *sqlite4JournalModename(int);
int sqlite4Checkpoint(sqlite4*, int, int, int*, int*);
int sqlite4WalDefaultHook(void*,sqlite4*,const char*,int);

/* Declarations for functions in fkey.c. All of these are replaced by
** no-op macros if OMIT_FOREIGN_KEY is defined. In this case no foreign
** key functionality is available. If OMIT_TRIGGER is defined but
** OMIT_FOREIGN_KEY is not, only some of the functions are no-oped. In
** this case foreign keys are parsed, but no other functionality is 
** provided (enforcement of FK constraints requires the triggers sub-system).
*/
#if !defined(SQLITE4_OMIT_FOREIGN_KEY) && !defined(SQLITE4_OMIT_TRIGGER)
  void sqlite4FkCheck(Parse*, Table*, int, int);
  void sqlite4FkDropTable(Parse*, SrcList *, Table*);
  void sqlite4FkActions(Parse*, Table*, ExprList*, int);
  int sqlite4FkRequired(Parse*, Table*, int*);
  u32 sqlite4FkOldmask(Parse*, Table*);
  FKey *sqlite4FkReferences(Table *);
#else
  #define sqlite4FkActions(a,b,c,d)
  #define sqlite4FkCheck(a,b,c,d)
  #define sqlite4FkDropTable(a,b,c)
  #define sqlite4FkOldmask(a,b)      0
  #define sqlite4FkRequired(a,b,c) 0
#endif
#ifndef SQLITE4_OMIT_FOREIGN_KEY
  void sqlite4FkDelete(sqlite4 *, Table*);
#else
  #define sqlite4FkDelete(a,b)
#endif


/*
** Available fault injectors.  Should be numbered beginning with 0.
*/
#define SQLITE4_FAULTINJECTOR_MALLOC     0
#define SQLITE4_FAULTINJECTOR_COUNT      1

/*
** The interface to the code in fault.c used for identifying "benign"
** malloc failures. This is only present if SQLITE4_OMIT_BUILTIN_TEST
** is not defined.
*/
#ifndef SQLITE4_OMIT_BUILTIN_TEST
  void sqlite4BeginBenignMalloc(sqlite4_env*);
  void sqlite4EndBenignMalloc(sqlite4_env*);
#else
  #define sqlite4BeginBenignMalloc(X)
  #define sqlite4EndBenignMalloc(X)
#endif

#define IN_INDEX_ROWID           1
#define IN_INDEX_EPH             2
#define IN_INDEX_INDEX           3
int sqlite4FindInIndex(Parse *, Expr *, int*);
Index *sqlite4FindExistingInIndex(Parse *, Expr *, int);


#if SQLITE4_MAX_EXPR_DEPTH>0
  void sqlite4ExprSetHeight(Parse *pParse, Expr *p);
  int sqlite4SelectExprHeight(Select *);
  int sqlite4ExprCheckHeight(Parse*, int);
#else
  #define sqlite4ExprSetHeight(x,y)
  #define sqlite4SelectExprHeight(x) 0
  #define sqlite4ExprCheckHeight(x,y)
#endif

u32 sqlite4Get4byte(const u8*);
void sqlite4Put4byte(u8*, u32);

#ifdef SQLITE4_ENABLE_UNLOCK_NOTIFY
  void sqlite4ConnectionBlocked(sqlite4 *, sqlite4 *);
  void sqlite4ConnectionUnlocked(sqlite4 *db);
  void sqlite4ConnectionClosed(sqlite4 *db);
#else
  #define sqlite4ConnectionBlocked(x,y)
  #define sqlite4ConnectionUnlocked(x)
  #define sqlite4ConnectionClosed(x)
#endif

#ifdef SQLITE4_DEBUG
  void sqlite4ParserTrace(FILE*, char *);
#endif

/*
** If the SQLITE4_ENABLE IOTRACE exists then the global variable
** sqlite4IoTrace is a pointer to a printf-like routine used to
** print I/O tracing messages. 
*/
#ifdef SQLITE4_ENABLE_IOTRACE
# define IOTRACE(A)  if( sqlite4IoTrace ){ sqlite4IoTrace A; }
  void sqlite4VdbeIOTraceSql(Vdbe*);
SQLITE4_EXTERN void (*sqlite4IoTrace)(const char*,...);
#else
# define IOTRACE(A)
# define sqlite4VdbeIOTraceSql(X)
#endif

/*
** These routines are available for the mem2.c debugging memory allocator
** only.  They are used to verify that different "types" of memory
** allocations are properly tracked by the system.
**
** sqlite4MemdebugSetType() sets the "type" of an allocation to one of
** the MEMTYPE_* macros defined below.  The type must be a bitmask with
** a single bit set.
**
** sqlite4MemdebugHasType() returns true if any of the bits in its second
** argument match the type set by the previous sqlite4MemdebugSetType().
** sqlite4MemdebugHasType() is intended for use inside assert() statements.
**
** sqlite4MemdebugNoType() returns true if none of the bits in its second
** argument match the type set by the previous sqlite4MemdebugSetType().
**
** Perhaps the most important point is the difference between MEMTYPE_HEAP
** and MEMTYPE_LOOKASIDE.  If an allocation is MEMTYPE_LOOKASIDE, that means
** it might have been allocated by lookaside, except the allocation was
** too large or lookaside was already full.  It is important to verify
** that allocations that might have been satisfied by lookaside are not
** passed back to non-lookaside free() routines.  Asserts such as the
** example above are placed on the non-lookaside free() routines to verify
** this constraint. 
**
** All of this is no-op for a production build.  It only comes into
** play when the SQLITE4_MEMDEBUG compile-time option is used.
*/
#ifdef SQLITE4_MEMDEBUG
  void sqlite4MemdebugSetType(void*,u8);
  int sqlite4MemdebugHasType(const void*,u8);
  int sqlite4MemdebugNoType(const void*,u8);
#else
# define sqlite4MemdebugSetType(X,Y)  /* no-op */
# define sqlite4MemdebugHasType(X,Y)  1
# define sqlite4MemdebugNoType(X,Y)   1
#endif
#define MEMTYPE_HEAP       0x01  /* General heap allocations */
#define MEMTYPE_LOOKASIDE  0x02  /* Might have been lookaside memory */
#define MEMTYPE_SCRATCH    0x04  /* Scratch allocations */
#define MEMTYPE_DB         0x10  /* Uses sqlite4DbMalloc, not sqlite_malloc */

int sqlite4InitFts5(sqlite4 *db);
int sqlite4InitFts5Func(sqlite4 *db);
void sqlite4ShutdownFts5(sqlite4 *db);
void sqlite4CreateUsingIndex(Parse*, CreateIndex*, ExprList*, Token*, Token*);

int sqlite4Fts5IndexSz(void);
void sqlite4Fts5IndexInit(Parse *, Index *, ExprList *);
void sqlite4Fts5IndexFree(sqlite4 *, Index *);

int sqlite4Fts5Update(sqlite4 *, Fts5Info *, int, Mem *, Mem *, int, char **);
void sqlite4Fts5FreeInfo(sqlite4 *db, Fts5Info *);
void sqlite4Fts5CodeUpdate(Parse *, Index *pIdx, int, int, int, int);
void sqlite4Fts5CodeCksum(Parse *, Index *, int, int, int);
void sqlite4Fts5CodeQuery(Parse *, Index *, int, int, int);

int sqlite4Fts5Pk(Fts5Cursor *, int, KVByteArray **, KVSize *);
int sqlite4Fts5Next(Fts5Cursor *pCsr);

int sqlite4Fts5EntryCksum(sqlite4 *, Fts5Info *, Mem *, Mem *, i64 *);
int sqlite4Fts5RowCksum(sqlite4 *, Fts5Info *, Mem *, Mem *, i64 *);
int sqlite4Fts5Open(sqlite4*, Fts5Info*, const char*, int, Fts5Cursor**,char**);
int sqlite4Fts5Valid(Fts5Cursor *);
void sqlite4Fts5Close(Fts5Cursor *);

#endif /* _SQLITEINT_H_ */
