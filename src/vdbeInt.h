/*
** 2003 September 6
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This is the header file for information that is private to the
** VDBE.  This information used to all be at the top of the single
** source code file "vdbe.c".  When that file became too big (over
** 6000 lines long) it was split up into several smaller files and
** this header information was factored out.
*/
#ifndef _VDBEINT_H_
#define _VDBEINT_H_

/*
** SQL is translated into a sequence of instructions to be
** executed by a virtual machine.  Each instruction is an instance
** of the following structure.
*/
typedef struct VdbeOp Op;

/*
** Boolean values
*/
typedef unsigned char Bool;

/* Opaque type used by code in vdbesort.c */
typedef struct VdbeSorter VdbeSorter;

/* Opaque type used by the explainer */
typedef struct Explain Explain;

/* Opaque type used by vdbecodec.c */
typedef struct ValueDecoder ValueDecoder;

/*
** A cursor is a pointer into a single database.
** The cursor can seek to an entry with a particular key, or
** loop over all entries.  You can also insert new
** entries or retrieve the key or data from the entry that the cursor
** is currently pointing to.
** 
** Every cursor that the virtual machine has open is represented by an
** instance of the following structure.
*/
struct VdbeCursor {
  KVCursor *pKVCur;     /* The cursor structure of the backend */
  KVStore *pTmpKV;      /* Separate file holding a temporary table */
  KeyInfo *pKeyInfo;    /* Info about index keys needed by index cursors */
  int iDb;              /* Index of cursor database in db->aDb[] (or -1) */
  int iRoot;            /* Root page of the table */
  int pseudoTableReg;   /* Register holding pseudotable content. */
  int nField;           /* Number of fields in the header */
  Bool zeroed;          /* True if zeroed out and ready for reuse */
  Bool atFirst;         /* True if pointing to first entry */
  Bool nullRow;         /* True if pointing to a row with no data */
  Bool isTable;         /* True if a table requiring integer keys */
  Bool isIndex;         /* True if an index containing keys only - no data */
  Bool isOrdered;       /* True if the underlying table is BTREE_UNORDERED */
  sqlite4_vtab_cursor *pVtabCursor;  /* The cursor for a virtual table */
  const sqlite4_module *pModule;     /* Module for cursor pVtabCursor */
  i64 seqCount;         /* Sequence counter */
  i64 movetoTarget;     /* Argument to the deferred move-to */
  VdbeSorter *pSorter;  /* Sorter object for OP_SorterOpen cursors */
  Fts5Cursor *pFts;     /* Fts5 cursor object (or NULL) */

  /* Result of last sqlite4-Moveto() done by an OP_NotExists or 
  ** OP_IsUnique opcode on this cursor. */
  int seekResult;
};

/*
** When a sub-program is executed (OP_Program), a structure of this type
** is allocated to store the current value of the program counter, as
** well as the current memory cell array and various other frame specific
** values stored in the Vdbe struct. When the sub-program is finished, 
** these values are copied back to the Vdbe from the VdbeFrame structure,
** restoring the state of the VM to as it was before the sub-program
** began executing.
**
** The memory for a VdbeFrame object is allocated and managed by a memory
** cell in the parent (calling) frame. When the memory cell is deleted or
** overwritten, the VdbeFrame object is not freed immediately. Instead, it
** is linked into the Vdbe.pDelFrame list. The contents of the Vdbe.pDelFrame
** list is deleted when the VM is reset in VdbeHalt(). The reason for doing
** this instead of deleting the VdbeFrame immediately is to avoid recursive
** calls to sqlite4VdbeMemRelease() when the memory cells belonging to the
** child frame are released.
**
** The currently executing frame is stored in Vdbe.pFrame. Vdbe.pFrame is
** set to NULL if the currently executing frame is the main program.
*/
typedef struct VdbeFrame VdbeFrame;
struct VdbeFrame {
  Vdbe *v;                /* VM this frame belongs to */
  int pc;                 /* Program Counter in parent (calling) frame */
  Op *aOp;                /* Program instructions for parent frame */
  int nOp;                /* Size of aOp array */
  Mem *aMem;              /* Array of memory cells for parent frame */
  int nMem;               /* Number of entries in aMem */
  u8 *aOnceFlag;          /* Array of OP_Once flags for parent frame */
  int nOnceFlag;          /* Number of entries in aOnceFlag */
  VdbeCursor **apCsr;     /* Array of Vdbe cursors for parent frame */
  u16 nCursor;            /* Number of entries in apCsr */
  void *token;            /* Copy of SubProgram.token */
  int nChildMem;          /* Number of memory cells for child frame */
  int nChildCsr;          /* Number of cursors for child frame */
  int nChange;            /* Statement changes (Vdbe.nChanges)     */
  VdbeFrame *pParent;     /* Parent of this frame, or NULL if parent is main */
};

#define VdbeFrameMem(p) ((Mem *)&((u8 *)p)[ROUND8(sizeof(VdbeFrame))])

/*
** A value for VdbeCursor.cacheValid that means the cache is always invalid.
*/
#define CACHE_STALE 0

/*
** Internally, the vdbe manipulates nearly all SQL values as Mem
** structures. Each Mem struct may cache multiple representations (string,
** integer etc.) of the same value.
*/
struct Mem {
  sqlite4 *db;        /* The associated database connection */
  char *z;            /* String or BLOB value */
  double r;           /* Real value */
  union {
    i64 i;              /* Integer value used when MEM_Int is set in flags */
    FuncDef *pDef;      /* Used only when flags==MEM_Agg */
    RowSet *pRowSet;    /* Used only when flags==MEM_RowSet */
    VdbeFrame *pFrame;  /* Used when flags==MEM_Frame */
  } u;
  int n;              /* Number of characters in string value, excluding '\0' */
  u16 flags;          /* Some combination of MEM_Null, MEM_Str, MEM_Dyn, etc. */
  u8  type;           /* One of SQLITE4_NULL, SQLITE4_TEXT, SQLITE4_INTEGER, etc */
  u8  enc;            /* SQLITE4_UTF8, SQLITE4_UTF16BE, SQLITE4_UTF16LE */
#ifdef SQLITE4_DEBUG
  Mem *pScopyFrom;    /* This Mem is a shallow copy of pScopyFrom */
  void *pFiller;      /* So that sizeof(Mem) is a multiple of 8 */
#endif
  void (*xDel)(void*,void*); /* Function to delete Mem.z */
  void *pDelArg;             /* First argument to xDel() */
  char *zMalloc;      /* Dynamic buffer allocated by sqlite4_malloc() */
};

/* One or more of the following flags are set to indicate the validOK
** representations of the value stored in the Mem struct.
**
** If the MEM_Null flag is set, then the value is an SQL NULL value.
** No other flags may be set in this case.
**
** If the MEM_Str flag is set then Mem.z points at a string representation.
** Usually this is encoded in the same unicode encoding as the main
** database (see below for exceptions). If the MEM_Term flag is also
** set, then the string is nul terminated. The MEM_Int and MEM_Real 
** flags may coexist with the MEM_Str flag.
*/
#define MEM_Null      0x0001   /* Value is NULL */
#define MEM_Str       0x0002   /* Value is a string */
#define MEM_Int       0x0004   /* Value is an integer */
#define MEM_Real      0x0008   /* Value is a real number */
#define MEM_Blob      0x0010   /* Value is a BLOB */
#define MEM_RowSet    0x0020   /* Value is a RowSet object */
#define MEM_Frame     0x0040   /* Value is a VdbeFrame object */
#define MEM_Invalid   0x0080   /* Value is undefined */
#define MEM_TypeMask  0x00ff   /* Mask of type bits */


/* Whenever Mem contains a valid string or blob representation, one of
** the following flags must be set to determine the memory management
** policy for Mem.z.  The MEM_Term flag tells us whether or not the
** string is \000 or \u0000 terminated
*/
#define MEM_Term      0x0200   /* String rep is nul terminated */
#define MEM_Dyn       0x0400   /* Need to call sqliteFree() on Mem.z */
#define MEM_Static    0x0800   /* Mem.z points to a static string */
#define MEM_Ephem     0x1000   /* Mem.z points to an ephemeral string */
#define MEM_Agg       0x2000   /* Mem.z points to an agg function context */

/*
** Clear any existing type flags from a Mem and replace them with f
*/
#define MemSetTypeFlag(p, f) \
   ((p)->flags = ((p)->flags&~(MEM_TypeMask))|f)

/*
** Return true if a memory cell is not marked as invalid.  This macro
** is for use inside assert() statements only.
*/
#ifdef SQLITE4_DEBUG
#define memIsValid(M)  ((M)->flags & MEM_Invalid)==0
#endif


/* A VdbeFunc is just a FuncDef (defined in sqliteInt.h) that contains
** additional information about auxiliary information bound to arguments
** of the function.  This is used to implement the sqlite4_get_auxdata()
** and sqlite4_set_auxdata() APIs.  The "auxdata" is some auxiliary data
** that can be associated with a constant argument to a function.  This
** allows functions such as "regexp" to compile their constant regular
** expression argument once and reused the compiled code for multiple
** invocations.
*/
struct VdbeFunc {
  FuncDef *pFunc;               /* The definition of the function */
  int nAux;                     /* Number of entries allocated for apAux[] */
  struct AuxData {
    void *pAux;                   /* Aux data for the i-th argument */
    void (*xDelete)(void*,void*); /* Destructor for the aux data */
    void *pDeleteArg;             /* First argument to xDelete */
  } apAux[1];                   /* One slot for each function argument */
};

/*
** The "context" argument for a installable function.  A pointer to an
** instance of this structure is the first argument to the routines used
** implement the SQL functions.
**
** There is a typedef for this structure in sqlite.h.  So all routines,
** even the public interface to SQLite, can use a pointer to this structure.
** But this file is the only place where the internal details of this
** structure are known.
**
** This structure is defined inside of vdbeInt.h because it uses substructures
** (Mem) which are only defined there.
*/
struct sqlite4_context {
  FuncDef *pFunc;       /* Pointer to function information.  MUST BE FIRST */
  VdbeFunc *pVdbeFunc;  /* Auxilary data, if created. */
  Mem s;                /* The return value is stored here */
  Mem *pMem;            /* Memory cell used to store aggregate context */
  int isError;          /* Error code returned by the function. */
  CollSeq *pColl;       /* Collating sequence */
  Fts5Cursor *pFts;     /* fts5 cursor for matchinfo functions */
};

/*
** An Explain object accumulates indented output which is helpful
** in describing recursive data structures.
*/
struct Explain {
  Vdbe *pVdbe;       /* Attach the explanation to this Vdbe */
  StrAccum str;      /* The string being accumulated */
  int nIndent;       /* Number of elements in aIndent */
  u16 aIndent[100];  /* Levels of indentation */
  char zBase[100];   /* Initial space */
};

/*
** An instance of the virtual machine.  This structure contains the complete
** state of the virtual machine.
**
** The "sqlite4_stmt" structure pointer that is returned by sqlite4_prepare()
** is really a pointer to an instance of this structure.
**
** The Vdbe.inVtabMethod variable is set to non-zero for the duration of
** any virtual table method invocations made by the vdbe program. It is
** set to 2 for xDestroy method calls and 1 for all other methods. This
** variable is used for two purposes: to allow xDestroy methods to execute
** "DROP TABLE" statements and to prevent some nasty side effects of
** malloc failure when SQLite is invoked recursively by a virtual table 
** method function.
*/
struct Vdbe {
  sqlite4 *db;            /* The database connection that owns this statement */
  Op *aOp;                /* Space to hold the virtual machine's program */
  Mem *aMem;              /* The memory locations */
  Mem **apArg;            /* Arguments to currently executing user function */
  Mem *aColName;          /* Column names to return */
  Mem *pResultSet;        /* Pointer to an array of results */
  int nMem;               /* Number of memory locations currently allocated */
  int nOp;                /* Number of instructions in the program */
  int nOpAlloc;           /* Number of slots allocated for aOp[] */
  int nLabel;             /* Number of labels used */
  int nLabelAlloc;        /* Number of slots allocated in aLabel[] */
  int *aLabel;            /* Space to hold the labels */
  u16 nResColumn;         /* Number of columns in one row of the result set */
  u16 nCursor;            /* Number of slots in apCsr[] */
  u32 magic;              /* Magic number for sanity checking */
  char *zErrMsg;          /* Error message written here */
  Vdbe *pPrev,*pNext;     /* Linked list of VDBEs with the same Vdbe.db */
  VdbeCursor **apCsr;     /* One element of this array for each open cursor */
  Mem *aVar;              /* Values for the OP_Variable opcode. */
  char **azVar;           /* Name of variables */
  ynVar nVar;             /* Number of entries in aVar[] */
  ynVar nzVar;            /* Number of entries in azVar[] */
  u32 cacheCtr;           /* VdbeCursor row cache generation counter */
  int pc;                 /* The program counter */
  int rc;                 /* Value to return */
  u8 errorAction;         /* Recovery action to do in case of an error */
  u8 explain;             /* True if EXPLAIN present on SQL command */
  u8 changeCntOn;         /* True to update the change-counter */
  u8 expired;             /* True if the VM needs to be recompiled */
  u8 runOnlyOnce;         /* Automatically expire on reset */
  u8 minWriteFileFormat;  /* Minimum file format for writable database files */
  u8 inVtabMethod;        /* See comments above */
  u8 needSavepoint;       /* True if a change might abort and needs savepoint */
  u8 readOnly;            /* True for read-only statements */
  int nChange;            /* Number of db changes made since last reset */
  yDbMask stmtTransMask;  /* db->aDb[] entries that have a subtransaction */
  int aCounter[3];        /* Counters used by sqlite4_stmt_status() */
#ifndef SQLITE4_OMIT_TRACE
  u64 startTime;          /* Time when query started - used for profiling */
#endif
  i64 nFkConstraint;      /* Number of imm. FK constraints this VM */
  i64 nStmtDefCons;       /* Number of def. constraints when stmt started */
  char *zSql;             /* Text of the SQL statement that generated this */
  void *pFree;            /* Free this when deleting the vdbe */
#ifdef SQLITE4_DEBUG
  FILE *trace;            /* Write an execution trace here, if not NULL */
#endif
#ifdef SQLITE4_ENABLE_TREE_EXPLAIN
  Explain *pExplain;      /* The explainer */
  char *zExplain;         /* Explanation of data structures */
#endif
  VdbeFrame *pFrame;      /* Parent frame */
  VdbeFrame *pDelFrame;   /* List of frame objects to free on VM reset */
  int nFrame;             /* Number of frames in pFrame list */
  u32 expmask;            /* Binding to these vars invalidates VM */
  SubProgram *pProgram;   /* Linked list of all sub-programs used by VM */
  int nOnceFlag;          /* Size of array aOnceFlag[] */
  u8 *aOnceFlag;          /* Flags for OP_Once */
};

/*
** The following are allowed values for Vdbe.magic
*/
#define VDBE_MAGIC_INIT     0x26bceaa5    /* Building a VDBE program */
#define VDBE_MAGIC_RUN      0xbdf20da3    /* VDBE is ready to execute */
#define VDBE_MAGIC_HALT     0x519c2973    /* VDBE has completed execution */
#define VDBE_MAGIC_DEAD     0xb606c3c8    /* The VDBE has been deallocated */

/*
** Function prototypes
*/
void sqlite4VdbeFreeCursor(Vdbe *, VdbeCursor*);
void sqliteVdbePopStack(Vdbe*,int);
#if defined(SQLITE4_DEBUG) || defined(VDBE_PROFILE)
void sqlite4VdbePrintOp(FILE*, int, Op*);
#endif
u32 sqlite4VdbeSerialTypeLen(u32);
u32 sqlite4VdbeSerialType(Mem*, int);
u32 sqlite4VdbeSerialPut(unsigned char*, int, Mem*, int);
u32 sqlite4VdbeSerialGet(const unsigned char*, u32, Mem*);
void sqlite4VdbeDeleteAuxData(VdbeFunc*, int);

int sqlite4VdbeCreateDecoder(
  sqlite4 *db,                /* The database connection */
  const unsigned char *aIn,   /* The input data blob */
  int nIn,                    /* Number of bytes in aIn[] */
  int mxCol,                  /* Maximum number of columns in aIn[] */
  ValueDecoder **ppOut        /* The newly generated decoder object */
);
int sqlite4VdbeDestroyDecoder(ValueDecoder *pDecoder);
int sqlite4VdbeDecodeValue(
  ValueDecoder *pDecoder,      /* The decoder for the whole string */
  int iVal,                    /* Index of the value to decode.  First is 0 */
  Mem *pDefault,               /* The default value.  Often NULL */
  Mem *pOut                    /* Write the result here */
);
int sqlite4VdbeEncodeData(
  sqlite4 *db,                /* The database connection */
  Mem *aIn,                   /* Array of values to encode */
  int nIn,                    /* Number of entries in aIn[] */
  u8 **pzOut,                 /* The output data record */
  int *pnOut                  /* Bytes of content in pzOut */
);
int sqlite4VdbeEncodeKey(
  sqlite4 *db,                 /* The database connection */
  Mem *aIn,                    /* Values to be encoded */
  int nIn,                     /* Number of entries in aIn[] */
  int nInTotal,                /* Number of values in complete key */
  int iTabno,                  /* The table this key applies to */
  KeyInfo *pKeyInfo,           /* Collating sequence information */
  u8 **pzOut,                  /* Write the resulting key here */
  int *pnOut,                  /* Number of bytes in the key */
  int nExtra                   /* Append extra bytes on end of key */
);
int sqlite4VdbeEncodeIntKey(u8 *aBuf,sqlite4_int64 v);
int sqlite4VdbeDecodeIntKey(const KVByteArray*, KVSize, sqlite4_int64*);
int sqlite4VdbeShortKey(const u8 *, int, int);
int sqlite4MemCompare(const Mem*, const Mem*, const CollSeq*);
int sqlite4VdbeExec(Vdbe*);
int sqlite4VdbeList(Vdbe*);
int sqlite4VdbeHalt(Vdbe*);
int sqlite4VdbeChangeEncoding(Mem *, int);
int sqlite4VdbeMemTooBig(Mem*);
int sqlite4VdbeMemCopy(Mem*, const Mem*);
void sqlite4VdbeMemShallowCopy(Mem*, const Mem*, int);
void sqlite4VdbeMemMove(Mem*, Mem*);
int sqlite4VdbeMemNulTerminate(Mem*);
int sqlite4VdbeMemSetStr(Mem*, const char*, int, u8,
                         void(*)(void*,void*),void*);
void sqlite4VdbeMemSetInt64(Mem*, i64);
#ifdef SQLITE4_OMIT_FLOATING_POINT
# define sqlite4VdbeMemSetDouble sqlite4VdbeMemSetInt64
#else
  void sqlite4VdbeMemSetDouble(Mem*, double);
#endif
void sqlite4VdbeMemSetNull(Mem*);
int sqlite4VdbeMemMakeWriteable(Mem*);
int sqlite4VdbeMemStringify(Mem*, int);
i64 sqlite4VdbeIntValue(Mem*);
int sqlite4VdbeMemIntegerify(Mem*);
double sqlite4VdbeRealValue(Mem*);
void sqlite4VdbeIntegerAffinity(Mem*);
int sqlite4VdbeMemRealify(Mem*);
int sqlite4VdbeMemNumerify(Mem*);
void sqlite4VdbeMemSetRowSet(Mem *pMem);

void sqlite4VdbeMemRelease(Mem *p);
void sqlite4VdbeMemReleaseExternal(Mem *p);
#define VdbeMemRelease(X)  \
  if((X)->flags&(MEM_Agg|MEM_Dyn|MEM_RowSet|MEM_Frame)) \
    sqlite4VdbeMemReleaseExternal(X);
int sqlite4VdbeMemFinalize(Mem*, FuncDef*);
const char *sqlite4OpcodeName(int);
int sqlite4VdbeMemGrow(Mem *pMem, int n, int preserve);
int sqlite4VdbeCloseStatement(Vdbe *, int);
void sqlite4VdbeFrameDelete(VdbeFrame*);
int sqlite4VdbeFrameRestore(VdbeFrame *);
void sqlite4VdbeMemStoreType(Mem *pMem);
int sqlite4VdbeTransferError(Vdbe *p);
int sqlite4VdbeSeekEnd(VdbeCursor*, int);
int sqlite4VdbeNext(VdbeCursor*);
int sqlite4VdbePrevious(VdbeCursor*);

int sqlite4VdbeRollback(sqlite4 *db, int iLevel);
int sqlite4VdbeCommit(sqlite4 *db, int iLevel);

#ifdef SQLITE4_DEBUG
void sqlite4VdbeMemAboutToChange(Vdbe*,Mem*);
#endif

#ifndef SQLITE4_OMIT_FOREIGN_KEY
int sqlite4VdbeCheckFk(Vdbe *, int);
#else
# define sqlite4VdbeCheckFk(p,i) 0
#endif

int sqlite4VdbeMemTranslate(Mem*, u8);
#ifdef SQLITE4_DEBUG
  void sqlite4VdbePrintSql(Vdbe*);
  void sqlite4VdbeMemPrettyPrint(Mem *pMem, char *zBuf);
#endif
int sqlite4VdbeMemHandleBom(Mem *pMem);


#endif /* !defined(_VDBEINT_H_) */
