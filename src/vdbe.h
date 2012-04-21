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
** Header file for the Virtual DataBase Engine (VDBE)
**
** This header defines the interface to the virtual database engine
** or VDBE.  The VDBE implements an abstract machine that runs a
** simple program to access and modify the underlying database.
*/
#ifndef _SQLITE_VDBE_H_
#define _SQLITE_VDBE_H_
#include <stdio.h>

/*
** A single VDBE is an opaque structure named "Vdbe".  Only routines
** in the source file sqliteVdbe.c are allowed to see the insides
** of this structure.
*/
typedef struct Vdbe Vdbe;

/*
** The names of the following types declared in vdbeInt.h are required
** for the VdbeOp definition.
*/
typedef struct VdbeFunc VdbeFunc;
typedef struct Mem Mem;
typedef struct SubProgram SubProgram;
typedef struct VdbeCursor VdbeCursor;

/*
** A single instruction of the virtual machine has an opcode
** and as many as three operands.  The instruction is recorded
** as an instance of the following structure:
*/
struct VdbeOp {
  u8 opcode;          /* What operation to perform */
  signed char p4type; /* One of the P4_xxx constants for p4 */
  u8 opflags;         /* Mask of the OPFLG_* flags in opcodes.h */
  u8 p5;              /* Fifth parameter is an unsigned character */
  int p1;             /* First operand */
  int p2;             /* Second parameter (often the jump destination) */
  int p3;             /* The third parameter */
  union {             /* fourth parameter */
    int i;                 /* Integer value if p4type==P4_INT32 */
    void *p;               /* Generic pointer */
    char *z;               /* Pointer to data for string (char array) types */
    i64 *pI64;             /* Used when p4type is P4_INT64 */
    double *pReal;         /* Used when p4type is P4_REAL */
    FuncDef *pFunc;        /* Used when p4type is P4_FUNCDEF */
    VdbeFunc *pVdbeFunc;   /* Used when p4type is P4_VDBEFUNC */
    CollSeq *pColl;        /* Used when p4type is P4_COLLSEQ */
    Mem *pMem;             /* Used when p4type is P4_MEM */
    VTable *pVtab;         /* Used when p4type is P4_VTAB */
    KeyInfo *pKeyInfo;     /* Used when p4type is P4_KEYINFO */
    int *ai;               /* Used when p4type is P4_INTARRAY */
    SubProgram *pProgram;  /* Used when p4type is P4_SUBPROGRAM */
    int (*xAdvance)(VdbeCursor*);
  } p4;
#ifdef SQLITE_DEBUG
  char *zComment;          /* Comment to improve readability */
#endif
#ifdef VDBE_PROFILE
  int cnt;                 /* Number of times this instruction was executed */
  u64 cycles;              /* Total time spent executing this instruction */
#endif
};
typedef struct VdbeOp VdbeOp;


/*
** A sub-routine used to implement a trigger program.
*/
struct SubProgram {
  VdbeOp *aOp;                  /* Array of opcodes for sub-program */
  int nOp;                      /* Elements in aOp[] */
  int nMem;                     /* Number of memory cells required */
  int nCsr;                     /* Number of cursors required */
  int nOnce;                    /* Number of OP_Once instructions */
  void *token;                  /* id that may be used to recursive triggers */
  SubProgram *pNext;            /* Next sub-program already visited */
};

/*
** A smaller version of VdbeOp used for the VdbeAddOpList() function because
** it takes up less space.
*/
struct VdbeOpList {
  u8 opcode;          /* What operation to perform */
  signed char p1;     /* First operand */
  signed char p2;     /* Second parameter (often the jump destination) */
  signed char p3;     /* Third parameter */
};
typedef struct VdbeOpList VdbeOpList;

/*
** Allowed values of VdbeOp.p4type
*/
#define P4_NOTUSED    0   /* The P4 parameter is not used */
#define P4_DYNAMIC  (-1)  /* Pointer to a string obtained from sqliteMalloc() */
#define P4_STATIC   (-2)  /* Pointer to a static string */
#define P4_COLLSEQ  (-4)  /* P4 is a pointer to a CollSeq structure */
#define P4_FUNCDEF  (-5)  /* P4 is a pointer to a FuncDef structure */
#define P4_KEYINFO  (-6)  /* P4 is a pointer to a KeyInfo structure */
#define P4_VDBEFUNC (-7)  /* P4 is a pointer to a VdbeFunc structure */
#define P4_MEM      (-8)  /* P4 is a pointer to a Mem*    structure */
#define P4_TRANSIENT  0   /* P4 is a pointer to a transient string */
#define P4_VTAB     (-10) /* P4 is a pointer to an sqlite4_vtab structure */
#define P4_MPRINTF  (-11) /* P4 is a string obtained from sqlite4_mprintf() */
#define P4_REAL     (-12) /* P4 is a 64-bit floating point value */
#define P4_INT64    (-13) /* P4 is a 64-bit signed integer */
#define P4_INT32    (-14) /* P4 is a 32-bit signed integer */
#define P4_INTARRAY (-15) /* P4 is a vector of 32-bit integers */
#define P4_SUBPROGRAM  (-18) /* P4 is a pointer to a SubProgram structure */
#define P4_ADVANCE  (-19) /* P4 is a pointer to BtreeNext() or BtreePrev() */

/* When adding a P4 argument using P4_KEYINFO, a copy of the KeyInfo structure
** is made.  That copy is freed when the Vdbe is finalized.  But if the
** argument is P4_KEYINFO_HANDOFF, the passed in pointer is used.  It still
** gets freed when the Vdbe is finalized so it still should be obtained
** from a single sqliteMalloc().  But no copy is made and the calling
** function should *not* try to free the KeyInfo.
*/
#define P4_KEYINFO_HANDOFF (-16)
#define P4_KEYINFO_STATIC  (-17)

/*
** The Vdbe.aColName array contains 5n Mem structures, where n is the 
** number of columns of data returned by the statement.
*/
#define COLNAME_NAME     0
#define COLNAME_DECLTYPE 1
#define COLNAME_DATABASE 2
#define COLNAME_TABLE    3
#define COLNAME_COLUMN   4
#ifdef SQLITE_ENABLE_COLUMN_METADATA
# define COLNAME_N        5      /* Number of COLNAME_xxx symbols */
#else
# ifdef SQLITE_OMIT_DECLTYPE
#   define COLNAME_N      1      /* Store only the name */
# else
#   define COLNAME_N      2      /* Store the name and decltype */
# endif
#endif

/*
** The following macro converts a relative address in the p2 field
** of a VdbeOp structure into a negative number so that 
** sqlite4VdbeAddOpList() knows that the address is relative.  Calling
** the macro again restores the address.
*/
#define ADDR(X)  (-1-(X))

/*
** The makefile scans the vdbe.c source file and creates the "opcodes.h"
** header file that defines a number for each opcode used by the VDBE.
*/
#include "opcodes.h"

/*
** Prototypes for the VDBE interface.  See comments on the implementation
** for a description of what each of these routines does.
*/
Vdbe *sqlite4VdbeCreate(sqlite4*);
int sqlite4VdbeAddOp0(Vdbe*,int);
int sqlite4VdbeAddOp1(Vdbe*,int,int);
int sqlite4VdbeAddOp2(Vdbe*,int,int,int);
int sqlite4VdbeAddOp3(Vdbe*,int,int,int,int);
int sqlite4VdbeAddOp4(Vdbe*,int,int,int,int,const char *zP4,int);
int sqlite4VdbeAddOp4Int(Vdbe*,int,int,int,int,int);
int sqlite4VdbeAddOpList(Vdbe*, int nOp, VdbeOpList const *aOp);
void sqlite4VdbeAddParseSchemaOp(Vdbe*,int,char*);
void sqlite4VdbeChangeP1(Vdbe*, u32 addr, int P1);
void sqlite4VdbeChangeP2(Vdbe*, u32 addr, int P2);
void sqlite4VdbeChangeP3(Vdbe*, u32 addr, int P3);
void sqlite4VdbeChangeP5(Vdbe*, u8 P5);
void sqlite4VdbeJumpHere(Vdbe*, int addr);
void sqlite4VdbeChangeToNoop(Vdbe*, int addr);
void sqlite4VdbeChangeP4(Vdbe*, int addr, const char *zP4, int N);
void sqlite4VdbeUsesStorage(Vdbe*, int);
VdbeOp *sqlite4VdbeGetOp(Vdbe*, int);
int sqlite4VdbeMakeLabel(Vdbe*);
void sqlite4VdbeRunOnlyOnce(Vdbe*);
void sqlite4VdbeDelete(Vdbe*);
void sqlite4VdbeDeleteObject(sqlite4*,Vdbe*);
void sqlite4VdbeMakeReady(Vdbe*,Parse*);
int sqlite4VdbeFinalize(Vdbe*);
void sqlite4VdbeResolveLabel(Vdbe*, int);
int sqlite4VdbeCurrentAddr(Vdbe*);
#ifdef SQLITE_DEBUG
  int sqlite4VdbeAssertMayAbort(Vdbe *, int);
  void sqlite4VdbeTrace(Vdbe*,FILE*);
#endif
void sqlite4VdbeResetStepResult(Vdbe*);
void sqlite4VdbeRewind(Vdbe*);
int sqlite4VdbeReset(Vdbe*);
void sqlite4VdbeSetNumCols(Vdbe*,int);
int sqlite4VdbeSetColName(Vdbe*, int, int, const char *, void(*)(void*));
void sqlite4VdbeCountChanges(Vdbe*);
sqlite4 *sqlite4VdbeDb(Vdbe*);
void sqlite4VdbeSetSql(Vdbe*, const char *z, int n);
void sqlite4VdbeSwap(Vdbe*,Vdbe*);
VdbeOp *sqlite4VdbeTakeOpArray(Vdbe*, int*, int*);
sqlite4_value *sqlite4VdbeGetValue(Vdbe*, int, u8);
void sqlite4VdbeSetVarmask(Vdbe*, int);
#ifndef SQLITE_OMIT_TRACE
  char *sqlite4VdbeExpandSql(Vdbe*, const char*);
#endif

void sqlite4VdbeRecordUnpack(KeyInfo*,int,const void*,UnpackedRecord*);
int sqlite4VdbeRecordCompare(int,const void*,UnpackedRecord*);
UnpackedRecord *sqlite4VdbeAllocUnpackedRecord(KeyInfo *, char *, int, char **);

#ifndef SQLITE_OMIT_TRIGGER
void sqlite4VdbeLinkSubProgram(Vdbe *, SubProgram *);
#endif


#ifndef NDEBUG
  void sqlite4VdbeComment(Vdbe*, const char*, ...);
# define VdbeComment(X)  sqlite4VdbeComment X
  void sqlite4VdbeNoopComment(Vdbe*, const char*, ...);
# define VdbeNoopComment(X)  sqlite4VdbeNoopComment X
#else
# define VdbeComment(X)
# define VdbeNoopComment(X)
#endif

#endif
