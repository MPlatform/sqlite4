#!/bin/sh
#
# This script is used to compile SQLite into a DLL.
#
# Two separate DLLs are generated.  "sqlite4.dll" is the core
# library.  "tclsqlite4.dll" contains the TCL bindings and is the
# library that is loaded into TCL in order to run SQLite.
#
make sqlite4.c
PATH=$PATH:/opt/mingw/bin
TCLDIR=/home/drh/tcltk/846/win/846win
TCLSTUBLIB=$TCLDIR/libtcl84stub.a
OPTS='-DUSE_TCL_STUBS=1 -DBUILD_sqlite=1 -DSQLITE_OS_WIN=1'
OPTS="$OPTS -DSQLITE_THREADSAFE=1"
OPTS="$OPTS -DSQLITE_ENABLE_FTS3=1"
OPTS="$OPTS -DSQLITE_ENABLE_RTREE=1"
OPTS="$OPTS -DSQLITE_ENABLE_COLUMN_METADATA=1"
CC="i386-mingw32msvc-gcc -Os $OPTS -Itsrc -I$TCLDIR"
NM="i386-mingw32msvc-nm"
CMD="$CC -c sqlite4.c"
echo $CMD
$CMD
CMD="$CC -c tclsqlite4.c"
echo $CMD
$CMD
echo 'EXPORTS' >tclsqlite4.def
$NM tclsqlite4.o | grep ' T ' >temp1
grep '_Init$' temp1 >temp2
grep '_SafeInit$' temp1 >>temp2
grep ' T _sqlite4_' temp1 >>temp2
echo 'EXPORTS' >tclsqlite4.def
sed 's/^.* T _//' temp2 | sort | uniq >>tclsqlite4.def
i386-mingw32msvc-dllwrap \
     --def tclsqlite4.def -v --export-all \
     --driver-name i386-mingw32msvc-gcc \
     --dlltool-name i386-mingw32msvc-dlltool \
     --as i386-mingw32msvc-as \
     --target i386-mingw32 \
     -dllname tclsqlite4.dll -lmsvcrt tclsqlite4.o $TCLSTUBLIB
$NM sqlite4.o | grep ' T ' >temp1
echo 'EXPORTS' >sqlite4.def
grep ' _sqlite4_' temp1 | sed 's/^.* _//' >>sqlite4.def
i386-mingw32msvc-dllwrap \
     --def sqlite4.def -v --export-all \
     --driver-name i386-mingw32msvc-gcc \
     --dlltool-name i386-mingw32msvc-dlltool \
     --as i386-mingw32msvc-as \
     --target i386-mingw32 \
     -dllname sqlite4.dll -lmsvcrt sqlite4.o
