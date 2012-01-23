#!/bin/sh
#
# This script is used to compile SQLite into a shared library on Linux.
#
# Two separate shared libraries are generated.  "sqlite4.so" is the core
# library.  "tclsqlite4.so" contains the TCL bindings and is the
# library that is loaded into TCL in order to run SQLite.
#
make target_source
cd tsrc
rm shell.c
TCLDIR=/home/drh/tcltk/846/linux/846linux
TCLSTUBLIB=$TCLDIR/libtclstub8.4g.a
OPTS='-DUSE_TCL_STUBS=1 -DNDEBUG=1 -DHAVE_DLOPEN=1'
OPTS="$OPTS -DSQLITE_THREADSAFE=1"
OPTS="$OPTS -DSQLITE_ENABLE_FTS3=1"
OPTS="$OPTS -DSQLITE_ENABLE_COLUMN_METADATA=1"
for i in *.c; do
  if test $i != 'keywordhash.c'; then
    CMD="cc -fPIC $OPTS -O2 -I. -I$TCLDIR -c $i"
    echo $CMD
    $CMD
  fi
done
echo gcc -shared *.o $TCLSTUBLIB -o tclsqlite4.so
gcc -shared *.o $TCLSTUBLIB -o tclsqlite4.so
strip tclsqlite4.so
rm tclsqlite.c tclsqlite.o
echo gcc -shared *.o -o sqlite4.so
gcc -shared *.o -o sqlite4.so
strip sqlite4.so
cd ..
