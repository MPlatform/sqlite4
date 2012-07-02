###############################################################################
# The following macros should be defined before this script is
# invoked:
#
# TOP              The toplevel directory of the source tree.  This is the
#                  directory that contains files "VERSION" and "README".
#
# BCC              C Compiler and options for use in building executables that
#                  will run on the platform that is doing the build.
#
# TCC              C Compiler and options for use in building executables that 
#                  will run on the target platform.  This is usually the same
#                  as BCC, unless you are cross-compiling.
#
# THREADLIB        Specify any extra linker options needed to make the library
#                  thread safe
#
# OPTS             Extra compiler command-line options for the TCC compiler
#
# EXE              The suffix to add to executable files.  ".exe" for windows
#                  and "" for Unix.
#
# AR               Tools used to build a static library.
# RANLIB
#
# TCL_FLAGS        Extra compiler options needed for programs that use the
#                  TCL library.
#
# LIBTCL           Linker options needed to link against the TCL library.
#
# READLINE_FLAGS   Compiler options needed for programs that use the
#                  readline() library.
#
# LIBREADLINE      Linker options needed by programs using readline() must
#                  link against.
#
# NAWK             Nawk compatible awk program.  Older (obsolete?) solaris
#                  systems need this to avoid using the original AT&T AWK.
#
# Once the macros above are defined, the rest of this make script will
# build the SQLite library and testing tools.
################################################################################

# FIXME:  Required options for now.
#
OPTS += -DLSM_MUTEX_NONE
OPTS += -DSQLITE4_DEBUG=1 -DLSM_DEBUG=1
OPTS += -DHAVE_GMTIME_R
OPTS += -DHAVE_LOCALTIME_R
OPTS += -DHAVE_MALLOC_USABLE_SIZE
OPTS += -DHAVE_USLEEP
OPTS += -DSQLITE4_MEMDEBUG=1
#OPTS += -DSQLITE4_NO_SYNC=1 -DLSM_NO_SYNC=1
OPTS += -DSQLITE4_OMIT_ANALYZE
OPTS += -DSQLITE4_OMIT_AUTOMATIC_INDEX
OPTS += -DSQLITE4_OMIT_BTREECOUNT
OPTS += -DSQLITE4_OMIT_VIRTUALTABLE=1
OPTS += -DSQLITE4_OMIT_XFER_OPT
OPTS += -DSQLITE4_THREADSAFE=0

# This is how we compile
#
TCCX =  $(TCC) $(OPTS) -I. -I$(TOP)/src -I$(TOP) 
TCCX += -I$(TOP)/ext/rtree -I$(TOP)/ext/icu -I$(TOP)/ext/fts3
TCCX += -I$(TOP)/ext/async


# Object files for the SQLite library.
#
FTS3_OBJ = fts3.o fts3_aux.o fts3_expr.o fts3_hash.o fts3_icu.o fts3_porter.o \
         fts3_snippet.o fts3_tokenizer.o fts3_tokenizer1.o \
         fts3_write.o
# To remove fts3 (if it won't compile for you), unset FTS3_OBJ:
# FTS3_OBJ =

LIBOBJ+= alter.o analyze.o attach.o auth.o \
         build.o \
         callback.o complete.o ctime.o date.o delete.o expr.o fault.o fkey.o \
         $(FTS3_OBJ) \
         func.o global.o hash.o \
         icu.o insert.o kvlsm.o kvmem.o legacy.o \
         lsm_ckpt.o lsm_file.o lsm_log.o lsm_main.o lsm_mem.o lsm_mutex.o \
         lsm_shared.o lsm_str.o lsm_sorted.o lsm_tree.o \
         lsm_unix.o lsm_varint.o \
         main.o malloc.o math.o mem0.o mem1.o mem2.o mem3.o mem5.o \
         mutex.o mutex_noop.o mutex_unix.o mutex_w32.o \
         opcodes.o os.o \
         parse.o pragma.o prepare.o printf.o \
         random.o resolve.o rowset.o rtree.o select.o status.o storage.o \
         tokenize.o trigger.o \
         update.o util.o varint.o \
         vdbe.o vdbeapi.o vdbeaux.o vdbecodec.o vdbecursor.o \
         vdbemem.o vdbetrace.o \
         walker.o where.o utf.o

# All of the source code files.
#
SRC = \
  $(TOP)/src/alter.c \
  $(TOP)/src/analyze.c \
  $(TOP)/src/attach.c \
  $(TOP)/src/auth.c \
  $(TOP)/src/build.c \
  $(TOP)/src/callback.c \
  $(TOP)/src/complete.c \
  $(TOP)/src/ctime.c \
  $(TOP)/src/date.c \
  $(TOP)/src/delete.c \
  $(TOP)/src/expr.c \
  $(TOP)/src/fault.c \
  $(TOP)/src/fkey.c \
  $(TOP)/src/func.c \
  $(TOP)/src/global.c \
  $(TOP)/src/hash.c \
  $(TOP)/src/hash.h \
  $(TOP)/src/hwtime.h \
  $(TOP)/src/insert.c \
  $(TOP)/src/kvlsm.c \
  $(TOP)/src/kvmem.c \
  $(TOP)/src/legacy.c \
  $(TOP)/src/lsm.h \
  $(TOP)/src/lsmInt.h \
  $(TOP)/src/lsm_ckpt.c \
  $(TOP)/src/lsm_file.c \
  $(TOP)/src/lsm_log.c \
  $(TOP)/src/lsm_main.c \
  $(TOP)/src/lsm_mem.c \
  $(TOP)/src/lsm_mutex.c \
  $(TOP)/src/lsm_shared.c \
  $(TOP)/src/lsm_str.c \
  $(TOP)/src/lsm_sorted.c \
  $(TOP)/src/lsm_tree.c \
  $(TOP)/src/lsm_unix.c \
  $(TOP)/src/lsm_varint.c \
  $(TOP)/src/main.c \
  $(TOP)/src/malloc.c \
  $(TOP)/src/math.c \
  $(TOP)/src/mem0.c \
  $(TOP)/src/mem1.c \
  $(TOP)/src/mem2.c \
  $(TOP)/src/mem3.c \
  $(TOP)/src/mem5.c \
  $(TOP)/src/mutex.c \
  $(TOP)/src/mutex.h \
  $(TOP)/src/mutex_noop.c \
  $(TOP)/src/mutex_unix.c \
  $(TOP)/src/mutex_w32.c \
  $(TOP)/src/os.c \
  $(TOP)/src/os.h \
  $(TOP)/src/parse.y \
  $(TOP)/src/pragma.c \
  $(TOP)/src/prepare.c \
  $(TOP)/src/printf.c \
  $(TOP)/src/random.c \
  $(TOP)/src/resolve.c \
  $(TOP)/src/rowset.c \
  $(TOP)/src/select.c \
  $(TOP)/src/shell.c \
  $(TOP)/src/sqlite.h.in \
  $(TOP)/src/sqliteInt.h \
  $(TOP)/src/sqliteLimit.h \
  $(TOP)/src/status.c \
  $(TOP)/src/storage.c \
  $(TOP)/src/storage.h \
  $(TOP)/src/tclsqlite.c \
  $(TOP)/src/tokenize.c \
  $(TOP)/src/trigger.c \
  $(TOP)/src/utf.c \
  $(TOP)/src/update.c \
  $(TOP)/src/util.c \
  $(TOP)/src/varint.c \
  $(TOP)/src/vdbe.c \
  $(TOP)/src/vdbe.h \
  $(TOP)/src/vdbeapi.c \
  $(TOP)/src/vdbeaux.c \
  $(TOP)/src/vdbecodec.c \
  $(TOP)/src/vdbecursor.c \
  $(TOP)/src/vdbemem.c \
  $(TOP)/src/vdbetrace.c \
  $(TOP)/src/vdbeInt.h \
  $(TOP)/src/walker.c \
  $(TOP)/src/where.c

# Source code for extensions
#
SRC += \
  $(TOP)/ext/fts3/fts3.c \
  $(TOP)/ext/fts3/fts3.h \
  $(TOP)/ext/fts3/fts3Int.h \
  $(TOP)/ext/fts3/fts3_aux.c \
  $(TOP)/ext/fts3/fts3_expr.c \
  $(TOP)/ext/fts3/fts3_hash.c \
  $(TOP)/ext/fts3/fts3_hash.h \
  $(TOP)/ext/fts3/fts3_icu.c \
  $(TOP)/ext/fts3/fts3_porter.c \
  $(TOP)/ext/fts3/fts3_snippet.c \
  $(TOP)/ext/fts3/fts3_tokenizer.h \
  $(TOP)/ext/fts3/fts3_tokenizer.c \
  $(TOP)/ext/fts3/fts3_tokenizer1.c \
  $(TOP)/ext/fts3/fts3_write.c
SRC += \
  $(TOP)/ext/icu/sqliteicu.h \
  $(TOP)/ext/icu/icu.c
SRC += \
  $(TOP)/ext/rtree/rtree.h \
  $(TOP)/ext/rtree/rtree.c


# Generated source code files
#
SRC += \
  keywordhash.h \
  opcodes.c \
  opcodes.h \
  parse.c \
  parse.h \
  sqlite4.h


# Source code to the test files.
#
TESTSRC = \
  $(TOP)/ext/fts3/fts3_term.c \
  $(TOP)/ext/fts3/fts3_test.c \
  $(TOP)/test/test_main.c \
  $(TOP)/test/test_thread0.c \
  $(TOP)/test/test_utf.c \
  $(TOP)/test/test_misc1.c \
  $(TOP)/test/test_config.c \
  $(TOP)/test/test_func.c \
  $(TOP)/test/test_hexio.c \
  $(TOP)/test/test_lsm.c \
  $(TOP)/test/test_malloc.c \
  $(TOP)/test/test_mutex.c \
  $(TOP)/test/test_storage.c \
  $(TOP)/test/test_storage2.c \
  $(TOP)/test/test_thread.c \
  $(TOP)/test/test_wsd.c

#TESTSRC += $(TOP)/ext/fts2/fts2_tokenizer.c
#TESTSRC += $(TOP)/ext/fts3/fts3_tokenizer.c

TESTSRC2 = \
  $(TOP)/src/attach.c \
  $(TOP)/src/build.c \
  $(TOP)/src/date.c \
  $(TOP)/src/expr.c \
  $(TOP)/src/func.c \
  $(TOP)/src/insert.c \
  $(TOP)/src/mem5.c \
  $(TOP)/src/os.c \
  $(TOP)/src/pragma.c \
  $(TOP)/src/prepare.c \
  $(TOP)/src/printf.c \
  $(TOP)/src/random.c \
  $(TOP)/src/select.c \
  $(TOP)/src/tokenize.c \
  $(TOP)/src/utf.c \
  $(TOP)/src/util.c \
  $(TOP)/src/vdbeapi.c \
  $(TOP)/src/vdbeaux.c \
  $(TOP)/src/vdbe.c \
  $(TOP)/src/vdbemem.c \
  $(TOP)/src/where.c \
  parse.c \
  $(TOP)/ext/fts3/fts3.c \
  $(TOP)/ext/fts3/fts3_aux.c \
  $(TOP)/ext/fts3/fts3_expr.c \
  $(TOP)/ext/fts3/fts3_tokenizer.c \
  $(TOP)/ext/fts3/fts3_write.c

# Header files used by all library source files.
#
HDR = \
   $(TOP)/src/hash.h \
   $(TOP)/src/hwtime.h \
   keywordhash.h \
   $(TOP)/src/lsm.h \
   $(TOP)/src/lsmInt.h \
   $(TOP)/src/mutex.h \
   opcodes.h \
   $(TOP)/src/os.h \
   parse.h  \
   sqlite4.h  \
   $(TOP)/src/sqliteInt.h  \
   $(TOP)/src/sqliteLimit.h \
   $(TOP)/src/storage.h \
   $(TOP)/src/vdbe.h \
   $(TOP)/src/vdbeInt.h

EXTHDR = \
  $(TOP)/ext/fts3/fts3.h \
  $(TOP)/ext/fts3/fts3Int.h \
  $(TOP)/ext/fts3/fts3_hash.h \
  $(TOP)/ext/fts3/fts3_tokenizer.h
EXTHDR += \
  $(TOP)/ext/rtree/rtree.h
EXTHDR += \
  $(TOP)/ext/icu/sqliteicu.h

LSMTESTSRC = $(TOP)/lsm-test/lsmtest1.c $(TOP)/lsm-test/lsmtest2.c           \
             $(TOP)/lsm-test/lsmtest3.c $(TOP)/lsm-test/lsmtest4.c           \
             $(TOP)/lsm-test/lsmtest5.c $(TOP)/lsm-test/lsmtest6.c           \
             $(TOP)/lsm-test/lsmtest7.c $(TOP)/lsm-test/lsmtest_datasource.c \
             $(TOP)/lsm-test/lsmtest_func.c $(TOP)/lsm-test/lsmtest_io.c     \
             $(TOP)/lsm-test/lsmtest_main.c $(TOP)/lsm-test/lsmtest_mem.c    \
             $(TOP)/lsm-test/lsmtest_tdb.c $(TOP)/lsm-test/lsmtest_tdb3.c    \
             $(TOP)/lsm-test/lsmtest_util.c 

LSMTESTHDR = $(TOP)/lsm-test/lsmtest.h $(TOP)/lsm-test/lsmtest_tdb.h

# This is the default Makefile target.  The objects listed here
# are what get build when you type just "make" with no arguments.
#
all:	sqlite4.h libsqlite4.a sqlite4$(EXE)

libsqlite4.a:	$(LIBOBJ)
	$(AR) libsqlite4.a $(LIBOBJ)
	$(RANLIB) libsqlite4.a

sqlite4$(EXE):	$(TOP)/src/shell.c libsqlite4.a sqlite4.h
	$(TCCX) $(READLINE_FLAGS) -o sqlite4$(EXE)                  \
		$(TOP)/src/shell.c                                  \
		libsqlite4.a $(LIBREADLINE) $(TLIBS) $(THREADLIB)

sqlite4.o:	sqlite4.c
	$(TCCX) -c sqlite4.c

# This target creates a directory named "tsrc" and fills it with
# copies of all of the C source code and header files needed to
# build on the target system.  Some of the C source code and header
# files are automatically generated.  This target takes care of
# all that automatic generation.
#
target_source:	$(SRC) $(TOP)/tool/vdbe-compress.tcl
	rm -rf tsrc
	mkdir tsrc
	cp -f $(SRC) tsrc
	rm tsrc/sqlite.h.in tsrc/parse.y
	tclsh $(TOP)/tool/vdbe-compress.tcl <tsrc/vdbe.c >vdbe.new
	mv vdbe.new tsrc/vdbe.c
	touch target_source

sqlite4.c:	target_source $(TOP)/tool/mksqlite4c.tcl
	tclsh $(TOP)/tool/mksqlite4c.tcl
	echo '#ifndef USE_SYSTEM_SQLITE' >tclsqlite4.c
	cat sqlite4.c >>tclsqlite4.c
	echo '#endif /* USE_SYSTEM_SQLITE */' >>tclsqlite4.c
	cat $(TOP)/src/tclsqlite.c >>tclsqlite4.c

sqlite4.c-debug:	target_source $(TOP)/tool/mksqlite4c.tcl
	tclsh $(TOP)/tool/mksqlite4c.tcl --linemacros
	echo '#ifndef USE_SYSTEM_SQLITE' >tclsqlite4.c
	cat sqlite4.c >>tclsqlite4.c
	echo '#endif /* USE_SYSTEM_SQLITE */' >>tclsqlite4.c
	echo '#line 1 "tclsqlite.c"' >>tclsqlite4.c
	cat $(TOP)/src/tclsqlite.c >>tclsqlite4.c

sqlite4-all.c:	sqlite4.c $(TOP)/tool/split-sqlite4c.tcl
	tclsh $(TOP)/tool/split-sqlite4c.tcl

fts2amal.c:	target_source $(TOP)/ext/fts2/mkfts2amal.tcl
	tclsh $(TOP)/ext/fts2/mkfts2amal.tcl

fts3amal.c:	target_source $(TOP)/ext/fts3/mkfts3amal.tcl
	tclsh $(TOP)/ext/fts3/mkfts3amal.tcl

# Rules to build the LEMON compiler generator
#
lemon:	$(TOP)/tool/lemon.c $(TOP)/src/lempar.c
	$(BCC) -o lemon $(TOP)/tool/lemon.c
	cp $(TOP)/src/lempar.c .

# Rules to build individual *.o files from generated *.c files. This
# applies to:
#
#     parse.o
#     opcodes.o
#
%.o: %.c $(HDR)
	$(TCCX) -c $<

# Rules to build individual *.o files from files in the src directory.
#
%.o: $(TOP)/src/%.c $(HDR)
	$(TCCX) -c $<

tclsqlite.o:	$(TOP)/src/tclsqlite.c $(HDR)
	$(TCCX) $(TCL_FLAGS) -c $(TOP)/src/tclsqlite.c



# Rules to build opcodes.c and opcodes.h
#
opcodes.c:	opcodes.h $(TOP)/tool/mkopcodec.awk
	$(NAWK) -f $(TOP)/tool/mkopcodec.awk opcodes.h >opcodes.c

opcodes.h:	parse.h $(TOP)/src/vdbe.c $(TOP)/tool/mkopcodeh.awk
	cat parse.h $(TOP)/src/vdbe.c | \
		$(NAWK) -f $(TOP)/tool/mkopcodeh.awk >opcodes.h

# Rules to build parse.c and parse.h - the outputs of lemon.
#
parse.h:	parse.c

parse.c:	$(TOP)/src/parse.y lemon $(TOP)/tool/addopcodes.awk
	cp $(TOP)/src/parse.y .
	rm -f parse.h
	./lemon $(OPTS) parse.y
	mv parse.h parse.h.temp
	$(NAWK) -f $(TOP)/tool/addopcodes.awk parse.h.temp >parse.h

sqlite4.h:	$(TOP)/src/sqlite.h.in $(TOP)/manifest.uuid $(TOP)/VERSION
	tclsh $(TOP)/tool/mksqlite4h.tcl $(TOP) >sqlite4.h

keywordhash.h:	$(TOP)/tool/mkkeywordhash.c
	$(BCC) -o mkkeywordhash $(OPTS) $(TOP)/tool/mkkeywordhash.c
	./mkkeywordhash >keywordhash.h



# Rules to build the extension objects.
#
icu.o:	$(TOP)/ext/icu/icu.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE4_CORE -c $(TOP)/ext/icu/icu.c

fts2.o:	$(TOP)/ext/fts2/fts2.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE4_CORE -c $(TOP)/ext/fts2/fts2.c

fts2_hash.o:	$(TOP)/ext/fts2/fts2_hash.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE4_CORE -c $(TOP)/ext/fts2/fts2_hash.c

fts2_icu.o:	$(TOP)/ext/fts2/fts2_icu.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE4_CORE -c $(TOP)/ext/fts2/fts2_icu.c

fts2_porter.o:	$(TOP)/ext/fts2/fts2_porter.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE4_CORE -c $(TOP)/ext/fts2/fts2_porter.c

fts2_tokenizer.o:	$(TOP)/ext/fts2/fts2_tokenizer.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE4_CORE -c $(TOP)/ext/fts2/fts2_tokenizer.c

fts2_tokenizer1.o:	$(TOP)/ext/fts2/fts2_tokenizer1.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE4_CORE -c $(TOP)/ext/fts2/fts2_tokenizer1.c

fts3.o:	$(TOP)/ext/fts3/fts3.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE4_CORE -c $(TOP)/ext/fts3/fts3.c

fts3_aux.o:	$(TOP)/ext/fts3/fts3_aux.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE4_CORE -c $(TOP)/ext/fts3/fts3_aux.c

fts3_expr.o:	$(TOP)/ext/fts3/fts3_expr.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE4_CORE -c $(TOP)/ext/fts3/fts3_expr.c

fts3_hash.o:	$(TOP)/ext/fts3/fts3_hash.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE4_CORE -c $(TOP)/ext/fts3/fts3_hash.c

fts3_icu.o:	$(TOP)/ext/fts3/fts3_icu.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE4_CORE -c $(TOP)/ext/fts3/fts3_icu.c

fts3_snippet.o:	$(TOP)/ext/fts3/fts3_snippet.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE4_CORE -c $(TOP)/ext/fts3/fts3_snippet.c

fts3_porter.o:	$(TOP)/ext/fts3/fts3_porter.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE4_CORE -c $(TOP)/ext/fts3/fts3_porter.c

fts3_tokenizer.o:	$(TOP)/ext/fts3/fts3_tokenizer.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE4_CORE -c $(TOP)/ext/fts3/fts3_tokenizer.c

fts3_tokenizer1.o:	$(TOP)/ext/fts3/fts3_tokenizer1.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE4_CORE -c $(TOP)/ext/fts3/fts3_tokenizer1.c

fts3_write.o:	$(TOP)/ext/fts3/fts3_write.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE4_CORE -c $(TOP)/ext/fts3/fts3_write.c

rtree.o:	$(TOP)/ext/rtree/rtree.c $(HDR) $(EXTHDR)
	$(TCCX) -DSQLITE4_CORE -c $(TOP)/ext/rtree/rtree.c


# Rules for building test programs and for running tests
#
tclsqlite4:	$(TOP)/src/tclsqlite.c libsqlite4.a
	$(TCCX) $(TCL_FLAGS) -DTCLSH=1 -o tclsqlite4 \
		$(TOP)/src/tclsqlite.c libsqlite4.a $(LIBTCL) $(THREADLIB)

# Rules to build the 'testfixture' application.
#
TESTFIXTURE_FLAGS  = -DSQLITE4_TEST=1 -DSQLITE4_CRASH_TEST=1
TESTFIXTURE_FLAGS += -DSQLITE4_SERVER=1 -DSQLITE4_PRIVATE="" -DSQLITE4_CORE 

TESTFIXTURE_PREREQ  = $(TESTSRC) $(TESTSRC2) 
TESTFIXTURE_PREREQ += $(TOP)/src/tclsqlite.c
TESTFIXTURE_PREREQ += libsqlite4.a

testfixture$(EXE): $(TESTFIXTURE_PREREQ)
	$(TCCX) $(TCL_FLAGS) -DTCLSH=1 $(TESTFIXTURE_FLAGS)                  \
		$(TESTSRC) $(TESTSRC2) $(TOP)/src/tclsqlite.c                \
		-o testfixture$(EXE) $(LIBTCL) $(THREADLIB) libsqlite4.a

amalgamation-testfixture$(EXE): sqlite4.c $(TESTSRC) $(TOP)/src/tclsqlite.c
	$(TCCX) $(TCL_FLAGS) -DTCLSH=1 $(TESTFIXTURE_FLAGS)                  \
		$(TESTSRC) $(TOP)/src/tclsqlite.c sqlite4.c                  \
		-o testfixture$(EXE) $(LIBTCL) $(THREADLIB)

fts3-testfixture$(EXE): sqlite4.c fts3amal.c $(TESTSRC) $(TOP)/src/tclsqlite.c
	$(TCCX) $(TCL_FLAGS) -DTCLSH=1 $(TESTFIXTURE_FLAGS)                  \
	-DSQLITE4_ENABLE_FTS3=1                                               \
		$(TESTSRC) $(TOP)/src/tclsqlite.c sqlite4.c fts3amal.c       \
		-o testfixture$(EXE) $(LIBTCL) $(THREADLIB)

fulltest:	testfixture$(EXE) sqlite4$(EXE)
	./testfixture$(EXE) $(TOP)/test/all.test

soaktest:	testfixture$(EXE) sqlite4$(EXE)
	./testfixture$(EXE) $(TOP)/test/all.test -soak=1

test:	testfixture$(EXE) sqlite4$(EXE)
	./testfixture$(EXE) $(TOP)/test/src4.test

# Rules to build the 'lsmtest' application.
#
lsmtest$(EXE): libsqlite4.a $(LSMTESTSRC) $(LSMTESTHDR)
	$(TCCX) $(LSMTESTSRC) libsqlite4.a -o lsmtest$(EXE) $(THREADLIB) -lsqlite3


varint$(EXE):	$(TOP)/src/varint.c
	$(TCCX) -DVARINT_TOOL -o varint$(EXE) $(TOP)/src/varint.c

# The next two rules are used to support the "threadtest" target. Building
# threadtest runs a few thread-safety tests that are implemented in C. This
# target is invoked by the releasetest.tcl script.
# 
threadtest3$(EXE): sqlite4.o $(TOP)/test/threadtest3.c $(TOP)/test/tt3_checkpoint.c
	$(TCCX) -O2 sqlite4.o $(TOP)/test/threadtest3.c \
		-o threadtest3$(EXE) $(THREADLIB)

threadtest: threadtest3$(EXE)
	./threadtest3$(EXE)

TEST_EXTENSION = $(SHPREFIX)testloadext.$(SO)
$(TEST_EXTENSION): $(TOP)/test/test_loadext.c
	$(MKSHLIB) $(TOP)/test/test_loadext.c -o $(TEST_EXTENSION)

extensiontest: testfixture$(EXE) $(TEST_EXTENSION)
	./testfixture$(EXE) $(TOP)/test/loadext.test

# This target will fail if the SQLite amalgamation contains any exported
# symbols that do not begin with "sqlite4_". It is run as part of the
# releasetest.tcl script.
#
checksymbols: sqlite4.o
	nm -g --defined-only sqlite4.o | grep -v " sqlite4_" ; test $$? -ne 0


# Standard install and cleanup targets
#
install:	sqlite4 libsqlite4.a sqlite4.h
	mv sqlite4 /usr/bin
	mv libsqlite4.a /usr/lib
	mv sqlite4.h /usr/include

clean:	
	rm -f *.o sqlite4 sqlite4.exe libsqlite4.a sqlite4.h opcodes.*
	rm -f lemon lemon.exe lempar.c parse.* sqlite*.tar.gz
	rm -f mkkeywordhash mkkeywordhash.exe keywordhash.h
	rm -f $(PUBLISH)
	rm -f *.da *.bb *.bbg gmon.out
	rm -rf tsrc target_source
	rm -f testloadext.dll libtestloadext.so
	rm -f amalgamation-testfixture amalgamation-testfixture.exe
	rm -f fts3-testfixture fts3-testfixture.exe
	rm -f testfixture testfixture.exe
	rm -f threadtest3 threadtest3.exe
	rm -f sqlite4.c fts?amal.c tclsqlite4.c
	rm -f sqlite4_analyzer sqlite4_analyzer.exe sqlite4_analyzer.c
