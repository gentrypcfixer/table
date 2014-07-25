#for 32 bit on c1d
#make INST_SET=i386 EXTRA_CXXFLAGS=-m32 EXTRA_LDFLAGS=-m32

INST_SET = x86_64
ABI_$(OS) = linux
ABI_Windows_NT = win
TRIPLET_DIR = /$(INST_SET)-$(ABI_$(OS))-gnu
INCLUDE_DIR = $(HOME)/include$(TRIPLET_DIR)
LIB_DIR = $(HOME)/lib$(TRIPLET_DIR)

TABLE_MAJOR = 1
TABLE_MINOR = 0

CXX_$(OS) = /usr/bin/g++
CXX_Windows_NT = /usr/bin/x86_64-w64-mingw32-g++.exe
CXX = $(CXX_$(OS))

# used this command to configure pcre for mingw in cygwin
#
# CC=/usr/bin/x86_64-w64-mingw32-gcc.exe ./configure --disable-cpp --disable-shared --enable-newline-is-anycrlf --enable-utf8 --enable-unicode-properties

CXXFLAGS_Windows_NT = -DPCRE_STATIC
CXXFLAGS = $(CXXFLAGS_$(OS)) -std=c++11 -Wall -I$(INCLUDE_DIR) $(EXTRA_CXXFLAGS)

LDFLAGS_Windows_NT = -static
LDFLAGS = -L$(LIB_DIR) -lpcre -lpthread $(LDFLAGS_$(OS)) $(EXTRA_LDFLAGS)

AR = ar
ARFLAGS = rcs

PROGS = table_stack table_test table_reg_test

.PHONY : all release debug clean install

all : release debug
release : libtable.a $(PROGS)
debug : libtable_debug.a $(addsuffix _debug,$(PROGS))

clean :
	rm -f *.o lib*.a *.exe $(PROGS) $(addsuffix _debug,$(PROGS))

install :
	mkdir -p $(LIB_DIR)
	cp libtable.a $(LIB_DIR)/libtable.a.$(TABLE_MAJOR).$(TABLE_MINOR)
	cd $(LIB_DIR) && ln --symbolic --force libtable.a.$(TABLE_MAJOR).$(TABLE_MINOR) libtable.a.$(TABLE_MAJOR)
	cd $(LIB_DIR) && ln --symbolic --force libtable.a.$(TABLE_MAJOR) libtable.a
	mkdir -p $(INCLUDE_DIR)
	sed -e 's/@TABLE_MAJOR@/$(TABLE_MAJOR)/' -e 's/@TABLE_MINOR@/$(TABLE_MINOR)/' < table.h > $(INCLUDE_DIR)/table.h.$(TABLE_MAJOR).$(TABLE_MINOR)
	cd $(INCLUDE_DIR) && ln --symbolic --force table.h.$(TABLE_MAJOR).$(TABLE_MINOR) table.h.$(TABLE_MAJOR)
	cd $(INCLUDE_DIR) && ln --symbolic --force table.h.$(TABLE_MAJOR) table.h

% : %.o
	$(CXX) $+ $(LDFLAGS) -o $@
	chmod 755 $@

lib%.a : %.o
	$(AR) $(ARFLAGS) $@ $+

%.o : %.cpp
	$(CXX) -c $(CXXFLAGS) -O2 $< -o $@

%_debug.o : %.cpp
	$(CXX) -c $(CXXFLAGS) -g $< -o $@


table.o : table.h
table_debug.o : table.h
numeric.o : table.h
numeric_debug.o : table.h
table_stack.o : table.h
table_stack_debug.o : table.h
table_split.o : table.h
table_split_debug.o : table.h
table_test.o : table.h
table_test_debug.o : table.h
table_reg_test.o : table.h
table_reg_test_debug.o : table.h

libtable.a : numeric.o
libtable_debug.a : numeric_debug.o

table_stack : libtable.a
table_stack_debug : libtable_debug.a
table_split : libtable.a
table_split_debug : libtable_debug.a
table_test : libtable.a
table_test_debug : libtable_debug.a
table_reg_test : libtable.a
table_reg_test_debug : libtable_debug.a

