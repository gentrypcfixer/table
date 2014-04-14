TABLE_MAJOR = 1
TABLE_MINOR = 0

CXX_$(OS) = /usr/bin/g++
CXX_Windows_NT = /usr/bin/x86_64-w64-mingw32-g++.exe
CXX = $(CXX_$(OS))

# used this command to configure pcre for mingw in cygwin
#
# CC=/usr/bin/x86_64-w64-mingw32-gcc.exe ./configure --disable-cpp --disable-shared --enable-newline-is-anycrlf --enable-utf8 --enable-unicode-properties

CXXFLAGS_Windows_NT = -DPCRE_STATIC
CXXFLAGS = $(CXXFLAGS_$(OS)) -Wall -I$(HOME)/include

LDFLAGS = -static -L$(HOME)/lib -lpcre -lpthread

AR = ar
ARFLAGS = rcs

.PHONY : all clean install

all : libtable.a libtable_debug.a table_stack table_stack_debug table_test table_test_debug table_reg_test table_reg_test_debug

clean :
	@rm -f *.o lib*.a *.exe table_stack table_stack_debug table_test table_test_debug table_reg_test table_reg_test_debug

install :
	mkdir -p $(HOME)/lib
	cp libtable.a $(HOME)/lib/libtable.a.$(TABLE_MAJOR).$(TABLE_MINOR)
	cd $(HOME)/lib && ln --symbolic --force libtable.a.$(TABLE_MAJOR).$(TABLE_MINOR) libtable.a.$(TABLE_MAJOR)
	cd $(HOME)/lib && ln --symbolic --force libtable.a.$(TABLE_MAJOR) libtable.a
	mkdir -p $(HOME)/include
	sed -e 's/@TABLE_MAJOR@/$(TABLE_MAJOR)/' -e 's/@TABLE_MINOR@/$(TABLE_MINOR)/' < table.h > $(HOME)/include/table.h.$(TABLE_MAJOR).$(TABLE_MINOR)
	cd $(HOME)/include && ln --symbolic --force table.h.$(TABLE_MAJOR).$(TABLE_MINOR) table.h.$(TABLE_MAJOR)
	cd $(HOME)/include && ln --symbolic --force table.h.$(TABLE_MAJOR) table.h

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

