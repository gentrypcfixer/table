TABLE_MAJOR = 1
TABLE_MINOR = 0

PCRE_DIR = $(HOME)/source/pcre-8.33

LIB_TYPE = SHARED

CXX = /usr/bin/g++
CXXFLAGS_STATIC = -I$(PCRE_DIR)
CXXFLAGS = -Wall $(CXXFLAGS_$(LIB_TYPE))

LDFLAGS_SHARED = -lpcre -lpthread
LDFLAGS_STATIC = -static -L$(PCRE_DIR)/.libs -lpcre -lpthread
LDFLAGS = $(LDFLAGS_$(LIB_TYPE))

AR = ar
ARFLAGS = rcs

.PHONY : all clean install

all : libtable.a libtable_debug.a table_stack table_stack_debug table_test table_test_debug table_reg_test table_reg_test_debug

clean :
	@rm -f *.o lib*.a *.exe table_stack table_stack_debug table_test table_test_debug table_reg_test table_reg_test_debug

install :
	mkdir -p $(HOME)/lib
	cp libtable.a $(HOME)/lib/libtable-$(TABLE_MAJOR).$(TABLE_MINOR).a
	ln --symbolic --force $(HOME)/lib/libtable-$(TABLE_MAJOR).$(TABLE_MINOR).a $(HOME)/lib/libtable-$(TABLE_MAJOR).a
	ln --symbolic --force $(HOME)/lib/libtable-$(TABLE_MAJOR).a $(HOME)/lib/libtable.a
	mkdir -p $(HOME)/include
	sed -e 's/@TABLE_MAJOR@/$(TABLE_MAJOR)/' -e 's/@TABLE_MINOR@/$(TABLE_MINOR)/' < table.h > $(HOME)/include/table-$(TABLE_MAJOR).$(TABLE_MINOR).h
	ln --symbolic --force $(HOME)/include/table-$(TABLE_MAJOR).$(TABLE_MINOR).h $(HOME)/include/table-$(TABLE_MAJOR).h
	ln --symbolic --force $(HOME)/include/table-$(TABLE_MAJOR).h $(HOME)/include/table.h

% : %.o
	$(CXX) $+ $(LDFLAGS) -o $@
	chmod 755 $@

lib%.a : %.o
	$(AR) $(ARFLAGS) $@ $+

%.o : %.cpp
	$(CXX) -c $(CXXFLAGS) -O2 $< -o $@

%_debug.o : %.cpp
	$(CXX) -c $(CXXFLAGS) -g $< -o $@


table.o : table.h numeric_imp.h
table_debug.o : table.h numeric_imp.h
numeric.o : table.h numeric_imp.h
numeric_debug.o : table.h numeric_imp.h
table_stack.o : table.h numeric_imp.h
table_stack_debug.o : table.h numeric_imp.h
table_split.o : table.h numeric_imp.h
table_split_debug.o : table.h numeric_imp.h
table_test.o : table.h numeric_imp.h
table_test_debug.o : table.h numeric_imp.h
table_reg_test.o : table.h numeric_imp.h
table_reg_test_debug.o : table.h numeric_imp.h

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

