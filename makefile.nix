CXX = /usr/bin/g++

CXXFLAGS = -Wall -O2

LDFLAGS = -lpcre -lpthread

.PHONY : all clean

all : libtable.a libtable.so table_stack table_col_adder table_test table_reg_test

clean :
	rm -f *.o libtable.a libtable.so* table_stack table_col_adder table_test table_reg_test

% : %.o
	$(CXX) $+ $(LDFLAGS) -o $@

%.o : %.cpp
	$(CXX) -c $(CXXFLAGS) $< -o $@

%_fPIC.o : %.cpp
	$(CXX) -c $(CXXFLAGS) -fPIC $< -o $@


#objects
table.o : table.h
table_fPIC.o : table.h
numeric.o : table.h
numeric_fPIC.o : table.h
table_stack.o : table.h
table_split.o : table.h
table_col_adder.o : table.h
table_test.o : table.h
table_reg_test.o : table.h


#libraries
libtable.a : table.o numeric.o
	ar rcs $@ $+

libtable.so.$(TABLE_MAJOR_VER).$(TABLE_MINOR_VER) : table_fPIC.o numeric_fPIC.o
	$(CXX) -shared -Wl,-soname,libtable.so.$(TABLE_MAJOR_VER) $+ -o $@

libtable.so.$(TABLE_MAJOR_VER) : libtable.so.$(TABLE_MAJOR_VER).$(TABLE_MINOR_VER)
	ln -fs $< $@

libtable.so : libtable.so.$(TABLE_MAJOR_VER)
	ln -fs $< $@


#binaries
table_stack : libtable.a
table_split : libtable.a
table_col_adder : libtable.a
table_test : libtable.a
table_reg_test : libtable.a

