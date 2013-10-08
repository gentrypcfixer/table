CPP = /usr/bin/g++
CPPFLAGS = -Wall
CPPFLAGS += -g
CPPFLAGS += -O2
LDFLAGS = -lpcre

AR = ar
ARFLAGS = rcs

.PHONY : all clean

all : table_stack table_test libtable.a

clean :
	@rm -f *.o table_stack table_split table_test libtable.a

% : %.o
	$(CPP) $+ $(LDFLAGS) -o $@
	chmod 755 $@

lib%.a : %.o
	$(AR) $(ARFLAGS) $@ $+

%.o : %.cpp
	$(CPP) -c $(CPPFLAGS) $< -o $@


table.o : table.h
stats.o : table.h
table_stack.o : table.h
table_split.o : table.h
table_test.o : table.h

libtable.a : stats.o

table_stack : libtable.a
table_split : libtable.a
table_test : libtable.a

