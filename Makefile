CPP = /usr/bin/g++
CPPFLAGS = -Wall
CPPFLAGS += -g
CPPFLAGS += -O2
LDFLAGS = -lpcrecpp

AR = ar
ARFLAGS = rcs

.PHONY : all clean

all : table_cat_date table_cat_die table_stack table_test libtable.a

clean :
	@rm -f *.o table_cat_date table_cat_die table_stack table_split table_test libtable.a

% : %.o
	$(CPP) $+ $(LDFLAGS) -o $@
	chmod 755 $@

lib%.a : %.o
	$(AR) $(ARFLAGS) $@ $+

%.o : %.cpp
	$(CPP) -c $(CPPFLAGS) $< -o $@


table.o : table.h
table_cat_date.o : table.h
table_cat_die.o : table.h
table_stack.o : table.h
table_split.o : table.h
table_test.o : table.h

table_cat_date : table.o
table_cat_die : table.o
table_stack : table.o
table_split : table.o
table_test : table.o

libtable.a :

