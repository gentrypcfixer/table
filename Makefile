CPP = /usr/bin/g++
CPPFLAGS = -Wall
CPPFLAGS += -g
CPPFLAGS += -O2
LDFLAGS = -lpcrecpp

AR = ar
ARFLAGS = rcs

.PHONY : all clean

all : csv_cat_date csv_cat_die csv_stack csv_test libcsv.a

clean :
	@rm -f *.o csv_cat_date csv_cat_die csv_stack csv_split csv_test libcsv.a

% : %.o
	$(CPP) $+ $(LDFLAGS) -o $@
	chmod 755 $@

lib%.a : %.o
	$(AR) $(ARFLAGS) $@ $+

%.o : %.cpp
	$(CPP) -c $(CPPFLAGS) $< -o $@


csv.o : csv.h
csv_cat_date.o : csv.h
csv_cat_die.o : csv.h
csv_stack.o : csv.h
csv_split.o : csv.h
csv_test.o : csv.h

csv_cat_date : csv.o
csv_cat_die : csv.o
csv_stack : csv.o
csv_split : csv.o
csv_test : csv.o

libcsv.a :

