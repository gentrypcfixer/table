PCRE_DIR = $(HOME)/source/pcre-8.33

LIB_TYPE = SHARED

CPP = /usr/bin/g++
CPPFLAGS_STATIC = -I$(PCRE_DIR)
CPPFLAGS = $(CPPFLAGS_$(LIB_TYPE))
CPPFLAGS += -Wall
#CPPFLAGS += -g
CPPFLAGS += -O2

LDFLAGS_SHARED = -lpcre
LDFLAGS_STATIC = -static -L$(PCRE_DIR)/.libs -lpcre -lpthread
LDFLAGS = $(LDFLAGS_$(LIB_TYPE))

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


table.o : table.h numeric_imp.h
numeric.o : table.h numeric_imp.h
table_stack.o : table.h numeric_imp.h
table_split.o : table.h numeric_imp.h
table_test.o : table.h numeric_imp.h

libtable.a : numeric.o

table_stack : libtable.a
table_split : libtable.a
table_test : libtable.a

