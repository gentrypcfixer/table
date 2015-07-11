#
# By: egentry
# Tested on Centos 5 and mingw64 under Cygwin
#
# use "make CXXFLAGS=-g" for debugging
#
# used this command to configure pcre for mingw in cygwin
# CC=/usr/bin/x86_64-w64-mingw32-gcc.exe ./configure --disable-cpp --disable-shared --enable-newline-is-anycrlf --enable-utf8 --enable-unicode-properties
#

TABLE_MAJOR_VER = $(shell sed -n -e '/TABLE_MAJOR_VER/{s/.*\([0-9]\+\).*/\1/p;q}' table.h)
TABLE_MINOR_VER = $(shell sed -n -e '/TABLE_MINOR_VER/{s/.*\([0-9]\+\).*/\1/p;q}' table.h)

ifeq ($(OS),Windows_NT)
include makefile.cygwin
else
include makefile.nix
endif

