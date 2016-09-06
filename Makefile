CFLAGS := -fPIC -O3 -g -Wall -Werror

CC := gcc
MAJOR := 0
MINOR := 1
NAME := alocs
VERSION := $(MAJOR).$(MINOR)

lib:	lib$(NAME).so.$(VERSION)

lib$(NAME).so.$(VERSION):	common.o lib$(NAME).o iceph.o ceph_lib.o cache.o util.o imds.o
				$(CC) -shared -Wl,-soname,lib$(NAME).so.$(MAJOR) common.o libalocs.o iceph.o ceph_lib.o cache.o util.o imds.o -o $@

imds.o:	imds.c imds.h
	$(COMPILE.c) imds.c -g -I/usr/lib/jvm/java-8-oracle/include -I/usr/lib/jvm/java-8-oracle/include/linux -L/usr/lib/jvm/java-8-oracle/jre/lib/amd64/server -ljvm

common.o:	common.c common.h
		$(COMPILE.c) -g common.c

libalocs.o:	libalocs.c libalocs.h
		$(COMPILE.c) -g libalocs.c

iceph.o:	iceph.c iceph.h
		$(COMPILE.c) -g iceph.c

ceph_lib.o:	ceph_lib.c ceph_lib.h
		$(COMPILE.c) -g ceph_lib.c

cache.o:	cache.c cache.h
		$(COMPILE.c) -g -lm cache.c

util.o:	util.c util.h
	$(COMPILE.c) -g util.c


clean:
	$(RM) *.o *.so*
