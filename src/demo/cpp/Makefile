#
# WOS demo code
#
CC = gcc
CPPFLAGS = -g -O2 -I../../include -I../../fuse/include -D_FILE_OFFSET_BITS=64 -DDEBUG -std=c++0x
LDFLAGS = -L../../lib64 -L../../fuse/lib/.libs
LIBS = -lwos_cpp

PROGS = wos_b_demo wos_nb_demo fusewos
all:	$(PROGS)

#
# wos_b_demo: a demo of the WOS blocking C++ api
wos_b_demo:	wos_b_demo.o
	${LINK.C} -o $@ $< ${LIBS}

#
# wos_nb_demo: a demo of the non-blocking WOS C++ api
wos_nb_demo:	wos_nb_demo.o
	${LINK.C} -o $@ $< ${LIBS}

#
# fusewos: a fuse based file system layer for WOS
fusewos:	fusewos.o
	${LINK.C} -o $@ $< ${LIBS} -lfuse

.PHONY: clean
clean: 
	rm -f *.o $(PROGS)

install:
	cp -f fusewos /usr/local/bin

