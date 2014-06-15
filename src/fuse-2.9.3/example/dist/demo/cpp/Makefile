#
# WOS demo code
#
CC = gcc
CPPFLAGS = -g -O2 -I../../include -I../../../include -D_FILE_OFFSET_BITS=64 -DDEBUG
LDFLAGS = -L../../lib64 -L../../../../lib/.libs
LIBS = -lwos_cpp

#fusewos_CPPFLAGS = -I../../../include
#fusewos_LDFLAGS = 

all:	wos_b_demo wos_nb_demo fusewos

#
# wos_b_demo: a demo of the WOS blocking C++ api
wos_b_demo:	wos_b_demo.o
	${LINK.C} -o $@ $< ${LIBS}

#
# wos_nb_demo: a demo of the non-blocking WOS C++ api
wos_nb_demo:	wos_nb_demo.o
	${LINK.C} -o $@ $< ${LIBS}

fusewos:	fusewos.o
	${LINK.C} -o $@ $< ${LIBS} -lfuse

