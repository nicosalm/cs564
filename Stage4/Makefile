#
# Makefile for the test programs
#

.SUFFIXES: .o .C

#
# Compiler and loader definitions
#
PROGRAMS = testfile tester

LD = ld
LDFLAGS =	

CXX = g++
CXXFLAGS = -g -Wall

PURIFY = purify -collector=/usr/ccs/bin/ld -g++

#
# general definitions
#
MAKEFILE = Makefile

#
# list of all object and source files
#
OBJS = db.o buf.o bufHash.o error.o page.o heapfile.o
SRCS = db.C buf.C bufHash.C error.C page.C heapfile.C

# Object files specific to each program
TESTFILE_OBJS = $(OBJS) testfile.o
TESTER_OBJS = $(OBJS) tester.o

all: $(PROGRAMS)

testfile: $(TESTFILE_OBJS)
	$(CXX) -o $@ $(TESTFILE_OBJS) $(LDFLAGS)

tester: $(TESTER_OBJS)
	$(CXX) -o $@ $(TESTER_OBJS) $(LDFLAGS)

testfile.pure: $(TESTFILE_OBJS)
	$(PURIFY) $(CXX) -o $@ $(TESTFILE_OBJS) $(LDFLAGS)

tester.pure: $(TESTER_OBJS)
	$(PURIFY) $(CXX) -o $@ $(TESTER_OBJS) $(LDFLAGS)

.C.o:
	$(CXX) $(CXXFLAGS) -c $<

clean:
	rm -f core *.bak *~ *.o $(PROGRAMS) *.pure .pure testpage

depend:
	makedepend -I /s/gcc/include/g++ -f$(MAKEFILE) \
	$(SRCS) testfile.C tester.C

# DO NOT DELETE THIS LINE -- make depend depends on it.
