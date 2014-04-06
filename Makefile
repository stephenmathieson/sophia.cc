
AR ?= ar
CC ?= gcc
CXX ?= g++
VALGRIND ?= valgrind
VALGRIND_OPTS ?= --leak-check=full

LIST_SRC = $(wildcard deps/list/*.c)
LIST_OBJS = $(LIST_SRC:.c=.o)

LDFLAGS += -lsophia -pthread
CPPFLAGS += -Ideps/list
CFLAGS = -std=c99

TEST_MAIN ?= sophia-test

test: $(TEST_MAIN)
	@rm -rf testdb
	./$(TEST_MAIN)

$(TEST_MAIN): test.o sophia.o $(LIST_OBJS)
	$(CXX) $(CPPFLAGS) $^ -o $@ $(LDFLAGS)

%.o: %.cc
	$(CXX) $< $(CPPFLAGS) -c -o $@

%.o: %.c
	$(CC) $< $(CFLAGS) -c -o $@

check: $(TEST_MAIN)
	$(VALGRIND) $(VALGRIND_OPTS) ./$(TEST_MAIN)

# stolen from sphia/libsphia's makefile
travis:
	rm -rf sophia
	git clone --depth=1 https://github.com/pmwkaa/sophia.git sophia
	$(MAKE) -C sophia/db
	rm -f sophia/db/*.so*
	CPPFLAGS="-Isophia/db" LIBRARY_PATH="./sophia/db" $(MAKE) test

clean:
	rm -f sophia.o test.o $(TEST_MAIN) $(LIST_OBJS)
	rm -rf testdb

.PHONY: clean check
