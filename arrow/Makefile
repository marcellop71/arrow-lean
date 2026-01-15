# Makefile for Arrow C wrapper library

CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -fPIC -O2
INCLUDES = -I.

# Source files
SOURCES = arrow_schema.c arrow_array.c arrow_stream.c arrow_data_access.c arrow_buffer.c parquet_reader_writer.c
OBJECTS = $(SOURCES:.c=.o)
HEADERS = arrow_c_abi.h arrow_wrapper.h parquet_wrapper.h

# Library targets
STATIC_LIB = libarrow_wrapper.a
SHARED_LIB = libarrow_wrapper.so

.PHONY: all clean static shared

all: static shared

static: $(STATIC_LIB)

shared: $(SHARED_LIB)

$(STATIC_LIB): $(OBJECTS)
	ar rcs $@ $^

$(SHARED_LIB): $(OBJECTS)
	$(CC) -shared -o $@ $^

%.o: %.c $(HEADERS)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

clean:
	rm -f $(OBJECTS) $(STATIC_LIB) $(SHARED_LIB)

install: all
	mkdir -p /usr/local/lib
	mkdir -p /usr/local/include
	cp $(STATIC_LIB) $(SHARED_LIB) /usr/local/lib/
	cp $(HEADERS) /usr/local/include/

.PHONY: test
test: shared
	$(CC) $(CFLAGS) $(INCLUDES) -L. -larrow_wrapper test.c -o test_arrow