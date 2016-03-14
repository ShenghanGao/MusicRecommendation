CC = gcc
CFLAGS = -Wall -O3

all: filter.o
	$(CC) $(CFLAGS) -o filter filter.o

filter.o: filter.c
	$(CC) $(CFLAGS) -c filter.c -o filter.o

clean:
	rm -rf filter filter.o

