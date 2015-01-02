.PHONY: all

all: BSONConversion TreadstoneConversion BSONAtomicAdd TreadstoneAtomicAdd

BSONConversion: BSONConversion.cc conversion.cc
	g++ -o $@ $^ -lbson-1.0 -lygor -lpopt -I/usr/local/include/libbson-1.0

TreadstoneConversion: TreadstoneConversion.cc conversion.cc
	g++ -o $@ $^ -ltreadstone -lygor -lpopt

BSONAtomicAdd: BSONAtomicAdd.cc atomic_add.cc
	g++ -o $@ $^ -lbson-1.0 -lygor -lpopt -I/usr/local/include/libbson-1.0

TreadstoneAtomicAdd: TreadstoneAtomicAdd.cc atomic_add.cc
	g++ -o $@ $^ -ltreadstone -lygor -lpopt
