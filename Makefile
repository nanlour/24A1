CXX = mpicxx
CXXFLAGS = -std=c++17 -O2 -Wall -Wextra -pedantic
SIMDJSON_DIR = ./simdjson
INCLUDE_DIR = $(SIMDJSON_DIR)

DEBUGFLAGS = -std=c++17 -g -O0 -Wall -Wextra -pedantic -DDEBUG

all: sample_mpi

debug: CXXFLAGS = $(DEBUGFLAGS)
debug: sample_mpi

sample_mpi: sample_mpi.cpp $(SIMDJSON_DIR)/simdjson.cpp
	rm -f *.tmp
	$(CXX) $(CXXFLAGS) -I$(INCLUDE_DIR) -o $@ $^ $(LDFLAGS)

clean:
	rm -f sample_mpi *.o *.tmp

tmp_clean:
	rm -f *.tmp