CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wall -Wextra -pedantic
SIMDJSON_DIR = ../simdjson
INCLUDE_DIR = $(SIMDJSON_DIR)

all: sample

sample: sample.cpp $(SIMDJSON_DIR)/simdjson.cpp config.h
	$(CXX) $(CXXFLAGS) -I$(INCLUDE_DIR) -o $@ $^ $(LDFLAGS)

clean:
	rm -f sample *.o