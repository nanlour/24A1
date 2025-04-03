CXX = g++
CXXFLAGS = -std=c++17 -O3 -Wall -Wextra -pedantic
SIMDJSON_DIR = simdjson
INCLUDE_DIR = $(SIMDJSON_DIR)

# Main targets
all: parser
	
# Build parser
parser: parser.cpp $(SIMDJSON_DIR)/simdjson.cpp
	$(CXX) $(CXXFLAGS) -I$(INCLUDE_DIR) -o $@ $^

clean:
	rm -f parser *.o