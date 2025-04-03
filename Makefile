CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wall -Wextra -pedantic
SIMDJSON_DIR = simdjson
INCLUDE_DIR = $(SIMDJSON_DIR)

# Debug flags with sanitizers
DEBUGFLAGS = -std=c++17 -g -O0 -Wall -Wextra -pedantic -DDEBUG
SANITIZE_FLAGS = -fsanitize=address -fsanitize=thread -fno-omit-frame-pointer

# Main targets
all: parser

# Debug target
debug: CXXFLAGS = $(DEBUGFLAGS)
debug: parser

# Debug with sanitizers
sanitize: CXXFLAGS = $(DEBUGFLAGS) $(SANITIZE_FLAGS)
sanitize: LDFLAGS = $(SANITIZE_FLAGS)
sanitize: parser

# Build parser
parser: parser.cpp $(SIMDJSON_DIR)/simdjson.cpp
	$(CXX) $(CXXFLAGS) -I$(INCLUDE_DIR) -o $@ $^ $(LDFLAGS)

clean:
	rm -f parser *.o