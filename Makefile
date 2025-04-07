CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wall -Wextra -pedantic
SIMDJSON_DIR = simdjson
INCLUDE_DIR = $(SIMDJSON_DIR)

# Debug flags with sanitizers
DEBUGFLAGS = -std=c++17 -g -O0 -Wall -Wextra -pedantic -DDEBUG
SANITIZE_FLAGS = -fsanitize=thread -fno-omit-frame-pointer

# Main targets
all: main sample_all

# Debug target
debug: CXXFLAGS = $(DEBUGFLAGS)
debug: main sample_debug

# Debug with sanitizers
sanitize: CXXFLAGS = $(DEBUGFLAGS) $(SANITIZE_FLAGS)
sanitize: LDFLAGS = $(SANITIZE_FLAGS)
sanitize: main sample_sanitize

# Build parser
main: main.cpp $(SIMDJSON_DIR)/simdjson.cpp config.h separator.h worker.h
	$(CXX) $(CXXFLAGS) -I$(INCLUDE_DIR) -o $@ $^ $(LDFLAGS)

# Sample targets that call sample's Makefile
sample_all:
	$(MAKE) -C sample

sample_debug:
	$(MAKE) -C sample CXXFLAGS="$(DEBUGFLAGS)"

sample_sanitize:
	$(MAKE) -C sample CXXFLAGS="$(DEBUGFLAGS) $(SANITIZE_FLAGS)" LDFLAGS="$(SANITIZE_FLAGS)"

clean:
	rm -f main *.o
	$(MAKE) -C sample clean