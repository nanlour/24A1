#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdio>
#include <filesystem>

namespace fs = std::filesystem;

// Function to get file size
std::uintmax_t get_file_size(const std::string& filename) {
    return fs::file_size(filename);
}

// Function to find the next newline position after a given offset
size_t find_next_newline(std::ifstream& file, size_t start_pos) {
    file.seekg(start_pos);
    char c;
    while (file.get(c)) {
        if (c == '\n') {
            return file.tellg();
        }
    }
    return file.tellg(); // Return EOF position if no newline found
}

/*  Function to calculate NDJSON file part boundaries
    Assume single json document would be no more then 10Mb
*/ 
void calculate_ndjson_offsets(const std::string& input_file, int chunk_size) {
    std::ifstream file(input_file, std::ios::binary);
    if (!file) {
        std::cerr << "Error opening file: " << input_file << std::endl;
        return;
    }

    size_t file_size = get_file_size(input_file);
    std::vector<std::pair<size_t, size_t>> chunks;

    size_t current_pos = 0;
    
    for (;current_pos < file_size;) {
        size_t approx_pos = std::min(current_pos + chunk_size, file_size);
        size_t actual_pos = find_next_newline(file, approx_pos);
        chunks.emplace_back(current_pos, actual_pos);
        current_pos = actual_pos + 1;
    }

    file.close();
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cout << "Usage: " << argv[0] << " <ndjson_file> <number_of_parts>" << std::endl;
        return 1;
    }
    
    std::string input_file = argv[1];
    int num_parts;
    try {
        num_parts = std::stoi(argv[2]);
        if (num_parts <= 0) throw std::invalid_argument("Parts must be positive");
    } catch (const std::exception& e) {
        std::cerr << "Invalid number of parts: " << argv[2] << std::endl;
        return 1;
    }
    
    calculate_ndjson_offsets(input_file, num_parts);
    
    return 0;
}