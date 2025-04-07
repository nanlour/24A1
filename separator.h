#ifndef SPERATOR_H
#define SPERATOR_H

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdio>
#include <filesystem>

class seperator {
  public:
  /*  Function to calculate NDJSON file part boundaries
      Assume single json document would be no more then 10Mb
  */ 
  inline static std::vector<std::pair<size_t, size_t>> calculate__offsets(const std::string& input_file, int chunk_size) {
    std::ifstream file(input_file, std::ios::binary);
    if (!file) {
        std::cerr << "Error opening file: " << input_file << std::endl;
    }

    size_t file_size = get_file_size(input_file);
    std::vector<std::pair<size_t, size_t>> chunks;

    size_t current_pos = 0;

    for (;current_pos < file_size;) {
      size_t approx_pos = std::min(current_pos + chunk_size, file_size - 1);
      size_t actual_pos = find_next_newline(file, approx_pos);
      chunks.emplace_back(current_pos, actual_pos - current_pos + 1);
      current_pos = actual_pos + 1;
    }

    file.close();
    return chunks;
  }

  private:
  seperator() = delete;

  // Function to get file size
  inline static std::uintmax_t get_file_size(const std::string& filename) {
    return std::filesystem::file_size(filename);
  }

  // Function to find the next newline position after a given offset
  inline static size_t find_next_newline(std::ifstream& file, size_t start_pos) {
    file.seekg(start_pos);
    char c;
    while (file.get(c)) {
      if (c == '\n') {
        return static_cast<size_t>(file.tellg()) - 1; // Adjust to return the position of '\n'
      }
    }
    return file.tellg(); // Return EOF position if no newline found
  }

};

#endif