#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <thread>
#include <fstream>
#include <vector>
#include <utility>

namespace ccc {

inline void write_file_blocks_to_file(const std::vector<std::pair<size_t, size_t>>& blocks, const std::string& filename) {
  std::ofstream outfile(filename, std::ios::binary);
  if (!outfile) {
    std::cerr << "Failed to open file for writing: " << filename << std::endl;
    return;
  }
  
  // Write size of vector first
  size_t size = blocks.size();
  outfile.write(reinterpret_cast<const char*>(&size), sizeof(size_t));
  
  // Write each pair
  outfile.write(reinterpret_cast<const char*>(&blocks.front().first), sizeof(size_t) * 2 * size);
  
  outfile.close();
}

std::vector<std::pair<size_t, size_t>> read_file_blocks_from_file(const std::string& filename) {
  std::vector<std::pair<size_t, size_t>> blocks;
  std::ifstream infile(filename, std::ios::binary);
  
  if (!infile) {
    std::cerr << "Failed to open file for reading: " << filename << std::endl;
    return blocks;
  }
  
  // Read size of vector
  size_t size;
  infile.read(reinterpret_cast<char*>(&size), sizeof(size_t));
  
  // Reserve space
  blocks.reserve(size);
  
  // Read each pair
  for (size_t i = 0; i < size; i++) {
    size_t first, second;
    infile.read(reinterpret_cast<char*>(&first), sizeof(size_t));
    infile.read(reinterpret_cast<char*>(&second), sizeof(size_t));
    blocks.emplace_back(first, second);
  }
  
  infile.close();

  return blocks;
}

} // namespace ccc
