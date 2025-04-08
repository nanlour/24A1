#include <mpi.h>
#include <iostream>
#include <unistd.h>
#include "../separator.h"
#include "file.h"

using namespace ccc;

const size_t KB = 1024;
const size_t MB = 1024 * KB;
const size_t GB = 1024 * MB;

size_t TASK_SIZE = 8 * KB;
size_t FILE_BUFFER_SIZE = TASK_SIZE + 10 * MB;

const int YEAR_OFFSET = 14;
const int YEAR_MASK = (1 << 30) - 1;
const int MONTH_OFFSET = 10;
const int MONTH_MASK = (1 << 4) - 1;
const int DAY_OFFSET = 5;
const int DAY_MASK = (1 << 5) - 1;
const int HOUR_OFFSET = 0;
const int HOUR_MASK = (1 << 5) - 1;

const std::string CHUNK_PATH = "./chunks.tmp";
const std::string PARSE_PATH = "./PARSE";
std::string JSON_PATH;

std::pair<size_t, size_t> parse(simdjson::parser &parser, char *file_buffer, size_t len, char *result_buffer);

int main(int argc, char *argv[]) {
  MPI_Init(&argc, &argv);

  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  // Master
  if (world_rank == 0) {
    JSON_PATH = std::string(argv[1]);
    std::vector<std::pair<size_t, size_t>> file_blocks {seperator::calculate__offsets(PATH, TASK_SIZE)};
    write_file_blocks_to_file(file_blocks, CHUNK_PATH);
  }

  MPI_Barrier(MPI_COMM_WORLD);

  std::vector<std::pair<size_t, size_t>> file_blocks = read_file_blocks_from_file(CHUNK_PATH);

  std::unique_ptr<char[]> file_buffer = new char[FILE_BUFFER_SIZE];
  std::unique_ptr<char[]> result_buffer = new char[FILE_BUFFER_SIZE];
  simdjson::parser parser;

  for (int i = 0; i < file_blocks.size(); i++) {
    if (i % (world_rank + 1)) {
      continue;
    }

    // Read file
    size_t offset = file_blocks[i].first, len = file_blocks[i].second;
    int fd = open(PATH.c_str(), O_RDONLY);
    ssize_t bytes_read = pread(fd, file_buffer, len, offset);
    if (bytes_read != len) {
      std::cerr << "Json file read fail" << std::endl;
    }

    // Parse json
    auto result_size = parse(parser, file_buffer.get(), len, result_buffer.get());

    std::string result_file_path = PARSE_PATH + std::to_string(i) + "tmp";
    std::ofstream result_file(result_file_path, std::ios::binary);
    if (!result_file) {
      std::cerr << "Failed to open file for writing: " << result_file_path << std::endl;
      continue;
    }
    result_file.write(&result_size.first, sizeof(size_t));
    result_file.write(result_buffer.get(), result_size.second);
    result_file.close();
  }

  MPI_Barrier(MPI_COMM_WORLD);

  MPI_Finalize();
  return 0;
}

std::pair<size_t, size_t> parse(simdjson::parser &parser, char *file_buffer, size_t len, char *result_buffer) {
  simdjson::dom::document_stream stream;
  if (parser_.parse_many(file_buffer, len, simdjson::dom::DEFAULT_BATCH_SIZE).get(stream) != simdjson::SUCCESS) {
    std::cerr << "parse failed" << std::endl;
    return {0, 0};
  }

  date_score_buffer ds_buffer;
  ds_buffer.reserve(PARSE_BUFFER_SIZE);
  id_score_buffer is_buffer;
  is_buffer.reserve(PARSE_BUFFER_SIZE);
  id_name_buffer in_buffer;
  in_buffer.reserve(PARSE_BUFFER_SIZE);

  // store parse result
  size_t count = 0;
  char* buffer_start = result_buffer;
  for (auto doc : stream) {
    ++count;
    #ifdef DEBUG
    debug_counter++;
    #endif
    int64_t id = strtoull(doc["doc"]["account"]["id"].get_c_str(), nullptr, 10);

    int year, month, day, hour;
    std::sscanf(doc["doc"]["createdAt"].get_c_str(), "%d-%d-%dT%d", &year, &month, &day, &hour);
    int date = (year << ccc::YEAR_OFFSET) + (month << ccc::MONTH_OFFSET) + (day << ccc::DAY_OFFSET) + (hour << ccc::HOUR_OFFSET);

    double score;
    auto error = doc["doc"]["sentiment"].get_double().get(score);
    if (e != simdjson::SUCCESS) {
      score = 0;
    }

    std::string_view username;
    doc["doc"]["account"]["username"].get_string().get(username);

    // Copy result to buffer
    *static_cast<size_t *>(result_buffer) = id;
    result_buffer += sizeof(size_t);
    *static_cast<int *>(result_buffer) = date;
    result_buffer += sizeof(int);
    *static_cast<double *>(result_buffer) = score;
    result_buffer += sizeof(double);
    *static_cast<size_t *>(result_buffer) = username.size();
    result_buffer += sizeof(size_t);
    std::memcpy(result_buffer, username.data(), username.size());
    result_buffer += username.size();
  }

  return {count, result_buffer - buffer_start};
}