#include <mpi.h>
#include <iostream>
#include <unistd.h>
#include <sys/stat.h>
#include <chrono>
#include "simdjson/simdjson.h"
#include "separator.h"
#include "file.h"

using namespace ccc;

const size_t KB = 1024;
const size_t MB = 1024 * KB;
const size_t GB = 1024 * MB;

size_t TASK_SIZE = 1 * GB;
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

int parse_chunks(int world_size, int rank, std::vector<std::pair<size_t, size_t>> &file_blocks);
std::pair<size_t, size_t> parse(simdjson::dom::parser &parser, char *file_buffer, size_t len, char *result_buffer);
void get_time(size_t date, int &year,int &month, int &day, int &hour) {
  year = (date >> YEAR_OFFSET) & YEAR_MASK;
  month = (date >> MONTH_OFFSET)& MONTH_MASK;
  day = (date >> DAY_OFFSET) & DAY_MASK;
  hour = (date >> HOUR_OFFSET) & HOUR_MASK;
}

int main(int argc, char *argv[]) {
  MPI_Init(&argc, &argv);

  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  auto start_time = std::chrono::steady_clock::now();

  // Master
  if (world_rank == 0) {
    JSON_PATH = std::string(argv[1]);
    std::vector<std::pair<size_t, size_t>> file_blocks {seperator::calculate__offsets(JSON_PATH, TASK_SIZE)};
    write_file_blocks_to_file(file_blocks, CHUNK_PATH);
  }

  // Wait for master split the json
  MPI_Barrier(MPI_COMM_WORLD);

  std::vector<std::pair<size_t, size_t>> file_blocks {read_file_blocks_from_file(CHUNK_PATH)};
  parse_chunks(world_size, world_rank, file_blocks);

  // Wait for everyone finish parse
  MPI_Barrier(MPI_COMM_WORLD);

  if (world_rank == 0) {
    std::unordered_map<int64_t, double> d_s;
    std::unordered_map<int64_t, double> id_s;
    std::unordered_map<int64_t, std::string> id_name;

    std::unique_ptr<char[]> result_buffer {new char[FILE_BUFFER_SIZE]};
    for (int i = 0; i < file_blocks.size(); i++) {
      int fd = open((PARSE_PATH + std::to_string(i) + ".tmp").c_str(), O_RDONLY);

      // Get file size and read file
      struct stat file_stat;
      if (fstat(fd, &file_stat) < 0) {
          std::cerr << "Failed to get file stats" << std::endl;
          close(fd);
          continue;
      }
      size_t file_size = file_stat.st_size;
      ssize_t bytes_read = pread(fd, result_buffer.get(), file_size, 0);
      close(fd);

      void * p = result_buffer.get();
      
      size_t size = *reinterpret_cast<size_t *>(p);
      p += sizeof(size_t);

      for (size_t i = 0; i < size; i++) {
        size_t id = 0, date = 0, name_size = 0;
        double score = 0;
        
        id = *reinterpret_cast<size_t *>(p);
        p += sizeof(size_t);
        date = *reinterpret_cast<size_t *>(p);
        p += sizeof(size_t);
        score = *reinterpret_cast<double *>(p);
        p += sizeof(double);
        name_size = *reinterpret_cast<size_t *>(p);
        p += sizeof(size_t);

        d_s[date] += score;
        id_s[id] += score;
        // Only build name string if needed
        if (id_name.count(id) == 0) {
          id_name.emplace(id, std::string(reinterpret_cast<char *>(p), name_size));
        }
        p += name_size;
      }
    }

    std::array<std::pair<double, int>, 6> q1;
    for (auto &p : q1) p.first = std::numeric_limits<double>::lowest();
    std::array<std::pair<double, int>, 6> q2;
    for (auto &p : q2) p.first = std::numeric_limits<double>::max();
    std::array<std::pair<double, uint64_t>, 6> q3;
    for (auto &p : q3) p.first = std::numeric_limits<double>::lowest();
    std::array<std::pair<double, uint64_t>, 6> q4;
    for (auto &p : q4) p.first = std::numeric_limits<double>::max();
  
    for (auto &[d, s] : d_s) {
      q1[5] = {s, d};
      q2[5] = {s, d};
      for (int i = 4; i >= 0; i--) {
        if (q1[i] < q1[i + 1]) std::swap(q1[i], q1[i + 1]);
        if (q2[i] > q2[i + 1]) std::swap(q2[i], q2[i + 1]);
      }
    }
  
    for (auto &[d, s] : id_s) {
      q3[5] = {s, d};
      q4[5] = {s, d};
      for (int i = 4; i >= 0; i--) {
        if (q3[i] < q3[i + 1]) std::swap(q3[i], q3[i + 1]);
        if (q4[i] > q4[i + 1]) std::swap(q4[i], q4[i + 1]);
      }
    }
    
    std::cout << "The happest hour are:" << std::endl;
    for (int i = 0; i < q1.size() - 1; i++) {
      auto &p = q1[i];
      int year, month, day, hour;
      get_time(p.second, year, month, day, hour);
      std::cout << i + 1 << "st happest hour is " << year << "." << month << "." << day << " at " << hour << " with score: " << p.first << std::endl;
    }
  
    std::cout << "The saddest hour are:" << std::endl;
    for (int i = 0; i < q2.size() - 1; i++) {
      auto &p = q2[i];
      int year, month, day, hour;
      get_time(p.second, year, month, day, hour);
      std::cout << i + 1 << "st saddest hour is " << year << "." << month << "." << day << " at " << p.first << std::endl;
    }
  
    std::cout << "The happest people are:" << std::endl;
    for (int i = 0; i < q3.size() - 1; i++) {
      auto &p = q3[i];
      std::cout << i + 1 << "st happest people is id:" << p.second << " "<< id_name[p.second] << " with score: " << p.first  << std::endl;
    }
  
    std::cout << "The saddest people are:" << std::endl;
    for (int i = 0; i < q4.size() - 1; i++) {
      auto &p = q4[i];
      std::cout << i + 1 << "st saddest people is id:" << p.second << " "<< id_name[p.second] << " with score: " << p.first  << std::endl;
    }

    auto end_time = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed = end_time - start_time;
    std::cout << "Total time: " << elapsed.count() << " seconds" << std::endl;
  }

  MPI_Finalize();
  return 0;
}

int parse_chunks(int world_size, int rank, std::vector<std::pair<size_t, size_t>> &file_blocks) {
  std::unique_ptr<char[]> file_buffer {new char[FILE_BUFFER_SIZE]};
  std::unique_ptr<char[]> result_buffer {new char[FILE_BUFFER_SIZE]};
  simdjson::dom::parser parser;

  for (int i = 0; i < file_blocks.size(); i++) {
    if (i % world_size != rank) {
      continue;
    }

    // Read file
    size_t offset = file_blocks[i].first, len = file_blocks[i].second;
    int fd = open(JSON_PATH.c_str(), O_RDONLY);
    ssize_t bytes_read = pread(fd, file_buffer.get(), len, offset);
    close(fd);
    if (bytes_read != len) {
      std::cerr << "Json file read fail" << std::endl;
    }

    // Parse json
    auto result_size = parse(parser, file_buffer.get(), len, result_buffer.get());

    std::string result_file_path = PARSE_PATH + std::to_string(i) + ".tmp";
    std::ofstream result_file(result_file_path, std::ios::binary);
    if (!result_file) {
      std::cerr << "Failed to open file for writing: " << result_file_path << std::endl;
      continue;
    }
    result_file.write(reinterpret_cast<char *>(&result_size.first), sizeof(size_t));
    result_file.write(result_buffer.get(), result_size.second);
    result_file.close();
  }

  return 0;
}

std::pair<size_t, size_t> parse(simdjson::dom::parser &parser, char *file_buffer, size_t len, char *result_buffer) {
  simdjson::dom::document_stream stream;
  if (parser.parse_many(file_buffer, len, simdjson::dom::DEFAULT_BATCH_SIZE).get(stream) != simdjson::SUCCESS) {
    std::cerr << "parse failed" << std::endl;
    return {0, 0};
  }

  // store parse result
  size_t count = 0;
  char* buffer_start = result_buffer;
  for (auto doc : stream) {
    ++count;
    size_t id = strtoull(doc["doc"]["account"]["id"].get_c_str(), nullptr, 10);

    int year, month, day, hour;
    std::sscanf(doc["doc"]["createdAt"].get_c_str(), "%d-%d-%dT%d", &year, &month, &day, &hour);
    size_t date = (year << YEAR_OFFSET) + (month << MONTH_OFFSET) + (day << DAY_OFFSET) + (hour << HOUR_OFFSET);

    double score;
    auto error = doc["doc"]["sentiment"].get_double().get(score);
    if (error != simdjson::SUCCESS) {
      score = 0;
    }

    std::string_view username;
    doc["doc"]["account"]["username"].get_string().get(username);

    // Copy result to buffer
    *reinterpret_cast<size_t *>(result_buffer) = id;
    result_buffer += sizeof(size_t);
    *reinterpret_cast<size_t *>(result_buffer) = date;
    result_buffer += sizeof(size_t);
    *reinterpret_cast<double *>(result_buffer) = score;
    result_buffer += sizeof(double);
    *reinterpret_cast<size_t *>(result_buffer) = username.size();
    result_buffer += sizeof(size_t);
    std::memcpy(result_buffer, username.data(), username.size());
    result_buffer += username.size();
  }

  return {count, result_buffer - buffer_start};
}