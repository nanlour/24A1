#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <thread>
#include "simdjson/simdjson.h"
#include "config.h"
#include "separator.h"
#include "worker.h"

namespace ccc {
size_t CORES = 1;
size_t WORKERS = 8;

size_t TASK_SIZE = 8 * KB;
size_t FILE_BUFFER_SIZE = TASK_SIZE + 10 * MB;

size_t PARSE_BUFFER_SIZE = 4 * MB;

std::string PATH;

// file buffer
std::deque<file_buffer> file_buffer_queue;
std::mutex file_buffer_queue_lock;
std::condition_variable file_buffer_queue_cv;

// date : score
std::queue<std::vector<std::pair<int, double>>> ds_buffer_queue;
std::mutex ds_buffer_queue_lock;
std::condition_variable ds_buffer_queue_cv;

// id : score
std::queue<std::vector<std::pair<uint64_t, double>>> is_buffer_queue;
std::mutex is_buffer_queue_lock;
std::condition_variable is_buffer_queue_cv;

// id : name
std::queue<std::vector<std::pair<uint64_t, std::string>>> in_buffer_queue;
std::mutex in_buffer_queue_lock;
std::condition_variable in_buffer_queue_cv;

#ifdef DEBUG
std::atomic<size_t> debug_counter = 0;
#endif

int schedular(int argc, char* argv[]) {
  int node_num = 0;
  PATH = std::string(argv[1]);
  std::vector<std::pair<size_t, size_t>> file_blocks {seperator::calculate__offsets(PATH, TASK_SIZE)};

  std::unique_lock<std::mutex> lock(file_buffer_queue_lock);
  for (size_t i = 0; i < WORKERS + 2; i++) {
    file_buffer fb{static_cast<char*>(std::aligned_alloc(4096, FILE_BUFFER_SIZE)), 0, 0, true};
    file_buffer_queue.emplace_back(fb);
  }

  int fd = open(PATH.c_str(), O_RDONLY | O_DIRECT);
  
  for (size_t i = 0; i < file_blocks.size(); i++) {
    while (!file_buffer_queue.back().empty) {
      file_buffer_queue_cv.wait(lock);
    }
    char *buffer = file_buffer_queue.back().buffer;
    file_buffer_queue.pop_back();
    lock.unlock();

    // Calculate aligned values for Direct I/O
    size_t aligned_offset = file_blocks[i].first & ~(4096-1);  // Round down to 4K boundary
    size_t offset_adjustment = file_blocks[i].first - aligned_offset;
    size_t aligned_size = ((file_blocks[i].second + offset_adjustment + 4096-1) & ~(4096-1));  // Round up

    // Read with proper EOF handling
    ssize_t bytes_read = pread(fd, buffer, aligned_size, aligned_offset);

    // Check if we read all the data we need
    if (bytes_read >= 0) {
      // Success or partial read (EOF)
      size_t actual_data_size = bytes_read - offset_adjustment;
      if (bytes_read < offset_adjustment) {
        // Not enough data - didn't even reach our starting point
        std::cerr << "Error: Attempt to read beyond file boundary" << std::endl;
        return 1;
      } else {
        // We have at least some data to process (handle partial file blocks)
        lock.lock();
        file_buffer fb{buffer, offset_adjustment, file_blocks[i].second, false};
        file_buffer_queue.emplace_front(fb);
      }
    } else {
      // Error occurred
      std::cerr << "Read error: " << strerror(errno) << std::endl;
      return 1;
    }
  }

  return 0;
}

}

int main(int argc, char* argv[]) {
  std::vector<std::thread> thread_pool;
  thread_pool.reserve(ccc::WORKERS + 1);
  std::vector<ccc::worker> workers(ccc::WORKERS);
  thread_pool.emplace_back([&argc, &argv]() {
    ccc::schedular(argc, argv);
  });

  for (size_t i = 0; i < ccc::WORKERS; ++i) {
    thread_pool.emplace_back([&workers, i]() {
      workers[i].work();
    });
  }

  // Join all threads before exiting
  for (auto& thread : thread_pool) {
    thread.join();
  }

  return 0;
}