#ifndef CONFIG_H
#define CONFIG_H

#include <cstddef>
#include <queue>
#include <string>
#include <mutex>
#include <condition_variable>
#include <memory>

namespace ccc {

const size_t KB = 1024;
const size_t MB = 1024 * KB;
const size_t GB = 1024 * MB;

const int YEAR_OFFSET = 14;
const int YEAR_MASK = (1 << 30) - 1;
const int MONTH_OFFSET = 10;
const int MONTH_MASK = (1 << 4) - 1;
const int DAY_OFFSET = 5;
const int DAY_MASK = (1 << 5) - 1;
const int HOUR_OFFSET = 0;
const int HOUR_MASK = (1 << 5) - 1;

extern size_t CORES;
extern size_t WORKERS;

extern size_t TASK_SIZE;
extern size_t FILE_BUFFER_SIZE;

extern size_t PARSE_BUFFER_SIZE;

extern std::string PATH;

struct file_buffer {
  char *buffer;
  size_t offset;
  size_t len;
  bool empty;
};

extern std::deque<file_buffer> file_buffer_queue;
extern std::mutex file_buffer_queue_lock;
extern std::condition_variable file_buffer_queue_cv;

// date : score
using date_score_buffer = std::vector<std::pair<int, double>>;
extern std::queue<date_score_buffer> ds_buffer_queue;
extern std::mutex ds_buffer_queue_lock;
extern std::condition_variable ds_buffer_queue_cv;

// id : score
using id_score_buffer = std::vector<std::pair<uint64_t, double>>;
extern std::queue<id_score_buffer> is_buffer_queue;
extern std::mutex is_buffer_queue_lock;
extern std::condition_variable is_buffer_queue_cv;

// id : name
using id_name_buffer = std::vector<std::pair<uint64_t, std::string>>;
extern std::queue<id_name_buffer> in_buffer_queue;
extern std::mutex in_buffer_queue_lock;
extern std::condition_variable in_buffer_queue_cv;

#ifdef DEBUG
extern std::atomic<size_t> debug_counter;
#endif
  
}

#endif // CONFIG_H