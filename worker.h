#ifndef WORKER_H
#define WORKER_H

#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <bits/this_thread_sleep.h>
#include "simdjson/simdjson.h"
#include "config.h"
#include "separator.h"

namespace ccc {

class worker {
  public:
  worker() : parser_(new simdjson::dom::parser()), fb_lock_(std::unique_lock<std::mutex>(file_buffer_queue_lock, std::defer_lock)) {};

  int parse() {
    if (!fb_lock_.try_lock()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      return 0;
    }

    if (file_buffer_queue.empty() || file_buffer_queue.front().empty) {
      fb_lock_.unlock();
      return 0;
    }
    auto fb = file_buffer_queue.front();
    file_buffer_queue.pop_front();
    fb_lock_.unlock();

    simdjson::dom::document_stream stream;
    if (parser_->parse_many(fb.buffer + fb.offset, fb.len, simdjson::dom::DEFAULT_BATCH_SIZE).get(stream) != simdjson::SUCCESS) {
      std::cerr << "parse failed" << std::endl;
      return 1;
    }

    date_score_buffer ds_buffer;
    ds_buffer.reserve(PARSE_BUFFER_SIZE);
    id_score_buffer is_buffer;
    is_buffer.reserve(PARSE_BUFFER_SIZE);
    id_name_buffer in_buffer;
    in_buffer.reserve(PARSE_BUFFER_SIZE);

    // store parse result
    int count{0};
    for (auto doc : stream) {
      #ifdef DEBUG
      debug_counter++;
      #endif
      int64_t id = strtoull(doc["doc"]["account"]["id"].get_c_str(), nullptr, 10);

      int year, month, day, hour;
      std::sscanf(doc["doc"]["createdAt"].get_c_str(), "%d-%d-%dT%d", &year, &month, &day, &hour);
      int date = (year << ccc::YEAR_OFFSET) + (month << ccc::MONTH_OFFSET) + (day << ccc::DAY_OFFSET) + (hour << ccc::HOUR_OFFSET);
  
      double score;
      auto e = doc["doc"]["sentiment"].get_double().get(score);
  
      std::string_view username;
      doc["doc"]["account"]["username"].get_string().get(username);
      
      ds_buffer.emplace_back(date, score);
      is_buffer.emplace_back(id, score);
      in_buffer.emplace_back(id, username);
      
      if (++count == PARSE_BUFFER_SIZE) {
        ds_buffer_queue_lock.lock();
        ds_buffer_queue.emplace(std::move(ds_buffer));
        ds_buffer_queue_lock.unlock();

        is_buffer_queue_lock.lock();
        is_buffer_queue.emplace(std::move(is_buffer));
        is_buffer_queue_lock.unlock();

        in_buffer_queue_lock.lock();
        in_buffer_queue.emplace(std::move(in_buffer));
        in_buffer_queue_lock.unlock();

        ds_buffer = date_score_buffer();
        is_buffer = id_score_buffer();
        in_buffer = id_name_buffer();
        ds_buffer.reserve(PARSE_BUFFER_SIZE);
        is_buffer.reserve(PARSE_BUFFER_SIZE);
        in_buffer.reserve(PARSE_BUFFER_SIZE);
      }
    }

    fb_lock_.lock();
    fb.empty = true;
    file_buffer_queue.emplace_back(fb);
    file_buffer_queue_cv.notify_all();
    fb_lock_.unlock();

    ds_buffer_queue_lock.lock();
    ds_buffer_queue.emplace(std::move(ds_buffer));
    ds_buffer_queue_lock.unlock();

    is_buffer_queue_lock.lock();
    is_buffer_queue.emplace(std::move(is_buffer));
    is_buffer_queue_lock.unlock();

    in_buffer_queue_lock.lock();
    in_buffer_queue.emplace(std::move(in_buffer));
    in_buffer_queue_lock.unlock();

    return 0;
  }

  int work() {
    for (;1;) {
      parse();

      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }

  private:
  std::unique_ptr<simdjson::dom::parser> parser_;
  std::unique_lock<std::mutex> fb_lock_;
};

}
#endif