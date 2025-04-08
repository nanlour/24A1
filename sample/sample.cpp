#include <cstdlib>
#include <string>
#include <unordered_map>
#include <array>
#include <utility>
#include <limits>
#include "../config.h"
#include "../simdjson/simdjson.h"


void get_time(size_t date, int &year,int &month, int &day, int &hour) {
  year = (date >> ccc::YEAR_OFFSET) & ccc::YEAR_MASK;
  month = (date >> ccc::MONTH_OFFSET)& ccc::MONTH_MASK;
  day = (date >> ccc::DAY_OFFSET) & ccc::DAY_MASK;
  hour = (date >> ccc::HOUR_OFFSET) & ccc::HOUR_MASK;
}

int main(int argc, char* argv[]) {
  simdjson::dom::parser parser;
  simdjson::dom::document_stream stream;
  simdjson::padded_string json;
  simdjson::padded_string::load(argv[1]).get(json);

  std::unordered_map<int, double> d_s;
  std::unordered_map<int64_t, double> id_s;
  std::unordered_map<int64_t, std::string> id_name;

  auto error = parser.parse_many(json).get(stream);
  for (auto doc : stream) {
    int64_t id = strtoull(doc["doc"]["account"]["id"].get_c_str(), nullptr, 10);

    std::string_view date = doc["doc"]["createdAt"];
    int year, month, day, hour;
    std::sscanf(doc["doc"]["createdAt"].get_c_str(), "%d-%d-%dT%d", &year, &month, &day, &hour);
    int data = (year << ccc::YEAR_OFFSET) + (month << ccc::MONTH_OFFSET) + (day << ccc::DAY_OFFSET) + (hour << ccc::HOUR_OFFSET);

    double score = 0;
    auto e = doc["doc"]["sentiment"].get_double().get(score);

    std::string_view username;
    doc["doc"]["account"]["username"].get_string().get(username);

    d_s[data] += score;
    id_s[id] += score;
    if (id_name.count(id) == 0) {
      id_name.emplace(id, username);
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

  return 0;
}