#include <iostream>
#include "simdjson/simdjson.h"

int main(int argc, char* argv[]) {
    simdjson::padded_string json;
    simdjson::padded_string::load(argv[1]).get(json);
    simdjson::dom::parser parser;
    simdjson::dom::document_stream stream;
    auto error = parser.parse_many(json).get(stream);
    auto i = stream.begin();
    size_t count{0};
    for(; i != stream.end(); ++i) {
        auto doc = *i;
        if (!doc.error()) {
          std::cout << "got full document at " << i.current_index() << std::endl;
          std::cout << i.source() << std::endl;
          count++;
        } else {
          std::cout << "got broken document at " << i.current_index() << std::endl;
          return false;
        }
    }

    return 0;
}