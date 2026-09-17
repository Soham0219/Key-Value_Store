#pragma once

#include <string>

namespace kvstore {
namespace store {

struct Mutation {
    bool erase = false;
    std::string key;
    std::string value;
};
}
}
