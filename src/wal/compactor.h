#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace kvstore {
namespace wal {

struct CompactionResult {
    bool ok = false;
    std::uint64_t recordsWritten = 0;
    std::uint64_t bytesBefore = 0;
    std::uint64_t bytesAfter = 0;
    std::string error;
};

class Compactor {
public:

    static CompactionResult rewrite(const std::string& path,
                                    const std::vector<std::pair<std::string, std::string>>& entries);

    static constexpr const char* kTempSuffix = ".compact";
};
}
}
