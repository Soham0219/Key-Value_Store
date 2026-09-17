#pragma once

#include <array>
#include <cstdint>
#include <deque>

#include "pool/task_queue.h"

namespace kvstore {
namespace pool {

class PriorityQueue final : public TaskQueue {
public:

    explicit PriorityQueue(bool aging, std::uint64_t agingThreshold = kDefaultAgingThreshold);
    ~PriorityQueue() override;

    void push(Task task) override;
    bool pop(Task& out) override;

    bool empty() const override { return count_ == 0; }
    std::size_t size() const override { return count_; }
    const char* name() const override { return aging_ ? "priority+aging" : "priority"; }

    static constexpr std::uint64_t kDefaultAgingThreshold = 32;

    std::uint64_t agingThreshold() const noexcept { return agingThreshold_; }

    std::uint64_t promotions() const noexcept { return promotions_; }

private:
    std::array<std::deque<Task>, kPriorityLevels> levels_;
    bool aging_;
    std::uint64_t agingThreshold_;
    std::size_t count_ = 0;
    std::uint64_t nextSequence_ = 0;
    std::uint64_t served_ = 0;
    std::uint64_t promotions_ = 0;
    std::uint64_t servesSincePromotion_ = 0;
};
}
}
