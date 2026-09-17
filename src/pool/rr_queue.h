#pragma once

#include <cstdint>
#include <deque>

#include "pool/task_queue.h"

namespace kvstore {
namespace pool {

class RoundRobinQueue final : public TaskQueue {
public:
    RoundRobinQueue() = default;
    ~RoundRobinQueue() override;

    void push(Task task) override;
    bool pop(Task& out) override;

    bool empty() const override { return tasks_.empty(); }
    std::size_t size() const override { return tasks_.size(); }
    const char* name() const override { return "round-robin"; }

    std::uint64_t requeues() const noexcept { return requeues_; }

private:
    std::deque<Task> tasks_;
    std::uint64_t nextSequence_ = 0;
    std::uint64_t requeues_ = 0;
};
}
}
