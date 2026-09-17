#pragma once

#include <cstdint>
#include <deque>

#include "pool/task_queue.h"

namespace kvstore {
namespace pool {

class FcfsQueue final : public TaskQueue {
public:
    FcfsQueue() = default;
    ~FcfsQueue() override;

    void push(Task task) override;
    bool pop(Task& out) override;

    bool empty() const override { return tasks_.empty(); }
    std::size_t size() const override { return tasks_.size(); }
    const char* name() const override { return "fcfs"; }

private:
    std::deque<Task> tasks_;
    std::uint64_t nextSequence_ = 0;
};
}
}
