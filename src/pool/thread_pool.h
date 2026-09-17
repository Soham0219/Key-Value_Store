#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "pool/task_queue.h"

namespace kvstore {
namespace pool {

class ThreadPool {
public:

    explicit ThreadPool(std::size_t threadCount,
                        SchedulingPolicy policy = SchedulingPolicy::Fcfs,
                        bool aging = false,
                        std::uint64_t agingThreshold = 32);

    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    bool submit(TaskFn task, TaskPriority priority = TaskPriority::Normal);

    bool submit(std::function<void()> task, TaskPriority priority = TaskPriority::Normal);

    void shutdown();

    std::size_t threadCount() const noexcept { return workers_.size(); }

    std::uint64_t tasksExecuted() const noexcept {
        return tasksExecuted_.load(std::memory_order_relaxed);
    }
    std::uint64_t taskYields() const noexcept {
        return taskYields_.load(std::memory_order_relaxed);
    }
    const char* policyName() const noexcept { return queue_ ? queue_->name() : "none"; }

    const TaskQueue* queue() const noexcept { return queue_.get(); }

private:
    void workerLoop();

    std::vector<std::thread> workers_;
    std::unique_ptr<TaskQueue> queue_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool stopping_ = false;

    std::atomic<std::uint64_t> tasksExecuted_{0};
    std::atomic<std::uint64_t> taskYields_{0};
};
}
}
