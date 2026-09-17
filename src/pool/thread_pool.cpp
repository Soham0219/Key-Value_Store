#include "pool/thread_pool.h"

#include <stdexcept>
#include <utility>

namespace kvstore {
namespace pool {

ThreadPool::ThreadPool(std::size_t threadCount, SchedulingPolicy policy, bool aging,
                       std::uint64_t agingThreshold) {
    if (threadCount == 0) {
        throw std::invalid_argument("ThreadPool requires at least one thread");
    }

    queue_ = makeTaskQueue(policy, aging, agingThreshold);

    workers_.reserve(threadCount);

    for (std::size_t i = 0; i < threadCount; ++i) {
        workers_.emplace_back([this] { workerLoop(); });
    }
}

ThreadPool::~ThreadPool() {
    shutdown();
}

bool ThreadPool::submit(TaskFn task, TaskPriority priority) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            return false;
        }

        Task queued;
        queued.run = std::move(task);
        queued.priority = priority;
        queue_->push(std::move(queued));
    }

    cv_.notify_one();
    return true;
}

bool ThreadPool::submit(std::function<void()> task, TaskPriority priority) {
    return submit(TaskFn([fn = std::move(task)]() {
                      fn();
                      return true;
                  }),
                  priority);
}

void ThreadPool::shutdown() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            return;
        }
        stopping_ = true;
    }

    cv_.notify_all();

    for (std::thread& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();
}

void ThreadPool::workerLoop() {
    for (;;) {
        Task task;

        {
            std::unique_lock<std::mutex> lock(mutex_);

            cv_.wait(lock, [this] { return stopping_ || !queue_->empty(); });

            if (stopping_ && queue_->empty()) {
                return;
            }

            if (!queue_->pop(task)) {
                continue;
            }
        }

        bool finished = true;
        try {
            finished = task.run();
        } catch (...) {
            finished = true;
        }

        tasksExecuted_.fetch_add(1, std::memory_order_relaxed);

        if (!finished) {
            taskYields_.fetch_add(1, std::memory_order_relaxed);
            {
                std::lock_guard<std::mutex> lock(mutex_);
                queue_->push(std::move(task));
            }
            cv_.notify_one();
        }
    }
}
}
}
