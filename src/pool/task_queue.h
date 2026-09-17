#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace kvstore {
namespace pool {

using TaskFn = std::function<bool()>;

enum class TaskPriority {
    High = 0,
    Normal = 1,
    Low = 2
};

constexpr std::size_t kPriorityLevels = 3;

const char* taskPriorityName(TaskPriority priority);

struct Task {
    TaskFn run;
    TaskPriority priority = TaskPriority::Normal;
    std::uint64_t sequence = 0;
};

class TaskQueue {
public:
    virtual ~TaskQueue();

    virtual void push(Task task) = 0;

    virtual bool pop(Task& out) = 0;

    virtual bool empty() const = 0;
    virtual std::size_t size() const = 0;
    virtual const char* name() const = 0;

protected:
    TaskQueue() = default;
    TaskQueue(const TaskQueue&) = delete;
    TaskQueue& operator=(const TaskQueue&) = delete;
};

enum class SchedulingPolicy {
    Fcfs,
    Priority,
    RoundRobin
};

bool parseSchedulingPolicy(const std::string& text, SchedulingPolicy& out);
const char* schedulingPolicyName(SchedulingPolicy policy);

std::unique_ptr<TaskQueue> makeTaskQueue(SchedulingPolicy policy, bool aging,
                                         std::uint64_t agingThreshold = 32);
}
}
