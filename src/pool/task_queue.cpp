#include "pool/task_queue.h"

#include <utility>

#include "pool/fcfs_queue.h"
#include "pool/priority_queue.h"
#include "pool/rr_queue.h"

namespace kvstore {
namespace pool {

TaskQueue::~TaskQueue() = default;

const char* taskPriorityName(TaskPriority priority) {
    switch (priority) {
        case TaskPriority::High:   return "high";
        case TaskPriority::Normal: return "normal";
        case TaskPriority::Low:    return "low";
    }
    return "unknown";
}

bool parseSchedulingPolicy(const std::string& text, SchedulingPolicy& out) {
    if (text == "fcfs") {
        out = SchedulingPolicy::Fcfs;
    } else if (text == "priority") {
        out = SchedulingPolicy::Priority;
    } else if (text == "rr" || text == "round-robin") {
        out = SchedulingPolicy::RoundRobin;
    } else {
        return false;
    }
    return true;
}

const char* schedulingPolicyName(SchedulingPolicy policy) {
    switch (policy) {
        case SchedulingPolicy::Fcfs:       return "fcfs";
        case SchedulingPolicy::Priority:   return "priority";
        case SchedulingPolicy::RoundRobin: return "round-robin";
    }
    return "unknown";
}

std::unique_ptr<TaskQueue> makeTaskQueue(SchedulingPolicy policy, bool aging,
                                         std::uint64_t agingThreshold) {
    switch (policy) {
        case SchedulingPolicy::Priority:
            return std::make_unique<PriorityQueue>(aging, agingThreshold);
        case SchedulingPolicy::RoundRobin:
            return std::make_unique<RoundRobinQueue>();
        case SchedulingPolicy::Fcfs:
        default:

            return std::make_unique<FcfsQueue>();
    }
}

FcfsQueue::~FcfsQueue() = default;

void FcfsQueue::push(Task task) {
    task.sequence = nextSequence_++;
    tasks_.push_back(std::move(task));
}

bool FcfsQueue::pop(Task& out) {
    if (tasks_.empty()) {
        return false;
    }
    out = std::move(tasks_.front());
    tasks_.pop_front();
    return true;
}

PriorityQueue::PriorityQueue(bool aging, std::uint64_t agingThreshold)
    : aging_(aging),

      agingThreshold_(agingThreshold == 0 ? 1 : agingThreshold) {}

PriorityQueue::~PriorityQueue() = default;

void PriorityQueue::push(Task task) {
    task.sequence = ++nextSequence_;

    const std::size_t level = static_cast<std::size_t>(task.priority);
    levels_[level].push_back(std::move(task));
    ++count_;
}

bool PriorityQueue::pop(Task& out) {
    if (count_ == 0) {
        return false;
    }

    if (aging_ && servesSincePromotion_ >= agingThreshold_) {
        for (std::size_t level = kPriorityLevels; level-- > 1;) {
            if (levels_[level].empty()) {
                continue;
            }
            out = std::move(levels_[level].front());
            levels_[level].pop_front();
            --count_;
            ++served_;
            ++promotions_;
            servesSincePromotion_ = 0;
            return true;
        }
    }

    for (std::size_t level = 0; level < kPriorityLevels; ++level) {
        if (levels_[level].empty()) {
            continue;
        }
        out = std::move(levels_[level].front());
        levels_[level].pop_front();
        --count_;
        ++served_;
        ++servesSincePromotion_;
        return true;
    }
    return false;
}

RoundRobinQueue::~RoundRobinQueue() = default;

void RoundRobinQueue::push(Task task) {
    if (task.sequence != 0) {
        ++requeues_;
    }
    task.sequence = ++nextSequence_;
    tasks_.push_back(std::move(task));
}

bool RoundRobinQueue::pop(Task& out) {
    if (tasks_.empty()) {
        return false;
    }
    out = std::move(tasks_.front());
    tasks_.pop_front();
    return true;
}
}
}
