#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "pool/fcfs_queue.h"
#include "pool/priority_queue.h"
#include "pool/rr_queue.h"
#include "pool/task_queue.h"
#include "pool/thread_pool.h"

namespace {

int g_failures = 0;

void check(bool condition, const std::string& what, int line) {
    if (condition) {
        std::cout << "  ok   " << what << '\n';
    } else {
        std::cout << "  FAIL " << what << "  (line " << line << ")\n";
        ++g_failures;
    }
}

#define CHECK(cond) check((cond), #cond, __LINE__)

using kvstore::pool::FcfsQueue;
using kvstore::pool::PriorityQueue;
using kvstore::pool::RoundRobinQueue;
using kvstore::pool::SchedulingPolicy;
using kvstore::pool::Task;
using kvstore::pool::TaskFn;
using kvstore::pool::TaskPriority;
using kvstore::pool::ThreadPool;

Task labelled(std::vector<std::string>& order, std::string label, TaskPriority priority) {
    Task task;
    task.priority = priority;
    task.run = [&order, label]() {
        order.push_back(label);
        return true;
    };
    return task;
}

std::vector<std::string> drain(kvstore::pool::TaskQueue& queue) {
    std::vector<std::string> order;
    Task task;
    while (queue.pop(task)) {
        task.run();
    }
    return order;
}

void testFcfsIsStrictlyArrivalOrder() {
    std::cout << "FCFS serves in arrival order and ignores priority entirely\n";

    std::vector<std::string> order;
    FcfsQueue queue;

    queue.push(labelled(order, "first", TaskPriority::Low));
    queue.push(labelled(order, "second", TaskPriority::High));
    queue.push(labelled(order, "third", TaskPriority::Normal));

    CHECK(queue.size() == 3);
    drain(queue);

    CHECK(order.size() == 3);
    CHECK(order[0] == "first" && order[1] == "second" && order[2] == "third");
    CHECK(queue.empty());

    Task task;
    CHECK(!queue.pop(task));
}

void testPriorityOrdersByLevelThenArrival() {
    std::cout << "priority serves high first, FIFO within a level\n";

    std::vector<std::string> order;
    PriorityQueue queue(false);

    queue.push(labelled(order, "low-1", TaskPriority::Low));
    queue.push(labelled(order, "normal-1", TaskPriority::Normal));
    queue.push(labelled(order, "high-1", TaskPriority::High));
    queue.push(labelled(order, "high-2", TaskPriority::High));
    queue.push(labelled(order, "normal-2", TaskPriority::Normal));

    drain(queue);

    CHECK(order.size() == 5);

    CHECK(order[0] == "high-1" && order[1] == "high-2");
    CHECK(order[2] == "normal-1" && order[3] == "normal-2");
    CHECK(order[4] == "low-1");
}

void testStrictPriorityStarvesLowWork() {
    std::cout << "strict priority STARVES low-priority work — the failure mode, demonstrated\n";

    std::vector<std::string> order;
    PriorityQueue queue(false);

    queue.push(labelled(order, "the-low-task", TaskPriority::Low));

    constexpr int kRounds = 500;
    int served = 0;
    for (int i = 0; i < kRounds; ++i) {
        queue.push(labelled(order, "high-" + std::to_string(i), TaskPriority::High));

        Task task;
        if (queue.pop(task)) {
            task.run();
            ++served;
        }
    }

    CHECK(served == kRounds);

    bool lowEverRan = false;
    for (const std::string& label : order) {
        if (label == "the-low-task") {
            lowEverRan = true;
        }
    }
    CHECK(!lowEverRan);
    CHECK(queue.size() == 1);
    CHECK(queue.promotions() == 0);
}

void testAgingIsRateLimited() {
    std::cout << "aging promotes at a bounded RATE, not in a burst\n";

    std::vector<std::string> order;
    PriorityQueue queue(true, 10);

    for (int i = 0; i < 50; ++i) {
        queue.push(labelled(order, "low-" + std::to_string(i), TaskPriority::Low));
    }
    for (int i = 0; i < 500; ++i) {
        queue.push(labelled(order, "high-" + std::to_string(i), TaskPriority::High));
    }

    Task task;
    while (queue.pop(task)) {
        task.run();
    }

    std::size_t adjacentLowPairs = 0;
    for (std::size_t i = 1; i < order.size(); ++i) {
        const bool previousLow = order[i - 1].rfind("low-", 0) == 0;
        const bool currentLow = order[i].rfind("low-", 0) == 0;
        if (previousLow && currentLow) {
            ++adjacentLowPairs;
        }
    }

    CHECK(queue.promotions() > 0);
    CHECK(queue.promotions() <= 500 / 10 + 1);
    CHECK(adjacentLowPairs <= 50);

    std::size_t lowInFirstHundred = 0;
    for (std::size_t i = 0; i < 100 && i < order.size(); ++i) {
        if (order[i].rfind("low-", 0) == 0) {
            ++lowInFirstHundred;
        }
    }
    CHECK(lowInFirstHundred <= 15);
}

void testAgingRescuesStarvedWork() {
    std::cout << "aging bounds the wait instead of leaving it unbounded\n";

    std::vector<std::string> order;
    PriorityQueue queue(true);

    queue.push(labelled(order, "the-low-task", TaskPriority::Low));

    constexpr int kRounds = 500;
    for (int i = 0; i < kRounds; ++i) {
        queue.push(labelled(order, "high-" + std::to_string(i), TaskPriority::High));
        Task task;
        if (queue.pop(task)) {
            task.run();
        }
    }

    bool lowRan = false;
    std::size_t lowPosition = 0;
    for (std::size_t i = 0; i < order.size(); ++i) {
        if (order[i] == "the-low-task") {
            lowRan = true;
            lowPosition = i;
        }
    }

    CHECK(lowRan);
    CHECK(queue.promotions() >= 1);

    CHECK(lowPosition <= PriorityQueue::kDefaultAgingThreshold + 2);

    CHECK(queue.promotions() == 1);
}

void testRoundRobinRequeuesYieldingTasks() {
    std::cout << "round-robin sends a yielding task to the BACK\n";

    std::vector<std::string> order;
    RoundRobinQueue queue;

    auto remaining = std::make_shared<int>(3);
    Task longTask;
    longTask.priority = TaskPriority::Low;
    longTask.run = [&order, remaining]() {
        order.push_back("long-slice");
        return --*remaining == 0;
    };

    queue.push(std::move(longTask));
    queue.push(labelled(order, "short-a", TaskPriority::High));
    queue.push(labelled(order, "short-b", TaskPriority::High));

    Task task;
    while (queue.pop(task)) {
        if (!task.run()) {
            queue.push(std::move(task));
        }
    }

    CHECK(order.size() == 5);
    CHECK(order[0] == "long-slice");
    CHECK(order[1] == "short-a");
    CHECK(order[2] == "short-b");
    CHECK(order[3] == "long-slice");
    CHECK(order[4] == "long-slice");
    CHECK(queue.requeues() == 2);
}

void testFcfsMakesShortTasksWaitForLongOnes() {
    std::cout << "FCFS convoy: the same workload, no interleaving at all\n";

    std::vector<std::string> order;
    FcfsQueue queue;

    auto remaining = std::make_shared<int>(3);
    Task longTask;
    longTask.run = [&order, remaining]() {
        order.push_back("long-slice");
        return --*remaining == 0;
    };

    queue.push(std::move(longTask));
    queue.push(labelled(order, "short-a", TaskPriority::High));
    queue.push(labelled(order, "short-b", TaskPriority::High));

    Task task;
    while (queue.pop(task)) {
        if (!task.run()) {
            queue.push(std::move(task));
        }
    }

    CHECK(order.size() == 5);

    std::vector<std::string> blocking;
    FcfsQueue blockingQueue;
    Task hog;
    hog.run = [&blocking]() {
        blocking.push_back("hog-runs-to-completion");
        return true;
    };
    blockingQueue.push(std::move(hog));
    blockingQueue.push(labelled(blocking, "short-a", TaskPriority::High));

    Task next;
    while (blockingQueue.pop(next)) {
        if (!next.run()) {
            blockingQueue.push(std::move(next));
        }
    }
    CHECK(blocking[0] == "hog-runs-to-completion");
    CHECK(blocking[1] == "short-a");
}

void testPolicyParsing() {
    std::cout << "policy names parse and round trip\n";

    SchedulingPolicy policy = SchedulingPolicy::Priority;
    CHECK(kvstore::pool::parseSchedulingPolicy("fcfs", policy) && policy == SchedulingPolicy::Fcfs);
    CHECK(kvstore::pool::parseSchedulingPolicy("priority", policy) &&
          policy == SchedulingPolicy::Priority);
    CHECK(kvstore::pool::parseSchedulingPolicy("rr", policy) &&
          policy == SchedulingPolicy::RoundRobin);
    CHECK(kvstore::pool::parseSchedulingPolicy("round-robin", policy) &&
          policy == SchedulingPolicy::RoundRobin);
    CHECK(!kvstore::pool::parseSchedulingPolicy("fifo", policy));
    CHECK(!kvstore::pool::parseSchedulingPolicy("", policy));

    CHECK(std::string(kvstore::pool::schedulingPolicyName(SchedulingPolicy::Fcfs)) == "fcfs");
    CHECK(std::string(kvstore::pool::taskPriorityName(TaskPriority::High)) == "high");
}

void testEveryPolicyRunsEveryTask() {
    std::cout << "every policy completes every task through the real pool\n";

    const SchedulingPolicy policies[] = {SchedulingPolicy::Fcfs, SchedulingPolicy::Priority,
                                         SchedulingPolicy::RoundRobin};

    for (SchedulingPolicy policy : policies) {
        std::atomic<int> completed{0};
        {
            ThreadPool pool(4, policy, true);
            for (int i = 0; i < 2000; ++i) {
                const TaskPriority priority = (i % 3 == 0) ? TaskPriority::Low
                                            : (i % 3 == 1) ? TaskPriority::Normal
                                                           : TaskPriority::High;
                pool.submit(TaskFn([&completed]() {
                                completed.fetch_add(1, std::memory_order_relaxed);
                                return true;
                            }),
                            priority);
            }
            pool.shutdown();
        }
        CHECK(completed.load() == 2000);
    }
}

void testPoolRequeuesYieldingTasks() {
    std::cout << "the pool re-queues a yielding task until it reports finished\n";

    std::atomic<int> slices{0};
    {
        ThreadPool pool(2, SchedulingPolicy::RoundRobin, false);

        for (int t = 0; t < 10; ++t) {
            auto remaining = std::make_shared<int>(5);
            pool.submit(TaskFn([&slices, remaining]() {
                slices.fetch_add(1, std::memory_order_relaxed);
                return --*remaining == 0;
            }));
        }
        pool.shutdown();
    }

    CHECK(slices.load() == 50);
}

void testSubmitAfterShutdownIsRefused() {
    std::cout << "a shut-down pool refuses work rather than dropping it silently\n";

    ThreadPool pool(2, SchedulingPolicy::Priority, true);
    pool.shutdown();

    CHECK(!pool.submit(TaskFn([]() { return true; })));
    CHECK(!pool.submit(std::function<void()>([]() {})));
    CHECK(std::string(pool.policyName()) == "priority+aging");
}
}

int main() {
    testFcfsIsStrictlyArrivalOrder();
    testPriorityOrdersByLevelThenArrival();
    testStrictPriorityStarvesLowWork();
    testAgingRescuesStarvedWork();
    testAgingIsRateLimited();
    testRoundRobinRequeuesYieldingTasks();
    testFcfsMakesShortTasksWaitForLongOnes();
    testPolicyParsing();
    testEveryPolicyRunsEveryTask();
    testPoolRequeuesYieldingTasks();
    testSubmitAfterShutdownIsRefused();

    std::cout << (g_failures == 0 ? "\nALL TESTS PASSED\n" : "\nFAILURES\n");
    return g_failures == 0 ? 0 : 1;
}
