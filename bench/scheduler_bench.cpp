#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include "pool/priority_queue.h"
#include "pool/rr_queue.h"
#include "pool/task_queue.h"
#include "pool/thread_pool.h"

namespace {

using Clock = std::chrono::steady_clock;

struct Config {
    kvstore::pool::SchedulingPolicy policy = kvstore::pool::SchedulingPolicy::Fcfs;
    bool aging = false;
    std::size_t threads = 4;
    std::size_t shortTasks = 20000;
    std::size_t longTasks = 200;
    int shortWorkUs = 10;
    int longWorkUs = 2000;
    int quantumUs = 200;
};

struct ClassStats {
    std::mutex mutex;
    std::vector<std::uint64_t> waitUs;
    std::vector<std::uint64_t> turnaroundUs;
    std::uint64_t completed = 0;

    void record(std::uint64_t wait, std::uint64_t turnaround) {
        std::lock_guard<std::mutex> lock(mutex);
        waitUs.push_back(wait);
        turnaroundUs.push_back(turnaround);
        ++completed;
    }
};

void burnCpu(int microseconds) {
    const Clock::time_point deadline = Clock::now() + std::chrono::microseconds(microseconds);

    volatile std::uint64_t sink = 0;
    while (Clock::now() < deadline) {
        for (int i = 0; i < 64; ++i) {
            sink = sink + static_cast<std::uint64_t>(i);
        }
    }
    (void)sink;
}

std::uint64_t percentile(std::vector<std::uint64_t> values, double fraction) {
    if (values.empty()) {
        return 0;
    }
    std::sort(values.begin(), values.end());
    std::size_t index = static_cast<std::size_t>(fraction * static_cast<double>(values.size()));
    if (index >= values.size()) {
        index = values.size() - 1;
    }
    return values[index];
}

std::uint64_t mean(const std::vector<std::uint64_t>& values) {
    if (values.empty()) {
        return 0;
    }
    std::uint64_t total = 0;
    for (std::uint64_t value : values) {
        total += value;
    }
    return total / values.size();
}

void runPolicy(const Config& config,
               kvstore::pool::SchedulingPolicy policy,
               bool aging,
               const char* label,
               std::uint64_t agingThreshold = 32) {
    using kvstore::pool::TaskPriority;

    kvstore::pool::ThreadPool pool(config.threads, policy, aging, agingThreshold);

    const bool yieldingEnabled = (policy == kvstore::pool::SchedulingPolicy::RoundRobin);

    ClassStats shortStats;
    ClassStats longStats;

    const std::size_t total = config.shortTasks + config.longTasks;
    const std::size_t longEvery =
        (config.longTasks > 0) ? std::max<std::size_t>(1, total / config.longTasks) : total + 1;

    const Clock::time_point runStart = Clock::now();
    std::size_t longSubmitted = 0;

    for (std::size_t i = 0; i < total; ++i) {
        const bool isLong = (config.longTasks > 0) && (i % longEvery == 0) &&
                            (longSubmitted < config.longTasks);
        const Clock::time_point enqueuedAt = Clock::now();

        if (isLong) {
            ++longSubmitted;

            auto remaining = std::make_shared<int>(config.longWorkUs);
            auto started = std::make_shared<Clock::time_point>();
            auto hasStarted = std::make_shared<bool>(false);

            const int quantum = yieldingEnabled ? config.quantumUs : config.longWorkUs;

            pool.submit(kvstore::pool::TaskFn([&longStats, remaining, started, hasStarted,
                                               enqueuedAt, quantum]() {
                            if (!*hasStarted) {
                                *started = Clock::now();
                                *hasStarted = true;
                            }
                            const int slice = std::min(quantum, *remaining);
                            burnCpu(slice);
                            *remaining -= slice;
                            if (*remaining > 0) {
                                return false;
                            }
                            const Clock::time_point done = Clock::now();
                            longStats.record(
                                static_cast<std::uint64_t>(
                                    std::chrono::duration_cast<std::chrono::microseconds>(
                                        *started - enqueuedAt).count()),
                                static_cast<std::uint64_t>(
                                    std::chrono::duration_cast<std::chrono::microseconds>(
                                        done - enqueuedAt).count()));
                            return true;
                        }),
                        TaskPriority::Low);
        } else {
            const int work = config.shortWorkUs;
            pool.submit(kvstore::pool::TaskFn([&shortStats, enqueuedAt, work]() {
                            const Clock::time_point started = Clock::now();
                            burnCpu(work);
                            const Clock::time_point done = Clock::now();
                            shortStats.record(
                                static_cast<std::uint64_t>(
                                    std::chrono::duration_cast<std::chrono::microseconds>(
                                        started - enqueuedAt).count()),
                                static_cast<std::uint64_t>(
                                    std::chrono::duration_cast<std::chrono::microseconds>(
                                        done - enqueuedAt).count()));
                            return true;
                        }),
                        TaskPriority::High);
        }
    }

    pool.shutdown();
    const double elapsedSeconds = std::chrono::duration<double>(Clock::now() - runStart).count();

    const double throughput =
        (elapsedSeconds > 0.0) ? static_cast<double>(total) / elapsedSeconds : 0.0;

    std::printf("%-18s %-7s %9llu %9llu %9llu %9llu %11.0f\n",
                label, "short",
                static_cast<unsigned long long>(mean(shortStats.waitUs)),
                static_cast<unsigned long long>(percentile(shortStats.waitUs, 0.95)),
                static_cast<unsigned long long>(percentile(shortStats.waitUs, 0.99)),
                static_cast<unsigned long long>(mean(shortStats.turnaroundUs)),
                throughput);
    std::printf("%-18s %-7s %9llu %9llu %9llu %9llu %11s\n",
                "", "long",
                static_cast<unsigned long long>(mean(longStats.waitUs)),
                static_cast<unsigned long long>(percentile(longStats.waitUs, 0.95)),
                static_cast<unsigned long long>(percentile(longStats.waitUs, 0.99)),
                static_cast<unsigned long long>(mean(longStats.turnaroundUs)),
                "");

    const std::uint64_t longDone = longStats.completed;
    if (longDone < config.longTasks) {
        std::printf("%-18s %-7s STARVED: only %llu of %zu long tasks finished before drain\n",
                    "", "",
                    static_cast<unsigned long long>(longDone), config.longTasks);
    }

    std::printf("%-18s %-7s tasks=%llu yields=%llu\n", "", "",
                static_cast<unsigned long long>(pool.tasksExecuted()),
                static_cast<unsigned long long>(pool.taskYields()));
}

void printUsage(const char* program) {
    std::cerr << "usage: " << program << " [options]\n"
              << "  --threads N        worker threads (default 4)\n"
              << "  --short-tasks N    number of short tasks (default 20000)\n"
              << "  --long-tasks N     number of long tasks (default 200)\n"
              << "  --short-work-us N  CPU microseconds per short task (default 10)\n"
              << "  --long-work-us N   CPU microseconds per long task (default 2000)\n"
              << "  --quantum-us N     round-robin slice for long tasks (default 200)\n";
}

bool parseArgs(int argc, char** argv, Config& config) {
    for (int i = 1; i < argc; ++i) {
        const bool hasValue = (i + 1 < argc);
        if (std::strcmp(argv[i], "--threads") == 0 && hasValue) {
            config.threads = std::strtoul(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--short-tasks") == 0 && hasValue) {
            config.shortTasks = std::strtoul(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--long-tasks") == 0 && hasValue) {
            config.longTasks = std::strtoul(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--short-work-us") == 0 && hasValue) {
            config.shortWorkUs = static_cast<int>(std::strtol(argv[++i], nullptr, 10));
        } else if (std::strcmp(argv[i], "--long-work-us") == 0 && hasValue) {
            config.longWorkUs = static_cast<int>(std::strtol(argv[++i], nullptr, 10));
        } else if (std::strcmp(argv[i], "--quantum-us") == 0 && hasValue) {
            config.quantumUs = static_cast<int>(std::strtol(argv[++i], nullptr, 10));
        } else {
            return false;
        }
    }
    return config.threads > 0 && config.quantumUs > 0;
}
}

int main(int argc, char** argv) {
    Config config;
    if (!parseArgs(argc, argv, config)) {
        printUsage(argv[0]);
        return EXIT_FAILURE;
    }

    std::printf("\nthreads=%zu  short=%zu x %dus (high priority)  long=%zu x %dus (low priority)"
                "  quantum=%dus\n\n",
                config.threads, config.shortTasks, config.shortWorkUs,
                config.longTasks, config.longWorkUs, config.quantumUs);

    std::printf("%-18s %-7s %9s %9s %9s %9s %11s\n",
                "policy", "class", "wait avg", "wait p95", "wait p99", "turn avg", "tasks/sec");
    std::printf("%-18s %-7s %9s %9s %9s %9s %11s\n",
                "", "", "(us)", "(us)", "(us)", "(us)", "");

    runPolicy(config, kvstore::pool::SchedulingPolicy::Fcfs, false, "fcfs");
    runPolicy(config, kvstore::pool::SchedulingPolicy::Priority, false, "priority");

    runPolicy(config, kvstore::pool::SchedulingPolicy::Priority, true, "aging(32)", 32);
    runPolicy(config, kvstore::pool::SchedulingPolicy::Priority, true, "aging(256)", 256);
    runPolicy(config, kvstore::pool::SchedulingPolicy::RoundRobin, false, "round-robin");

    std::printf(R"(
Reading these:

  wait      enqueue -> first time on a CPU. This is what a scheduling policy
            actually controls, and the column to compare first.

  turn avg  enqueue -> finished. For long tasks under round-robin this INCLUDES
            the time spent waiting between slices, so it rises even as short-task
            waits fall. Interleaving does not make the long work faster; it stops
            it monopolising the pool.

  tasks/sec Should be roughly EQUAL across all four rows, and that is expected:
            the same total work is done either way. Scheduling does not create
            capacity, it decides who waits. Compare the wait columns, not this one.

  fcfs      The CONVOY EFFECT. Short tasks queue behind whatever long task landed
            in front of them, so their wait is set by work that has nothing to do
            with them. Nobody starves, but the fast are punished for the slow.

  priority  Short tasks jump the queue, so their wait collapses. Watch the long
            row: if a STARVED line appears, low-priority work never ran at all.
            That is strict priority working exactly as specified, and it is why
            "reads before writes" is a dangerous thing to ship unqualified.

  aging     Compare the TWO aging rows before concluding anything. The threshold
            counts TASKS, not WORK, and here a long task costs 200x a short one —
            so at 32 the low class is promoted so often that short-task waits end
            up WORSE than under FCFS. Aging did not soften priority, it inverted
            it. At 512 the advantage survives and the long tasks still complete.

            The lesson is not "pick a bigger number": a count-based threshold is
            the wrong instrument when task costs differ. Real schedulers age by
            accumulated waiting TIME or a work-weighted credit — Linux's CFS uses
            virtual runtime for precisely this reason.

  round-rob Long tasks yield every quantum and go to the back, so short tasks
            interleave instead of queueing. Short waits improve over FCFS without
            any priority information at all. Note `yields` in the diagnostic line:
            zero yields means the policy had nothing to do and the row is
            meaningless.

  THE CAVEAT: these are numbers about the THREAD POOL, not about the server. The
  server runs one long-lived task per connection, so its queue is never deep
  enough for a policy to matter. Making scheduling matter end to end needs one
  task per command, which needs an event loop owning the sockets.
)");

    return EXIT_SUCCESS;
}
