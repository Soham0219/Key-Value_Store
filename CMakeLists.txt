cmake_minimum_required(VERSION 3.16)

project(kvstore LANGUAGES CXX VERSION 1.0.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(KVSTORE_SANITIZE "Build with AddressSanitizer + UndefinedBehaviorSanitizer" ON)
option(KVSTORE_TSAN "Build with ThreadSanitizer instead (mutually exclusive with KVSTORE_SANITIZE)" OFF)

add_library(kvstore_flags INTERFACE)

target_compile_options(kvstore_flags INTERFACE
  -Wall
  -Wextra
  -Wpedantic
  -g
)

if(KVSTORE_TSAN)
  target_compile_options(kvstore_flags INTERFACE -fsanitize=thread)
  target_link_options(kvstore_flags INTERFACE -fsanitize=thread)
elseif(KVSTORE_SANITIZE)
  target_compile_options(kvstore_flags INTERFACE
    -fsanitize=address,undefined
    -fno-omit-frame-pointer
  )
  target_link_options(kvstore_flags INTERFACE -fsanitize=address,undefined)
endif()

find_package(Threads REQUIRED)

add_library(kvstore_core STATIC
  src/net/socket.cpp
  src/net/server.cpp
  src/net/connection.cpp
  src/net/poller.cpp
  src/net/event_loop.cpp
  src/pool/thread_pool.cpp
  src/pool/task_queue.cpp
  src/protocol/parser.cpp
  src/protocol/session.cpp
  src/protocol/command.cpp
  src/protocol/commands.cpp
  src/store/store.cpp
  src/store/sharded_map.cpp
  src/store/eviction_policy.cpp
  src/store/lru_policy.cpp
  src/store/record_applier.cpp
  src/wal/record.cpp
  src/wal/wal.cpp
  src/wal/compactor.cpp
  src/txn/transaction.cpp
  src/repl/replicator.cpp
  src/repl/leader.cpp
  src/repl/follower.cpp
)

target_include_directories(kvstore_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(kvstore_core PUBLIC kvstore_flags Threads::Threads)

add_executable(kvstore src/main.cpp)
target_link_libraries(kvstore PRIVATE kvstore_core)

set(KVSTORE_TESTS
  networking
  concurrency
  eviction
  persistence
  transactions
  scheduling
  replication
  event_loop
)

foreach(suite IN LISTS KVSTORE_TESTS)
  add_executable(test_${suite} tests/test_${suite}.cpp)
  target_link_libraries(test_${suite} PRIVATE kvstore_core)
endforeach()

add_executable(benchmark bench/benchmark.cpp)
target_link_libraries(benchmark PRIVATE kvstore_core)

add_executable(scheduler_bench bench/scheduler_bench.cpp)
target_link_libraries(scheduler_bench PRIVATE kvstore_core)

enable_testing()

foreach(suite IN LISTS KVSTORE_TESTS)
  add_test(NAME ${suite} COMMAND test_${suite})
endforeach()
