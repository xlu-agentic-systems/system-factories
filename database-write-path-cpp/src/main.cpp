#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "db/table.hpp"

namespace {

void print_usage(const char* binary) {
    std::cout << "Usage:\n"
              << "  " << binary << " trace\n"
              << "  " << binary << " hot-row [threads] [updates_per_thread]\n"
              << "  " << binary << " sharded [shards] [threads] [updates_per_thread]\n";
}

std::uint64_t arg_or_default(char** argv, int argc, int index, std::uint64_t fallback) {
    if (argc <= index) {
        return fallback;
    }
    return static_cast<std::uint64_t>(std::strtoull(argv[index], nullptr, 10));
}

void run_trace() {
    db::VideoTable table({db::VideoSeed{42, 100, "system design interview"}});
    const auto trace = table.increment_view(42);

    std::cout << "UPDATE videos SET view_count = view_count + 1 WHERE video_id = 42;\n\n";
    for (const auto& step : trace.steps) {
        std::cout << step << '\n';
    }
    std::cout << "\nview_count: " << trace.old_view_count << " -> " << trace.new_view_count << "\n\n";
    std::cout << table.metrics_report() << '\n';
}

void run_hot_row(std::uint64_t threads, std::uint64_t updates_per_thread) {
    db::VideoTable table({db::VideoSeed{1, 0, "viral video"}});
    std::vector<std::thread> workers;
    const auto start = std::chrono::steady_clock::now();
    for (std::uint64_t i = 0; i < threads; ++i) {
        workers.emplace_back([&table, updates_per_thread]() {
            for (std::uint64_t j = 0; j < updates_per_thread; ++j) {
                table.increment_view(1);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    const auto end = std::chrono::steady_clock::now();
    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    const auto row = table.get_by_video_id(1).value();
    std::cout << "hot-row updates=" << row.view_count << " elapsed_ms=" << elapsed_ms << '\n';
    std::cout << table.metrics_report() << '\n';
}

void run_sharded(std::uint64_t shards, std::uint64_t threads, std::uint64_t updates_per_thread) {
    std::vector<db::VideoSeed> seeds;
    for (std::uint64_t i = 0; i < shards; ++i) {
        seeds.push_back(db::VideoSeed{1000 + i, 0, "viral video shard"});
    }
    db::VideoTable table(seeds);
    std::vector<std::thread> workers;

    const auto start = std::chrono::steady_clock::now();
    for (std::uint64_t i = 0; i < threads; ++i) {
        workers.emplace_back([&table, shards, updates_per_thread, i]() {
            for (std::uint64_t j = 0; j < updates_per_thread; ++j) {
                const auto shard = (i + j) % shards;
                table.increment_view(1000 + shard);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    const auto end = std::chrono::steady_clock::now();
    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    std::uint64_t total = 0;
    for (std::uint64_t i = 0; i < shards; ++i) {
        total += table.get_by_video_id(1000 + i).value().view_count;
    }
    std::cout << "sharded-counter shards=" << shards
              << " updates=" << total
              << " elapsed_ms=" << elapsed_ms << '\n';
    std::cout << table.metrics_report() << '\n';
}

}  // namespace

int main(int argc, char** argv) {
    const std::string mode = argc > 1 ? argv[1] : "trace";
    if (mode == "trace") {
        run_trace();
        return 0;
    }
    if (mode == "hot-row") {
        run_hot_row(arg_or_default(argv, argc, 2, 8), arg_or_default(argv, argc, 3, 1000));
        return 0;
    }
    if (mode == "sharded") {
        run_sharded(arg_or_default(argv, argc, 2, 16),
                    arg_or_default(argv, argc, 3, 8),
                    arg_or_default(argv, argc, 4, 1000));
        return 0;
    }
    print_usage(argv[0]);
    return 1;
}
