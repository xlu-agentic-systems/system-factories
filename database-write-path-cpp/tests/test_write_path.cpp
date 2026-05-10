#include <cassert>
#include <cstdio>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "db/table.hpp"

namespace {

void test_indexed_increment_moves_secondary_index() {
    db::VideoTable table({db::VideoSeed{7, 10, "demo"}});
    const auto trace = table.increment_view(7);
    const auto row = table.get_by_video_id(7).value();

    assert(row.view_count == 11);
    assert(trace.old_view_count == 10);
    assert(trace.new_view_count == 11);
    assert(!table.view_index_contains(10, row.row_id));
    assert(table.view_index_contains(11, row.row_id));

    const auto metrics = table.metrics();
    assert(metrics.primary_lookups == 1);
    assert(metrics.secondary_deletes == 1);
    assert(metrics.secondary_inserts == 1);
    assert(metrics.non_hot_updates == 1);
    assert(metrics.wal_records == 5);
}

void test_unindexed_title_update_is_hot_like() {
    db::VideoTable table({db::VideoSeed{8, 20, "old"}});
    table.update_title(8, "new");
    const auto row = table.get_by_video_id(8).value();
    const auto metrics = table.metrics();

    assert(row.title == "new");
    assert(row.view_count == 20);
    assert(metrics.hot_updates == 1);
    assert(metrics.secondary_deletes == 0);
    assert(metrics.secondary_inserts == 0);
}

void test_wal_recovery_replays_only_committed_updates() {
    const std::string wal_path = "/tmp/db_write_path_wal_test.log";
    std::remove(wal_path.c_str());

    {
        db::VideoTable table({db::VideoSeed{9, 30, "recover"}}, wal_path);
        table.increment_view(9);
        table.append_uncommitted_increment_for_demo(9);
    }

    auto recovered = db::VideoTable::recover_from_wal({db::VideoSeed{9, 30, "recover"}}, wal_path);
    const auto row = recovered.get_by_video_id(9).value();
    assert(row.view_count == 31);
    assert(recovered.view_index_contains(31, row.row_id));
    assert(!recovered.view_index_contains(32, row.row_id));

    std::remove(wal_path.c_str());
}

void test_hot_row_contention_preserves_all_updates() {
    db::VideoTable table({db::VideoSeed{1, 0, "viral"}});
    constexpr int threads = 4;
    constexpr int updates_per_thread = 250;
    std::vector<std::thread> workers;

    for (int i = 0; i < threads; ++i) {
        workers.emplace_back([&table]() {
            for (int j = 0; j < updates_per_thread; ++j) {
                table.increment_view(1);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }

    const auto row = table.get_by_video_id(1).value();
    const auto metrics = table.metrics();
    assert(row.view_count == threads * updates_per_thread);
    assert(metrics.row_updates == threads * updates_per_thread);
    assert(metrics.secondary_deletes == threads * updates_per_thread);
    assert(metrics.secondary_inserts == threads * updates_per_thread);
}

}  // namespace

int main() {
    test_indexed_increment_moves_secondary_index();
    test_unindexed_title_update_is_hot_like();
    test_wal_recovery_replays_only_committed_updates();
    test_hot_row_contention_preserves_all_updates();
    return 0;
}
