#pragma once

#include <atomic>
#include <cstdint>
#include <string>

namespace db {

struct MetricsSnapshot {
    std::uint64_t primary_lookups = 0;
    std::uint64_t secondary_inserts = 0;
    std::uint64_t secondary_deletes = 0;
    std::uint64_t logical_btree_steps = 0;
    std::uint64_t wal_records = 0;
    std::uint64_t wal_bytes = 0;
    std::uint64_t row_updates = 0;
    std::uint64_t hot_updates = 0;
    std::uint64_t non_hot_updates = 0;
    std::uint64_t dirty_pages = 0;
    std::uint64_t lock_wait_ns = 0;
};

class Metrics {
public:
    void reset();
    MetricsSnapshot snapshot() const;
    std::string report() const;

    void add_primary_lookup(std::uint64_t btree_steps);
    void add_secondary_insert(std::uint64_t btree_steps);
    void add_secondary_delete(std::uint64_t btree_steps);
    void add_wal_record(std::uint64_t bytes);
    void add_row_update(bool hot_update);
    void add_dirty_pages(std::uint64_t pages);
    void add_lock_wait_ns(std::uint64_t ns);

private:
    std::atomic<std::uint64_t> primary_lookups_{0};
    std::atomic<std::uint64_t> secondary_inserts_{0};
    std::atomic<std::uint64_t> secondary_deletes_{0};
    std::atomic<std::uint64_t> logical_btree_steps_{0};
    std::atomic<std::uint64_t> wal_records_{0};
    std::atomic<std::uint64_t> wal_bytes_{0};
    std::atomic<std::uint64_t> row_updates_{0};
    std::atomic<std::uint64_t> hot_updates_{0};
    std::atomic<std::uint64_t> non_hot_updates_{0};
    std::atomic<std::uint64_t> dirty_pages_{0};
    std::atomic<std::uint64_t> lock_wait_ns_{0};
};

}  // namespace db
