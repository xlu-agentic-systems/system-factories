#include "db/metrics.hpp"

#include <sstream>

namespace db {

void Metrics::reset() {
    primary_lookups_ = 0;
    secondary_inserts_ = 0;
    secondary_deletes_ = 0;
    logical_btree_steps_ = 0;
    wal_records_ = 0;
    wal_bytes_ = 0;
    row_updates_ = 0;
    hot_updates_ = 0;
    non_hot_updates_ = 0;
    dirty_pages_ = 0;
    lock_wait_ns_ = 0;
}

MetricsSnapshot Metrics::snapshot() const {
    MetricsSnapshot snapshot;
    snapshot.primary_lookups = primary_lookups_.load();
    snapshot.secondary_inserts = secondary_inserts_.load();
    snapshot.secondary_deletes = secondary_deletes_.load();
    snapshot.logical_btree_steps = logical_btree_steps_.load();
    snapshot.wal_records = wal_records_.load();
    snapshot.wal_bytes = wal_bytes_.load();
    snapshot.row_updates = row_updates_.load();
    snapshot.hot_updates = hot_updates_.load();
    snapshot.non_hot_updates = non_hot_updates_.load();
    snapshot.dirty_pages = dirty_pages_.load();
    snapshot.lock_wait_ns = lock_wait_ns_.load();
    return snapshot;
}

std::string Metrics::report() const {
    const auto s = snapshot();
    std::ostringstream out;
    out << "primary_lookups=" << s.primary_lookups << '\n'
        << "secondary_deletes=" << s.secondary_deletes << '\n'
        << "secondary_inserts=" << s.secondary_inserts << '\n'
        << "logical_btree_steps=" << s.logical_btree_steps << '\n'
        << "wal_records=" << s.wal_records << '\n'
        << "wal_bytes=" << s.wal_bytes << '\n'
        << "row_updates=" << s.row_updates << '\n'
        << "hot_updates=" << s.hot_updates << '\n'
        << "non_hot_updates=" << s.non_hot_updates << '\n'
        << "dirty_pages=" << s.dirty_pages << '\n'
        << "lock_wait_ns=" << s.lock_wait_ns;
    return out.str();
}

void Metrics::add_primary_lookup(std::uint64_t btree_steps) {
    ++primary_lookups_;
    logical_btree_steps_ += btree_steps;
}

void Metrics::add_secondary_insert(std::uint64_t btree_steps) {
    ++secondary_inserts_;
    logical_btree_steps_ += btree_steps;
}

void Metrics::add_secondary_delete(std::uint64_t btree_steps) {
    ++secondary_deletes_;
    logical_btree_steps_ += btree_steps;
}

void Metrics::add_wal_record(std::uint64_t bytes) {
    ++wal_records_;
    wal_bytes_ += bytes;
}

void Metrics::add_row_update(bool hot_update) {
    ++row_updates_;
    if (hot_update) {
        ++hot_updates_;
    } else {
        ++non_hot_updates_;
    }
}

void Metrics::add_dirty_pages(std::uint64_t pages) {
    dirty_pages_ += pages;
}

void Metrics::add_lock_wait_ns(std::uint64_t ns) {
    lock_wait_ns_ += ns;
}

}  // namespace db
