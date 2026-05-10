#include "db/btree.hpp"

#include <cmath>

namespace db {

std::uint64_t logical_btree_cost(std::size_t entry_count) {
    if (entry_count <= 1) {
        return 1;
    }
    return static_cast<std::uint64_t>(std::ceil(std::log2(static_cast<double>(entry_count + 1))));
}

std::optional<RowId> PrimaryIndex::find(VideoId video_id, Metrics* metrics) const {
    if (metrics != nullptr) {
        metrics->add_primary_lookup(logical_btree_cost(entries_.size()));
    }
    const auto it = entries_.find(video_id);
    if (it == entries_.end()) {
        return std::nullopt;
    }
    return it->second;
}

void PrimaryIndex::insert(VideoId video_id, RowId row_id) {
    entries_[video_id] = row_id;
}

void PrimaryIndex::erase(VideoId video_id) {
    entries_.erase(video_id);
}

std::size_t PrimaryIndex::size() const {
    return entries_.size();
}

void ViewCountIndex::insert(std::uint64_t view_count, RowId row_id, Metrics* metrics) {
    if (metrics != nullptr) {
        metrics->add_secondary_insert(logical_btree_cost(entries_.size()));
    }
    entries_[view_count].insert(row_id);
}

void ViewCountIndex::erase(std::uint64_t view_count, RowId row_id, Metrics* metrics) {
    if (metrics != nullptr) {
        metrics->add_secondary_delete(logical_btree_cost(entries_.size()));
    }
    const auto it = entries_.find(view_count);
    if (it == entries_.end()) {
        return;
    }
    it->second.erase(row_id);
    if (it->second.empty()) {
        entries_.erase(it);
    }
}

bool ViewCountIndex::contains(std::uint64_t view_count, RowId row_id) const {
    const auto it = entries_.find(view_count);
    if (it == entries_.end()) {
        return false;
    }
    return it->second.find(row_id) != it->second.end();
}

std::size_t ViewCountIndex::row_count_for_key(std::uint64_t view_count) const {
    const auto it = entries_.find(view_count);
    if (it == entries_.end()) {
        return 0;
    }
    return it->second.size();
}

std::size_t ViewCountIndex::distinct_key_count() const {
    return entries_.size();
}

}  // namespace db
