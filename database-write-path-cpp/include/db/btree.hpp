#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <set>

#include "db/metrics.hpp"
#include "db/row.hpp"

namespace db {

std::uint64_t logical_btree_cost(std::size_t entry_count);

class PrimaryIndex {
public:
    std::optional<RowId> find(VideoId video_id, Metrics* metrics) const;
    void insert(VideoId video_id, RowId row_id);
    void erase(VideoId video_id);
    std::size_t size() const;

private:
    std::map<VideoId, RowId> entries_;
};

class ViewCountIndex {
public:
    void insert(std::uint64_t view_count, RowId row_id, Metrics* metrics);
    void erase(std::uint64_t view_count, RowId row_id, Metrics* metrics);
    bool contains(std::uint64_t view_count, RowId row_id) const;
    std::size_t row_count_for_key(std::uint64_t view_count) const;
    std::size_t distinct_key_count() const;

private:
    std::map<std::uint64_t, std::set<RowId>> entries_;
};

}  // namespace db
