#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "db/btree.hpp"
#include "db/lock_manager.hpp"
#include "db/metrics.hpp"
#include "db/row.hpp"
#include "db/transaction.hpp"
#include "db/wal.hpp"

namespace db {

struct UpdateTrace {
    std::vector<std::string> steps;
    std::uint64_t old_view_count = 0;
    std::uint64_t new_view_count = 0;
    MetricsSnapshot metrics_after;
};

class VideoTable {
public:
    explicit VideoTable(std::string wal_path = {});
    VideoTable(std::vector<VideoSeed> seeds, std::string wal_path = {});

    RowId insert_video(VideoId video_id, std::uint64_t view_count, std::string title = {});
    UpdateTrace increment_view(VideoId video_id);
    void update_title(VideoId video_id, const std::string& title);
    void append_uncommitted_increment_for_demo(VideoId video_id);

    std::optional<VideoRow> get_by_video_id(VideoId video_id) const;
    bool view_index_contains(std::uint64_t view_count, RowId row_id) const;
    std::size_t view_index_row_count(std::uint64_t view_count) const;
    MetricsSnapshot metrics() const;
    std::string metrics_report() const;
    const Wal& wal() const;

    static VideoTable recover_from_wal(const std::vector<VideoSeed>& seeds,
                                       const std::string& wal_path);

private:
    VideoTable(const std::vector<VideoSeed>& seeds, const std::vector<WalRecord>& records);

    RowId insert_video_unlocked(VideoId video_id, std::uint64_t view_count, std::string title);
    void apply_recovered_update(VideoId video_id, std::uint64_t new_view_count);

    mutable std::mutex mutex_;
    std::vector<VideoRow> rows_;
    PrimaryIndex primary_index_;
    ViewCountIndex view_count_index_;
    LockManager lock_manager_;
    TransactionIdGenerator tx_ids_;
    Wal wal_;
    Metrics metrics_;
};

}  // namespace db
