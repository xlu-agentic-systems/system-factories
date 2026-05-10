#include "db/table.hpp"

#include <sstream>
#include <stdexcept>
#include <unordered_set>

namespace db {

VideoTable::VideoTable(std::string wal_path) : wal_(std::move(wal_path)) {}

VideoTable::VideoTable(std::vector<VideoSeed> seeds, std::string wal_path)
    : wal_(std::move(wal_path)) {
    std::lock_guard<std::mutex> guard(mutex_);
    for (const auto& seed : seeds) {
        insert_video_unlocked(seed.video_id, seed.view_count, seed.title);
    }
}

VideoTable::VideoTable(const std::vector<VideoSeed>& seeds, const std::vector<WalRecord>& records) {
    {
        std::lock_guard<std::mutex> guard(mutex_);
        for (const auto& seed : seeds) {
            insert_video_unlocked(seed.video_id, seed.view_count, seed.title);
        }
    }

    std::unordered_set<TxId> committed;
    for (const auto& record : records) {
        if (record.type == WalRecordType::Commit) {
            committed.insert(record.tx_id);
        }
    }

    for (const auto& record : records) {
        if (record.type != WalRecordType::UpdateRow) {
            continue;
        }
        if (committed.find(record.tx_id) == committed.end()) {
            continue;
        }
        apply_recovered_update(record.video_id, record.new_view_count);
    }
}

RowId VideoTable::insert_video(VideoId video_id, std::uint64_t view_count, std::string title) {
    std::lock_guard<std::mutex> guard(mutex_);
    return insert_video_unlocked(video_id, view_count, std::move(title));
}

RowId VideoTable::insert_video_unlocked(VideoId video_id,
                                        std::uint64_t view_count,
                                        std::string title) {
    if (primary_index_.find(video_id, nullptr).has_value()) {
        throw std::invalid_argument("duplicate video_id");
    }
    const RowId row_id = static_cast<RowId>(rows_.size());
    rows_.push_back(VideoRow{row_id, video_id, view_count, std::move(title)});
    primary_index_.insert(video_id, row_id);
    view_count_index_.insert(view_count, row_id, nullptr);
    return row_id;
}

UpdateTrace VideoTable::increment_view(VideoId video_id) {
    UpdateTrace trace;

    std::optional<RowId> maybe_row_id;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        maybe_row_id = primary_index_.find(video_id, &metrics_);
    }
    if (!maybe_row_id.has_value()) {
        throw std::out_of_range("video_id not found");
    }

    const RowId row_id = *maybe_row_id;
    trace.steps.push_back("1. primary-key B-tree lookup: video_id -> row_id");
    auto row_lock = lock_manager_.lock_row(row_id, &metrics_);
    trace.steps.push_back("2. row lock acquired for the target tuple");

    const TxId tx_id = tx_ids_.next();
    std::uint64_t old_count = 0;
    std::uint64_t new_count = 0;

    {
        std::lock_guard<std::mutex> guard(mutex_);
        old_count = rows_.at(row_id).view_count;
        new_count = old_count + 1;
    }

    wal_.append(tx_id, WalRecordType::Begin, row_id, video_id, old_count, new_count, &metrics_);
    wal_.append(tx_id, WalRecordType::UpdateRow, row_id, video_id, old_count, new_count, &metrics_);
    wal_.append(tx_id,
                WalRecordType::DeleteSecondaryIndex,
                row_id,
                video_id,
                old_count,
                new_count,
                &metrics_);
    wal_.append(tx_id,
                WalRecordType::InsertSecondaryIndex,
                row_id,
                video_id,
                old_count,
                new_count,
                &metrics_);
    trace.steps.push_back("3. WAL records appended before mutating heap/index state");

    {
        std::lock_guard<std::mutex> guard(mutex_);
        rows_.at(row_id).view_count = new_count;
        metrics_.add_row_update(false);
        metrics_.add_dirty_pages(1);
        trace.steps.push_back("4. heap row updated: view_count old -> new");

        view_count_index_.erase(old_count, row_id, &metrics_);
        trace.steps.push_back("5. secondary index delete: remove old view_count entry");

        view_count_index_.insert(new_count, row_id, &metrics_);
        trace.steps.push_back("6. secondary index insert: add new view_count entry");
        metrics_.add_dirty_pages(2);
    }

    wal_.append(tx_id, WalRecordType::Commit, row_id, video_id, old_count, new_count, &metrics_);
    trace.steps.push_back("7. COMMIT WAL record appended");

    trace.old_view_count = old_count;
    trace.new_view_count = new_count;
    trace.metrics_after = metrics_.snapshot();
    return trace;
}

void VideoTable::update_title(VideoId video_id, const std::string& title) {
    std::optional<RowId> maybe_row_id;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        maybe_row_id = primary_index_.find(video_id, &metrics_);
    }
    if (!maybe_row_id.has_value()) {
        throw std::out_of_range("video_id not found");
    }

    auto row_lock = lock_manager_.lock_row(*maybe_row_id, &metrics_);
    const TxId tx_id = tx_ids_.next();

    wal_.append(tx_id, WalRecordType::Begin, *maybe_row_id, video_id, 0, 0, &metrics_);
    {
        std::lock_guard<std::mutex> guard(mutex_);
        rows_.at(*maybe_row_id).title = title;
        metrics_.add_row_update(true);
        metrics_.add_dirty_pages(1);
    }
    wal_.append(tx_id, WalRecordType::Commit, *maybe_row_id, video_id, 0, 0, &metrics_);
}

void VideoTable::append_uncommitted_increment_for_demo(VideoId video_id) {
    std::optional<RowId> maybe_row_id;
    std::uint64_t old_count = 0;
    std::uint64_t new_count = 0;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        maybe_row_id = primary_index_.find(video_id, &metrics_);
        if (!maybe_row_id.has_value()) {
            throw std::out_of_range("video_id not found");
        }
        old_count = rows_.at(*maybe_row_id).view_count;
        new_count = old_count + 1;
    }

    const TxId tx_id = tx_ids_.next();
    wal_.append(tx_id, WalRecordType::Begin, *maybe_row_id, video_id, old_count, new_count, &metrics_);
    wal_.append(tx_id, WalRecordType::UpdateRow, *maybe_row_id, video_id, old_count, new_count, &metrics_);
}

std::optional<VideoRow> VideoTable::get_by_video_id(VideoId video_id) const {
    std::lock_guard<std::mutex> guard(mutex_);
    auto maybe_row_id = primary_index_.find(video_id, nullptr);
    if (!maybe_row_id.has_value()) {
        return std::nullopt;
    }
    return rows_.at(*maybe_row_id);
}

bool VideoTable::view_index_contains(std::uint64_t view_count, RowId row_id) const {
    std::lock_guard<std::mutex> guard(mutex_);
    return view_count_index_.contains(view_count, row_id);
}

std::size_t VideoTable::view_index_row_count(std::uint64_t view_count) const {
    std::lock_guard<std::mutex> guard(mutex_);
    return view_count_index_.row_count_for_key(view_count);
}

MetricsSnapshot VideoTable::metrics() const {
    return metrics_.snapshot();
}

std::string VideoTable::metrics_report() const {
    return metrics_.report();
}

const Wal& VideoTable::wal() const {
    return wal_;
}

VideoTable VideoTable::recover_from_wal(const std::vector<VideoSeed>& seeds,
                                        const std::string& wal_path) {
    return VideoTable(seeds, Wal::read_file(wal_path));
}

void VideoTable::apply_recovered_update(VideoId video_id, std::uint64_t new_view_count) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto maybe_row_id = primary_index_.find(video_id, nullptr);
    if (!maybe_row_id.has_value()) {
        return;
    }
    const RowId row_id = *maybe_row_id;
    const auto old_view_count = rows_.at(row_id).view_count;
    rows_.at(row_id).view_count = new_view_count;
    view_count_index_.erase(old_view_count, row_id, nullptr);
    view_count_index_.insert(new_view_count, row_id, nullptr);
}

}  // namespace db
