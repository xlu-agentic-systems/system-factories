#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "db/metrics.hpp"
#include "db/row.hpp"

namespace db {

enum class WalRecordType {
    Begin,
    UpdateRow,
    DeleteSecondaryIndex,
    InsertSecondaryIndex,
    Commit
};

struct WalRecord {
    Lsn lsn = 0;
    TxId tx_id = 0;
    WalRecordType type = WalRecordType::Begin;
    RowId row_id = 0;
    VideoId video_id = 0;
    std::uint64_t old_view_count = 0;
    std::uint64_t new_view_count = 0;
};

std::string to_string(WalRecordType type);
WalRecordType wal_record_type_from_string(const std::string& value);

class Wal {
public:
    explicit Wal(std::string path = {});

    WalRecord append(TxId tx_id,
                     WalRecordType type,
                     RowId row_id,
                     VideoId video_id,
                     std::uint64_t old_view_count,
                     std::uint64_t new_view_count,
                     Metrics* metrics);

    std::vector<WalRecord> records() const;
    const std::string& path() const;

    static std::string serialize(const WalRecord& record);
    static std::optional<WalRecord> parse_line(const std::string& line);
    static std::vector<WalRecord> read_file(const std::string& path);

private:
    mutable std::mutex mutex_;
    std::string path_;
    Lsn next_lsn_ = 1;
    std::vector<WalRecord> records_;
};

}  // namespace db
