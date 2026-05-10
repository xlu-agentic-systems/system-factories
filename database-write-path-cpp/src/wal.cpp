#include "db/wal.hpp"

#include <fstream>
#include <sstream>
#include <stdexcept>

namespace db {

std::string to_string(WalRecordType type) {
    switch (type) {
        case WalRecordType::Begin:
            return "BEGIN";
        case WalRecordType::UpdateRow:
            return "UPDATE_ROW";
        case WalRecordType::DeleteSecondaryIndex:
            return "DELETE_SECONDARY";
        case WalRecordType::InsertSecondaryIndex:
            return "INSERT_SECONDARY";
        case WalRecordType::Commit:
            return "COMMIT";
    }
    return "UNKNOWN";
}

WalRecordType wal_record_type_from_string(const std::string& value) {
    if (value == "BEGIN") {
        return WalRecordType::Begin;
    }
    if (value == "UPDATE_ROW") {
        return WalRecordType::UpdateRow;
    }
    if (value == "DELETE_SECONDARY") {
        return WalRecordType::DeleteSecondaryIndex;
    }
    if (value == "INSERT_SECONDARY") {
        return WalRecordType::InsertSecondaryIndex;
    }
    if (value == "COMMIT") {
        return WalRecordType::Commit;
    }
    throw std::invalid_argument("unknown WAL record type: " + value);
}

Wal::Wal(std::string path) : path_(std::move(path)) {}

WalRecord Wal::append(TxId tx_id,
                      WalRecordType type,
                      RowId row_id,
                      VideoId video_id,
                      std::uint64_t old_view_count,
                      std::uint64_t new_view_count,
                      Metrics* metrics) {
    std::lock_guard<std::mutex> guard(mutex_);
    WalRecord record;
    record.lsn = next_lsn_++;
    record.tx_id = tx_id;
    record.type = type;
    record.row_id = row_id;
    record.video_id = video_id;
    record.old_view_count = old_view_count;
    record.new_view_count = new_view_count;
    records_.push_back(record);

    const auto line = serialize(record);
    if (!path_.empty()) {
        std::ofstream file(path_, std::ios::app);
        file << line << '\n';
    }
    if (metrics != nullptr) {
        metrics->add_wal_record(line.size() + 1);
    }
    return record;
}

std::vector<WalRecord> Wal::records() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return records_;
}

const std::string& Wal::path() const {
    return path_;
}

std::string Wal::serialize(const WalRecord& record) {
    std::ostringstream out;
    out << record.lsn << '|'
        << record.tx_id << '|'
        << to_string(record.type) << '|'
        << record.row_id << '|'
        << record.video_id << '|'
        << record.old_view_count << '|'
        << record.new_view_count;
    return out.str();
}

std::optional<WalRecord> Wal::parse_line(const std::string& line) {
    if (line.empty()) {
        return std::nullopt;
    }

    std::vector<std::string> parts;
    std::stringstream stream(line);
    std::string part;
    while (std::getline(stream, part, '|')) {
        parts.push_back(part);
    }
    if (parts.size() != 7) {
        return std::nullopt;
    }

    WalRecord record;
    record.lsn = static_cast<Lsn>(std::stoull(parts[0]));
    record.tx_id = static_cast<TxId>(std::stoull(parts[1]));
    record.type = wal_record_type_from_string(parts[2]);
    record.row_id = static_cast<RowId>(std::stoull(parts[3]));
    record.video_id = static_cast<VideoId>(std::stoull(parts[4]));
    record.old_view_count = std::stoull(parts[5]);
    record.new_view_count = std::stoull(parts[6]);
    return record;
}

std::vector<WalRecord> Wal::read_file(const std::string& path) {
    std::ifstream file(path);
    std::vector<WalRecord> records;
    std::string line;
    while (std::getline(file, line)) {
        auto record = parse_line(line);
        if (record.has_value()) {
            records.push_back(*record);
        }
    }
    return records;
}

}  // namespace db
