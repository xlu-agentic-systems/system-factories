#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>

#include "db/metrics.hpp"
#include "db/row.hpp"

namespace db {

class RowLock {
public:
    RowLock(std::shared_ptr<std::mutex> mutex, std::unique_lock<std::mutex> lock);
    RowLock(RowLock&&) noexcept = default;
    RowLock& operator=(RowLock&&) noexcept = default;

    RowLock(const RowLock&) = delete;
    RowLock& operator=(const RowLock&) = delete;

private:
    std::shared_ptr<std::mutex> mutex_;
    std::unique_lock<std::mutex> lock_;
};

class LockManager {
public:
    RowLock lock_row(RowId row_id, Metrics* metrics);

private:
    std::mutex mutex_;
    std::unordered_map<RowId, std::shared_ptr<std::mutex>> row_locks_;
};

}  // namespace db
