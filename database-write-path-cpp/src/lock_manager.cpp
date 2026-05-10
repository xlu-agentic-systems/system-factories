#include "db/lock_manager.hpp"

#include <chrono>

namespace db {

RowLock::RowLock(std::shared_ptr<std::mutex> mutex, std::unique_lock<std::mutex> lock)
    : mutex_(std::move(mutex)), lock_(std::move(lock)) {}

RowLock LockManager::lock_row(RowId row_id, Metrics* metrics) {
    std::shared_ptr<std::mutex> row_mutex;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        auto& slot = row_locks_[row_id];
        if (!slot) {
            slot = std::make_shared<std::mutex>();
        }
        row_mutex = slot;
    }

    const auto start = std::chrono::steady_clock::now();
    std::unique_lock<std::mutex> lock(*row_mutex);
    const auto end = std::chrono::steady_clock::now();
    if (metrics != nullptr) {
        const auto waited = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
        metrics->add_lock_wait_ns(static_cast<std::uint64_t>(waited.count()));
    }
    return RowLock(row_mutex, std::move(lock));
}

}  // namespace db
