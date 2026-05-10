#pragma once

#include <atomic>

#include "db/row.hpp"

namespace db {

class TransactionIdGenerator {
public:
    TxId next();

private:
    std::atomic<TxId> next_tx_id_{1};
};

}  // namespace db
