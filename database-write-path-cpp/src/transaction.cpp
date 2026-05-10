#include "db/transaction.hpp"

namespace db {

TxId TransactionIdGenerator::next() {
    return next_tx_id_.fetch_add(1);
}

}  // namespace db
