#pragma once

#include <cstdint>
#include <string>

namespace db {

using VideoId = std::uint64_t;
using RowId = std::uint64_t;
using TxId = std::uint64_t;
using Lsn = std::uint64_t;

struct VideoRow {
    RowId row_id = 0;
    VideoId video_id = 0;
    std::uint64_t view_count = 0;
    std::string title;
};

struct VideoSeed {
    VideoId video_id = 0;
    std::uint64_t view_count = 0;
    std::string title;
};

}  // namespace db
