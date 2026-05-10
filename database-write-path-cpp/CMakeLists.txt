cmake_minimum_required(VERSION 3.16)
project(database_write_path_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(db_write_path
    src/btree.cpp
    src/lock_manager.cpp
    src/metrics.cpp
    src/table.cpp
    src/transaction.cpp
    src/wal.cpp
)

target_include_directories(db_write_path PUBLIC include)

add_executable(db_write_path_demo src/main.cpp)
target_link_libraries(db_write_path_demo PRIVATE db_write_path)

add_executable(db_write_path_tests tests/test_write_path.cpp)
target_link_libraries(db_write_path_tests PRIVATE db_write_path)

enable_testing()
add_test(NAME db_write_path_tests COMMAND db_write_path_tests)
