#!/usr/bin/env bash
set -euo pipefail

cmake -S . -B build
cmake --build build

echo "Single hot row:"
./build/db_write_path_demo hot-row 8 2000

echo
echo "Sharded counter:"
./build/db_write_path_demo sharded 16 8 2000
