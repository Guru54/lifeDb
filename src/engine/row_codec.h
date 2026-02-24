#pragma once
#include <vector>
#include <cstdint>
#include "sql/ast.h"
#include "engine/catalog.h"

namespace minisql::engine {

std::vector<uint8_t> encodeRow(const TableMeta& meta,
                               const std::vector<minisql::sql::Value>& row);

std::vector<minisql::sql::Value> decodeRow(const TableMeta& meta,
                                           const uint8_t* data, size_t len);

} // namespace minisql::engine
