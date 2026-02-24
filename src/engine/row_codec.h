#pragma once
#include <vector>
#include <cstdint>

#include "sql/ast.h"
#include "engine/catalog.h"

namespace minisql::engine
{

    using namespace std;
    using minisql::sql::Value;

    // Row ko binary bytes mein encode karo
    // INT  column -> 8 bytes (int64_t, little-endian)
    // TEXT column -> [4-byte len][bytes]
    vector<uint8_t> encodeRow(const TableMeta &meta, const vector<Value> &row);

    // Binary bytes se row wapas decode karo
    vector<Value> decodeRow(const TableMeta &meta, const uint8_t *data, size_t len);

} // namespace minisql::engine
